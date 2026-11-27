package io.jlsm.mem;

import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent, sorted in-memory representation of entries backed by a {@link ConcurrentSkipListMap}.
 * Keys are ordered lexicographically by their unsigned byte values via {@link ByteKey#compareTo(ByteKey)}.
 * Maintains historical versions for point-in-time snapshot reads and range tombstones.
 */
public final class DefaultMemTable implements MemTable {

    // Estimated per-entry overhead in skip list (node pointers, wrapper objects)
    private static final int ENTRY_OVERHEAD_BYTES = 32;

    private final ConcurrentSkipListMap<ByteKey, InternalRecord> map;
    private final ConcurrentSkipListMap<InternalKey, InternalRecord> historyMap;
    private final List<InternalRecord.RangeDelete> rangeTombstones;
    private final AtomicLong byteSize;
    private final Set<Long> attachedWalIds;

    public DefaultMemTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.historyMap = new ConcurrentSkipListMap<>();
        this.rangeTombstones = new CopyOnWriteArrayList<>();
        this.byteSize = new AtomicLong(0L);
        this.attachedWalIds = ConcurrentHashMap.newKeySet();
    }

    @Override
    public long insert(ByteKey key, InternalRecord record) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(record, "record cannot be null");

        if (record instanceof InternalRecord.RangeDelete rd) {
            rangeTombstones.add(rd);
            long deltaBytes = calculateRecordBytes(key, record);
            byteSize.addAndGet(deltaBytes);
            return deltaBytes;
        }

        long recordBytes = calculateRecordBytes(key, record);
        InternalRecord oldRecord = map.put(key, record);

        long deltaBytes;
        if (oldRecord != null) {
            historyMap.put(new InternalKey(key, oldRecord.sequence()), oldRecord);
            long oldBytes = calculateRecordBytes(key, oldRecord);
            deltaBytes = recordBytes - oldBytes;
        } else {
            deltaBytes = recordBytes;
        }
        byteSize.addAndGet(deltaBytes);
        return deltaBytes;
    }

    @Override
    public Optional<InternalRecord> get(ByteKey key) {
        return get(key, Long.MAX_VALUE);
    }

    @Override
    public Optional<InternalRecord> get(ByteKey key, long maxSequence) {
        Objects.requireNonNull(key, "key cannot be null");

        // 1. Find highest sequence matching range tombstone
        InternalRecord.RangeDelete matchingRangeDelete = null;
        for (InternalRecord.RangeDelete rd : rangeTombstones) {
            if (rd.sequence() <= maxSequence && rd.fromKey().compareTo(key) <= 0 && key.compareTo(rd.toKey()) < 0) {
                if (matchingRangeDelete == null || rd.sequence() > matchingRangeDelete.sequence()) {
                    matchingRangeDelete = rd;
                }
            }
        }

        // 2. Find point record at or before maxSequence
        InternalRecord pointRecord = null;
        InternalRecord latest = map.get(key);
        if (latest != null && latest.sequence() <= maxSequence) {
            pointRecord = latest;
        } else if (!historyMap.isEmpty()) {
            Map.Entry<InternalKey, InternalRecord> entry =
                    historyMap.ceilingEntry(new InternalKey(key, maxSequence));
            if (entry != null && entry.getKey().key().equals(key)) {
                pointRecord = entry.getValue();
            }
        }

        // 3. Resolve conflict
        if (pointRecord != null && matchingRangeDelete != null) {
            if (matchingRangeDelete.sequence() > pointRecord.sequence()) {
                return Optional.of(new InternalRecord.Delete(key, matchingRangeDelete.sequence()));
            } else {
                return Optional.of(pointRecord);
            }
        } else if (pointRecord != null) {
            return Optional.of(pointRecord);
        } else if (matchingRangeDelete != null) {
            return Optional.of(new InternalRecord.Delete(key, matchingRangeDelete.sequence()));
        }

        return Optional.empty();
    }

    @Override
    public RecordCursor scan(ByteKey fromKey, ByteKey toKey) {
        return scan(fromKey, toKey, Long.MAX_VALUE);
    }

    @Override
    public RecordCursor scan(ByteKey fromKey, ByteKey toKey, long maxSequence) {
        if (fromKey != null && toKey != null && fromKey.compareTo(toKey) > 0) {
            return new EmptyRecordCursor();
        }

        ConcurrentNavigableMap<ByteKey, InternalRecord> keyView = rangeView(map, fromKey, toKey);
        NavigableSet<ByteKey> allKeys = new TreeSet<>(keyView.keySet());

        if (!historyMap.isEmpty()) {
            InternalKey historyFrom = fromKey != null ? new InternalKey(fromKey, Long.MAX_VALUE) : null;
            InternalKey historyTo = toKey != null ? new InternalKey(toKey, Long.MAX_VALUE) : null;
            for (InternalKey ik : rangeView(historyMap, historyFrom, historyTo).keySet()) {
                allKeys.add(ik.key());
            }
        }

        List<InternalRecord> result = new ArrayList<>();
        for (ByteKey k : allKeys) {
            get(k, maxSequence).ifPresent(result::add);
        }

        for (InternalRecord.RangeDelete rd : rangeTombstones) {
            if (rd.sequence() <= maxSequence) {
                if (fromKey != null && rd.toKey().compareTo(fromKey) <= 0) {
                    continue;
                }
                if (toKey != null && rd.fromKey().compareTo(toKey) >= 0) {
                    continue;
                }
                result.add(rd);
            }
        }

        result.sort((r1, r2) -> {
            int cmp = r1.key().compareTo(r2.key());
            if (cmp != 0) return cmp;
            return Long.compare(r2.sequence(), r1.sequence());
        });

        return new MemTableRecordCursor(result.iterator());
    }

    @Override
    public List<InternalRecord.RangeDelete> rangeTombstones() {
        return Collections.unmodifiableList(rangeTombstones);
    }

    @Override
    public long byteSize() {
        return Math.max(0L, byteSize.get());
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty() && rangeTombstones.isEmpty();
    }

    @Override
    public void clear() {
        map.clear();
        historyMap.clear();
        rangeTombstones.clear();
        byteSize.set(0L);
        attachedWalIds.clear();
    }

    @Override
    public void attachWalId(long walId) {
        attachedWalIds.add(walId);
    }

    @Override
    public Set<Long> attachedWalIds() {
        return Set.copyOf(attachedWalIds);
    }

    private static <K, V> ConcurrentNavigableMap<K, V> rangeView(
            ConcurrentNavigableMap<K, V> navMap, K from, K to) {
        if (from == null && to == null) {
            return navMap;
        } else if (from == null) {
            return navMap.headMap(to, false);
        } else if (to == null) {
            return navMap.tailMap(from, true);
        } else {
            return navMap.subMap(from, true, to, false);
        }
    }

    private static long calculateRecordBytes(ByteKey key, InternalRecord record) {
        byte[] val = record.value();
        int valLen = (val != null) ? val.length : 0;
        return ENTRY_OVERHEAD_BYTES + (long) key.length() + valLen;
    }

    private record InternalKey(ByteKey key, long sequence) implements Comparable<InternalKey> {
        @Override
        public int compareTo(InternalKey other) {
            int cmp = this.key.compareTo(other.key);
            if (cmp != 0) {
                return cmp;
            }
            // Descending by sequence number
            return Long.compare(other.sequence, this.sequence);
        }
    }

    private static final class MemTableRecordCursor implements RecordCursor {
        private final Iterator<InternalRecord> iterator;
        private InternalRecord currentRecord;
        private boolean closed;

        private MemTableRecordCursor(Iterator<InternalRecord> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean advance() {
            if (closed) {
                currentRecord = null;
                return false;
            }
            if (iterator.hasNext()) {
                currentRecord = iterator.next();
                return true;
            }
            currentRecord = null;
            return false;
        }

        @Override
        public InternalRecord current() {
            if (closed || currentRecord == null) {
                throw new IllegalStateException("Cursor is either not positioned on a valid record or already closed");
            }
            return currentRecord;
        }

        @Override
        public void close() {
            closed = true;
            currentRecord = null;
        }
    }

    private static final class EmptyRecordCursor implements RecordCursor {
        @Override
        public boolean advance() {
            return false;
        }

        @Override
        public InternalRecord current() {
            throw new IllegalStateException("Cursor is empty");
        }

        @Override
        public void close() {
        }
    }
}
