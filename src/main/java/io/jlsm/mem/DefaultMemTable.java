package io.jlsm.mem;

import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;

import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent, sorted in-memory representation of entries backed by a {@link ConcurrentSkipListMap}.
 * Keys are ordered lexicographically by their unsigned byte values via {@link ByteKey#compareTo(ByteKey)}.
 * Maintains historical versions for point-in-time snapshot reads.
 */
public final class DefaultMemTable implements MemTable {

    // Estimated per-entry overhead in skip list (node pointers, wrapper objects)
    private static final int ENTRY_OVERHEAD_BYTES = 32;

    private final ConcurrentSkipListMap<ByteKey, InternalRecord> map;
    private final ConcurrentSkipListMap<InternalKey, InternalRecord> historyMap;
    private final AtomicLong byteSize;
    private final Set<Long> attachedWalIds;

    public DefaultMemTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.historyMap = new ConcurrentSkipListMap<>();
        this.byteSize = new AtomicLong(0L);
        this.attachedWalIds = ConcurrentHashMap.newKeySet();
    }

    @Override
    public long insert(ByteKey key, InternalRecord record) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(record, "record cannot be null");

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
        Objects.requireNonNull(key, "key cannot be null");
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public Optional<InternalRecord> get(ByteKey key, long maxSequence) {
        Objects.requireNonNull(key, "key cannot be null");
        InternalRecord latest = map.get(key);
        if (latest != null && latest.sequence() <= maxSequence) {
            return Optional.of(latest);
        }

        // Latest record is newer than maxSequence or absent; search history
        Map.Entry<InternalKey, InternalRecord> entry =
                historyMap.ceilingEntry(new InternalKey(key, maxSequence));
        if (entry != null && entry.getKey().key().equals(key)) {
            return Optional.of(entry.getValue());
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

        if (maxSequence == Long.MAX_VALUE && historyMap.isEmpty()) {
            return new MemTableRecordCursor(new ArrayList<>(keyView.values()).iterator());
        }

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

        return new MemTableRecordCursor(result.iterator());
    }

    @Override
    public long byteSize() {
        return Math.max(0L, byteSize.get());
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void clear() {
        map.clear();
        historyMap.clear();
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
