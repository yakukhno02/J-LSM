package io.jlsm.mem;

import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent, sorted in-memory representation of entries backed by a {@link ConcurrentSkipListMap}.
 * Keys are ordered lexicographically by their unsigned byte values via {@link ByteKey#compareTo(ByteKey)}.
 */
public final class DefaultMemTable implements MemTable {

    // Estimated per-entry overhead in skip list (node pointers, wrapper objects)
    private static final int ENTRY_OVERHEAD_BYTES = 32;

    private final ConcurrentSkipListMap<ByteKey, InternalRecord> map;
    private final AtomicLong byteSize;

    public DefaultMemTable() {
        this.map = new ConcurrentSkipListMap<>();
        this.byteSize = new AtomicLong(0L);
    }

    @Override
    public void insert(ByteKey key, InternalRecord record) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(record, "record cannot be null");

        long recordBytes = calculateRecordBytes(key, record);
        InternalRecord oldRecord = map.put(key, record);

        if (oldRecord != null) {
            long oldBytes = calculateRecordBytes(key, oldRecord);
            byteSize.addAndGet(recordBytes - oldBytes);
        } else {
            byteSize.addAndGet(recordBytes);
        }
    }

    @Override
    public Optional<InternalRecord> get(ByteKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public RecordCursor scan(ByteKey fromKey, ByteKey toKey) {
        ConcurrentNavigableMap<ByteKey, InternalRecord> view;

        if (fromKey == null && toKey == null) {
            view = map;
        } else if (fromKey == null) {
            view = map.headMap(toKey, false);
        } else if (toKey == null) {
            view = map.tailMap(fromKey, true);
        } else {
            if (fromKey.compareTo(toKey) > 0) {
                return new EmptyRecordCursor();
            }
            view = map.subMap(fromKey, true, toKey, false);
        }

        return new MemTableRecordCursor(view.values().iterator());
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
        byteSize.set(0L);
    }

    private static long calculateRecordBytes(ByteKey key, InternalRecord record) {
        byte[] val = record.value();
        int valLength = (val != null) ? val.length : 0;
        return ENTRY_OVERHEAD_BYTES + (long) key.length() + valLength;
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
