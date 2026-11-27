package io.jlsm.sst;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RangeTombstone;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.merge.DefaultMergedIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of {@link DiskStorageReader} providing point lookup across
 * active SSTables with sequence-based conflict resolution, range tombstones, and range scanning via K-way merge.
 */
public final class DefaultDiskStorageReader implements DiskStorageReader {

    @Override
    public Optional<InternalRecord> search(List<TableHandle> pinnedTables, ByteKey key) throws StorageException {
        return search(pinnedTables, key, Long.MAX_VALUE);
    }

    @Override
    public Optional<InternalRecord> search(List<TableHandle> pinnedTables, ByteKey key, long maxSequence) throws StorageException {
        Objects.requireNonNull(pinnedTables, "pinnedTables cannot be null");
        Objects.requireNonNull(key, "key cannot be null");

        InternalRecord best = null;

        // Check range tombstones in pinned tables
        for (TableHandle handle : pinnedTables) {
            if (handle.minSequence() > maxSequence) {
                continue;
            }
            for (RangeTombstone rt : handle.rangeTombstones()) {
                if (rt.covers(key, Long.MIN_VALUE) && rt.sequenceNumber() <= maxSequence) {
                    if (best == null || rt.sequenceNumber() > best.sequence()) {
                        best = new InternalRecord.RangeDelete(rt.fromKey(), rt.toKey(), rt.sequenceNumber());
                    }
                }
            }
        }

        for (TableHandle handle : pinnedTables) {
            // Prune search if SSTable was written strictly after maxSequence
            if (handle.minSequence() > maxSequence) {
                continue;
            }
            // Prune search if this SSTable's maxSequence cannot beat our current best record
            if (best != null && handle.maxSequence() <= best.sequence()) {
                continue;
            }
            // Check per-SSTable Bloom Filter before performing SparseIndex binary search or 4KB disk block read
            if (!handle.mightContain(key)) {
                continue;
            }

            Optional<InternalRecord> candidate = handle.get(key);

            if (candidate.isPresent()) {
                InternalRecord record = candidate.get();
                if (record.sequence() <= maxSequence) {
                    if (best == null || record.sequence() > best.sequence()) {
                        best = record;
                    }
                }
            }
        }

        return Optional.ofNullable(best);
    }

    @Override
    public RecordCursor scanDisk(List<TableHandle> pinnedTables, ByteKey fromKey, ByteKey toKey) throws StorageException {
        return scanDisk(pinnedTables, fromKey, toKey, Long.MAX_VALUE);
    }

    @Override
    public RecordCursor scanDisk(List<TableHandle> pinnedTables, ByteKey fromKey, ByteKey toKey, long maxSequence) throws StorageException {
        Objects.requireNonNull(pinnedTables, "pinnedTables cannot be null");

        if (fromKey != null && toKey != null && fromKey.compareTo(toKey) > 0) {
            return new EmptyRecordCursor();
        }

        List<RecordCursor> cursors = new ArrayList<>(pinnedTables.size());
        List<RangeTombstone> rangeTombstones = new ArrayList<>();
        for (TableHandle handle : pinnedTables) {
            if (handle.minSequence() > maxSequence) {
                continue;
            }
            for (RangeTombstone rt : handle.rangeTombstones()) {
                if (rt.sequenceNumber() <= maxSequence) {
                    rangeTombstones.add(rt);
                }
            }
            RecordCursor rawCursor = handle.scan(fromKey, toKey);
            cursors.add(new SequenceFilterRecordCursor(rawCursor, maxSequence));
        }

        // In disk scan, tombstones are filtered out (retainTombstones = false)
        return new DefaultMergedIterator(cursors, false, rangeTombstones);
    }

    private static final class SequenceFilterRecordCursor implements RecordCursor {
        private final RecordCursor delegate;
        private final long maxSequence;
        private InternalRecord currentRecord;

        private SequenceFilterRecordCursor(RecordCursor delegate, long maxSequence) {
            this.delegate = delegate;
            this.maxSequence = maxSequence;
        }

        @Override
        public boolean advance() throws StorageException {
            while (delegate.advance()) {
                InternalRecord rec = delegate.current();
                if (rec.sequence() <= maxSequence) {
                    this.currentRecord = rec;
                    return true;
                }
            }
            this.currentRecord = null;
            return false;
        }

        @Override
        public InternalRecord current() {
            if (currentRecord == null) {
                throw new IllegalStateException("Cursor current() called before advance() or after EOF");
            }
            return currentRecord;
        }

        @Override
        public void close() throws StorageException {
            delegate.close();
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
            throw new IllegalStateException("Empty cursor has no entries");
        }

        @Override
        public void close() {}
    }
}
