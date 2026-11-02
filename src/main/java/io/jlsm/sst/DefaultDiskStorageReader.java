package io.jlsm.sst;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.merge.DefaultMergedIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of {@link DiskStorageReader} providing point lookup across
 * active SSTables with sequence-based conflict resolution and range scanning via K-way merge.
 */
public final class DefaultDiskStorageReader implements DiskStorageReader {

    @Override
    public Optional<InternalRecord> search(List<TableHandle> pinnedTables, ByteKey key) throws StorageException {
        Objects.requireNonNull(pinnedTables, "pinnedTables cannot be null");
        Objects.requireNonNull(key, "key cannot be null");

        InternalRecord best = null;

        for (TableHandle handle : pinnedTables) {
            // Prune search if this SSTable's maxSequence cannot beat our current best record
            if (best != null && handle.maxSequence() <= best.sequence()) {
                continue;
            }

            Optional<InternalRecord> candidate;
            if (handle instanceof DefaultTableHandle defaultHandle) {
                candidate = defaultHandle.get(key);
            } else {
                try (RecordCursor cursor = ((DefaultTableHandle) handle).scan(key, null)) {
                    if (cursor.advance() && cursor.current().key().equals(key)) {
                        candidate = Optional.of(cursor.current());
                    } else {
                        candidate = Optional.empty();
                    }
                }
            }

            if (candidate.isPresent()) {
                InternalRecord record = candidate.get();
                if (best == null || record.sequence() > best.sequence()) {
                    best = record;
                }
            }
        }

        return Optional.ofNullable(best);
    }

    @Override
    public RecordCursor scanDisk(List<TableHandle> pinnedTables, ByteKey fromKey, ByteKey toKey) throws StorageException {
        Objects.requireNonNull(pinnedTables, "pinnedTables cannot be null");

        if (fromKey != null && toKey != null && fromKey.compareTo(toKey) > 0) {
            return new EmptyRecordCursor();
        }

        List<RecordCursor> cursors = new ArrayList<>(pinnedTables.size());
        for (TableHandle handle : pinnedTables) {
            if (handle instanceof DefaultTableHandle defaultHandle) {
                cursors.add(defaultHandle.scan(fromKey, toKey));
            }
        }

        // In disk scan, tombstones are filtered out (retainTombstones = false)
        return new DefaultMergedIterator(cursors, false);
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
