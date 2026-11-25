package io.jlsm.sst;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import java.util.List;
import java.util.Optional;

public interface DiskStorageReader {
    // Point lookup across pinned SSTables. Entry with highest sequence wins.
    Optional<InternalRecord> search(List<TableHandle> pinnedTables, ByteKey key) throws StorageException;

    default Optional<InternalRecord> search(List<TableHandle> pinnedTables, ByteKey key, long maxSequence) throws StorageException {
        return search(pinnedTables, key);
    }

    // Range scan across disk with K-way merge and version deduplication.
    RecordCursor scanDisk(List<TableHandle> pinnedTables, ByteKey fromKey, ByteKey toKey) throws StorageException;

    default RecordCursor scanDisk(List<TableHandle> pinnedTables, ByteKey fromKey, ByteKey toKey, long maxSequence) throws StorageException {
        return scanDisk(pinnedTables, fromKey, toKey);
    }
}
