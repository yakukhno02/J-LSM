package io.jlsm.sst;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import java.util.List;
import java.util.Optional;

public interface DiskStorageReader {
    // Точковий пошук по списку відкритих таблиць. Перемагає запис з максимальним sequence!
    Optional<InternalRecord> search(List<TableHandle> pinnedTables, ByteKey key) throws StorageException;

    // Діапазонний скан по диску з K-way злиттям і дедуплікацією версій
    RecordCursor scanDisk(List<TableHandle> pinnedTables, ByteKey fromKey, ByteKey toKey) throws StorageException;
}
