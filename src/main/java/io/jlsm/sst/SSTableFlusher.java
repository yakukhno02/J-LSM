package io.jlsm.sst;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.manifest.TableMeta;

public interface SSTableFlusher {
    // Створює durable .sst файл на диску.
    // Важливо: Flusher НЕ змінює Manifest і НЕ видаляє WAL! Це робить Dev 1 після завершення.
    TableMeta build(long fileId, RecordCursor sortedEntries) throws StorageException;
}
