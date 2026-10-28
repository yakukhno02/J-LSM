package io.jlsm.wal;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.RecordCursor;

public interface WalReader extends AutoCloseable {
    RecordCursor openCursor() throws StorageException;
    @Override
    void close() throws StorageException;
}
