package io.jlsm.internal.model;

import io.jlsm.api.StorageException;

public interface RecordCursor extends AutoCloseable {
    boolean advance() throws StorageException;
    InternalRecord current(); // Кидає IllegalStateException, якщо advance() не викликано або повернув false
    @Override
    void close() throws StorageException;
}
