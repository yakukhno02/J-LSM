package io.jlsm.wal;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.BatchOperation;
import io.jlsm.internal.model.RecordKind;

import java.util.List;

public interface WalWriter extends AutoCloseable {
    long append(long sequence, RecordKind kind, byte[] key, byte[] value) throws StorageException;
    long appendBatch(long startSequence, List<BatchOperation> operations) throws StorageException;
    void sync() throws StorageException;
    @Override
    void close() throws StorageException;
}
