package io.jlsm.wal;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.BatchOperation;
import io.jlsm.internal.model.RecordKind;

import java.util.List;

public interface WalWriter extends AutoCloseable {
    long append(long sequence, RecordKind kind, byte[] key, byte[] value) throws StorageException;

    default long appendRangeDelete(long sequence, byte[] fromKey, byte[] toKey) throws StorageException {
        return append(sequence, RecordKind.RANGE_DELETE, fromKey, toKey);
    }

    long appendBatch(long startSequence, List<BatchOperation> operations) throws StorageException;
    void sync() throws StorageException;
    @Override
    void close() throws StorageException;
}
