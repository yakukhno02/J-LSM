package io.jlsm.sst;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;

import java.util.Optional;

public interface TableHandle extends AutoCloseable {
    long id();
    long minSequence();
    long maxSequence();
    long recordCount();
    long fileSize();

    void pin();
    void unpin();
    boolean isClosed();

    Optional<InternalRecord> get(ByteKey key) throws StorageException;
    RecordCursor scan(ByteKey fromKey, ByteKey toKey) throws StorageException;

    @Override
    void close();
}
