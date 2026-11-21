package io.jlsm.engine;

import io.jlsm.api.WriteBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class DefaultWriteBatch implements WriteBatch {

    private final List<BatchOperation> operations = new ArrayList<>();
    private long totalBytes = 0L;
    private boolean closed = false;

    public DefaultWriteBatch() {}

    @Override
    public WriteBatch put(byte[] key, byte[] value) {
        ensureOpen();
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        BatchOperation op = BatchOperation.put(key, value);
        operations.add(op);
        totalBytes += op.byteSize();
        return this;
    }

    @Override
    public WriteBatch delete(byte[] key) {
        ensureOpen();
        Objects.requireNonNull(key, "key cannot be null");
        BatchOperation op = BatchOperation.delete(key);
        operations.add(op);
        totalBytes += op.byteSize();
        return this;
    }

    @Override
    public int operationCount() {
        ensureOpen();
        return operations.size();
    }

    @Override
    public long byteSize() {
        ensureOpen();
        return totalBytes;
    }

    @Override
    public boolean isEmpty() {
        ensureOpen();
        return operations.isEmpty();
    }

    @Override
    public void clear() {
        ensureOpen();
        operations.clear();
        totalBytes = 0L;
    }

    public List<BatchOperation> operations() {
        ensureOpen();
        return Collections.unmodifiableList(operations);
    }

    @Override
    public void close() {
        if (!closed) {
            operations.clear();
            totalBytes = 0L;
            closed = true;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("WriteBatch is closed");
        }
    }
}
