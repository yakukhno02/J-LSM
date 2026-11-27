package io.jlsm.api;

public interface WriteBatch extends AutoCloseable {

    static WriteBatch create() {
        return new io.jlsm.engine.DefaultWriteBatch();
    }

    WriteBatch put(byte[] key, byte[] value);

    WriteBatch delete(byte[] key);

    WriteBatch deleteRange(byte[] fromKey, byte[] toKey);

    int operationCount();

    long byteSize();

    boolean isEmpty();

    void clear();

    @Override
    void close();
}
