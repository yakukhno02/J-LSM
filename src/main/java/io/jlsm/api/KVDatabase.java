package io.jlsm.api;

import java.nio.file.Path;

public interface KVDatabase extends AutoCloseable {
    static KVDatabase open(Path directory, DatabaseOptions options) throws StorageException {
        return io.jlsm.engine.DatabaseFactory.open(directory, options);
    }

    void put(byte[] key, byte[] value) throws StorageException;
    byte[] get(byte[] key) throws StorageException;
    void delete(byte[] key) throws StorageException;

    /**
     * Atomically applies all operations in the given batch.
     */
    void write(WriteBatch batch) throws StorageException;

    ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException;

    Snapshot snapshot() throws StorageException;

    void sync() throws StorageException;

    default StorageMetrics metrics() throws StorageException {
        return null;
    }

    @Override
    void close() throws StorageException;
}
