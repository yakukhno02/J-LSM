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
     *
     * @param batch the write batch to apply (must be non-null and non-empty)
     * @throws StorageException if the storage engine encounters an I/O or state error
     * @throws IllegalArgumentException if batch is null or empty
     */
    void write(WriteBatch batch) throws StorageException;

    ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException;

    void sync() throws StorageException;

    /**
     * Returns an immutable snapshot of real-time database telemetry and storage metrics.
     */
    default StorageMetrics metrics() throws StorageException {
        return null;
    }

    @Override
    void close() throws StorageException;
}
