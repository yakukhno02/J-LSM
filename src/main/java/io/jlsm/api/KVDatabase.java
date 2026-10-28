package io.jlsm.api;

import java.nio.file.Path;

public interface KVDatabase extends AutoCloseable {
    static KVDatabase open(Path directory, DatabaseOptions options) throws StorageException {
        // Делегує до внутрішнього фабричного методу Dev 1
        return io.jlsm.engine.DatabaseFactory.open(directory, options);
    }

    void put(byte[] key, byte[] value) throws StorageException;
    byte[] get(byte[] key) throws StorageException;
    void delete(byte[] key) throws StorageException;

    // Діапазон [fromKey, toKey). null означає unbounded
    ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException;

    // Примусовий fsync усіх незафіксованих буферів на диск
    void sync() throws StorageException;

    @Override
    void close() throws StorageException;
}
