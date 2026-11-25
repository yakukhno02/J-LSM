package io.jlsm.api;

/**
 * A consistent point-in-time view of the database.
 */
public interface Snapshot extends AutoCloseable {

    byte[] get(byte[] key) throws StorageException;

    ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException;

    long sequenceNumber();

    @Override
    void close();
}
