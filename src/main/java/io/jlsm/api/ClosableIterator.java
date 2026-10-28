package io.jlsm.api;

public interface ClosableIterator<T> extends AutoCloseable {
    boolean hasNext() throws StorageException;
    T next() throws StorageException; // Кидає NoSuchElementException, якщо досягнуто кінця
    @Override
    void close() throws StorageException;
}
