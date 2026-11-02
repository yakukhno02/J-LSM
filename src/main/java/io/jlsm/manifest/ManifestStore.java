package io.jlsm.manifest;

import io.jlsm.api.StorageException;

public interface ManifestStore extends AutoCloseable {
    ManifestSnapshot currentSnapshot();
    void commit(ManifestSnapshot snapshot) throws StorageException;
    @Override
    void close() throws StorageException;
}
