package io.jlsm.manifest;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StorageIO;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class DefaultManifestCompactor {

    private final StorageIO storageIO;
    private final Path directory;

    public DefaultManifestCompactor(StorageIO storageIO, Path directory) {
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
        this.directory = Objects.requireNonNull(directory, "directory cannot be null");
    }

    public long compact(ManifestSnapshot currentSnapshot) throws StorageException {
        Objects.requireNonNull(currentSnapshot, "currentSnapshot cannot be null");

        Path manifestPath = directory.resolve("MANIFEST");
        Path tmpPath = directory.resolve("MANIFEST.tmp");

        try {
            storageIO.deleteIfExists(tmpPath);
        } catch (IOException ignored) {}

        ChannelIO tmpChannel;
        try {
            tmpChannel = storageIO.createNew(tmpPath);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to create MANIFEST.tmp for compaction", e);
        }

        long compactedSize;
        try {
            try (DefaultManifestStore tmpStore = DefaultManifestStore.open(tmpChannel, tmpPath, ManifestSnapshot.empty())) {
                tmpStore.commit(currentSnapshot);
            }
            compactedSize = Files.size(tmpPath);
            storageIO.atomicMove(tmpPath, manifestPath);
            storageIO.syncDirectory(directory);
        } catch (Exception e) {
            try {
                storageIO.deleteIfExists(tmpPath);
            } catch (IOException ignored) {}
            if (e instanceof StorageException se) {
                throw se;
            }
            throw new StorageException(ErrorCode.IO, "Failed to atomically publish compacted MANIFEST", e);
        }

        return compactedSize;
    }

    public Path directory() {
        return directory;
    }
}
