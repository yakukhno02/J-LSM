package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;

import java.nio.file.Path;
import java.util.Objects;

public final class DatabaseFactory {
    private DatabaseFactory() {}

    public static KVDatabase open(Path directory, DatabaseOptions options) throws StorageException {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        StorageIO storageIO = new StandardStorageIO();
        RecoveryResult recoveryResult = new RecoveryManager(storageIO, directory, options).recover();
        return new DefaultStorageEngine(storageIO, directory, options, recoveryResult);
    }
}
