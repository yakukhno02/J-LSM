package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import java.nio.file.Path;

public final class DatabaseFactory {
    private DatabaseFactory() {}

    public static KVDatabase open(Path directory, DatabaseOptions options) throws StorageException {
        throw new UnsupportedOperationException("DatabaseFactory.open is not implemented yet (Milestone M3)");
    }
}
