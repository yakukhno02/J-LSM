package io.jlsm.api;

import java.nio.file.Path;

public final class CorruptedLogException extends StorageException {
    private final Path path;
    private final long offset;

    public CorruptedLogException(Path path, long offset, String detail) {
        super(ErrorCode.CORRUPTION, "Corrupted log at " + path + ":" + offset + " - " + detail);
        this.path = path;
        this.offset = offset;
    }

    public Path path() {
        return path;
    }

    public long offset() {
        return offset;
    }
}
