package io.jlsm.api;

public class StorageException extends Exception {
    private final ErrorCode code;

    public StorageException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public StorageException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
