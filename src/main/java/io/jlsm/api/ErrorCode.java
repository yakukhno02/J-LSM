package io.jlsm.api;

public enum ErrorCode {
    IO,
    CORRUPTION,
    UNSUPPORTED_FORMAT,
    UNSUPPORTED_STORAGE,
    LOCKED,
    CLOSED,
    FAILED,
    INTERRUPTED,
    LIMIT_EXCEEDED,
    RESOURCE_LIMIT
}
