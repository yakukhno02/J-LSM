package io.jlsm.internal.model;

import java.util.Arrays;
import java.util.Objects;

public record BatchOperation(RecordKind kind, byte[] key, byte[] value) {

    public BatchOperation {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        if (key.length == 0) {
            throw new IllegalArgumentException("key cannot be empty");
        }
        if (key.length > 65536) {
            throw new IllegalArgumentException("key length exceeds maximum of 64KB: " + key.length);
        }
        if (kind == RecordKind.PUT) {
            Objects.requireNonNull(value, "value cannot be null for PUT operation");
            if (value.length > 2 * 1024 * 1024) {
                throw new IllegalArgumentException("value length exceeds maximum of 2MB: " + value.length);
            }
        } else if (kind == RecordKind.DELETE) {
            if (value != null && value.length > 0) {
                throw new IllegalArgumentException("value must be null or empty for DELETE operation");
            }
            value = null;
        } else {
            throw new IllegalArgumentException("Unsupported batch operation kind: " + kind);
        }
    }

    public static BatchOperation put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return new BatchOperation(RecordKind.PUT, key.clone(), value.clone());
    }

    public static BatchOperation delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new BatchOperation(RecordKind.DELETE, key.clone(), null);
    }

    public long byteSize() {
        return (long) key.length + (value == null ? 0 : value.length) + 32;
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    public byte[] rawKey() {
        return key;
    }

    public byte[] rawValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BatchOperation that)) return false;
        return kind == that.kind && Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(kind);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "BatchOperation[kind=" + kind + ", keyLen=" + key.length + ", valLen=" + (value == null ? 0 : value.length) + "]";
    }
}
