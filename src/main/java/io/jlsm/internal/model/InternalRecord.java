package io.jlsm.internal.model;

import java.util.Arrays;
import java.util.Objects;

public record InternalRecord(
        ByteKey key,
        byte[] value, // null для DELETE
        RecordKind kind,
        long sequence
) {
    public InternalRecord {
        value = value == null ? null : value.clone();
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    public boolean isTombstone() {
        return kind == RecordKind.DELETE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InternalRecord other)) return false;
        return sequence == other.sequence
                && kind == other.kind
                && Objects.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Objects.hashCode(kind);
        result = 31 * result + Long.hashCode(sequence);
        return result;
    }
}
