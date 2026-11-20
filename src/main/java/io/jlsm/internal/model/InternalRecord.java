package io.jlsm.internal.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Sealed hierarchy representing an in-memory or decoded storage record (PUT or DELETE).
 */
public sealed interface InternalRecord permits InternalRecord.Put, InternalRecord.Delete {

    ByteKey key();

    byte[] value();

    RecordKind kind();

    long sequence();

    boolean isTombstone();

    static InternalRecord of(ByteKey key, byte[] value, RecordKind kind, long sequence) {
        if (kind == RecordKind.DELETE) {
            return new Delete(key, sequence);
        }
        return new Put(key, value == null ? new byte[0] : value, sequence);
    }

    static Put put(ByteKey key, byte[] value, long sequence) {
        return new Put(key, value, sequence);
    }

    static Delete delete(ByteKey key, long sequence) {
        return new Delete(key, sequence);
    }

    record Put(ByteKey key, byte[] value, long sequence) implements InternalRecord {
        public Put {
            Objects.requireNonNull(key, "key cannot be null");
            Objects.requireNonNull(value, "value cannot be null for Put");
            value = value.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }

        @Override
        public RecordKind kind() {
            return RecordKind.PUT;
        }

        @Override
        public boolean isTombstone() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Put other)) return false;
            return sequence == other.sequence
                    && Objects.equals(key, other.key)
                    && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(key);
            result = 31 * result + Arrays.hashCode(value);
            result = 31 * result + Long.hashCode(sequence);
            return result;
        }
    }

    record Delete(ByteKey key, long sequence) implements InternalRecord {
        public Delete {
            Objects.requireNonNull(key, "key cannot be null");
        }

        @Override
        public byte[] value() {
            return null;
        }

        @Override
        public RecordKind kind() {
            return RecordKind.DELETE;
        }

        @Override
        public boolean isTombstone() {
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Delete other)) return false;
            return sequence == other.sequence && Objects.equals(key, other.key);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(key);
            result = 31 * result + Long.hashCode(sequence);
            return result;
        }
    }
}
