package io.jlsm.internal.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Sealed hierarchy representing an in-memory or decoded storage record (PUT, DELETE, or RANGE_DELETE).
 */
public sealed interface InternalRecord permits InternalRecord.Put, InternalRecord.Delete, InternalRecord.RangeDelete {

    ByteKey key();

    byte[] value();

    RecordKind kind();

    long sequence();

    boolean isTombstone();

    static InternalRecord of(ByteKey key, byte[] value, RecordKind kind, long sequence) {
        if (kind == RecordKind.DELETE) {
            return new Delete(key, sequence);
        }
        if (kind == RecordKind.RANGE_DELETE) {
            Objects.requireNonNull(value, "toKey bytes cannot be null for RANGE_DELETE");
            return new RangeDelete(key, ByteKey.copyOf(value), sequence);
        }
        return new Put(key, value == null ? new byte[0] : value, sequence);
    }

    static Put put(ByteKey key, byte[] value, long sequence) {
        return new Put(key, value, sequence);
    }

    static Delete delete(ByteKey key, long sequence) {
        return new Delete(key, sequence);
    }

    static RangeDelete rangeDelete(ByteKey fromKey, ByteKey toKey, long sequence) {
        return new RangeDelete(fromKey, toKey, sequence);
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

    record RangeDelete(ByteKey fromKey, ByteKey toKey, long sequence) implements InternalRecord {
        public RangeDelete {
            Objects.requireNonNull(fromKey, "fromKey cannot be null");
            Objects.requireNonNull(toKey, "toKey cannot be null");
            if (fromKey.compareTo(toKey) >= 0) {
                throw new IllegalArgumentException("fromKey must be strictly smaller than toKey: ["
                        + fromKey + ", " + toKey + ")");
            }
            if (sequence < 0) {
                throw new IllegalArgumentException("sequence cannot be negative: " + sequence);
            }
        }

        @Override
        public ByteKey key() {
            return fromKey;
        }

        @Override
        public byte[] value() {
            return toKey.bytes();
        }

        @Override
        public RecordKind kind() {
            return RecordKind.RANGE_DELETE;
        }

        @Override
        public boolean isTombstone() {
            return true;
        }

        public RangeTombstone toRangeTombstone() {
            return new RangeTombstone(fromKey, toKey, sequence);
        }

        public boolean covers(ByteKey targetKey, long targetSequence) {
            return fromKey.compareTo(targetKey) <= 0
                    && targetKey.compareTo(toKey) < 0
                    && sequence > targetSequence;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RangeDelete other)) return false;
            return sequence == other.sequence
                    && Objects.equals(fromKey, other.fromKey)
                    && Objects.equals(toKey, other.toKey);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(fromKey);
            result = 31 * result + Objects.hashCode(toKey);
            result = 31 * result + Long.hashCode(sequence);
            return result;
        }
    }
}
