package io.jlsm.internal.model;

import java.util.Objects;

/**
 * Immutable range tombstone covering all keys in [fromKey, toKey) older than sequenceNumber.
 */
public record RangeTombstone(ByteKey fromKey, ByteKey toKey, long sequenceNumber) {

    public RangeTombstone {
        Objects.requireNonNull(fromKey, "fromKey cannot be null");
        Objects.requireNonNull(toKey, "toKey cannot be null");
        if (fromKey.compareTo(toKey) >= 0) {
            throw new IllegalArgumentException("fromKey must be strictly smaller than toKey: ["
                    + fromKey + ", " + toKey + ")");
        }
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("sequenceNumber cannot be negative: " + sequenceNumber);
        }
    }

    public boolean covers(ByteKey targetKey, long targetSequence) {
        Objects.requireNonNull(targetKey, "targetKey cannot be null");
        return fromKey.compareTo(targetKey) <= 0
                && targetKey.compareTo(toKey) < 0
                && sequenceNumber > targetSequence;
    }

    public boolean containsKey(ByteKey targetKey) {
        Objects.requireNonNull(targetKey, "targetKey cannot be null");
        return fromKey.compareTo(targetKey) <= 0 && targetKey.compareTo(toKey) < 0;
    }
}
