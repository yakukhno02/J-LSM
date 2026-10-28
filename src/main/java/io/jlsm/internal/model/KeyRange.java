package io.jlsm.internal.model;

import java.util.Objects;

public record KeyRange(ByteKey fromKey, ByteKey toKey) {
    public KeyRange {
        if (fromKey != null && toKey != null && fromKey.compareTo(toKey) > 0) {
            throw new IllegalArgumentException("fromKey must be <= toKey");
        }
    }

    public boolean contains(ByteKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        if (fromKey != null && key.compareTo(fromKey) < 0) {
            return false;
        }
        if (toKey != null && key.compareTo(toKey) >= 0) {
            return false;
        }
        return true;
    }
}
