package io.jlsm.api;

import java.util.Arrays;

public final class Entry {
    private final byte[] key;
    private final byte[] value;
    private final boolean isTombstone;

    public Entry(byte[] key, byte[] value, boolean isTombstone) {
        this.key = key == null ? new byte[0] : key.clone();
        this.value = value == null ? null : value.clone();
        this.isTombstone = isTombstone;
    }

    public byte[] key() {
        return key.clone();
    }

    public byte[] value() {
        return value == null ? null : value.clone();
    }

    public boolean isTombstone() {
        return isTombstone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Entry other)) return false;
        return isTombstone == other.isTombstone
                && Arrays.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return 31 * result + Boolean.hashCode(isTombstone);
    }
}
