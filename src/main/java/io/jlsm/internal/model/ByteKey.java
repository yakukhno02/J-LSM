package io.jlsm.internal.model;

import java.util.Arrays;

public final class ByteKey implements Comparable<ByteKey> {
    private final byte[] bytes;
    private final int hashCode;

    private ByteKey(byte[] bytes) {
        this.bytes = bytes.clone();
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    public static ByteKey copyOf(byte[] bytes) {
        return new ByteKey(bytes == null ? new byte[0] : bytes);
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    public int length() {
        return bytes.length;
    }

    @Override
    public int compareTo(ByteKey other) {
        return Arrays.compareUnsigned(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteKey other)) return false;
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return Arrays.toString(bytes);
    }
}
