package io.jlsm.sst;

import java.util.Arrays;
import java.util.Objects;

/**
 * Metadata entry for a single 4KB data block in an SSTable sparse index.
 *
 * @param blockOffset offset from the start of the SSTable file where this data block begins
 * @param blockSize   size of the data block in bytes (strictly 4096)
 * @param firstKey    the first key stored in this data block
 */
public record BlockMeta(
        long blockOffset,
        int blockSize,
        byte[] firstKey
) {
    public BlockMeta {
        if (blockOffset < 0) {
            throw new IllegalArgumentException("blockOffset cannot be negative: " + blockOffset);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive: " + blockSize);
        }
        Objects.requireNonNull(firstKey, "firstKey cannot be null");
        firstKey = firstKey.clone();
    }

    @Override
    public byte[] firstKey() {
        return firstKey.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BlockMeta other)) return false;
        return blockOffset == other.blockOffset
                && blockSize == other.blockSize
                && Arrays.equals(firstKey, other.firstKey);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(blockOffset);
        result = 31 * result + blockSize;
        result = 31 * result + Arrays.hashCode(firstKey);
        return result;
    }

    @Override
    public String toString() {
        return "BlockMeta[offset=" + blockOffset + ", size=" + blockSize + ", firstKeyLen=" + firstKey.length + "]";
    }
}
