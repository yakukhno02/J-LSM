package io.jlsm.sst;

import java.util.Arrays;
import java.util.Objects;

/**
 * Encapsulates a finalized 4KB SSTable data block with its first key and record count.
 *
 * @param blockBytes  the complete 4096-byte array containing block headers, records, zero padding, and CRC32
 * @param firstKey    the key of the first record written into this block
 * @param recordCount number of records stored in this block
 */
public record FinishedBlock(
        byte[] blockBytes,
        byte[] firstKey,
        int recordCount
) {
    public FinishedBlock {
        Objects.requireNonNull(blockBytes, "blockBytes cannot be null");
        Objects.requireNonNull(firstKey, "firstKey cannot be null");
        if (blockBytes.length != DataBlockEncoder.BLOCK_SIZE) {
            throw new IllegalArgumentException("blockBytes must be strictly " + DataBlockEncoder.BLOCK_SIZE + " bytes");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive: " + recordCount);
        }
        blockBytes = blockBytes.clone();
        firstKey = firstKey.clone();
    }

    @Override
    public byte[] blockBytes() {
        return blockBytes.clone();
    }

    @Override
    public byte[] firstKey() {
        return firstKey.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FinishedBlock other)) return false;
        return recordCount == other.recordCount
                && Arrays.equals(blockBytes, other.blockBytes)
                && Arrays.equals(firstKey, other.firstKey);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(blockBytes);
        result = 31 * result + Arrays.hashCode(firstKey);
        result = 31 * result + recordCount;
        return result;
    }
}
