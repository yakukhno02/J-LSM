package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Representation and codec for SSTable Sparse Index block.
 */
public record SparseIndex(List<BlockMeta> blocks) {

    public SparseIndex {
        Objects.requireNonNull(blocks, "blocks cannot be null");
        blocks = List.copyOf(blocks);
    }

    /**
     * Serializes this sparse index into a byte array formatted according to specification.
     * Includes blockCount, block entries, and trailing indexCRC32.
     *
     * @return byte array containing the full encoded sparse index block
     */
    public byte[] encode() {
        return encode(blocks);
    }

    /**
     * Encodes a list of {@link BlockMeta} entries into a byte array.
     *
     * @param blocks list of block metadata
     * @return byte array containing encoded index
     */
    public static byte[] encode(List<BlockMeta> blocks) {
        Objects.requireNonNull(blocks, "blocks cannot be null");

        int payloadSize = 4; // blockCount (int)
        for (BlockMeta meta : blocks) {
            payloadSize += 8; // blockOffset (long)
            payloadSize += 4; // blockSize (int)
            payloadSize += 4; // firstKeyLen (int)
            payloadSize += meta.firstKey().length; // firstKeyBytes
        }

        ByteBuffer buffer = ByteBuffer.allocate(payloadSize + 4).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(blocks.size());

        for (BlockMeta meta : blocks) {
            byte[] keyBytes = meta.firstKey();
            buffer.putLong(meta.blockOffset());
            buffer.putInt(meta.blockSize());
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, payloadSize);
        buffer.putInt((int) crc32.getValue());

        return buffer.array();
    }

    /**
     * Deserializes a sparse index from a byte buffer.
     * Validates CRC32 checksum.
     *
     * @param buffer buffer positioned at the start of the sparse index
     * @return decoded {@link SparseIndex}
     * @throws StorageException if data is corrupted or truncated
     */
    public static SparseIndex decode(ByteBuffer buffer) throws StorageException {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        buffer.order(ByteOrder.BIG_ENDIAN);

        int startPos = buffer.position();
        if (buffer.remaining() < 8) { // at least blockCount (4) + CRC32 (4)
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Insufficient bytes for sparse index: " + buffer.remaining());
        }

        int blockCount = buffer.getInt();
        if (blockCount < 0) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Negative block count in sparse index: " + blockCount);
        }

        List<BlockMeta> blocks = new ArrayList<>(blockCount);
        for (int i = 0; i < blockCount; i++) {
            if (buffer.remaining() < 16) { // blockOffset (8) + blockSize (4) + firstKeyLen (4)
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Truncated block metadata entry in sparse index at block " + i);
            }
            long blockOffset = buffer.getLong();
            int blockSize = buffer.getInt();
            int firstKeyLen = buffer.getInt();

            if (firstKeyLen < 0 || buffer.remaining() < firstKeyLen + 4) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Invalid key length or truncated first key in sparse index at block " + i);
            }

            byte[] firstKey = new byte[firstKeyLen];
            buffer.get(firstKey);
            blocks.add(new BlockMeta(blockOffset, blockSize, firstKey));
        }

        if (buffer.remaining() < 4) {
            throw new StorageException(ErrorCode.CORRUPTION, "Missing indexCRC32 in sparse index");
        }

        int payloadEndPos = buffer.position();
        int expectedCrc = buffer.getInt();
        int payloadLen = payloadEndPos - startPos;

        byte[] payloadBytes = new byte[payloadLen];
        int currentPos = buffer.position();
        buffer.position(startPos);
        buffer.get(payloadBytes);
        buffer.position(currentPos);

        CRC32 crc32 = new CRC32();
        crc32.update(payloadBytes);
        int actualCrc = (int) crc32.getValue();

        if (actualCrc != expectedCrc) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("Sparse index CRC32 mismatch: expected 0x%08X, actual 0x%08X",
                            expectedCrc, actualCrc));
        }

        return new SparseIndex(blocks);
    }
}
