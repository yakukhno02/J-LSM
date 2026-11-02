package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * 40-byte SSTable trailer record (Footer) located at the end of every SSTable file.
 *
 * @param indexOffset      offset where the Sparse Index block begins in the SSTable file
 * @param indexLength      length of the Sparse Index block in bytes (including indexCRC32)
 * @param totalRecordCount total number of entries stored across all data blocks in this SSTable
 * @param blockCount       number of 4KB data blocks in this SSTable
 * @param formatVersion    format version number (must be 1)
 */
public record Footer(
        long indexOffset,
        long indexLength,
        long totalRecordCount,
        int blockCount,
        int formatVersion
) {
    public static final int FOOTER_SIZE = 40;
    public static final int FOOTER_MAGIC = 0x4A4C534D; // ASCII: "JLSM"
    public static final int CURRENT_VERSION = 1;

    public Footer {
        if (indexOffset < 0) {
            throw new IllegalArgumentException("indexOffset cannot be negative: " + indexOffset);
        }
        if (indexLength < 0) {
            throw new IllegalArgumentException("indexLength cannot be negative: " + indexLength);
        }
        if (totalRecordCount < 0) {
            throw new IllegalArgumentException("totalRecordCount cannot be negative: " + totalRecordCount);
        }
        if (blockCount < 0) {
            throw new IllegalArgumentException("blockCount cannot be negative: " + blockCount);
        }
    }

    /**
     * Serializes this Footer into a 40-byte array with big-endian encoding and trailing CRC32.
     *
     * @return 40-byte array
     */
    public byte[] encode() {
        return encode(this);
    }

    /**
     * Serializes a Footer instance into a 40-byte array.
     *
     * @param footer the Footer to serialize
     * @return 40-byte array
     */
    public static byte[] encode(Footer footer) {
        Objects.requireNonNull(footer, "footer cannot be null");

        ByteBuffer buffer = ByteBuffer.allocate(FOOTER_SIZE).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(footer.indexOffset());
        buffer.putLong(footer.indexLength());
        buffer.putLong(footer.totalRecordCount());
        buffer.putInt(footer.blockCount());
        buffer.putInt(footer.formatVersion());
        buffer.putInt(FOOTER_MAGIC);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, 36);
        buffer.putInt((int) crc32.getValue());

        return buffer.array();
    }

    /**
     * Deserializes and validates a 40-byte Footer from a ByteBuffer.
     *
     * @param buffer buffer containing at least 40 bytes
     * @return validated {@link Footer}
     * @throws StorageException if CRC32 fails, magic is invalid, or version is unsupported
     */
    public static Footer decode(ByteBuffer buffer) throws StorageException {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        buffer.order(ByteOrder.BIG_ENDIAN);

        if (buffer.remaining() < FOOTER_SIZE) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Insufficient bytes for SSTable footer: " + buffer.remaining());
        }

        byte[] raw = new byte[FOOTER_SIZE];
        buffer.get(raw);

        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        long indexOffset = bb.getLong();
        long indexLength = bb.getLong();
        long totalRecordCount = bb.getLong();
        int blockCount = bb.getInt();
        int formatVersion = bb.getInt();
        int magic = bb.getInt();
        int expectedCrc = bb.getInt();

        CRC32 crc32 = new CRC32();
        crc32.update(raw, 0, 36);
        int actualCrc = (int) crc32.getValue();

        if (actualCrc != expectedCrc) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("Footer CRC32 mismatch: expected 0x%08X, actual 0x%08X", expectedCrc, actualCrc));
        }

        if (magic != FOOTER_MAGIC) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("Invalid SSTable footer magic: 0x%08X (expected 0x%08X)", magic, FOOTER_MAGIC));
        }

        if (formatVersion != CURRENT_VERSION) {
            throw new StorageException(ErrorCode.UNSUPPORTED_FORMAT,
                    "Unsupported SSTable format version: " + formatVersion);
        }

        return new Footer(indexOffset, indexLength, totalRecordCount, blockCount, formatVersion);
    }
}
