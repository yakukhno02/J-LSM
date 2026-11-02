package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * 32-byte common header for SSTable files.
 *
 * @param fileId        unique identifier of this SSTable file
 * @param blockSize     block size in bytes (4096)
 * @param formatVersion version of the file format (1)
 */
public record SSTableHeader(
        long fileId,
        int blockSize,
        int formatVersion
) {
    public static final int HEADER_SIZE = 32;
    public static final int SST_MAGIC = 0x4A535354; // ASCII: "JSST"
    public static final int FORMAT_VERSION = 1;
    public static final int DEFAULT_BLOCK_SIZE = 4096;

    public SSTableHeader {
        if (fileId < 0) {
            throw new IllegalArgumentException("fileId cannot be negative: " + fileId);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive: " + blockSize);
        }
    }

    /**
     * Serializes an SSTable header into a 32-byte array with default block size and format version.
     *
     * @param fileId the file ID
     * @return 32-byte array
     */
    public static byte[] encode(long fileId) {
        return encode(fileId, DEFAULT_BLOCK_SIZE, FORMAT_VERSION);
    }

    /**
     * Serializes an SSTable header into a 32-byte array.
     *
     * @param fileId        file ID
     * @param blockSize     data block size
     * @param formatVersion format version
     * @return 32-byte array
     */
    public static byte[] encode(long fileId, int blockSize, int formatVersion) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(SST_MAGIC);
        buf.putInt(formatVersion);
        buf.putLong(fileId);
        buf.putInt(blockSize);
        buf.putInt(0); // reserved
        buf.putInt(HEADER_SIZE); // header size = 32

        CRC32 crc32 = new CRC32();
        crc32.update(buf.array(), 0, 28);
        buf.putInt((int) crc32.getValue());

        return buf.array();
    }

    /**
     * Deserializes and validates a 32-byte SSTable header from a ByteBuffer.
     *
     * @param buffer buffer containing at least 32 bytes
     * @return validated {@link SSTableHeader}
     * @throws StorageException if CRC32 fails, magic is invalid, or version is unsupported
     */
    public static SSTableHeader decode(ByteBuffer buffer) throws StorageException {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        buffer.order(ByteOrder.BIG_ENDIAN);

        if (buffer.remaining() < HEADER_SIZE) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Insufficient bytes for SSTable header: " + buffer.remaining());
        }

        byte[] raw = new byte[HEADER_SIZE];
        buffer.get(raw);

        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        int magic = bb.getInt();
        int version = bb.getInt();
        long fileId = bb.getLong();
        int blockSize = bb.getInt();
        int reserved = bb.getInt();
        int headerSize = bb.getInt();
        int expectedCrc = bb.getInt();

        CRC32 crc32 = new CRC32();
        crc32.update(raw, 0, 28);
        int actualCrc = (int) crc32.getValue();

        if (actualCrc != expectedCrc) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("SSTable header CRC32 mismatch: expected 0x%08X, actual 0x%08X", expectedCrc, actualCrc));
        }

        if (magic != SST_MAGIC) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("Invalid SSTable header magic: 0x%08X (expected 0x%08X)", magic, SST_MAGIC));
        }

        if (version != FORMAT_VERSION) {
            throw new StorageException(ErrorCode.UNSUPPORTED_FORMAT,
                    "Unsupported SSTable header version: " + version);
        }

        return new SSTableHeader(fileId, blockSize, version);
    }
}
