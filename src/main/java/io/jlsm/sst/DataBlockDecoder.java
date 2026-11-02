package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordKind;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Decodes 4096-byte SSTable data blocks, validates IEEE 802.3 CRC32 checksums,
 * and extracts {@link InternalRecord} instances.
 */
public final class DataBlockDecoder {

    private DataBlockDecoder() {}

    /**
     * Reads a 4096-byte data block from a channel and decodes its records.
     *
     * @param channel     channel to read from
     * @param blockOffset offset where the data block begins
     * @return list of decoded records
     * @throws StorageException if CRC32 fails or data block is corrupted
     * @throws IOException      if I/O error occurs
     */
    public static List<InternalRecord> readAndDecode(ChannelIO channel, long blockOffset)
            throws StorageException, IOException {
        Objects.requireNonNull(channel, "channel cannot be null");
        ByteBuffer buffer = ByteBuffer.allocate(DataBlockEncoder.BLOCK_SIZE).order(ByteOrder.BIG_ENDIAN);
        long currentPos = blockOffset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, currentPos);
            if (read < 0) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Unexpected end of channel reading data block at offset " + blockOffset);
            }
            currentPos += read;
        }
        buffer.flip();
        return decode(buffer);
    }

    /**
     * Decodes records from a 4096-byte array.
     *
     * @param blockBytes 4096-byte array
     * @return list of records
     * @throws StorageException if CRC32 fails or data is corrupted
     */
    public static List<InternalRecord> decode(byte[] blockBytes) throws StorageException {
        Objects.requireNonNull(blockBytes, "blockBytes cannot be null");
        if (blockBytes.length != DataBlockEncoder.BLOCK_SIZE) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Invalid data block length: " + blockBytes.length + " (expected " + DataBlockEncoder.BLOCK_SIZE + ")");
        }
        return decode(ByteBuffer.wrap(blockBytes).order(ByteOrder.BIG_ENDIAN));
    }

    /**
     * Decodes records from a ByteBuffer positioned at the start of a 4096-byte data block.
     *
     * @param buffer byte buffer containing at least 4096 bytes
     * @return list of decoded records
     * @throws StorageException if CRC32 fails or data is corrupted
     */
    public static List<InternalRecord> decode(ByteBuffer buffer) throws StorageException {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        buffer.order(ByteOrder.BIG_ENDIAN);

        if (buffer.remaining() < DataBlockEncoder.BLOCK_SIZE) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Insufficient bytes for data block: " + buffer.remaining());
        }

        int startPos = buffer.position();
        byte[] blockBytes = new byte[DataBlockEncoder.BLOCK_SIZE];
        buffer.get(blockBytes);

        ByteBuffer bb = ByteBuffer.wrap(blockBytes).order(ByteOrder.BIG_ENDIAN);

        // 1. Verify CRC32
        CRC32 crc32 = new CRC32();
        crc32.update(blockBytes, 0, DataBlockEncoder.BLOCK_DATA_PAYLOAD_LIMIT);
        int actualCrc = (int) crc32.getValue();
        int expectedCrc = bb.getInt(DataBlockEncoder.BLOCK_DATA_PAYLOAD_LIMIT);

        if (actualCrc != expectedCrc) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("Data block CRC32 mismatch: expected 0x%08X, actual 0x%08X", expectedCrc, actualCrc));
        }

        // 2. Read headers
        int recordCount = bb.getInt(0);
        int usedBytes = bb.getInt(4);

        if (recordCount < 0) {
            throw new StorageException(ErrorCode.CORRUPTION, "Negative record count in data block: " + recordCount);
        }
        if (usedBytes < DataBlockEncoder.BLOCK_HEADER_SIZE || usedBytes > DataBlockEncoder.BLOCK_DATA_PAYLOAD_LIMIT) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Invalid usedBytes in data block: " + usedBytes + " (must be between 8 and 4092)");
        }

        // 3. Parse records
        List<InternalRecord> records = new ArrayList<>(recordCount);
        int pos = DataBlockEncoder.BLOCK_HEADER_SIZE;

        for (int i = 0; i < recordCount; i++) {
            if (pos + 17 > usedBytes) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Truncated record header in data block at record index " + i + " at offset " + pos);
            }

            long sequence = bb.getLong(pos);
            int keyLen = bb.getInt(pos + 8);
            int valLen = bb.getInt(pos + 12);
            byte flags = bb.get(pos + 16);
            pos += 17;

            if (keyLen < 0 || valLen < 0) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Negative keyLen or valLen at record index " + i);
            }

            if ((long) pos + keyLen + (flags == DataBlockEncoder.FLAG_PUT ? valLen : 0) > usedBytes) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Record payload exceeds usedBytes boundary at record index " + i);
            }

            byte[] keyBytes = new byte[keyLen];
            bb.position(pos);
            bb.get(keyBytes);
            pos += keyLen;

            byte[] valBytes = null;
            RecordKind kind;

            if (flags == DataBlockEncoder.FLAG_PUT) {
                kind = RecordKind.PUT;
                valBytes = new byte[valLen];
                bb.get(valBytes);
                pos += valLen;
            } else if (flags == DataBlockEncoder.FLAG_DELETE) {
                kind = RecordKind.DELETE;
            } else {
                throw new StorageException(ErrorCode.CORRUPTION,
                        String.format("Unknown record flags 0x%02X at record index %d", flags, i));
            }

            records.add(new InternalRecord(ByteKey.copyOf(keyBytes), valBytes, kind, sequence));
        }

        return records;
    }

    /**
     * Performs a point search in a single data block for a specific key.
     *
     * @param buffer    data block buffer
     * @param targetKey key to search for
     * @return Optional containing the matching record, or empty if not present
     * @throws StorageException if data block is corrupted
     */
    public static Optional<InternalRecord> searchInBlock(ByteBuffer buffer, ByteKey targetKey)
            throws StorageException {
        Objects.requireNonNull(targetKey, "targetKey cannot be null");
        List<InternalRecord> records = decode(buffer);
        for (InternalRecord record : records) {
            if (targetKey.equals(record.key())) {
                return Optional.of(record);
            }
        }
        return Optional.empty();
    }
}
