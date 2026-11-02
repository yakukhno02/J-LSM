package io.jlsm.sst;

import io.jlsm.internal.model.RecordKind;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Encodes records into fixed-size 4096-byte SSTable data blocks.
 * <p>
 * Block binary layout:
 * <pre>
 * [recordCount (4B)] [usedBytes (4B)] [Records...] [Zero Padding] [blockCRC32 (4B)]
 * </pre>
 * Each record:
 * <pre>
 * [Sequence (8B)] [KeyLen (4B)] [ValLen (4B)] [Flags (1B)] [Key (KeyLen B)] [Val (ValLen B)]
 * </pre>
 */
public final class DataBlockEncoder {

    public static final int BLOCK_SIZE = 4096;
    public static final int BLOCK_HEADER_SIZE = 8; // recordCount (4B) + usedBytes (4B)
    public static final int BLOCK_DATA_PAYLOAD_LIMIT = 4092; // 4096 - 4B CRC32
    public static final int MAX_KEY_VALUE_TOTAL_BYTES = 4063;

    public static final byte FLAG_PUT = 0x00;
    public static final byte FLAG_DELETE = 0x01;

    private final byte[] buffer = new byte[BLOCK_SIZE];
    private final ByteBuffer byteBuffer;
    private int recordCount;
    private int usedBytes;
    private byte[] firstKey;

    public DataBlockEncoder() {
        this.byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
        reset();
    }

    /**
     * Checks if a record with the specified key and value lengths can fit into the current block.
     *
     * @param keyLen length of the key in bytes
     * @param valLen length of the value in bytes (0 for DELETE)
     * @return true if the record fits in the current block, false otherwise
     */
    public boolean canFit(int keyLen, int valLen) {
        if (keyLen < 0 || valLen < 0) {
            return false;
        }
        int recordSize = 17 + keyLen + valLen;
        return (usedBytes + recordSize) <= BLOCK_DATA_PAYLOAD_LIMIT;
    }

    /**
     * Appends a record to the current block.
     *
     * @param sequence sequence number
     * @param kind     record kind (PUT or DELETE)
     * @param key      key bytes (non-null)
     * @param value    value bytes (may be null for DELETE)
     * @throws IllegalArgumentException if arguments are invalid or key+val length exceeds 4063 bytes
     * @throws IllegalStateException    if record does not fit in the current block
     */
    public void append(long sequence, RecordKind kind, byte[] key, byte[] value) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");

        int keyLen = key.length;
        int valLen = (kind == RecordKind.PUT && value != null) ? value.length : 0;

        if ((long) keyLen + valLen > MAX_KEY_VALUE_TOTAL_BYTES) {
            throw new IllegalArgumentException("Combined key and value length (" + (keyLen + valLen)
                    + ") exceeds maximum allowed limit of " + MAX_KEY_VALUE_TOTAL_BYTES + " bytes");
        }

        if (!canFit(keyLen, valLen)) {
            throw new IllegalStateException("Record does not fit into the current data block");
        }

        if (recordCount == 0) {
            this.firstKey = key.clone();
        }

        byteBuffer.position(usedBytes);
        byteBuffer.putLong(sequence);
        byteBuffer.putInt(keyLen);
        byteBuffer.putInt(valLen);
        byteBuffer.put(kind == RecordKind.PUT ? FLAG_PUT : FLAG_DELETE);
        byteBuffer.put(key);

        if (kind == RecordKind.PUT && valLen > 0) {
            byteBuffer.put(value);
        }

        usedBytes = byteBuffer.position();
        recordCount++;
    }

    /**
     * Finalizes the current block: writes block header, pads with zero bytes up to offset 4092,
     * calculates CRC32 over the first 4092 bytes, and appends the checksum.
     *
     * @return immutable {@link FinishedBlock} containing the complete 4096-byte block
     * @throws IllegalStateException if block has no records
     */
    public FinishedBlock finishBlock() {
        if (recordCount == 0) {
            throw new IllegalStateException("Cannot finish an empty data block");
        }

        byteBuffer.putInt(0, recordCount);
        byteBuffer.putInt(4, usedBytes);

        // Zero-fill padding from usedBytes to 4092
        Arrays.fill(buffer, usedBytes, BLOCK_DATA_PAYLOAD_LIMIT, (byte) 0);

        // Calculate CRC32 over first 4092 bytes
        CRC32 crc32 = new CRC32();
        crc32.update(buffer, 0, BLOCK_DATA_PAYLOAD_LIMIT);
        int crcValue = (int) crc32.getValue();

        byteBuffer.putInt(BLOCK_DATA_PAYLOAD_LIMIT, crcValue);

        return new FinishedBlock(buffer, firstKey, recordCount);
    }

    /**
     * Resets the encoder for a fresh data block.
     */
    public void reset() {
        this.recordCount = 0;
        this.usedBytes = BLOCK_HEADER_SIZE;
        this.firstKey = null;
        Arrays.fill(buffer, (byte) 0);
    }

    public boolean hasRecords() {
        return recordCount > 0;
    }

    public int recordCount() {
        return recordCount;
    }

    public int usedBytes() {
        return usedBytes;
    }

    public byte[] firstKey() {
        return firstKey == null ? null : firstKey.clone();
    }
}
