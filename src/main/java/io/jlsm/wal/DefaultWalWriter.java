package io.jlsm.wal;

import io.jlsm.api.DurabilityMode;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.model.RecordKind;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32;

public final class DefaultWalWriter implements WalWriter {

    public static final int WAL_MAGIC = 0x4A57414C; // ASCII: "JWAL"
    public static final int FORMAT_VERSION = 1;
    public static final int HEADER_SIZE = 32;
    public static final int MAX_KEY_VALUE_TOTAL_BYTES = 4063;

    private static final byte TYPE_PUT = 0x01;
    private static final byte TYPE_DELETE = 0x02;

    private final ChannelIO channel;
    private final DurabilityMode durabilityMode;
    private long writeOffset;
    private boolean closed;

    private DefaultWalWriter(ChannelIO channel, DurabilityMode durabilityMode, long initialOffset) {
        this.channel = channel;
        this.durabilityMode = durabilityMode;
        this.writeOffset = initialOffset;
    }

    public static DefaultWalWriter create(ChannelIO channel, long fileId, DurabilityMode durabilityMode) throws StorageException {
        Objects.requireNonNull(channel, "channel cannot be null");
        Objects.requireNonNull(durabilityMode, "durabilityMode cannot be null");

        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        headerBuf.putInt(WAL_MAGIC);
        headerBuf.putInt(FORMAT_VERSION);
        headerBuf.putLong(fileId);
        headerBuf.putInt(0); // Block size = 0 for WAL
        headerBuf.putInt(0); // Reserved = 0
        headerBuf.putInt(HEADER_SIZE); // Header size = 32

        CRC32 crc32 = new CRC32();
        crc32.update(headerBuf.array(), 0, 28);
        headerBuf.putInt((int) crc32.getValue());
        headerBuf.flip();

        try {
            writeFully(channel, headerBuf, 0L);
            if (durabilityMode == DurabilityMode.SYNC) {
                channel.force(false);
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to write WAL header for fileId: " + fileId, e);
        }

        return new DefaultWalWriter(channel, durabilityMode, HEADER_SIZE);
    }

    public static DefaultWalWriter openExisting(ChannelIO channel, DurabilityMode durabilityMode) throws StorageException {
        Objects.requireNonNull(channel, "channel cannot be null");
        Objects.requireNonNull(durabilityMode, "durabilityMode cannot be null");
        try {
            long size = channel.size();
            return new DefaultWalWriter(channel, durabilityMode, size);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to inspect WAL channel size", e);
        }
    }

    @Override
    public synchronized long append(long sequence, RecordKind kind, byte[] key, byte[] value) throws StorageException {
        ensureOpen();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");

        int keyLen = key.length;
        int valLen = 0;
        byte type;

        if (kind == RecordKind.PUT) {
            Objects.requireNonNull(value, "value cannot be null for PUT");
            valLen = value.length;
            type = TYPE_PUT;
        } else {
            type = TYPE_DELETE;
            // For DELETE, value length must be 0 and bytes omitted
            value = null;
        }

        if ((long) keyLen + valLen > MAX_KEY_VALUE_TOTAL_BYTES) {
            throw new IllegalArgumentException("Combined key and value length (" + (keyLen + valLen)
                    + ") exceeds maximum allowed limit of " + MAX_KEY_VALUE_TOTAL_BYTES + " bytes");
        }

        // Frame: CRC32(4B) + Sequence(8B) + Type(1B) + KeyLen(4B) + ValLen(4B) + Key(KeyLen) + Val(ValLen)
        int payloadLen = 8 + 1 + 4 + 4 + keyLen + valLen;
        int totalFrameLen = 4 + payloadLen;

        ByteBuffer frameBuf = ByteBuffer.allocate(totalFrameLen).order(ByteOrder.BIG_ENDIAN);
        frameBuf.position(4); // reserve 4 bytes for CRC32

        frameBuf.putLong(sequence);
        frameBuf.put(type);
        frameBuf.putInt(keyLen);
        frameBuf.putInt(valLen);
        frameBuf.put(key);
        if (value != null && valLen > 0) {
            frameBuf.put(value);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(frameBuf.array(), 4, payloadLen);
        int computedCrc = (int) crc32.getValue();

        frameBuf.putInt(0, computedCrc);
        frameBuf.position(0);

        long offsetWritten = writeOffset;
        try {
            writeFully(channel, frameBuf, offsetWritten);
            writeOffset += totalFrameLen;

            if (durabilityMode == DurabilityMode.SYNC) {
                channel.force(false);
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to append record at sequence: " + sequence, e);
        }

        return offsetWritten;
    }

    @Override
    public synchronized void sync() throws StorageException {
        ensureOpen();
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to sync WAL channel", e);
        }
    }

    @Override
    public synchronized void close() throws StorageException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            channel.close();
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to close WAL channel", e);
        }
    }

    public synchronized long writeOffset() {
        return writeOffset;
    }

    private void ensureOpen() throws StorageException {
        if (closed) {
            throw new StorageException(ErrorCode.CLOSED, "WalWriter is closed");
        }
    }

    private static void writeFully(ChannelIO channel, ByteBuffer buffer, long position) throws IOException {
        long currentPos = position;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, currentPos);
            if (written < 0) {
                throw new IOException("Unexpected end of channel while writing WAL data");
            }
            currentPos += written;
        }
    }
}
