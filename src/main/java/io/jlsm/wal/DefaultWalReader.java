package io.jlsm.wal;

import io.jlsm.api.CorruptedLogException;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.CRC32;

public final class DefaultWalReader implements WalReader {

    public static final int HEADER_SIZE = DefaultWalWriter.HEADER_SIZE;
    public static final int WAL_MAGIC = DefaultWalWriter.WAL_MAGIC;
    public static final int FORMAT_VERSION = DefaultWalWriter.FORMAT_VERSION;
    private static final int FRAME_HEADER_LEN = 21; // CRC(4) + Seq(8) + Type(1) + KeyLen(4) + ValLen(4)

    private final ChannelIO channel;
    private final Path path;
    private final long fileId;
    private boolean closed;

    private DefaultWalReader(ChannelIO channel, Path path, long fileId) {
        this.channel = channel;
        this.path = path;
        this.fileId = fileId;
    }

    public static DefaultWalReader open(ChannelIO channel, Path path) throws StorageException {
        Objects.requireNonNull(channel, "channel cannot be null");
        Objects.requireNonNull(path, "path cannot be null");

        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        try {
            long size = channel.size();
            if (size < HEADER_SIZE) {
                throw new CorruptedLogException(path, 0L, "File size (" + size + ") is smaller than WAL header (32 bytes)");
            }
            readFully(channel, headerBuf, 0L);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to read WAL header from: " + path, e);
        }

        headerBuf.flip();
        int magic = headerBuf.getInt();
        if (magic != WAL_MAGIC) {
            throw new CorruptedLogException(path, 0L, String.format("Invalid WAL magic: 0x%08X (expected: 0x%08X)", magic, WAL_MAGIC));
        }

        int version = headerBuf.getInt();
        if (version != FORMAT_VERSION) {
            throw new CorruptedLogException(path, 4L, "Unsupported WAL version: " + version + " (expected: " + FORMAT_VERSION + ")");
        }

        long fileId = headerBuf.getLong();
        int blockSize = headerBuf.getInt();
        if (blockSize != 0) {
            throw new CorruptedLogException(path, 16L, "Invalid WAL block size: " + blockSize + " (expected 0)");
        }

        headerBuf.getInt(); // reserved (4B)
        int headerSize = headerBuf.getInt();
        if (headerSize != HEADER_SIZE) {
            throw new CorruptedLogException(path, 24L, "Invalid header size: " + headerSize + " (expected 32)");
        }

        int storedCrc = headerBuf.getInt();
        CRC32 crc32 = new CRC32();
        crc32.update(headerBuf.array(), 0, 28);
        int calculatedCrc = (int) crc32.getValue();
        if (storedCrc != calculatedCrc) {
            throw new CorruptedLogException(path, 28L, String.format("Header CRC mismatch: stored 0x%08X, calculated 0x%08X", storedCrc, calculatedCrc));
        }

        return new DefaultWalReader(channel, path, fileId);
    }

    public long fileId() {
        return fileId;
    }

    public Path path() {
        return path;
    }

    @Override
    public RecordCursor openCursor() throws StorageException {
        ensureOpen();
        return new WalRecordCursor(channel, path);
    }

    public synchronized long recoverAndTruncateTail() throws StorageException {
        ensureOpen();
        long currentOffset = HEADER_SIZE;
        long lastValidOffset = HEADER_SIZE;

        try {
            long fileSize = channel.size();

            while (currentOffset < fileSize) {
                ParsedFrame frame;
                try {
                    frame = readAndValidateFrame(channel, path, currentOffset, fileSize);
                } catch (CorruptedLogException e) {
                    if (currentOffset + FRAME_HEADER_LEN >= fileSize) {
                        truncateTail(lastValidOffset);
                        return lastValidOffset;
                    }
                    throw e;
                }

                if (frame == null) {
                    truncateTail(lastValidOffset);
                    return lastValidOffset;
                }

                currentOffset += frame.totalFrameLen();
                lastValidOffset = currentOffset;
            }

            return lastValidOffset;
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "I/O error during WAL tail recovery on " + path, e);
        }
    }

    private void truncateTail(long validSize) throws IOException {
        channel.truncate(validSize);
        channel.force(true);
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
            throw new StorageException(ErrorCode.IO, "Failed to close WAL reader channel", e);
        }
    }

    private void ensureOpen() throws StorageException {
        if (closed) {
            throw new StorageException(ErrorCode.CLOSED, "WalReader is closed");
        }
    }

    private static void readFully(ChannelIO channel, ByteBuffer dst, long position) throws IOException {
        long currentPos = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, currentPos);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading WAL");
            }
            currentPos += read;
        }
    }

    private static ParsedFrame readAndValidateFrame(ChannelIO channel, Path path, long offset, long fileSize)
            throws IOException, StorageException {
        long remaining = fileSize - offset;
        if (remaining < FRAME_HEADER_LEN) {
            return null;
        }

        ByteBuffer headerBuf = ByteBuffer.allocate(FRAME_HEADER_LEN).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, headerBuf, offset);
        headerBuf.flip();

        int storedCrc = headerBuf.getInt();
        long seq = headerBuf.getLong();
        byte typeByte = headerBuf.get();
        int keyLen = headerBuf.getInt();
        int valLen = headerBuf.getInt();

        RecordKind kind;
        if (typeByte == 0x01) {
            kind = RecordKind.PUT;
        } else if (typeByte == 0x02) {
            kind = RecordKind.DELETE;
            valLen = 0;
        } else {
            throw new CorruptedLogException(path, offset + 12L, "Unknown record type: " + typeByte);
        }

        if (keyLen < 0 || valLen < 0 || ((long) keyLen + valLen > DefaultWalWriter.MAX_KEY_VALUE_TOTAL_BYTES)) {
            throw new CorruptedLogException(path, offset + 13L, "Invalid keyLen (" + keyLen + ") or valLen (" + valLen + ")");
        }

        int payloadLen = 8 + 1 + 4 + 4 + keyLen + valLen;
        int totalFrameLen = 4 + payloadLen;

        if (remaining < totalFrameLen) {
            return null;
        }

        ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, payloadBuf, offset + 4);

        CRC32 crc32 = new CRC32();
        crc32.update(payloadBuf.array(), 0, payloadLen);
        int calculatedCrc = (int) crc32.getValue();

        if (storedCrc != calculatedCrc) {
            throw new CorruptedLogException(path, offset,
                    String.format("Frame CRC mismatch: stored 0x%08X, calculated 0x%08X", storedCrc, calculatedCrc));
        }

        payloadBuf.position(8 + 1 + 4 + 4);
        byte[] key = new byte[keyLen];
        payloadBuf.get(key);

        byte[] val = null;
        if (kind == RecordKind.PUT && valLen > 0) {
            val = new byte[valLen];
            payloadBuf.get(val);
        }

        InternalRecord record = new InternalRecord(ByteKey.copyOf(key), val, kind, seq);
        return new ParsedFrame(record, totalFrameLen);
    }

    private record ParsedFrame(InternalRecord record, int totalFrameLen) {}

    private static final class WalRecordCursor implements RecordCursor {
        private final ChannelIO channel;
        private final Path path;
        private long currentOffset;
        private InternalRecord currentRecord;
        private boolean closed;

        private WalRecordCursor(ChannelIO channel, Path path) {
            this.channel = channel;
            this.path = path;
            this.currentOffset = HEADER_SIZE;
        }

        @Override
        public boolean advance() throws StorageException {
            if (closed) {
                currentRecord = null;
                return false;
            }

            try {
                long fileSize = channel.size();
                if (currentOffset >= fileSize) {
                    currentRecord = null;
                    return false;
                }

                ParsedFrame frame = readAndValidateFrame(channel, path, currentOffset, fileSize);
                if (frame == null) {
                    throw new CorruptedLogException(path, currentOffset, "Incomplete frame at offset " + currentOffset);
                }

                currentRecord = frame.record();
                currentOffset += frame.totalFrameLen();
                return true;

            } catch (IOException e) {
                throw new StorageException(ErrorCode.IO, "I/O error while reading WAL record", e);
            }
        }

        @Override
        public InternalRecord current() {
            if (closed || currentRecord == null) {
                throw new IllegalStateException("Cursor is not positioned on a valid record or already closed");
            }
            return currentRecord;
        }

        @Override
        public void close() {
            closed = true;
            currentRecord = null;
        }
    }
}
