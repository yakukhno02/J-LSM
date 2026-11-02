package io.jlsm.manifest;

import io.jlsm.api.CorruptedLogException;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

public final class DefaultManifestStore implements ManifestStore {

    public static final int HEADER_SIZE = 16;
    public static final int MANIFEST_MAGIC = 0x4A4D414E; // ASCII: "JMAN"
    public static final int FORMAT_VERSION = 1;

    private static final int MIN_PAYLOAD_LEN = 32; // 8B + 8B + 8B + 4B + 4B

    private final ChannelIO channel;
    private final Path path;
    private long writeOffset;
    private volatile ManifestSnapshot currentSnapshot;
    private boolean closed;

    private DefaultManifestStore(ChannelIO channel, Path path, long writeOffset, ManifestSnapshot initialSnapshot) {
        this.channel = channel;
        this.path = path;
        this.writeOffset = writeOffset;
        this.currentSnapshot = initialSnapshot;
    }

    public static DefaultManifestStore open(ChannelIO channel, Path path, ManifestSnapshot defaultSnapshot)
            throws StorageException {
        Objects.requireNonNull(channel, "channel cannot be null");
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(defaultSnapshot, "defaultSnapshot cannot be null");

        try {
            long size = channel.size();
            if (size == 0L) {
                writeHeader(channel, path);
                DefaultManifestStore store = new DefaultManifestStore(channel, path, HEADER_SIZE, defaultSnapshot);
                store.commit(defaultSnapshot);
                return store;
            }

            if (size < HEADER_SIZE) {
                throw new CorruptedLogException(path, 0L, "File size (" + size + ") is smaller than MANIFEST header (16 bytes)");
            }

            validateHeader(channel, path);

            long currentOffset = HEADER_SIZE;
            long lastValidOffset = HEADER_SIZE;
            ManifestSnapshot lastValidSnapshot = null;

            ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);

            while (currentOffset < size) {
                long remaining = size - currentOffset;
                if (remaining < 8) { // 4B length + at least 4B CRC
                    truncateTail(channel, lastValidOffset);
                    break;
                }

                lenBuf.clear();
                readFully(channel, lenBuf, currentOffset);
                lenBuf.flip();
                int payloadLength = lenBuf.getInt();

                if (payloadLength < MIN_PAYLOAD_LEN) {
                    if (currentOffset + 4 >= size || isAtEof(currentOffset, payloadLength, size)) {
                        truncateTail(channel, lastValidOffset);
                        break;
                    }
                    throw new CorruptedLogException(path, currentOffset, "Invalid payload length: " + payloadLength);
                }

                int totalFrameLen = 4 + payloadLength + 4;
                if (remaining < totalFrameLen) {
                    truncateTail(channel, lastValidOffset);
                    break;
                }

                ByteBuffer frameBuf = ByteBuffer.allocate(totalFrameLen).order(ByteOrder.BIG_ENDIAN);
                readFully(channel, frameBuf, currentOffset);

                int storedCrc = frameBuf.getInt(totalFrameLen - 4);

                CRC32 crc32 = new CRC32();
                crc32.update(frameBuf.array(), 0, totalFrameLen - 4);
                int calculatedCrc = (int) crc32.getValue();

                if (storedCrc != calculatedCrc) {
                    if (currentOffset + totalFrameLen == size) {
                        truncateTail(channel, lastValidOffset);
                        break;
                    }
                    throw new CorruptedLogException(path, currentOffset,
                            String.format("Manifest frame CRC mismatch: stored 0x%08X, calculated 0x%08X", storedCrc, calculatedCrc));
                }

                frameBuf.position(4); // skip payloadLength
                frameBuf.limit(4 + payloadLength);
                lastValidSnapshot = parseSnapshotPayload(frameBuf, payloadLength, path, currentOffset);

                currentOffset += totalFrameLen;
                lastValidOffset = currentOffset;
            }

            if (lastValidSnapshot == null) {
                DefaultManifestStore store = new DefaultManifestStore(channel, path, lastValidOffset, defaultSnapshot);
                store.commit(defaultSnapshot);
                return store;
            }

            return new DefaultManifestStore(channel, path, lastValidOffset, lastValidSnapshot);

        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to open MANIFEST from: " + path, e);
        }
    }

    @Override
    public ManifestSnapshot currentSnapshot() {
        return currentSnapshot;
    }

    @Override
    public synchronized void commit(ManifestSnapshot snapshot) throws StorageException {
        ensureOpen();
        Objects.requireNonNull(snapshot, "snapshot cannot be null");

        ByteBuffer frameBuf = encodeSnapshotFrame(snapshot);
        try {
            writeFully(channel, frameBuf, writeOffset);
            channel.force(true);
            writeOffset += frameBuf.capacity();
            currentSnapshot = snapshot;
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to commit manifest snapshot at offset: " + writeOffset, e);
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
            throw new StorageException(ErrorCode.IO, "Failed to close MANIFEST channel", e);
        }
    }

    public synchronized long writeOffset() {
        return writeOffset;
    }

    private void ensureOpen() throws StorageException {
        if (closed) {
            throw new StorageException(ErrorCode.CLOSED, "ManifestStore is closed");
        }
    }

    private static void writeHeader(ChannelIO channel, Path path) throws IOException {
        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        headerBuf.putInt(MANIFEST_MAGIC);
        headerBuf.putInt(FORMAT_VERSION);
        headerBuf.putInt(0); // reserved

        CRC32 crc32 = new CRC32();
        crc32.update(headerBuf.array(), 0, 12);
        headerBuf.putInt((int) crc32.getValue());
        headerBuf.flip();

        writeFully(channel, headerBuf, 0L);
        channel.force(true);
    }

    private static void validateHeader(ChannelIO channel, Path path) throws IOException, StorageException {
        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, headerBuf, 0L);
        headerBuf.flip();

        int magic = headerBuf.getInt();
        if (magic != MANIFEST_MAGIC) {
            throw new CorruptedLogException(path, 0L,
                    String.format("Invalid MANIFEST magic: 0x%08X (expected: 0x%08X)", magic, MANIFEST_MAGIC));
        }

        int version = headerBuf.getInt();
        if (version != FORMAT_VERSION) {
            throw new CorruptedLogException(path, 4L,
                    "Unsupported MANIFEST version: " + version + " (expected: " + FORMAT_VERSION + ")");
        }

        headerBuf.getInt(); // reserved (4B)

        int storedCrc = headerBuf.getInt();
        CRC32 crc32 = new CRC32();
        crc32.update(headerBuf.array(), 0, 12);
        int calculatedCrc = (int) crc32.getValue();

        if (storedCrc != calculatedCrc) {
            throw new CorruptedLogException(path, 12L,
                    String.format("MANIFEST header CRC mismatch: stored 0x%08X, calculated 0x%08X", storedCrc, calculatedCrc));
        }
    }

    private static ByteBuffer encodeSnapshotFrame(ManifestSnapshot snapshot) {
        int walCount = snapshot.wals().size();
        int tableCount = snapshot.tables().size();
        int payloadLength = MIN_PAYLOAD_LEN + (walCount * 9) + (tableCount * 40);
        int totalFrameLen = 4 + payloadLength + 4;

        ByteBuffer buf = ByteBuffer.allocate(totalFrameLen).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(payloadLength);
        buf.putLong(snapshot.epoch());
        buf.putLong(snapshot.nextFileId());
        buf.putLong(snapshot.highSequence());
        buf.putInt(walCount);
        buf.putInt(tableCount);

        for (WalMeta wal : snapshot.wals()) {
            buf.putLong(wal.walId());
            buf.put((byte) (wal.isSealed() ? 0x01 : 0x00));
        }

        for (TableMeta table : snapshot.tables()) {
            buf.putLong(table.tableId());
            buf.putLong(table.minSeq());
            buf.putLong(table.maxSeq());
            buf.putLong(table.recordCount());
            buf.putLong(table.fileSize());
        }

        CRC32 crc32 = new CRC32();
        crc32.update(buf.array(), 0, 4 + payloadLength);
        buf.putInt((int) crc32.getValue());

        buf.flip();
        return buf;
    }

    private static ManifestSnapshot parseSnapshotPayload(ByteBuffer buf, int payloadLength, Path path, long offset)
            throws StorageException {
        long epoch = buf.getLong();
        long nextFileId = buf.getLong();
        long highSequence = buf.getLong();
        int walCount = buf.getInt();
        int tableCount = buf.getInt();

        if (walCount < 0 || tableCount < 0) {
            throw new CorruptedLogException(path, offset, "Negative walCount or tableCount in manifest payload");
        }

        int expectedLength = MIN_PAYLOAD_LEN + (walCount * 9) + (tableCount * 40);
        if (expectedLength != payloadLength) {
            throw new CorruptedLogException(path, offset,
                    String.format("Manifest payload size mismatch: declared %d, calculated %d", payloadLength, expectedLength));
        }

        List<WalMeta> wals = new ArrayList<>(walCount);
        for (int i = 0; i < walCount; i++) {
            long walId = buf.getLong();
            byte sealedByte = buf.get();
            wals.add(new WalMeta(walId, sealedByte != 0));
        }

        List<TableMeta> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++) {
            long tableId = buf.getLong();
            long minSeq = buf.getLong();
            long maxSeq = buf.getLong();
            long recordCount = buf.getLong();
            long fileSize = buf.getLong();
            tables.add(new TableMeta(tableId, minSeq, maxSeq, recordCount, fileSize));
        }

        return new ManifestSnapshot(epoch, nextFileId, highSequence, wals, tables);
    }

    private static boolean isAtEof(long currentOffset, int payloadLength, long totalSize) {
        return (currentOffset + 4 + (long) payloadLength + 4) >= totalSize;
    }

    private static void truncateTail(ChannelIO channel, long validSize) throws IOException {
        channel.truncate(validSize);
        channel.force(true);
    }

    private static void readFully(ChannelIO channel, ByteBuffer dst, long position) throws IOException {
        long currentPos = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, currentPos);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading MANIFEST");
            }
            currentPos += read;
        }
    }

    private static void writeFully(ChannelIO channel, ByteBuffer src, long position) throws IOException {
        long currentPos = position;
        while (src.hasRemaining()) {
            int written = channel.write(src, currentPos);
            if (written < 0) {
                throw new IOException("Unexpected end of channel while writing MANIFEST");
            }
            currentPos += written;
        }
    }
}
