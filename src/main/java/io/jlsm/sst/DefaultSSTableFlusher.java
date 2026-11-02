package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.manifest.TableMeta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Standard implementation of {@link SSTableFlusher} implementing atomic shadow writing (.sst.tmp -&gt; .sst),
 * 4KB block alignment, sparse index building, and directory synchronization.
 */
public final class DefaultSSTableFlusher implements SSTableFlusher {

    public static final String SST_EXTENSION = ".sst";
    public static final String TMP_EXTENSION = ".sst.tmp";

    private final Path baseDir;
    private final StorageIO storageIO;

    public DefaultSSTableFlusher(Path baseDir, StorageIO storageIO) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir cannot be null");
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
    }

    public Path sstPath(long fileId) {
        return baseDir.resolve(String.format("%06d%s", fileId, SST_EXTENSION));
    }

    public Path tmpPath(long fileId) {
        return baseDir.resolve(String.format("%06d%s", fileId, TMP_EXTENSION));
    }

    @Override
    public TableMeta build(long fileId, RecordCursor sortedEntries) throws StorageException {
        Objects.requireNonNull(sortedEntries, "sortedEntries cannot be null");
        if (fileId < 0) {
            throw new IllegalArgumentException("fileId cannot be negative: " + fileId);
        }

        Path sstFile = sstPath(fileId);
        Path tmpFile = tmpPath(fileId);

        try {
            storageIO.deleteIfExists(tmpFile);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to clean up stale temporary SSTable: " + tmpFile, e);
        }

        ChannelIO channel = null;
        boolean success = false;
        try {
            channel = storageIO.createNew(tmpFile);

            long currentOffset = 0L;
            long minSeq = Long.MAX_VALUE;
            long maxSeq = Long.MIN_VALUE;
            long totalRecordCount = 0L;

            // 1. Write Common Header (32 bytes)
            byte[] headerBytes = SSTableHeader.encode(fileId);
            writeFully(channel, ByteBuffer.wrap(headerBytes), currentOffset);
            currentOffset += headerBytes.length;

            // 2. Iterate through sorted entries and encode 4KB data blocks
            DataBlockEncoder encoder = new DataBlockEncoder();
            List<BlockMeta> blockIndex = new ArrayList<>();
            byte[] lastKey = null;

            while (sortedEntries.advance()) {
                InternalRecord record = sortedEntries.current();
                if (record == null) {
                    throw new IllegalStateException("RecordCursor returned null record");
                }

                ByteKey byteKey = Objects.requireNonNull(record.key(), "Record key cannot be null");
                byte[] keyBytes = byteKey.bytes();
                byte[] valBytes = record.value();

                if (lastKey != null && Arrays.compareUnsigned(lastKey, keyBytes) > 0) {
                    throw new IllegalArgumentException("RecordCursor entries are not sorted in ascending order");
                }
                lastKey = keyBytes;

                long seq = record.sequence();
                if (seq < minSeq) minSeq = seq;
                if (seq > maxSeq) maxSeq = seq;
                totalRecordCount++;

                int keyLen = keyBytes.length;
                int valLen = (valBytes != null) ? valBytes.length : 0;

                if (!encoder.canFit(keyLen, valLen)) {
                    FinishedBlock finishedBlock = encoder.finishBlock();
                    writeFully(channel, ByteBuffer.wrap(finishedBlock.blockBytes()), currentOffset);
                    blockIndex.add(new BlockMeta(currentOffset, DataBlockEncoder.BLOCK_SIZE, finishedBlock.firstKey()));
                    currentOffset += DataBlockEncoder.BLOCK_SIZE;
                    encoder.reset();
                }

                encoder.append(seq, record.kind(), keyBytes, valBytes);
            }

            // Flush remaining data block if any
            if (encoder.hasRecords()) {
                FinishedBlock finishedBlock = encoder.finishBlock();
                writeFully(channel, ByteBuffer.wrap(finishedBlock.blockBytes()), currentOffset);
                blockIndex.add(new BlockMeta(currentOffset, DataBlockEncoder.BLOCK_SIZE, finishedBlock.firstKey()));
                currentOffset += DataBlockEncoder.BLOCK_SIZE;
                encoder.reset();
            }

            if (totalRecordCount == 0L) {
                minSeq = 0L;
                maxSeq = 0L;
            }

            // 3. Write Sparse Index
            long indexOffset = currentOffset;
            byte[] indexBytes = SparseIndex.encode(blockIndex);
            writeFully(channel, ByteBuffer.wrap(indexBytes), currentOffset);
            currentOffset += indexBytes.length;
            long indexLength = indexBytes.length;

            // 4. Write Footer (40 bytes)
            Footer footer = new Footer(indexOffset, indexLength, totalRecordCount, blockIndex.size(), Footer.CURRENT_VERSION);
            byte[] footerBytes = footer.encode();
            writeFully(channel, ByteBuffer.wrap(footerBytes), currentOffset);
            currentOffset += footerBytes.length;

            // 5. Force data to disk and close channel
            channel.force(true);
            channel.close();
            channel = null;

            // 6. Atomic shadow file promotion (.sst.tmp -> .sst) and directory fsync
            storageIO.atomicMove(tmpFile, sstFile);
            storageIO.syncDirectory(baseDir);

            success = true;
            return new TableMeta(fileId, minSeq, maxSeq, totalRecordCount, currentOffset);

        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "I/O error while building SSTable for fileId: " + fileId, e);
        } catch (StorageException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException(ErrorCode.IO, "Unexpected error building SSTable for fileId: " + fileId, e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {}
            }
            if (!success) {
                try {
                    storageIO.deleteIfExists(tmpFile);
                } catch (Exception ignored) {}
            }
        }
    }

    private static void writeFully(ChannelIO channel, ByteBuffer buffer, long position) throws IOException {
        long currentPos = position;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, currentPos);
            if (written < 0) {
                throw new IOException("Unexpected end of channel while writing SSTable");
            }
            currentPos += written;
        }
    }
}
