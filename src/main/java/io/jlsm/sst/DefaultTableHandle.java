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
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standard implementation of {@link TableHandle} representing an open SSTable on disk.
 * Holds cached header, sparse index, and footer in memory, while streaming 4KB data blocks on demand.
 */
public final class DefaultTableHandle implements TableHandle {

    private final ChannelIO channel;
    private final SSTableHeader header;
    private final Footer footer;
    private final SparseIndex sparseIndex;
    private final long minSequence;
    private final long maxSequence;
    private final long fileSize;
    private final AtomicInteger refCount = new AtomicInteger(1);
    private volatile boolean closed;

    private DefaultTableHandle(
            ChannelIO channel,
            SSTableHeader header,
            Footer footer,
            SparseIndex sparseIndex,
            long minSequence,
            long maxSequence,
            long fileSize
    ) {
        this.channel = channel;
        this.header = header;
        this.footer = footer;
        this.sparseIndex = sparseIndex;
        this.minSequence = minSequence;
        this.maxSequence = maxSequence;
        this.fileSize = fileSize;
    }

    /**
     * Opens an SSTable from disk using the provided StorageIO and Path, without external manifest metadata.
     */
    public static DefaultTableHandle open(StorageIO storageIO, Path file) throws StorageException {
        return open(storageIO, file, null);
    }

    /**
     * Opens an SSTable from disk using the provided StorageIO, Path, and manifest {@link TableMeta}.
     */
    public static DefaultTableHandle open(StorageIO storageIO, Path file, TableMeta meta) throws StorageException {
        Objects.requireNonNull(storageIO, "storageIO cannot be null");
        Objects.requireNonNull(file, "file cannot be null");

        ChannelIO channel;
        try {
            channel = storageIO.openRead(file);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to open SSTable file: " + file, e);
        }

        try {
            return open(channel, meta);
        } catch (Exception e) {
            try {
                channel.close();
            } catch (IOException ignored) {}
            if (e instanceof StorageException se) {
                throw se;
            }
            throw new StorageException(ErrorCode.IO, "Failed to read SSTable structures for file: " + file, e);
        }
    }

    /**
     * Opens an SSTable from an open ChannelIO and manifest TableMeta.
     */
    public static DefaultTableHandle open(ChannelIO channel, TableMeta meta) throws StorageException {
        Objects.requireNonNull(channel, "channel cannot be null");

        long size;
        try {
            size = channel.size();
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to inspect channel size", e);
        }

        if (size < SSTableHeader.HEADER_SIZE + Footer.FOOTER_SIZE) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "SSTable file size (" + size + " bytes) is smaller than required header + footer");
        }

        // 1. Read and decode 32-byte Common Header
        ByteBuffer headerBuf = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, headerBuf, 0L);
        headerBuf.flip();
        SSTableHeader header = SSTableHeader.decode(headerBuf);

        // 2. Read and decode 40-byte Footer
        ByteBuffer footerBuf = ByteBuffer.allocate(Footer.FOOTER_SIZE).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, footerBuf, size - Footer.FOOTER_SIZE);
        footerBuf.flip();
        Footer footer = Footer.decode(footerBuf);

        // 3. Validate index offsets from footer
        if (footer.indexOffset() < SSTableHeader.HEADER_SIZE
                || footer.indexOffset() + footer.indexLength() > size - Footer.FOOTER_SIZE) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Invalid index bounds in footer: offset " + footer.indexOffset()
                            + ", length " + footer.indexLength() + ", file size " + size);
        }

        // 4. Read and decode Sparse Index
        ByteBuffer indexBuf = ByteBuffer.allocate((int) footer.indexLength()).order(ByteOrder.BIG_ENDIAN);
        readFully(channel, indexBuf, footer.indexOffset());
        indexBuf.flip();
        SparseIndex sparseIndex = SparseIndex.decode(indexBuf);

        if (sparseIndex.blocks().size() != footer.blockCount()) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Mismatch in block count between footer (" + footer.blockCount()
                            + ") and sparse index (" + sparseIndex.blocks().size() + ")");
        }

        long minSeq = meta != null ? meta.minSeq() : 0L;
        long maxSeq = meta != null ? meta.maxSeq() : Long.MAX_VALUE;

        return new DefaultTableHandle(channel, header, footer, sparseIndex, minSeq, maxSeq, size);
    }

    @Override
    public long id() {
        return header.fileId();
    }

    @Override
    public long minSequence() {
        return minSequence;
    }

    @Override
    public long maxSequence() {
        return maxSequence;
    }

    @Override
    public long recordCount() {
        return footer.totalRecordCount();
    }

    @Override
    public long fileSize() {
        return fileSize;
    }

    public SSTableHeader header() {
        return header;
    }

    public Footer footer() {
        return footer;
    }

    public SparseIndex sparseIndex() {
        return sparseIndex;
    }

    public ChannelIO channel() {
        return channel;
    }

    public void pin() {
        refCount.incrementAndGet();
    }

    public void unpin() {
        if (refCount.decrementAndGet() <= 0) {
            closeChannel();
        }
    }

    @Override
    public void close() {
        unpin();
    }

    public boolean isClosed() {
        return closed || refCount.get() <= 0;
    }

    /**
     * Performs an efficient point search in this SSTable for the given key using binary search on SparseIndex.
     */
    public Optional<InternalRecord> get(ByteKey key) throws StorageException {
        Objects.requireNonNull(key, "key cannot be null");
        ensureOpen();

        List<BlockMeta> blocks = sparseIndex.blocks();
        if (blocks.isEmpty()) {
            return Optional.empty();
        }

        // Check if key is strictly smaller than the first key of the entire SSTable
        if (key.compareTo(ByteKey.copyOf(blocks.get(0).firstKey())) < 0) {
            return Optional.empty();
        }

        // Binary search to find the block with the largest firstKey <= key
        int candidateIndex = findCandidateBlockIndex(key, blocks);
        if (candidateIndex < 0) {
            return Optional.empty();
        }

        BlockMeta candidateBlock = blocks.get(candidateIndex);
        List<InternalRecord> records;
        try {
            records = DataBlockDecoder.readAndDecode(channel, candidateBlock.blockOffset());
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO,
                    "Failed to read data block at offset " + candidateBlock.blockOffset() + " for tableId: " + id(), e);
        }

        for (InternalRecord record : records) {
            if (key.equals(record.key())) {
                return Optional.of(record);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns a {@link RecordCursor} streaming entries within {@code [fromKey, toKey)} from this SSTable.
     */
    public RecordCursor scan(ByteKey fromKey, ByteKey toKey) throws StorageException {
        ensureOpen();
        return new SingleTableCursor(fromKey, toKey);
    }

    private int findCandidateBlockIndex(ByteKey key, List<BlockMeta> blocks) {
        int low = 0;
        int high = blocks.size() - 1;
        int candidate = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = ByteKey.copyOf(blocks.get(mid).firstKey()).compareTo(key);
            if (cmp <= 0) {
                candidate = mid;
                low = mid + 1; // Try to find a later block whose firstKey is still <= key
            } else {
                high = mid - 1;
            }
        }
        return candidate;
    }

    private void ensureOpen() throws StorageException {
        if (isClosed()) {
            throw new StorageException(ErrorCode.CLOSED, "TableHandle for tableId " + id() + " is closed");
        }
    }

    private void closeChannel() {
        closed = true;
        try {
            channel.close();
        } catch (IOException ignored) {}
    }

    private static void readFully(ChannelIO channel, ByteBuffer buffer, long position) throws StorageException {
        long currentPos = position;
        while (buffer.hasRemaining()) {
            int read;
            try {
                read = channel.read(buffer, currentPos);
            } catch (IOException e) {
                throw new StorageException(ErrorCode.IO, "I/O error reading SSTable at position " + currentPos, e);
            }
            if (read < 0) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Unexpected EOF reading SSTable at position " + currentPos);
            }
            currentPos += read;
        }
    }

    private final class SingleTableCursor implements RecordCursor {
        private final ByteKey fromKey;
        private final ByteKey toKey;
        private int currentBlockIndex;
        private List<InternalRecord> currentBlockRecords;
        private int recordIndexInBlock;
        private InternalRecord currentRecord;
        private boolean cursorClosed;

        SingleTableCursor(ByteKey fromKey, ByteKey toKey) {
            this.fromKey = fromKey;
            this.toKey = toKey;
            pin();

            List<BlockMeta> blocks = sparseIndex.blocks();
            if (blocks.isEmpty() || (fromKey != null && toKey != null && fromKey.compareTo(toKey) > 0)) {
                this.currentBlockIndex = blocks.size(); // EOF
            } else if (fromKey == null) {
                this.currentBlockIndex = 0;
            } else {
                int start = findCandidateBlockIndex(fromKey, blocks);
                this.currentBlockIndex = Math.max(0, start);
            }
        }

        @Override
        public boolean advance() throws StorageException {
            if (cursorClosed) {
                throw new StorageException(ErrorCode.CLOSED, "SingleTableCursor is closed");
            }

            List<BlockMeta> blocks = sparseIndex.blocks();
            while (true) {
                if (currentBlockRecords == null || recordIndexInBlock >= currentBlockRecords.size()) {
                    if (currentBlockIndex >= blocks.size()) {
                        currentRecord = null;
                        return false;
                    }

                    BlockMeta meta = blocks.get(currentBlockIndex);
                    // Early exit if the entire block is >= toKey
                    if (toKey != null && ByteKey.copyOf(meta.firstKey()).compareTo(toKey) >= 0) {
                        currentRecord = null;
                        currentBlockIndex = blocks.size();
                        return false;
                    }

                    try {
                        currentBlockRecords = DataBlockDecoder.readAndDecode(channel, meta.blockOffset());
                    } catch (IOException e) {
                        throw new StorageException(ErrorCode.IO,
                                "Failed reading SSTable data block at offset " + meta.blockOffset(), e);
                    }
                    recordIndexInBlock = 0;
                    currentBlockIndex++;
                }

                while (recordIndexInBlock < currentBlockRecords.size()) {
                    InternalRecord candidate = currentBlockRecords.get(recordIndexInBlock++);
                    if (fromKey != null && candidate.key().compareTo(fromKey) < 0) {
                        continue;
                    }
                    if (toKey != null && candidate.key().compareTo(toKey) >= 0) {
                        currentRecord = null;
                        currentBlockIndex = blocks.size();
                        return false;
                    }
                    currentRecord = candidate;
                    return true;
                }
            }
        }

        @Override
        public InternalRecord current() {
            if (cursorClosed) {
                throw new IllegalStateException("Cursor is closed");
            }
            if (currentRecord == null) {
                throw new IllegalStateException("Cursor current() called before advance() or after EOF");
            }
            return currentRecord;
        }

        @Override
        public void close() {
            if (!cursorClosed) {
                cursorClosed = true;
                currentRecord = null;
                currentBlockRecords = null;
                unpin();
            }
        }
    }
}
