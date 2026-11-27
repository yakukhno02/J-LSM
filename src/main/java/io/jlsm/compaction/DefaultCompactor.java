package io.jlsm.compaction;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RangeTombstone;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.manifest.TableMeta;
import io.jlsm.merge.DefaultMergedIterator;
import io.jlsm.sst.DataBlockEncoder;
import io.jlsm.sst.DefaultSSTableFlusher;
import io.jlsm.sst.Footer;
import io.jlsm.sst.SSTableFlusher;
import io.jlsm.sst.SSTableHeader;
import io.jlsm.sst.TableHandle;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Standard implementation of {@link Compactor} that performs a K-way merge over a list of pinned
 * {@link TableHandle} instances, deduplicating records by sequence number and optionally purging
 * tombstones before writing out either a single consolidated SSTable or a sequence of size-bounded
 * SSTables (SSTable Splitting).
 */
public final class DefaultCompactor implements Compactor {

    private static final int RECORD_HEADER_OVERHEAD = 17; // 8B seq + 4B keyLen + 4B valLen + 1B flags
    private static final int DATA_BLOCK_USABLE_PAYLOAD = DataBlockEncoder.BLOCK_SIZE - 12; // 4084 bytes
    private static final int SPARSE_INDEX_ENTRY_OVERHEAD = 16; // 8B offset + 4B blockSize + 4B keyLen
    private static final int BLOOM_HEADER_AND_FOOTER_OVERHEAD = 40; // JBLM header + CRC + word rounding

    private final SSTableFlusher flusher;
    private final Path baseDir;
    private final StorageIO storageIO;
    private final long defaultTargetFileSizeBytes;

    public DefaultCompactor(Path baseDir, StorageIO storageIO) {
        this(baseDir, storageIO, DEFAULT_TARGET_FILE_SIZE);
    }

    public DefaultCompactor(Path baseDir, StorageIO storageIO, long defaultTargetFileSizeBytes) {
        this.baseDir = Objects.requireNonNull(baseDir, "baseDir cannot be null");
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
        if (defaultTargetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("defaultTargetFileSizeBytes must be positive: " + defaultTargetFileSizeBytes);
        }
        this.defaultTargetFileSizeBytes = defaultTargetFileSizeBytes;
        this.flusher = new DefaultSSTableFlusher(baseDir, storageIO);
    }

    public DefaultCompactor(SSTableFlusher flusher) {
        this.flusher = Objects.requireNonNull(flusher, "flusher cannot be null");
        this.baseDir = null;
        this.storageIO = null;
        this.defaultTargetFileSizeBytes = DEFAULT_TARGET_FILE_SIZE;
    }

    public long defaultTargetFileSizeBytes() {
        return defaultTargetFileSizeBytes;
    }

    @Override
    public Optional<TableMeta> build(long outputId, List<TableHandle> pinnedInputs, boolean retainTombstones)
            throws StorageException {
        if (outputId < 0) {
            throw new IllegalArgumentException("outputId cannot be negative: " + outputId);
        }
        Objects.requireNonNull(pinnedInputs, "pinnedInputs cannot be null");

        if (pinnedInputs.isEmpty()) {
            return Optional.empty();
        }

        List<RecordCursor> cursors = openCursors(pinnedInputs);
        List<RangeTombstone> rangeTombstones = new ArrayList<>();
        for (TableHandle handle : pinnedInputs) {
            rangeTombstones.addAll(handle.rangeTombstones());
        }
        try (DefaultMergedIterator merged = new DefaultMergedIterator(cursors, retainTombstones, rangeTombstones)) {
            if (!merged.advance()) {
                // All input tables are empty or all records were annihilated by tombstones
                return Optional.empty();
            }

            InternalRecord firstRecord = merged.current();
            RecordCursor peekedCursor = new PrependRecordCursor(firstRecord, merged);
            TableMeta meta = flusher.build(outputId, peekedCursor);
            return Optional.of(meta);
        }
    }

    /**
     * Executes a K-way merge over {@code pinnedInputs} and splits the output stream into multiple
     * SSTable files using {@link #defaultTargetFileSizeBytes()}.
     */
    public List<TableMeta> buildSplit(
            LongSupplier nextFileIdSupplier,
            List<TableHandle> pinnedInputs,
            boolean retainTombstones
    ) throws StorageException {
        return buildSplit(nextFileIdSupplier, pinnedInputs, retainTombstones, defaultTargetFileSizeBytes);
    }

    @Override
    public List<TableMeta> buildSplit(
            LongSupplier nextFileIdSupplier,
            List<TableHandle> pinnedInputs,
            boolean retainTombstones,
            long targetFileSizeBytes
    ) throws StorageException {
        Objects.requireNonNull(nextFileIdSupplier, "nextFileIdSupplier cannot be null");
        Objects.requireNonNull(pinnedInputs, "pinnedInputs cannot be null");
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive: " + targetFileSizeBytes);
        }

        if (pinnedInputs.isEmpty()) {
            return List.of();
        }

        List<RecordCursor> cursors = openCursors(pinnedInputs);
        List<TableMeta> producedTables = new ArrayList<>();
        List<RangeTombstone> rangeTombstones = new ArrayList<>();
        for (TableHandle handle : pinnedInputs) {
            rangeTombstones.addAll(handle.rangeTombstones());
        }

        try (DefaultMergedIterator merged = new DefaultMergedIterator(cursors, retainTombstones, rangeTombstones)) {
            if (!merged.advance()) {
                return List.of();
            }

            SplitRecordCursor splitCursor = new SplitRecordCursor(merged, targetFileSizeBytes);
            while (splitCursor.hasRemainingPartitions()) {
                long fileId = nextFileIdSupplier.getAsLong();
                if (fileId < 0) {
                    throw new IllegalArgumentException("nextFileIdSupplier returned negative fileId: " + fileId);
                }
                splitCursor.beginNextPartition();
                TableMeta partMeta = flusher.build(fileId, splitCursor);
                producedTables.add(partMeta);
            }
            return Collections.unmodifiableList(producedTables);
        } catch (StorageException | RuntimeException e) {
            cleanupProducedTables(producedTables);
            throw e;
        } catch (Exception e) {
            cleanupProducedTables(producedTables);
            throw new StorageException(ErrorCode.IO, "Unexpected failure during split compaction", e);
        }
    }

    private List<RecordCursor> openCursors(List<TableHandle> pinnedInputs) throws StorageException {
        List<RecordCursor> cursors = new ArrayList<>(pinnedInputs.size());
        try {
            for (TableHandle handle : pinnedInputs) {
                Objects.requireNonNull(handle, "pinnedInputs cannot contain null TableHandle");
                cursors.add(handle.scan(null, null));
            }
            return cursors;
        } catch (Exception e) {
            closeQuietly(cursors);
            if (e instanceof StorageException se) {
                throw se;
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new StorageException(ErrorCode.IO, "Failed to open table cursors for compaction", e);
        }
    }

    private void cleanupProducedTables(List<TableMeta> producedTables) {
        if (producedTables.isEmpty()) {
            return;
        }
        for (TableMeta meta : producedTables) {
            try {
                if (flusher instanceof DefaultSSTableFlusher defaultFlusher) {
                    Path sstPath = defaultFlusher.sstPath(meta.tableId());
                    if (storageIO != null) {
                        storageIO.deleteIfExists(sstPath);
                    } else {
                        Files.deleteIfExists(sstPath);
                    }
                } else if (baseDir != null) {
                    Path sstPath = baseDir.resolve(String.format("%06d.sst", meta.tableId()));
                    if (storageIO != null) {
                        storageIO.deleteIfExists(sstPath);
                    } else {
                        Files.deleteIfExists(sstPath);
                    }
                }
            } catch (Exception ignored) {
            }
        }
    }

    private static void closeQuietly(List<RecordCursor> cursors) {
        for (RecordCursor cursor : cursors) {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Cursor wrapper that yields a pre-fetched first record before delegating subsequent
     * advances to the underlying {@link RecordCursor}.
     */
    private static final class PrependRecordCursor implements RecordCursor {
        private final InternalRecord firstRecord;
        private final RecordCursor delegate;
        private int state = 0; // 0 = before first advance, 1 = on firstRecord, 2 = delegating, 3 = EOF/closed

        PrependRecordCursor(InternalRecord firstRecord, RecordCursor delegate) {
            this.firstRecord = firstRecord;
            this.delegate = delegate;
        }

        @Override
        public boolean advance() throws StorageException {
            if (state == 0) {
                state = 1;
                return true;
            }
            if (state == 1 || state == 2) {
                if (delegate.advance()) {
                    state = 2;
                    return true;
                }
                state = 3;
                return false;
            }
            return false;
        }

        @Override
        public InternalRecord current() {
            if (state == 1) {
                return firstRecord;
            }
            if (state == 2) {
                return delegate.current();
            }
            throw new IllegalStateException("Cursor current() called before advance() or after EOF");
        }

        @Override
        public void close() throws StorageException {
            state = 3;
            delegate.close();
        }
    }

    /**
     * Stateful partitioning cursor that slices a single underlying {@link RecordCursor}
     * (whose first record has already been positioned via {@code advance()}) into multiple
     * non-overlapping partitions bounded by {@code targetFileSizeBytes}.
     *
     * <p>Tracks exact 4KB data block packing, SparseIndex entry growth, BloomFilter bit-array size,
     * and fixed header/footer overhead so that each partition flushes to an SSTable whose on-disk
     * size does not exceed {@code targetFileSizeBytes} (unless a single record requires at least
     * one 4KB block that exceeds a very small {@code targetFileSizeBytes}).
     */
    private static final class SplitRecordCursor implements RecordCursor {
        private final RecordCursor delegate;
        private final long targetFileSizeBytes;

        private InternalRecord pendingRecord;
        private InternalRecord currentRecord;
        private boolean delegateExhausted;
        private boolean partitionEnded;

        // Per-partition size estimation state
        private long recordsInPartition;
        private int blocksInPartition;
        private int currentBlockPayloadUsed;
        private long sparseIndexPayloadBytes;

        SplitRecordCursor(RecordCursor positionedDelegate, long targetFileSizeBytes) {
            this.delegate = positionedDelegate;
            this.targetFileSizeBytes = targetFileSizeBytes;
            this.pendingRecord = positionedDelegate.current();
            this.delegateExhausted = false;
        }

        boolean hasRemainingPartitions() {
            return pendingRecord != null || !delegateExhausted;
        }

        void beginNextPartition() {
            this.currentRecord = null;
            this.partitionEnded = false;
            this.recordsInPartition = 0L;
            this.blocksInPartition = 0;
            this.currentBlockPayloadUsed = 0;
            this.sparseIndexPayloadBytes = 8L; // 4B blockCount + 4B indexCRC32
        }

        @Override
        public boolean advance() throws StorageException {
            if (partitionEnded) {
                currentRecord = null;
                return false;
            }

            InternalRecord candidate = pendingRecord;
            if (candidate == null) {
                if (delegateExhausted) {
                    partitionEnded = true;
                    currentRecord = null;
                    return false;
                }
                if (delegate.advance()) {
                    candidate = delegate.current();
                } else {
                    delegateExhausted = true;
                    partitionEnded = true;
                    currentRecord = null;
                    return false;
                }
            }

            int keyLen = candidate.key().length();
            int valLen = candidate.value() != null ? candidate.value().length : 0;
            int recordBytes = RECORD_HEADER_OVERHEAD + keyLen + valLen;

            boolean startsNewBlock = (blocksInPartition == 0)
                    || (currentBlockPayloadUsed + recordBytes > DATA_BLOCK_USABLE_PAYLOAD);

            int nextBlocks = startsNewBlock ? (blocksInPartition + 1) : blocksInPartition;
            long nextIndexBytes = startsNewBlock
                    ? (sparseIndexPayloadBytes + SPARSE_INDEX_ENTRY_OVERHEAD + keyLen)
                    : sparseIndexPayloadBytes;
            long nextRecords = recordsInPartition + 1L;
            long nextBloomBytes = BLOOM_HEADER_AND_FOOTER_OVERHEAD + ((nextRecords * 10L + 63L) / 64L) * 8L;

            long projectedFileSize = SSTableHeader.HEADER_SIZE
                    + ((long) nextBlocks * DataBlockEncoder.BLOCK_SIZE)
                    + nextBloomBytes
                    + nextIndexBytes
                    + Footer.FOOTER_SIZE;

            if (recordsInPartition > 0 && projectedFileSize > targetFileSizeBytes) {
                // Hold candidate as pendingRecord for the next SSTable partition
                pendingRecord = candidate;
                partitionEnded = true;
                currentRecord = null;
                return false;
            }

            // Accept candidate into the current SSTable partition
            pendingRecord = null;
            currentRecord = candidate;
            recordsInPartition = nextRecords;
            blocksInPartition = nextBlocks;
            sparseIndexPayloadBytes = nextIndexBytes;
            currentBlockPayloadUsed = startsNewBlock
                    ? recordBytes
                    : (currentBlockPayloadUsed + recordBytes);
            return true;
        }

        @Override
        public InternalRecord current() {
            if (currentRecord == null) {
                throw new IllegalStateException("Cursor current() called before advance() or after partition EOF");
            }
            return currentRecord;
        }

        @Override
        public void close() {
            // Do not close the underlying merged cursor between partitions;
            // DefaultCompactor closes the merged iterator in try-with-resources when all partitions complete.
            partitionEnded = true;
            currentRecord = null;
        }
    }
}
