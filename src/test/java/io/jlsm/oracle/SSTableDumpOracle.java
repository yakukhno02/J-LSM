package io.jlsm.oracle;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.sst.BlockMeta;
import io.jlsm.sst.DataBlockDecoder;
import io.jlsm.sst.DataBlockEncoder;
import io.jlsm.sst.Footer;
import io.jlsm.sst.SSTableHeader;
import io.jlsm.sst.SparseIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Structural verification oracle for SSTable files (Section 6.1.2 of DESIGN.md).
 * <p>
 * Reads an {@code .sst} file exclusively via {@link StorageIO} and 4KB {@link ByteBuffer}s
 * and verifies all structural invariants:
 * <ul>
 *     <li>32-byte Common Header magic, version, block size, and CRC32</li>
 *     <li>40-byte Footer magic, version, index bounds, and CRC32</li>
 *     <li>Sparse Index CRC32, block count parity, 4KB-aligned block offsets, and strict unsigned key monotonicity</li>
 *     <li>Every 4KB Data Block's CRC32, zero-padding area, first-key parity with Sparse Index, and global key ordering</li>
 * </ul>
 */
public final class SSTableDumpOracle {

    private SSTableDumpOracle() {}

    public record BlockDump(
            int blockIndex,
            long blockOffset,
            int recordCount,
            int usedBytes,
            int crc32,
            List<InternalRecord> records
    ) {
        public BlockDump {
            records = List.copyOf(records);
        }
    }

    public record DumpReport(
            SSTableHeader header,
            Footer footer,
            SparseIndex sparseIndex,
            List<BlockDump> blocks,
            List<InternalRecord> allRecords,
            long fileSize
    ) {
        public DumpReport {
            blocks = List.copyOf(blocks);
            allRecords = List.copyOf(allRecords);
        }
    }

    /**
     * Inspects an SSTable file and validates all binary format and ordering invariants.
     *
     * @param storageIO storage I/O abstraction
     * @param sstFile   path to the SSTable file
     * @return validated {@link DumpReport} containing parsed structures and records
     * @throws StorageException if any structural, checksum, or key-ordering invariant is violated
     */
    public static DumpReport inspectAndValidate(StorageIO storageIO, Path sstFile) throws StorageException {
        Objects.requireNonNull(storageIO, "storageIO cannot be null");
        Objects.requireNonNull(sstFile, "sstFile cannot be null");

        try (ChannelIO channel = storageIO.openRead(sstFile)) {
            long fileSize = channel.size();
            if (fileSize < SSTableHeader.HEADER_SIZE + Footer.FOOTER_SIZE) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "SSTable file size (" + fileSize + ") is smaller than minimum header + footer (72 bytes)");
            }

            // 1. Validate 32B Header
            ByteBuffer headerBuf = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, headerBuf, 0L);
            headerBuf.flip();
            SSTableHeader header = SSTableHeader.decode(headerBuf);
            if (header.blockSize() != DataBlockEncoder.BLOCK_SIZE) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Unexpected blockSize in header: " + header.blockSize());
            }

            // 2. Validate 40B Footer
            ByteBuffer footerBuf = ByteBuffer.allocate(Footer.FOOTER_SIZE).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, footerBuf, fileSize - Footer.FOOTER_SIZE);
            footerBuf.flip();
            Footer footer = Footer.decode(footerBuf);

            long dataBlocksEndOffset = SSTableHeader.HEADER_SIZE
                    + (long) footer.blockCount() * DataBlockEncoder.BLOCK_SIZE;
            if (footer.indexOffset() < dataBlocksEndOffset) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Footer indexOffset (" + footer.indexOffset()
                                + ") is smaller than end of data blocks (" + dataBlocksEndOffset + ")");
            }
            if (footer.indexOffset() + footer.indexLength() != fileSize - Footer.FOOTER_SIZE) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Footer indexOffset + indexLength does not align with start of Footer");
            }

            // 2b. Validate optional Bloom Filter Block between Data Blocks and Sparse Index
            io.jlsm.sst.BloomFilter bloomFilter = null;
            if (footer.indexOffset() > dataBlocksEndOffset) {
                int bloomLen = (int) (footer.indexOffset() - dataBlocksEndOffset);
                ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLen).order(ByteOrder.BIG_ENDIAN);
                readFully(channel, bloomBuf, dataBlocksEndOffset);
                bloomBuf.flip();
                bloomFilter = io.jlsm.sst.BloomFilter.decode(bloomBuf);
            }

            // 3. Validate SparseIndex
            ByteBuffer indexBuf = ByteBuffer.allocate((int) footer.indexLength()).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, indexBuf, footer.indexOffset());
            indexBuf.flip();
            SparseIndex sparseIndex = SparseIndex.decode(indexBuf);

            List<BlockMeta> indexBlocks = sparseIndex.blocks();
            if (indexBlocks.size() != footer.blockCount()) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "SparseIndex blockCount (" + indexBlocks.size()
                                + ") != Footer blockCount (" + footer.blockCount() + ")");
            }

            // Check strict ascending monotonicity of firstKey in SparseIndex and exact block offsets
            byte[] prevBlockFirstKey = null;
            for (int i = 0; i < indexBlocks.size(); i++) {
                BlockMeta meta = indexBlocks.get(i);
                long expectedOffset = SSTableHeader.HEADER_SIZE + (long) i * DataBlockEncoder.BLOCK_SIZE;
                if (meta.blockOffset() != expectedOffset) {
                    throw new StorageException(ErrorCode.CORRUPTION,
                            "Block " + i + " offset mismatch: expected " + expectedOffset + ", got " + meta.blockOffset());
                }
                if (meta.blockSize() != DataBlockEncoder.BLOCK_SIZE) {
                    throw new StorageException(ErrorCode.CORRUPTION,
                            "Block " + i + " size mismatch: " + meta.blockSize());
                }
                byte[] firstKey = meta.firstKey();
                if (prevBlockFirstKey != null && Arrays.compareUnsigned(prevBlockFirstKey, firstKey) >= 0) {
                    throw new StorageException(ErrorCode.CORRUPTION,
                            "SparseIndex firstKey at block " + i + " is not strictly greater than previous block's firstKey");
                }
                prevBlockFirstKey = firstKey;
            }

            // 4. Validate each 4KB Data Block
            List<BlockDump> blockDumps = new ArrayList<>(indexBlocks.size());
            List<InternalRecord> allRecords = new ArrayList<>();
            ByteKey prevRecordKey = null;

            ByteBuffer rawBlockBuf = ByteBuffer.allocate(DataBlockEncoder.BLOCK_SIZE).order(ByteOrder.BIG_ENDIAN);
            for (int i = 0; i < indexBlocks.size(); i++) {
                BlockMeta meta = indexBlocks.get(i);
                rawBlockBuf.clear();
                readFully(channel, rawBlockBuf, meta.blockOffset());
                rawBlockBuf.flip();

                byte[] rawBytes = rawBlockBuf.array();
                int recordCount = rawBlockBuf.getInt(0);
                int usedBytes = rawBlockBuf.getInt(4);
                int blockCrc = rawBlockBuf.getInt(DataBlockEncoder.BLOCK_DATA_PAYLOAD_LIMIT);

                List<InternalRecord> records = DataBlockDecoder.decode(ByteBuffer.wrap(rawBytes));
                if (records.isEmpty()) {
                    throw new StorageException(ErrorCode.CORRUPTION, "Data block " + i + " has 0 records");
                }

                // Verify zero padding between usedBytes and 4092
                for (int p = usedBytes; p < DataBlockEncoder.BLOCK_DATA_PAYLOAD_LIMIT; p++) {
                    if (rawBytes[p] != 0) {
                        throw new StorageException(ErrorCode.CORRUPTION,
                                "Non-zero padding byte at offset " + p + " in data block " + i);
                    }
                }

                // Verify first record key matches SparseIndex entry
                ByteKey expectedFirstKey = ByteKey.copyOf(meta.firstKey());
                if (!records.getFirst().key().equals(expectedFirstKey)) {
                    throw new StorageException(ErrorCode.CORRUPTION,
                            "Block " + i + " first record key does not match SparseIndex firstKey");
                }

                // Verify global ascending key order across all records and BloomFilter membership
                for (InternalRecord record : records) {
                    if (prevRecordKey != null && prevRecordKey.compareTo(record.key()) > 0) {
                        throw new StorageException(ErrorCode.CORRUPTION,
                                "Out-of-order key detected in data block " + i);
                    }
                    if (bloomFilter != null && !bloomFilter.mightContain(record.key())) {
                        throw new StorageException(ErrorCode.CORRUPTION,
                                "BloomFilter false negative detected for key in data block " + i);
                    }
                    prevRecordKey = record.key();
                    allRecords.add(record);
                }

                blockDumps.add(new BlockDump(i, meta.blockOffset(), recordCount, usedBytes, blockCrc, records));
            }

            if (allRecords.size() != footer.totalRecordCount()) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Total decoded records (" + allRecords.size()
                                + ") != Footer totalRecordCount (" + footer.totalRecordCount() + ")");
            }

            return new DumpReport(header, footer, sparseIndex, blockDumps, allRecords, fileSize);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "I/O error inspecting SSTable: " + sstFile, e);
        }
    }

    /**
     * Formats a human-readable diagnostic summary of a {@link DumpReport}.
     */
    public static String formatReport(DumpReport report) {
        Objects.requireNonNull(report, "report cannot be null");
        HexFormat hex = HexFormat.of();
        StringBuilder sb = new StringBuilder();
        sb.append("=== SSTable Dump Report ===\n");
        sb.append(String.format("File ID: %d, File Size: %d bytes, Format Version: %d%n",
                report.header().fileId(), report.fileSize(), report.header().formatVersion()));
        sb.append(String.format("Total Records: %d, Block Count: %d, Index Offset: %d, Index Length: %d%n",
                report.footer().totalRecordCount(),
                report.footer().blockCount(),
                report.footer().indexOffset(),
                report.footer().indexLength()));

        for (BlockDump block : report.blocks()) {
            byte[] firstKey = report.sparseIndex().blocks().get(block.blockIndex()).firstKey();
            sb.append(String.format("  Block #%d [offset=%d, records=%d, usedBytes=%d, crc32=0x%08X, firstKey=%s]%n",
                    block.blockIndex(),
                    block.blockOffset(),
                    block.recordCount(),
                    block.usedBytes(),
                    block.crc32(),
                    hex.formatHex(firstKey)));
        }
        return sb.toString();
    }

    private static void readFully(ChannelIO channel, ByteBuffer dst, long position) throws IOException, StorageException {
        long currentPos = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, currentPos);
            if (read < 0) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "Unexpected EOF reading SSTable at offset " + currentPos);
            }
            currentPos += read;
        }
    }
}
