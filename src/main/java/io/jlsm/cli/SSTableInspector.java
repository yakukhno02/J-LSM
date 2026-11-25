package io.jlsm.cli;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.sst.BlockMeta;
import io.jlsm.sst.BloomFilter;
import io.jlsm.sst.DataBlockDecoder;
import io.jlsm.sst.DataBlockEncoder;
import io.jlsm.sst.Footer;
import io.jlsm.sst.SSTableHeader;
import io.jlsm.sst.SparseIndex;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Low-level binary inspector for {@code .sst} files used by the J-LSM CLI demonstration utility.
 * Reads SSTable structures strictly in 4KB blocks via {@link StorageIO}.
 */
public final class SSTableInspector {

    private SSTableInspector() {}

    public static void dumpSSTable(Path sstFile, PrintStream out) throws StorageException {
        dumpSSTable(new StandardStorageIO(), sstFile, out);
    }

    public static void dumpSSTable(StorageIO storageIO, Path sstFile, PrintStream out) throws StorageException {
        Objects.requireNonNull(storageIO, "storageIO cannot be null");
        Objects.requireNonNull(sstFile, "sstFile cannot be null");
        Objects.requireNonNull(out, "out cannot be null");

        try (ChannelIO channel = storageIO.openRead(sstFile)) {
            long fileSize = channel.size();
            if (fileSize < SSTableHeader.HEADER_SIZE + Footer.FOOTER_SIZE) {
                throw new StorageException(ErrorCode.CORRUPTION,
                        "File is too small to be a valid SSTable (" + fileSize + " bytes): " + sstFile);
            }

            ByteBuffer headerBuf = ByteBuffer.allocate(SSTableHeader.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, headerBuf, 0L);
            headerBuf.flip();
            SSTableHeader header = SSTableHeader.decode(headerBuf);

            ByteBuffer footerBuf = ByteBuffer.allocate(Footer.FOOTER_SIZE).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, footerBuf, fileSize - Footer.FOOTER_SIZE);
            footerBuf.flip();
            Footer footer = Footer.decode(footerBuf);

            ByteBuffer indexBuf = ByteBuffer.allocate((int) footer.indexLength()).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, indexBuf, footer.indexOffset());
            indexBuf.flip();
            SparseIndex sparseIndex = SparseIndex.decode(indexBuf);

            long dataEndOffset = SSTableHeader.HEADER_SIZE + (long) footer.blockCount() * header.blockSize();
            BloomFilter bloomFilter = null;
            if (footer.indexOffset() > dataEndOffset) {
                int bloomLen = (int) (footer.indexOffset() - dataEndOffset);
                ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLen).order(ByteOrder.BIG_ENDIAN);
                readFully(channel, bloomBuf, dataEndOffset);
                bloomBuf.flip();
                bloomFilter = BloomFilter.decode(bloomBuf);
            }

            out.println("================================================================================");
            out.println("                           SSTABLE BINARY STRUCTURE DUMP                        ");
            out.println("================================================================================");
            out.printf("  File Path         : %s%n", sstFile);
            out.printf("  File Size         : %d bytes%n", fileSize);
            out.printf("  Header [0..31]    : Magic=0x%08X (JSST), Version=%d, FileId=%d, BlockSize=%d%n",
                    SSTableHeader.SST_MAGIC, header.formatVersion(), header.fileId(), header.blockSize());
            out.printf("  Footer [%d..%d] : Magic=0x%08X (JLSM), Version=%d, Blocks=%d, TotalRecords=%d%n",
                    fileSize - Footer.FOOTER_SIZE, fileSize - 1,
                    Footer.FOOTER_MAGIC, footer.formatVersion(), footer.blockCount(), footer.totalRecordCount());
            if (bloomFilter != null) {
                out.printf(java.util.Locale.US,
                        "  Bloom Filter      : Offset=%d, Length=%d bytes, Bits=%d, k=%d, Fill=%.1f%%, EstFPP=%.4f%%%n",
                        dataEndOffset, footer.indexOffset() - dataEndOffset,
                        bloomFilter.bitSize(), bloomFilter.hashCount(),
                        bloomFilter.fillRatio() * 100.0,
                        bloomFilter.estimatedFalsePositiveRate() * 100.0);
            }
            out.printf("  Sparse Index      : Offset=%d, Length=%d bytes, Entries=%d%n",
                    footer.indexOffset(), footer.indexLength(), sparseIndex.blocks().size());
            out.println("--------------------------------------------------------------------------------");

            List<BlockMeta> blocks = sparseIndex.blocks();
            ByteBuffer blockBuf = ByteBuffer.allocate(DataBlockEncoder.BLOCK_SIZE).order(ByteOrder.BIG_ENDIAN);

            for (int i = 0; i < blocks.size(); i++) {
                BlockMeta meta = blocks.get(i);
                blockBuf.clear();
                readFully(channel, blockBuf, meta.blockOffset());
                blockBuf.flip();

                int recordCount = blockBuf.getInt(0);
                int usedBytes = blockBuf.getInt(4);
                int crc32 = blockBuf.getInt(DataBlockEncoder.BLOCK_DATA_PAYLOAD_LIMIT);
                List<InternalRecord> records = DataBlockDecoder.decode(ByteBuffer.wrap(blockBuf.array()));

                String firstKeyStr = formatPreview(meta.firstKey());
                out.printf("  Data Block #%d [offset=%d, size=%dB, records=%d, used=%d/4092B, crc32=0x%08X, firstKey=\"%s\"]%n",
                        i, meta.blockOffset(), meta.blockSize(), recordCount, usedBytes, crc32, firstKeyStr);

                int previewLimit = Math.min(records.size(), 5);
                for (int r = 0; r < previewLimit; r++) {
                    InternalRecord rec = records.get(r);
                    String k = formatPreview(rec.key().bytes());
                    String v = rec.isTombstone() ? "<TOMBSTONE>" : "\"" + formatPreview(rec.value()) + "\"";
                    out.printf("    [%d] seq=%d | %-6s | key=\"%s\" -> %s%n",
                            r, rec.sequence(), rec.kind(), k, v);
                }
                if (records.size() > previewLimit) {
                    out.printf("    ... (%d more records in block)%n", records.size() - previewLimit);
                }
            }
            out.println("================================================================================");
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to inspect SSTable file: " + sstFile, e);
        }
    }

    static String formatPreview(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        String s = new String(bytes, StandardCharsets.UTF_8);
        if (s.length() > 32) {
            return s.substring(0, 29) + "...";
        }
        return s;
    }

    private static void readFully(ChannelIO channel, ByteBuffer dst, long position) throws IOException, StorageException {
        long currentPos = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, currentPos);
            if (read < 0) {
                throw new StorageException(ErrorCode.CORRUPTION, "Unexpected EOF at offset " + currentPos);
            }
            currentPos += read;
        }
    }
}
