package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.FaultInjectingStorage;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.TableMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultSSTableFlusherTest {

    @TempDir
    Path tempDir;

    private StandardStorageIO standardStorage;
    private DefaultSSTableFlusher flusher;

    @BeforeEach
    void setUp() {
        standardStorage = new StandardStorageIO();
        flusher = new DefaultSSTableFlusher(tempDir, standardStorage);
    }

    @Test
    @DisplayName("Matches verified binary fixture sstable-minimal.bin byte-for-byte")
    void testOutputMatchesFixtureByteForByte() throws Exception {
        byte[] expectedFixtureBytes;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            expectedFixtureBytes = in.readAllBytes();
        }

        List<InternalRecord> records = List.of(
                new InternalRecord(
                        ByteKey.copyOf("key1".getBytes(StandardCharsets.UTF_8)),
                        "value1".getBytes(StandardCharsets.UTF_8),
                        RecordKind.PUT,
                        1L
                ),
                new InternalRecord(
                        ByteKey.copyOf("key2".getBytes(StandardCharsets.UTF_8)),
                        null,
                        RecordKind.DELETE,
                        2L
                )
        );

        TableMeta meta = flusher.build(1L, new ListRecordCursor(records));

        assertThat(meta.tableId()).isEqualTo(1L);
        assertThat(meta.minSeq()).isEqualTo(1L);
        assertThat(meta.maxSeq()).isEqualTo(2L);
        assertThat(meta.recordCount()).isEqualTo(2L);
        assertThat(meta.fileSize()).isEqualTo(4196L);

        Path sstPath = flusher.sstPath(1L);
        assertThat(Files.exists(sstPath)).isTrue();
        assertThat(Files.exists(flusher.tmpPath(1L))).isFalse();

        byte[] actualBytes = Files.readAllBytes(sstPath);
        assertThat(actualBytes).hasSize(expectedFixtureBytes.length);
        assertThat(actualBytes).isEqualTo(expectedFixtureBytes);
    }

    @Test
    @DisplayName("Multi-block flush properly encodes blocks, index entries, and footer")
    void testMultiBlockFlush() throws Exception {
        // Build 150 entries each around ~50 bytes, which will spill over multiple 4KB blocks
        List<InternalRecord> records = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            String keyStr = String.format("key-%04d", i);
            String valStr = String.format("value-payload-data-%04d", i);
            records.add(new InternalRecord(
                    ByteKey.copyOf(keyStr.getBytes(StandardCharsets.UTF_8)),
                    valStr.getBytes(StandardCharsets.UTF_8),
                    RecordKind.PUT,
                    100L + i
            ));
        }

        TableMeta meta = flusher.build(2L, new ListRecordCursor(records));

        assertThat(meta.tableId()).isEqualTo(2L);
        assertThat(meta.minSeq()).isEqualTo(100L);
        assertThat(meta.maxSeq()).isEqualTo(249L);
        assertThat(meta.recordCount()).isEqualTo(150L);

        Path sstPath = flusher.sstPath(2L);
        assertThat(Files.exists(sstPath)).isTrue();
        long actualSize = Files.size(sstPath);
        assertThat(meta.fileSize()).isEqualTo(actualSize);

        // Read footer from disk
        try (ChannelIO channel = standardStorage.openRead(sstPath)) {
            ByteBuffer footerBuf = ByteBuffer.allocate(Footer.FOOTER_SIZE);
            channel.read(footerBuf, actualSize - Footer.FOOTER_SIZE);
            footerBuf.flip();

            Footer footer = Footer.decode(footerBuf);
            assertThat(footer.totalRecordCount()).isEqualTo(150L);
            assertThat(footer.blockCount()).isGreaterThan(1);
            assertThat(footer.formatVersion()).isEqualTo(1);

            // Read sparse index
            ByteBuffer indexBuf = ByteBuffer.allocate((int) footer.indexLength());
            channel.read(indexBuf, footer.indexOffset());
            indexBuf.flip();

            SparseIndex sparseIndex = SparseIndex.decode(indexBuf);
            assertThat(sparseIndex.blocks()).hasSize(footer.blockCount());

            // First block offset must be 32 (immediately after header)
            assertThat(sparseIndex.blocks().get(0).blockOffset()).isEqualTo(32L);
            assertThat(sparseIndex.blocks().get(0).blockSize()).isEqualTo(4096);
            assertThat(sparseIndex.blocks().get(0).firstKey()).isEqualTo("key-0000".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("Empty cursor creates valid minimal SSTable with 0 records")
    void testEmptyCursor() throws Exception {
        TableMeta meta = flusher.build(3L, new ListRecordCursor(List.of()));

        assertThat(meta.tableId()).isEqualTo(3L);
        assertThat(meta.minSeq()).isEqualTo(0L);
        assertThat(meta.maxSeq()).isEqualTo(0L);
        assertThat(meta.recordCount()).isEqualTo(0L);
        assertThat(meta.fileSize()).isEqualTo(80L); // 32 (header) + 8 (empty index) + 40 (footer)

        Path sstPath = flusher.sstPath(3L);
        assertThat(Files.exists(sstPath)).isTrue();
        assertThat(Files.size(sstPath)).isEqualTo(80L);

        try (ChannelIO channel = standardStorage.openRead(sstPath)) {
            ByteBuffer buf = ByteBuffer.allocate(80);
            channel.read(buf, 0L);
            buf.flip();

            SSTableHeader header = SSTableHeader.decode(buf);
            assertThat(header.fileId()).isEqualTo(3L);

            SparseIndex index = SparseIndex.decode(buf);
            assertThat(index.blocks()).isEmpty();

            Footer footer = Footer.decode(buf);
            assertThat(footer.blockCount()).isEqualTo(0);
            assertThat(footer.totalRecordCount()).isEqualTo(0L);
        }
    }

    @Test
    @DisplayName("Rejects unsorted records")
    void testRejectsUnsortedRecords() {
        List<InternalRecord> records = List.of(
                new InternalRecord(
                        ByteKey.copyOf("z".getBytes(StandardCharsets.UTF_8)),
                        "1".getBytes(StandardCharsets.UTF_8),
                        RecordKind.PUT,
                        1L
                ),
                new InternalRecord(
                        ByteKey.copyOf("a".getBytes(StandardCharsets.UTF_8)),
                        "2".getBytes(StandardCharsets.UTF_8),
                        RecordKind.PUT,
                        2L
                )
        );

        assertThatThrownBy(() -> flusher.build(4L, new ListRecordCursor(records)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not sorted in ascending order");

        // Temporary file should be cleaned up
        assertThat(Files.exists(flusher.tmpPath(4L))).isFalse();
        assertThat(Files.exists(flusher.sstPath(4L))).isFalse();
    }

    @Test
    @DisplayName("Rejects negative fileId")
    void testRejectsNegativeFileId() {
        assertThatThrownBy(() -> flusher.build(-1L, new ListRecordCursor(List.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fileId cannot be negative");
    }

    @Test
    @DisplayName("FI-03 Fault Injection: Crash during write cleans up .sst.tmp and leaves no orphan .sst")
    void testFaultInjectionCrashCleansUpTempFile() {
        FaultInjectingStorage faultStorage = new FaultInjectingStorage(standardStorage);
        DefaultSSTableFlusher faultFlusher = new DefaultSSTableFlusher(tempDir, faultStorage);

        List<InternalRecord> records = List.of(
                new InternalRecord(
                        ByteKey.copyOf("key1".getBytes(StandardCharsets.UTF_8)),
                        "value1".getBytes(StandardCharsets.UTF_8),
                        RecordKind.PUT,
                        1L
                )
        );

        // Fail after writing 100 bytes (header + partial data block)
        faultStorage.failAfterBytesWritten(100);

        assertThatThrownBy(() -> faultFlusher.build(5L, new ListRecordCursor(records)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.IO);
                });

        // Verify .sst.tmp is cleaned up and .sst is not created
        assertThat(Files.exists(faultFlusher.tmpPath(5L))).isFalse();
        assertThat(Files.exists(faultFlusher.sstPath(5L))).isFalse();
    }

    @Test
    @DisplayName("Fault Injection: Fsync failure cleans up .sst.tmp")
    void testFaultInjectionFsyncFailure() {
        FaultInjectingStorage faultStorage = new FaultInjectingStorage(standardStorage);
        DefaultSSTableFlusher faultFlusher = new DefaultSSTableFlusher(tempDir, faultStorage);

        faultStorage.failOnFsync(true);

        List<InternalRecord> records = List.of(
                new InternalRecord(
                        ByteKey.copyOf("key1".getBytes(StandardCharsets.UTF_8)),
                        "value1".getBytes(StandardCharsets.UTF_8),
                        RecordKind.PUT,
                        1L
                )
        );

        assertThatThrownBy(() -> faultFlusher.build(6L, new ListRecordCursor(records)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.IO);
                });

        assertThat(Files.exists(faultFlusher.tmpPath(6L))).isFalse();
        assertThat(Files.exists(faultFlusher.sstPath(6L))).isFalse();
    }

    private static final class ListRecordCursor implements RecordCursor {
        private final Iterator<InternalRecord> iterator;
        private InternalRecord current;
        private boolean closed;

        ListRecordCursor(List<InternalRecord> records) {
            this.iterator = records.iterator();
        }

        @Override
        public boolean advance() throws StorageException {
            if (closed) {
                throw new StorageException(ErrorCode.CLOSED, "Cursor is closed");
            }
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            }
            current = null;
            return false;
        }

        @Override
        public InternalRecord current() {
            if (current == null) {
                throw new IllegalStateException("Cursor current() called before advance or after EOF");
            }
            return current;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
