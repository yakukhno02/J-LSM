package io.jlsm.oracle;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.sst.DefaultSSTableFlusher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SSTableDumpOracleTest {

    @TempDir
    Path tempDir;

    private StandardStorageIO storageIO;
    private DefaultSSTableFlusher flusher;

    @BeforeEach
    void setUp() {
        storageIO = new StandardStorageIO();
        flusher = new DefaultSSTableFlusher(tempDir, storageIO);
    }

    private static InternalRecord put(String key, String val, long seq) {
        return InternalRecord.of(
                ByteKey.copyOf(key.getBytes(StandardCharsets.UTF_8)),
                val.getBytes(StandardCharsets.UTF_8),
                RecordKind.PUT,
                seq
        );
    }

    @Test
    @DisplayName("Validates sstable-minimal.bin fixture and produces diagnostic dump report")
    void testDumpFixtureMinimalSSTable() throws Exception {
        Path fixtureCopy = tempDir.resolve("000001.sst");
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            Files.copy(in, fixtureCopy);
        }

        SSTableDumpOracle.DumpReport report = SSTableDumpOracle.inspectAndValidate(storageIO, fixtureCopy);

        assertThat(report.header().fileId()).isEqualTo(1L);
        assertThat(report.footer().totalRecordCount()).isEqualTo(2L);
        assertThat(report.footer().blockCount()).isEqualTo(1);
        assertThat(report.blocks()).hasSize(1);
        assertThat(report.allRecords()).hasSize(2);
        assertThat(report.fileSize()).isEqualTo(4196L);

        String formatted = SSTableDumpOracle.formatReport(report);
        assertThat(formatted)
                .contains("SSTable Dump Report")
                .contains("File ID: 1")
                .contains("Total Records: 2")
                .contains("Block #0");
    }

    @Test
    @DisplayName("Validates multi-block SSTable and detects non-zero padding corruption")
    void testMultiBlockValidationAndPaddingCheck() throws Exception {
        List<InternalRecord> records = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            records.add(put(String.format("k-%04d", i), "payload-data-" + i, i + 1L));
        }

        flusher.build(2L, new ListCursor(records));
        Path sstPath = flusher.sstPath(2L);

        SSTableDumpOracle.DumpReport report = SSTableDumpOracle.inspectAndValidate(storageIO, sstPath);
        assertThat(report.blocks().size()).isGreaterThan(1);
        assertThat(report.allRecords()).isEqualTo(records);

        // Corrupt padding byte in first block (and recompute block CRC so only padding invariant fails)
        try (ChannelIO ch = storageIO.openReadWrite(sstPath)) {
            ByteBuffer blockBuf = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN);
            ch.read(blockBuf, 32L);
            byte[] blockBytes = blockBuf.array();
            int usedBytes = blockBuf.getInt(4);
            assertThat(usedBytes).isLessThan(4090);

            blockBytes[usedBytes + 1] = 0x7F; // non-zero byte in zero-padding region
            CRC32 crc = new CRC32();
            crc.update(blockBytes, 0, 4092);
            blockBuf.putInt(4092, (int) crc.getValue());

            blockBuf.position(0);
            ch.write(blockBuf, 32L);
        }

        assertThatThrownBy(() -> SSTableDumpOracle.inspectAndValidate(storageIO, sstPath))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Non-zero padding byte");
                });
    }

    private static final class ListCursor implements RecordCursor {
        private final Iterator<InternalRecord> it;
        private InternalRecord cur;

        ListCursor(List<InternalRecord> list) {
            this.it = list.iterator();
        }

        @Override
        public boolean advance() {
            if (it.hasNext()) {
                cur = it.next();
                return true;
            }
            cur = null;
            return false;
        }

        @Override
        public InternalRecord current() {
            if (cur == null) throw new IllegalStateException("EOF");
            return cur;
        }

        @Override
        public void close() {}
    }
}
