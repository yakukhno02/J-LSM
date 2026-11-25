package io.jlsm.compaction;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.FaultInjectingStorage;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.TableMeta;
import io.jlsm.sst.DefaultSSTableFlusher;
import io.jlsm.sst.DefaultTableHandle;
import io.jlsm.sst.TableHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultCompactorTest {

    @TempDir
    Path tempDir;

    private StandardStorageIO storageIO;
    private DefaultSSTableFlusher flusher;
    private DefaultCompactor compactor;

    @BeforeEach
    void setUp() {
        storageIO = new StandardStorageIO();
        flusher = new DefaultSSTableFlusher(tempDir, storageIO);
        compactor = new DefaultCompactor(tempDir, storageIO);
    }

    private static InternalRecord put(String key, String val, long seq) {
        return InternalRecord.of(
                ByteKey.copyOf(key.getBytes(StandardCharsets.UTF_8)),
                val.getBytes(StandardCharsets.UTF_8),
                RecordKind.PUT,
                seq
        );
    }

    private static InternalRecord del(String key, long seq) {
        return InternalRecord.of(
                ByteKey.copyOf(key.getBytes(StandardCharsets.UTF_8)),
                null,
                RecordKind.DELETE,
                seq
        );
    }

    private static ByteKey key(String str) {
        return ByteKey.copyOf(str.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Merges 4 SSTables, deduplicates versions, and purges tombstones when retainTombstones = false")
    void testStandardCompactionPurgesTombstonesAndDeduplicates() throws Exception {
        // Table 1 (oldest): a@1="a1", b@2="b1", c@3="c1"
        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(
                put("a", "a1", 1L),
                put("b", "b1", 2L),
                put("c", "c1", 3L)
        )));

        // Table 2: b@10="b2", d@11="d1"
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(
                put("b", "b2", 10L),
                put("d", "d1", 11L)
        )));

        // Table 3: a@20=DELETE, c@21="c2"
        TableMeta m3 = flusher.build(3L, new ListCursor(List.of(
                del("a", 20L),
                put("c", "c2", 21L)
        )));

        // Table 4 (newest): d@30=DELETE, e@31="e1"
        TableMeta m4 = flusher.build(4L, new ListCursor(List.of(
                del("d", 30L),
                put("e", "e1", 31L)
        )));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2);
             DefaultTableHandle h3 = DefaultTableHandle.open(storageIO, flusher.sstPath(3L), m3);
             DefaultTableHandle h4 = DefaultTableHandle.open(storageIO, flusher.sstPath(4L), m4)) {

            Optional<TableMeta> compactedOpt = compactor.build(10L, List.of(h1, h2, h3, h4), false);
            assertThat(compactedOpt).isPresent();

            TableMeta compactedMeta = compactedOpt.get();
            assertThat(compactedMeta.tableId()).isEqualTo(10L);
            // Surviving records: b@10, c@21, e@31 (3 records; 'a' and 'd' purged by tombstones)
            assertThat(compactedMeta.recordCount()).isEqualTo(3L);
            assertThat(compactedMeta.minSeq()).isEqualTo(10L);
            assertThat(compactedMeta.maxSeq()).isEqualTo(31L);

            try (DefaultTableHandle outHandle = DefaultTableHandle.open(storageIO, flusher.sstPath(10L), compactedMeta)) {
                assertThat(outHandle.get(key("a"))).isEmpty();
                assertThat(outHandle.get(key("d"))).isEmpty();

                Optional<InternalRecord> bRec = outHandle.get(key("b"));
                assertThat(bRec).isPresent();
                assertThat(bRec.get().sequence()).isEqualTo(10L);
                assertThat(bRec.get().value()).isEqualTo("b2".getBytes(StandardCharsets.UTF_8));

                Optional<InternalRecord> cRec = outHandle.get(key("c"));
                assertThat(cRec).isPresent();
                assertThat(cRec.get().sequence()).isEqualTo(21L);
                assertThat(cRec.get().value()).isEqualTo("c2".getBytes(StandardCharsets.UTF_8));

                Optional<InternalRecord> eRec = outHandle.get(key("e"));
                assertThat(eRec).isPresent();
                assertThat(eRec.get().sequence()).isEqualTo(31L);
                assertThat(eRec.get().value()).isEqualTo("e1".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    @DisplayName("Retains tombstones in output SSTable when retainTombstones = true")
    void testCompactionRetainsTombstonesWhenRequested() throws Exception {
        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(
                put("k1", "v1", 1L),
                put("k2", "v2", 2L)
        )));
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(
                del("k1", 10L)
        )));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2)) {

            Optional<TableMeta> compactedOpt = compactor.build(20L, List.of(h1, h2), true);
            assertThat(compactedOpt).isPresent();

            TableMeta meta = compactedOpt.get();
            assertThat(meta.recordCount()).isEqualTo(2L);
            assertThat(meta.minSeq()).isEqualTo(2L);
            assertThat(meta.maxSeq()).isEqualTo(10L);

            try (DefaultTableHandle outHandle = DefaultTableHandle.open(storageIO, flusher.sstPath(20L), meta)) {
                Optional<InternalRecord> k1 = outHandle.get(key("k1"));
                assertThat(k1).isPresent();
                assertThat(k1.get().isTombstone()).isTrue();
                assertThat(k1.get().sequence()).isEqualTo(10L);

                Optional<InternalRecord> k2 = outHandle.get(key("k2"));
                assertThat(k2).isPresent();
                assertThat(k2.get().isTombstone()).isFalse();
                assertThat(k2.get().sequence()).isEqualTo(2L);
            }
        }
    }

    @Test
    @DisplayName("Returns Optional.empty() and creates no file when all records are annihilated by tombstones")
    void testAllRecordsAnnihilatedByTombstonesReturnsEmpty() throws Exception {
        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(
                put("k1", "v1", 1L),
                put("k2", "v2", 2L)
        )));
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(
                del("k1", 10L),
                del("k2", 11L)
        )));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2)) {

            Optional<TableMeta> result = compactor.build(30L, List.of(h1, h2), false);
            assertThat(result).isEmpty();

            assertThat(Files.exists(flusher.sstPath(30L))).isFalse();
            assertThat(Files.exists(flusher.tmpPath(30L))).isFalse();
        }
    }

    @Test
    @DisplayName("Returns Optional.empty() for empty input table list or empty SSTables")
    void testEmptyInputsReturnEmpty() throws Exception {
        assertThat(compactor.build(40L, List.of(), false)).isEmpty();
        assertThat(Files.exists(flusher.sstPath(40L))).isFalse();

        TableMeta emptyMeta = flusher.build(1L, new ListCursor(List.of()));
        try (DefaultTableHandle emptyHandle = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), emptyMeta)) {
            Optional<TableMeta> res = compactor.build(41L, List.of(emptyHandle), false);
            assertThat(res).isEmpty();
            assertThat(Files.exists(flusher.sstPath(41L))).isFalse();
        }
    }

    @Test
    @DisplayName("Compacts multiple multi-block SSTables accurately")
    void testMultiBlockCompaction() throws Exception {
        List<InternalRecord> batch1 = new ArrayList<>();
        List<InternalRecord> batch2 = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            String k = String.format("key-%04d", i);
            batch1.add(put(k, "old-value-payload-" + i, 100L + i));
            if (i % 2 == 0) {
                batch2.add(put(k, "new-value-payload-" + i, 500L + i));
            } else if (i % 5 == 0) {
                batch2.add(del(k, 500L + i));
            }
        }

        TableMeta m1 = flusher.build(1L, new ListCursor(batch1));
        TableMeta m2 = flusher.build(2L, new ListCursor(batch2));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2)) {

            Optional<TableMeta> compactedOpt = compactor.build(50L, List.of(h1, h2), false);
            assertThat(compactedOpt).isPresent();

            try (DefaultTableHandle out = DefaultTableHandle.open(storageIO, flusher.sstPath(50L), compactedOpt.get())) {
                for (int i = 0; i < 120; i++) {
                    ByteKey k = key(String.format("key-%04d", i));
                    Optional<InternalRecord> rec = out.get(k);
                    if (i % 2 == 0) {
                        assertThat(rec).isPresent();
                        assertThat(rec.get().sequence()).isEqualTo(500L + i);
                        assertThat(rec.get().value()).isEqualTo(("new-value-payload-" + i).getBytes(StandardCharsets.UTF_8));
                    } else if (i % 5 == 0) {
                        assertThat(rec).isEmpty();
                    } else {
                        assertThat(rec).isPresent();
                        assertThat(rec.get().sequence()).isEqualTo(100L + i);
                        assertThat(rec.get().value()).isEqualTo(("old-value-payload-" + i).getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("FI-05 / FI-06: Crash during compaction cleans up .sst.tmp and preserves input SSTables")
    void testFaultInjectionDuringCompactionPreservesInputs() throws Exception {
        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(put("k1", "v1", 1L))));
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(put("k2", "v2", 2L))));

        FaultInjectingStorage faultStorage = new FaultInjectingStorage(storageIO);
        DefaultCompactor faultCompactor = new DefaultCompactor(tempDir, faultStorage);

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2)) {

            faultStorage.failAfterBytesWritten(64L);

            assertThatThrownBy(() -> faultCompactor.build(60L, List.of(h1, h2), false))
                    .isInstanceOfSatisfying(StorageException.class, e -> assertThat(e.code()).isEqualTo(ErrorCode.IO));

            // Temporary file must be deleted and output SSTable must not exist
            assertThat(Files.exists(flusher.tmpPath(60L))).isFalse();
            assertThat(Files.exists(flusher.sstPath(60L))).isFalse();

            // Input tables must remain completely intact and readable
            assertThat(h1.get(key("k1"))).isPresent();
            assertThat(h2.get(key("k2"))).isPresent();
        }
    }

    @Test
    @DisplayName("Validates arguments and handles closed TableHandle")
    void testValidationAndClosedInput() throws Exception {
        assertThatThrownBy(() -> compactor.build(-1L, List.of(), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> compactor.build(1L, null, false))
                .isInstanceOf(NullPointerException.class);

        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(put("k1", "v1", 1L))));
        DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
        h1.close();

        assertThatThrownBy(() -> compactor.build(70L, List.of(h1), false))
                .isInstanceOfSatisfying(StorageException.class, e -> assertThat(e.code()).isEqualTo(ErrorCode.CLOSED));
    }

    @Test
    @DisplayName("buildSplit partitions K-way merged output into multiple SSTables bounded by targetFileSizeBytes")
    void testBuildSplitProducesMultipleSizeBoundedSSTables() throws Exception {
        List<InternalRecord> batch1 = new ArrayList<>();
        List<InternalRecord> batch2 = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            String k = String.format("split-key-%05d", i);
            batch1.add(put(k, "initial-payload-value-for-splitting-" + i, 100L + i));
            if (i % 3 == 0) {
                batch2.add(put(k, "updated-payload-value-for-splitting-" + i, 1000L + i));
            } else if (i % 10 == 0) {
                batch2.add(del(k, 1000L + i));
            }
        }

        TableMeta m1 = flusher.build(1L, new ListCursor(batch1));
        TableMeta m2 = flusher.build(2L, new ListCursor(batch2));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2)) {

            long targetFileSize = 8500L; // ~2 data blocks (2 * 4096 + header + bloom + index + footer)
            java.util.concurrent.atomic.AtomicLong nextId = new java.util.concurrent.atomic.AtomicLong(100L);

            List<TableMeta> splitMetas = compactor.buildSplit(
                    nextId::getAndIncrement,
                    List.of(h1, h2),
                    false,
                    targetFileSize
            );

            assertThat(splitMetas).hasSizeGreaterThanOrEqualTo(3);

            long totalRecords = 0L;
            List<InternalRecord> allSplitRecords = new ArrayList<>();
            ByteKey previousPartitionLastKey = null;

            for (TableMeta meta : splitMetas) {
                assertThat(meta.fileSize()).isLessThanOrEqualTo(targetFileSize);
                assertThat(meta.recordCount()).isGreaterThan(0L);
                totalRecords += meta.recordCount();

                try (DefaultTableHandle partHandle = DefaultTableHandle.open(storageIO, flusher.sstPath(meta.tableId()), meta);
                     RecordCursor cursor = partHandle.scan(null, null)) {
                    ByteKey firstKeyInPart = null;
                    ByteKey lastKeyInPart = null;
                    while (cursor.advance()) {
                        InternalRecord rec = cursor.current();
                        if (firstKeyInPart == null) {
                            firstKeyInPart = rec.key();
                        }
                        lastKeyInPart = rec.key();
                        allSplitRecords.add(rec);
                    }
                    assertThat(firstKeyInPart).isNotNull();
                    if (previousPartitionLastKey != null) {
                        // Disjoint, strictly increasing key ranges across partitions
                        assertThat(previousPartitionLastKey.compareTo(firstKeyInPart)).isLessThan(0);
                    }
                    previousPartitionLastKey = lastKeyInPart;
                }
            }

            // Compare concatenated records from split tables against single-output compaction
            Optional<TableMeta> singleMetaOpt = compactor.build(500L, List.of(h1, h2), false);
            assertThat(singleMetaOpt).isPresent();
            assertThat(totalRecords).isEqualTo(singleMetaOpt.get().recordCount());

            List<InternalRecord> singleRecords = new ArrayList<>();
            try (DefaultTableHandle singleHandle = DefaultTableHandle.open(storageIO, flusher.sstPath(500L), singleMetaOpt.get());
                 RecordCursor cursor = singleHandle.scan(null, null)) {
                while (cursor.advance()) {
                    singleRecords.add(cursor.current());
                }
            }

            assertThat(allSplitRecords).isEqualTo(singleRecords);
        }
    }

    @Test
    @DisplayName("buildSplit returns empty list when all records are annihilated by tombstones or inputs are empty")
    void testBuildSplitEmptyAndAnnihilated() throws Exception {
        java.util.concurrent.atomic.AtomicLong nextId = new java.util.concurrent.atomic.AtomicLong(200L);
        assertThat(compactor.buildSplit(nextId::getAndIncrement, List.of(), false, 8192L)).isEmpty();
        assertThat(nextId.get()).isEqualTo(200L);

        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(put("k1", "v1", 1L))));
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(del("k1", 5L))));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2)) {
            List<TableMeta> res = compactor.buildSplit(nextId::getAndIncrement, List.of(h1, h2), false, 8192L);
            assertThat(res).isEmpty();
            assertThat(nextId.get()).isEqualTo(200L);
        }
    }

    @Test
    @DisplayName("buildSplit cleans up already-produced partition files if a later partition fails I/O")
    void testBuildSplitCleansUpPreviousPartitionsOnMidStreamFailure() throws Exception {
        List<InternalRecord> batch = new ArrayList<>();
        for (int i = 0; i < 160; i++) {
            batch.add(put(String.format("key-%04d", i), "payload-value-for-failure-test-" + i, i + 1L));
        }
        TableMeta m1 = flusher.build(1L, new ListCursor(batch));

        FaultInjectingStorage faultStorage = new FaultInjectingStorage(storageIO);
        DefaultCompactor faultCompactor = new DefaultCompactor(tempDir, faultStorage, 4500L);

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1)) {
            // Allow first partition (~4.2KB) to succeed, fail during second partition write
            faultStorage.failAfterBytesWritten(6000L);
            java.util.concurrent.atomic.AtomicLong nextId = new java.util.concurrent.atomic.AtomicLong(300L);

            assertThatThrownBy(() -> faultCompactor.buildSplit(nextId::getAndIncrement, List.of(h1), false))
                    .isInstanceOf(StorageException.class);

            // Both the completed first partition (300.sst) and any temporary second partition (301.sst.tmp) must be cleaned up
            assertThat(Files.exists(flusher.sstPath(300L))).isFalse();
            assertThat(Files.exists(flusher.sstPath(301L))).isFalse();
            assertThat(Files.exists(flusher.tmpPath(301L))).isFalse();
        }
    }

    @Test
    @DisplayName("Compactor interface default buildSplit delegates to build and validates arguments")
    void testCompactorDefaultBuildSplitAndAccessors() throws Exception {
        assertThat(compactor.defaultTargetFileSizeBytes()).isEqualTo(Compactor.DEFAULT_TARGET_FILE_SIZE);
        assertThatThrownBy(() -> new DefaultCompactor(tempDir, storageIO, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> compactor.buildSplit(null, List.of(), false, 4096L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> compactor.buildSplit(() -> 1L, null, false, 4096L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> compactor.buildSplit(() -> 1L, List.of(), false, 0L))
                .isInstanceOf(IllegalArgumentException.class);

        TableMeta expectedMeta = new TableMeta(99L, 1L, 5L, 2L, 4200L);
        Compactor customCompactor = (outputId, pinnedInputs, retainTombstones) -> Optional.of(expectedMeta);

        assertThatThrownBy(() -> customCompactor.buildSplit(null, List.of(), false, 4096L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> customCompactor.buildSplit(() -> 99L, null, false, 4096L))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> customCompactor.buildSplit(() -> 99L, List.of(), false, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(customCompactor.buildSplit(() -> 99L, List.of(), false, 4096L)).isEmpty();

        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(put("k1", "v1", 1L))));
        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1)) {
            List<TableMeta> res = customCompactor.buildSplit(() -> 99L, List.of(h1), false, 4096L);
            assertThat(res).containsExactly(expectedMeta);
        }
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
