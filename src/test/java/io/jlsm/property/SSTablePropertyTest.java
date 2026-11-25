package io.jlsm.property;

import io.jlsm.compaction.DefaultCompactor;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.TableMeta;
import io.jlsm.oracle.SSTableDumpOracle;
import io.jlsm.oracle.TreeMapOracle;
import io.jlsm.sst.BlockMeta;
import io.jlsm.sst.DataBlockDecoder;
import io.jlsm.sst.DefaultDiskStorageReader;
import io.jlsm.sst.DefaultSSTableFlusher;
import io.jlsm.sst.DefaultTableHandle;
import io.jlsm.sst.TableHandle;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L2 Property-Based Tests (P-01 through P-05 from Section 6.2 of DESIGN.md)
 * verifying SSTable encoding/decoding, SparseIndex invariants, K-Way Merge,
 * Compaction safety, and Tombstone priority using jqwik.
 */
public class SSTablePropertyTest {

    public record RawOperation(byte[] key, byte[] value, boolean isDelete) {}

    @Provide
    public Arbitrary<List<InternalRecord>> sortedUniqueRecords() {
        Arbitrary<byte[]> keyArb = Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(64);
        Arbitrary<byte[]> valArb = Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(256);
        Arbitrary<Boolean> delArb = Arbitraries.of(false, false, false, true);

        Arbitrary<RawOperation> opArb = Combinators.combine(keyArb, valArb, delArb).as(RawOperation::new);
        return opArb.list().ofMinSize(1).ofMaxSize(120).map(ops -> {
            TreeMap<ByteKey, InternalRecord> sorted = new TreeMap<>();
            long seq = 1L;
            for (RawOperation op : ops) {
                ByteKey bk = ByteKey.copyOf(op.key());
                InternalRecord rec = op.isDelete()
                        ? InternalRecord.of(bk, null, RecordKind.DELETE, seq++)
                        : InternalRecord.of(bk, op.value(), RecordKind.PUT, seq++);
                sorted.put(bk, rec);
            }
            return new ArrayList<>(sorted.values());
        });
    }

    @Provide
    public Arbitrary<List<List<RawOperation>>> multiTableBatches() {
        Arbitrary<byte[]> keyArb = Arbitraries.oneOf(
                Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(8),
                Arbitraries.of(
                        new byte[]{0x00},
                        new byte[]{0x01},
                        new byte[]{0x7F},
                        new byte[]{(byte) 0x80},
                        new byte[]{(byte) 0xFF},
                        new byte[]{0x01, (byte) 0xFF},
                        new byte[]{(byte) 0xFF, 0x00}
                )
        );
        Arbitrary<byte[]> valArb = Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(128);
        Arbitrary<Boolean> delArb = Arbitraries.of(false, false, true);

        Arbitrary<RawOperation> opArb = Combinators.combine(keyArb, valArb, delArb).as(RawOperation::new);
        Arbitrary<List<RawOperation>> batchArb = opArb.list().ofMinSize(1).ofMaxSize(40);
        return batchArb.list().ofMinSize(2).ofMaxSize(5);
    }

    /**
     * P-01 (Збереження та відновлення / Roundtrip Preservation):
     * Any generated sorted records flushed to an SSTable and read back must match 100%
     * and pass structural verification in {@link SSTableDumpOracle}.
     */
    @Property(tries = 40)
    public void p01_roundtripPreservation(@ForAll("sortedUniqueRecords") List<InternalRecord> records) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-p01-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);

            TableMeta meta = flusher.build(1L, new ListCursor(records));
            Path sstPath = flusher.sstPath(1L);

            SSTableDumpOracle.DumpReport report = SSTableDumpOracle.inspectAndValidate(storageIO, sstPath);
            assertThat(report.allRecords()).isEqualTo(records);

            try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, sstPath, meta);
                 RecordCursor cursor = handle.scan(null, null)) {
                List<InternalRecord> scanned = new ArrayList<>();
                while (cursor.advance()) {
                    scanned.add(cursor.current());
                }
                assertThat(scanned).isEqualTo(records);
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    /**
     * P-02 (Впорядкованість індексу / Sparse Index Monotonicity):
     * Keys in SparseIndex must strictly increase according to unsigned byte order.
     */
    @Property(tries = 40)
    public void p02_sparseIndexMonotonicity(@ForAll("sortedUniqueRecords") List<InternalRecord> records) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-p02-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);

            TableMeta meta = flusher.build(1L, new ListCursor(records));
            try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), meta)) {
                List<BlockMeta> blocks = handle.sparseIndex().blocks();
                assertThat(blocks).isNotEmpty();

                for (int i = 1; i < blocks.size(); i++) {
                    byte[] prevKey = blocks.get(i - 1).firstKey();
                    byte[] currKey = blocks.get(i).firstKey();
                    assertThat(Arrays.compareUnsigned(prevKey, currKey)).isLessThan(0);
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    /**
     * P-03 (Швидкий пошук за індексом / Index Point Lookup Accuracy):
     * Every existing key in an SSTable must reside in the exact 4KB block selected via SparseIndex binary search,
     * and point lookup must return the exact record.
     */
    @Property(tries = 35)
    public void p03_sparseIndexPointLookupAccuracy(@ForAll("sortedUniqueRecords") List<InternalRecord> records) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-p03-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);

            TableMeta meta = flusher.build(1L, new ListCursor(records));
            try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), meta)) {
                List<BlockMeta> blocks = handle.sparseIndex().blocks();

                for (InternalRecord expected : records) {
                    // 1. Verify point lookup via DefaultTableHandle.get
                    Optional<InternalRecord> found = handle.get(expected.key());
                    assertThat(found).contains(expected);

                    // 2. Verify binary search on SparseIndex points to the exact block containing the key
                    int blockIdx = findCandidateBlock(expected.key(), blocks);
                    assertThat(blockIdx).isGreaterThanOrEqualTo(0);

                    BlockMeta targetBlock = blocks.get(blockIdx);
                    List<InternalRecord> blockRecords = DataBlockDecoder.readAndDecode(
                            handle.channel(), targetBlock.blockOffset());
                    assertThat(blockRecords).contains(expected);
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    /**
     * P-04 (Безпечність фонового об'єднання / Compaction Safety Invariant):
     * Compacting multiple SSTables into one must preserve 100% of live records matching TreeMapOracle.
     */
    @Property(tries = 35)
    public void p04_compactionSafetyInvariant(@ForAll("multiTableBatches") List<List<RawOperation>> batches) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-p04-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);
            DefaultCompactor compactor = new DefaultCompactor(tempDir, storageIO);
            TreeMapOracle oracle = new TreeMapOracle();

            List<TableHandle> handles = new ArrayList<>();
            long nextFileId = 1L;
            try {
                for (List<RawOperation> batch : batches) {
                    TreeMap<ByteKey, InternalRecord> memTableBatch = new TreeMap<>();
                    for (RawOperation op : batch) {
                        InternalRecord rec = op.isDelete()
                                ? oracle.delete(op.key())
                                : oracle.put(op.key(), op.value());
                        memTableBatch.put(rec.key(), rec);
                    }
                    long id = nextFileId++;
                    TableMeta meta = flusher.build(id, new ListCursor(new ArrayList<>(memTableBatch.values())));
                    handles.add(DefaultTableHandle.open(storageIO, flusher.sstPath(id), meta));
                }

                long compactedId = nextFileId;
                Optional<TableMeta> compactedMetaOpt = compactor.build(compactedId, handles, false);
                List<InternalRecord> expectedLive = oracle.expectedCompactedRecords(false);

                if (expectedLive.isEmpty()) {
                    assertThat(compactedMetaOpt).isEmpty();
                } else {
                    assertThat(compactedMetaOpt).isPresent();
                    Path compactedPath = flusher.sstPath(compactedId);
                    SSTableDumpOracle.DumpReport report = SSTableDumpOracle.inspectAndValidate(storageIO, compactedPath);
                    assertThat(report.allRecords()).isEqualTo(expectedLive);
                }

                // Also verify split compaction (buildSplit) preserves 100% of live records across disjoint partitions
                java.util.concurrent.atomic.AtomicLong splitNextId = new java.util.concurrent.atomic.AtomicLong(compactedId + 1);
                List<TableMeta> splitMetas = compactor.buildSplit(
                        splitNextId::getAndIncrement,
                        handles,
                        false,
                        4500L // force splitting whenever output exceeds 1 data block
                );
                if (expectedLive.isEmpty()) {
                    assertThat(splitMetas).isEmpty();
                } else {
                    assertThat(splitMetas).isNotEmpty();
                    List<InternalRecord> combinedSplitRecords = new ArrayList<>();
                    ByteKey prevLastKey = null;
                    for (TableMeta splitMeta : splitMetas) {
                        Path splitPath = flusher.sstPath(splitMeta.tableId());
                        SSTableDumpOracle.DumpReport splitReport = SSTableDumpOracle.inspectAndValidate(storageIO, splitPath);
                        assertThat(splitReport.allRecords()).isNotEmpty();
                        ByteKey firstKey = splitReport.allRecords().get(0).key();
                        ByteKey lastKey = splitReport.allRecords().get(splitReport.allRecords().size() - 1).key();
                        if (prevLastKey != null) {
                            assertThat(prevLastKey.compareTo(firstKey)).isLessThan(0);
                        }
                        prevLastKey = lastKey;
                        combinedSplitRecords.addAll(splitReport.allRecords());
                    }
                    assertThat(combinedSplitRecords).isEqualTo(expectedLive);
                }
            } finally {
                for (TableHandle h : handles) {
                    h.close();
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    /**
     * P-05 (Пріоритет видалення / Tombstone Priority Invariant):
     * If a key's latest operation is DELETE, point lookup returns the tombstone (so engine returns null)
     * and scanDisk / compaction return no entry for that key, even if older PUT values exist in earlier SSTables.
     */
    @Property(tries = 35)
    public void p05_tombstonePriorityInvariant(@ForAll("multiTableBatches") List<List<RawOperation>> batches) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-p05-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);
            DefaultDiskStorageReader reader = new DefaultDiskStorageReader();
            TreeMapOracle oracle = new TreeMapOracle();

            List<TableHandle> handles = new ArrayList<>();
            long nextFileId = 1L;
            try {
                for (List<RawOperation> batch : batches) {
                    TreeMap<ByteKey, InternalRecord> memTableBatch = new TreeMap<>();
                    for (RawOperation op : batch) {
                        InternalRecord rec = op.isDelete()
                                ? oracle.delete(op.key())
                                : oracle.put(op.key(), op.value());
                        memTableBatch.put(rec.key(), rec);
                    }
                    long id = nextFileId++;
                    TableMeta meta = flusher.build(id, new ListCursor(new ArrayList<>(memTableBatch.values())));
                    handles.add(DefaultTableHandle.open(storageIO, flusher.sstPath(id), meta));
                }

                // Verify scanDisk matches oracle live records (all tombstoned keys excluded)
                List<InternalRecord> scannedLive = new ArrayList<>();
                try (RecordCursor cursor = reader.scanDisk(handles, null, null)) {
                    while (cursor.advance()) {
                        scannedLive.add(cursor.current());
                    }
                }
                assertThat(scannedLive).isEqualTo(oracle.expectedCompactedRecords(false));

                // Verify every record in oracle (including tombstones) matches point search
                for (InternalRecord oracleRec : oracle.expectedCompactedRecords(true)) {
                    Optional<InternalRecord> searchRes = reader.search(handles, oracleRec.key());
                    assertThat(searchRes).contains(oracleRec);
                    if (oracleRec.isTombstone()) {
                        assertThat(oracle.get(oracleRec.key().bytes())).isNull();
                    }
                }
            } finally {
                for (TableHandle h : handles) {
                    h.close();
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    @Example
    public void regressionPrefixKeysOn4KbBlockBoundary() throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-reg-prefix-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);

            List<InternalRecord> records = new ArrayList<>();
            for (int i = 0; i < 150; i++) {
                ByteKey bk = ByteKey.copyOf(String.format("boundary_test_prefix_%05d", i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                byte[] val = String.format("value_for_boundary_testing_%05d", i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                records.add(new InternalRecord.Put(bk, val, i + 1L));
            }

            TableMeta meta = flusher.build(101L, new ListCursor(records));
            assertThat(meta.fileSize()).isGreaterThan(4096L);

            Path sstPath = tempDir.resolve(String.format("%06d.sst", 101L));
            try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, sstPath, meta)) {
                for (int i = 0; i < 150; i++) {
                    ByteKey bk = ByteKey.copyOf(String.format("boundary_test_prefix_%05d", i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    Optional<InternalRecord> found = handle.get(bk);
                    assertThat(found).isPresent();
                    assertThat(found.get().sequence()).isEqualTo(i + 1L);
                }

                ByteKey from = ByteKey.copyOf("boundary_test_prefix_00030".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                ByteKey to = ByteKey.copyOf("boundary_test_prefix_00120".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                try (RecordCursor cursor = handle.scan(from, to)) {
                    int count = 0;
                    while (cursor.advance()) {
                        count++;
                    }
                    assertThat(count).isEqualTo(90);
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    @Example
    public void regressionHighBitRangeScans() throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-reg-highbit-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);

            List<InternalRecord> records = new ArrayList<>();
            byte[][] rawKeys = new byte[][] {
                    new byte[]{0x01},
                    new byte[]{0x7F},
                    new byte[]{(byte) 0x80},
                    new byte[]{(byte) 0xAA},
                    new byte[]{(byte) 0xFE},
                    new byte[]{(byte) 0xFF}
            };

            for (int i = 0; i < rawKeys.length; i++) {
                records.add(new InternalRecord.Put(ByteKey.copyOf(rawKeys[i]), new byte[]{(byte) (i * 10)}, i + 1L));
            }

            TableMeta meta = flusher.build(202L, new ListCursor(records));
            Path sstPath = tempDir.resolve(String.format("%06d.sst", 202L));
            try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, sstPath, meta)) {
                ByteKey from = ByteKey.copyOf(new byte[]{0x7F});
                ByteKey to = ByteKey.copyOf(new byte[]{(byte) 0xFF});
                try (RecordCursor cursor = handle.scan(from, to)) {
                    List<ByteKey> scanned = new ArrayList<>();
                    while (cursor.advance()) {
                        scanned.add(cursor.current().key());
                    }
                    assertThat(scanned).containsExactly(
                            ByteKey.copyOf(new byte[]{0x7F}),
                            ByteKey.copyOf(new byte[]{(byte) 0x80}),
                            ByteKey.copyOf(new byte[]{(byte) 0xAA}),
                            ByteKey.copyOf(new byte[]{(byte) 0xFE})
                    );
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    private static int findCandidateBlock(ByteKey key, List<BlockMeta> blocks) {
        int low = 0;
        int high = blocks.size() - 1;
        int candidate = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = ByteKey.copyOf(blocks.get(mid).firstKey()).compareTo(key);
            if (cmp <= 0) {
                candidate = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return candidate;
    }

    private static void deleteDirectoryQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
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
