package io.jlsm.compaction;

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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SizeTieredCompactionPlannerTest {

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

    @Test
    @DisplayName("Returns Optional.empty() when live tables are fewer than minTablesPerTier")
    void testBelowThresholdReturnsEmpty() {
        SizeTieredCompactionPlanner planner = new SizeTieredCompactionPlanner(4, 65536L);
        TableHandle t1 = stubTable(1L, 1L, 10L, 10L, 4200L);
        TableHandle t2 = stubTable(2L, 11L, 20L, 10L, 4200L);
        TableHandle t3 = stubTable(3L, 21L, 30L, 10L, 4200L);

        assertThat(planner.plan(List.of())).isEmpty();
        assertThat(planner.plan(List.of(t1, t2, t3))).isEmpty();
        assertThat(planner.planFullCompaction(List.of())).isEmpty();
    }

    @Test
    @DisplayName("Selects oldest size-similar bucket and sets retainTombstones=false when base generation is included")
    void testSelectsBaseTierAndPurgesTombstones() {
        SizeTieredCompactionPlanner planner = new SizeTieredCompactionPlanner(4, 1024L * 1024);
        TableHandle t1 = stubTable(1L, 1L, 100L, 50L, 8192L);
        TableHandle t2 = stubTable(2L, 101L, 200L, 50L, 8400L);
        TableHandle t3 = stubTable(3L, 201L, 300L, 50L, 8100L);
        TableHandle t4 = stubTable(4L, 301L, 400L, 50L, 8300L);

        Optional<CompactionPlan> planOpt = planner.plan(List.of(t1, t2, t3, t4));
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.selectedTables()).containsExactly(t1, t2, t3, t4);
        assertThat(plan.includesBaseGeneration()).isTrue();
        assertThat(plan.retainTombstones()).isFalse();
        assertThat(plan.minSequence()).isEqualTo(1L);
        assertThat(plan.maxSequence()).isEqualTo(400L);
        assertThat(plan.totalInputRecords()).isEqualTo(200L);
        assertThat(plan.totalInputBytes()).isEqualTo(8192L + 8400L + 8100L + 8300L);
    }

    @Test
    @DisplayName("Selects newer minor tier when base tier has 1 huge table, preserving tombstones (retainTombstones=true)")
    void testSelectsYoungerMinorTierAndRetainsTombstones() {
        SizeTieredCompactionPlanner planner = new SizeTieredCompactionPlanner(
                3, 16, 512L * 1024, 4096L, 0.5, 1.5);

        // Base generation: 1 large compacted table (1 MB)
        TableHandle baseHuge = stubTable(1L, 1L, 5000L, 2000L, 1024L * 1024);
        // Newer generation: 3 small flushed tables (~16 KB each)
        TableHandle small1 = stubTable(2L, 5001L, 5100L, 40L, 16384L);
        TableHandle small2 = stubTable(3L, 5101L, 5200L, 40L, 16500L);
        TableHandle small3 = stubTable(4L, 5201L, 5300L, 40L, 16200L);

        Optional<CompactionPlan> planOpt = planner.plan(List.of(baseHuge, small1, small2, small3));
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.selectedTables()).containsExactly(small1, small2, small3);
        assertThat(plan.includesBaseGeneration()).isFalse();
        // Crucial LSM invariant: cannot purge tombstones when baseHuge (older table) is not part of compaction!
        assertThat(plan.retainTombstones()).isTrue();
        assertThat(plan.tierIndex()).isEqualTo(1);
    }

    @Test
    @DisplayName("Falls back to oldest contiguous prefix when heterogeneous tables exceed minTablesPerTier")
    void testFallbackOnHeterogeneousSizes() {
        SizeTieredCompactionPlanner planner = new SizeTieredCompactionPlanner(
                3, 3, 512L * 1024, 1024L, 0.8, 1.2);

        TableHandle t1 = stubTable(1L, 1L, 100L, 100L, 10_000L);
        TableHandle t2 = stubTable(2L, 101L, 200L, 200L, 50_000L);
        TableHandle t3 = stubTable(3L, 201L, 300L, 400L, 250_000L);
        TableHandle t4 = stubTable(4L, 301L, 400L, 800L, 1_250_000L);

        Optional<CompactionPlan> planOpt = planner.plan(List.of(t1, t2, t3, t4));
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.selectedTables()).containsExactly(t1, t2, t3);
        assertThat(plan.includesBaseGeneration()).isTrue();
        assertThat(plan.retainTombstones()).isFalse();
    }

    @Test
    @DisplayName("End-to-end integration: SizeTieredCompactionPlanner + DefaultCompactor.buildSplit preserves shadowed deletions across partial minor compaction")
    void testPlannerAndSplitCompactorEndToEndWithShadowedTombstone() throws Exception {
        // Table 1 (Large base generation): key "alpha" = "old-alpha", key "beta" = "old-beta"
        List<InternalRecord> baseRecords = new ArrayList<>();
        baseRecords.add(put("alpha", "old-alpha", 1L));
        baseRecords.add(put("beta", "old-beta", 2L));
        for (int i = 0; i < 180; i++) {
            baseRecords.add(put(String.format("pad-%04d", i), "large-base-padding-payload-" + i, 3L + i));
        }
        TableMeta m1 = flusher.build(1L, new ListCursor(baseRecords));

        // 3 small delta tables where "alpha" is deleted and "beta" is updated
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(
                del("alpha", 500L),
                put("gamma", "g1", 501L)
        )));
        TableMeta m3 = flusher.build(3L, new ListCursor(List.of(
                put("beta", "new-beta", 600L),
                put("delta", "d1", 601L)
        )));
        TableMeta m4 = flusher.build(4L, new ListCursor(List.of(
                put("epsilon", "e1", 700L)
        )));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2);
             DefaultTableHandle h3 = DefaultTableHandle.open(storageIO, flusher.sstPath(3L), m3);
             DefaultTableHandle h4 = DefaultTableHandle.open(storageIO, flusher.sstPath(4L), m4)) {

            SizeTieredCompactionPlanner planner = new SizeTieredCompactionPlanner(
                    3, 10, 4500L, 4300L, 0.5, 1.5);

            // Step 1: Minor compaction selects [h2, h3, h4] and retains tombstones because h1 is older
            Optional<CompactionPlan> minorPlanOpt = planner.plan(List.of(h1, h2, h3, h4));
            assertThat(minorPlanOpt).isPresent();
            CompactionPlan minorPlan = minorPlanOpt.get();
            assertThat(minorPlan.selectedTables()).containsExactly(h2, h3, h4);
            assertThat(minorPlan.retainTombstones()).isTrue();

            java.util.concurrent.atomic.AtomicLong nextId = new java.util.concurrent.atomic.AtomicLong(10L);
            List<TableMeta> minorOutputs = compactor.buildSplit(
                    nextId::getAndIncrement,
                    minorPlan.selectedTables(),
                    minorPlan.retainTombstones(),
                    minorPlan.targetFileSizeBytes()
            );
            assertThat(minorOutputs).isNotEmpty();

            // Step 2: Major full compaction of [h1 + minorOutputs] purges the "alpha" tombstone
            List<TableHandle> majorInputs = new ArrayList<>();
            majorInputs.add(h1);
            for (TableMeta meta : minorOutputs) {
                majorInputs.add(DefaultTableHandle.open(storageIO, flusher.sstPath(meta.tableId()), meta));
            }

            try {
                CompactionPlan majorPlan = planner.planFullCompaction(majorInputs).orElseThrow();
                assertThat(majorPlan.retainTombstones()).isFalse();

                List<TableMeta> majorOutputs = compactor.buildSplit(
                        nextId::getAndIncrement,
                        majorPlan.selectedTables(),
                        majorPlan.retainTombstones(),
                        majorPlan.targetFileSizeBytes()
                );
                assertThat(majorOutputs).hasSizeGreaterThanOrEqualTo(2);

                ByteKey alphaKey = ByteKey.copyOf("alpha".getBytes(StandardCharsets.UTF_8));
                ByteKey betaKey = ByteKey.copyOf("beta".getBytes(StandardCharsets.UTF_8));
                Optional<InternalRecord> foundBeta = Optional.empty();

                for (TableMeta outMeta : majorOutputs) {
                    try (DefaultTableHandle outH = DefaultTableHandle.open(storageIO, flusher.sstPath(outMeta.tableId()), outMeta)) {
                        assertThat(outH.get(alphaKey)).isEmpty();
                        Optional<InternalRecord> b = outH.get(betaKey);
                        if (b.isPresent()) {
                            foundBeta = b;
                        }
                    }
                }
                assertThat(foundBeta).isPresent();
                assertThat(foundBeta.get().value()).isEqualTo("new-beta".getBytes(StandardCharsets.UTF_8));
            } finally {
                for (int i = 1; i < majorInputs.size(); i++) {
                    majorInputs.get(i).close();
                }
            }
        }
    }

    @Test
    @DisplayName("Validates planner and CompactionPlan arguments")
    void testPlannerAndPlanValidation() {
        assertThatThrownBy(() -> new SizeTieredCompactionPlanner(1, 4, 4096L, 1024L, 0.5, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeTieredCompactionPlanner(4, 2, 4096L, 1024L, 0.5, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeTieredCompactionPlanner(4, 8, 0L, 1024L, 0.5, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeTieredCompactionPlanner(4, 8, 4096L, 0L, 0.5, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeTieredCompactionPlanner(4, 8, 4096L, 1024L, 1.0, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeTieredCompactionPlanner(4, 8, 4096L, 1024L, 0.5, 1.0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new CompactionPlan(List.of(), 4096L, false, true, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompactionPlan(List.of(stubTable(1L, 1L, 2L, 1L, 100L)), 0L, false, true, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompactionPlan(List.of(stubTable(1L, 1L, 2L, 1L, 100L)), 4096L, false, true, -1))
                .isInstanceOf(IllegalArgumentException.class);

        SizeTieredCompactionPlanner defaultPlanner = new SizeTieredCompactionPlanner();
        assertThat(defaultPlanner.minTablesPerTier()).isEqualTo(SizeTieredCompactionPlanner.DEFAULT_MIN_TABLES);
        assertThat(defaultPlanner.maxTablesPerTier()).isEqualTo(SizeTieredCompactionPlanner.DEFAULT_MAX_TABLES);
        assertThat(defaultPlanner.targetFileSizeBytes()).isEqualTo(Compactor.DEFAULT_TARGET_FILE_SIZE);
        assertThat(defaultPlanner.buildContiguousSizeBuckets(List.of())).isEmpty();
    }

    @Test
    @DisplayName("isHigherPriority prefers bucket with more tables, then smaller total bytes, then earlier startIndex")
    void testMultiBucketPriorityTieBreaking() {
        SizeTieredCompactionPlanner planner = new SizeTieredCompactionPlanner(
                2, 4, 512L * 1024, 1024L, 0.7, 1.3);

        // Bucket 0: 2 tables of ~100KB each (total 200KB)
        TableHandle b0_t1 = stubTable(1L, 1L, 10L, 10L, 100_000L);
        TableHandle b0_t2 = stubTable(2L, 11L, 20L, 10L, 100_000L);
        // Bucket 1: 3 tables of ~500KB each -> should win by table count (3 > 2)
        TableHandle b1_t1 = stubTable(3L, 21L, 30L, 50L, 500_000L);
        TableHandle b1_t2 = stubTable(4L, 31L, 40L, 50L, 500_000L);
        TableHandle b1_t3 = stubTable(5L, 41L, 50L, 50L, 500_000L);

        Optional<CompactionPlan> planByCount = planner.plan(List.of(b0_t1, b0_t2, b1_t1, b1_t2, b1_t3));
        assertThat(planByCount).isPresent();
        assertThat(planByCount.get().selectedTables()).containsExactly(b1_t1, b1_t2, b1_t3);

        // Tie in table count (2 vs 2): prefer smaller total bytes (Bucket 1 has 20KB each vs Bucket 0 has 100KB each)
        TableHandle s1 = stubTable(6L, 51L, 60L, 5L, 20_000L);
        TableHandle s2 = stubTable(7L, 61L, 70L, 5L, 20_000L);
        Optional<CompactionPlan> planByBytes = planner.plan(List.of(b0_t1, b0_t2, s1, s2));
        assertThat(planByBytes).isPresent();
        assertThat(planByBytes.get().selectedTables()).containsExactly(s1, s2);

        // Chronological sorting when input list is unordered
        Optional<CompactionPlan> unorderedPlan = planner.plan(List.of(s2, b0_t2, s1, b0_t1));
        assertThat(unorderedPlan).isPresent();
        assertThat(unorderedPlan.get().selectedTables()).containsExactly(s1, s2);
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

    private static TableHandle stubTable(long id, long minSeq, long maxSeq, long recordCount, long fileSize) {
        return new TableHandle() {
            @Override public long id() { return id; }
            @Override public long minSequence() { return minSeq; }
            @Override public long maxSequence() { return maxSeq; }
            @Override public long recordCount() { return recordCount; }
            @Override public long fileSize() { return fileSize; }
            @Override public void pin() {}
            @Override public void unpin() {}
            @Override public boolean isClosed() { return false; }
            @Override public Optional<InternalRecord> get(ByteKey key) { return Optional.empty(); }
            @Override public RecordCursor scan(ByteKey fromKey, ByteKey toKey) { throw new UnsupportedOperationException(); }
            @Override public void close() {}
        };
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
