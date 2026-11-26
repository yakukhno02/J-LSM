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

class LeveledCompactionPlannerTest {

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
    @DisplayName("Default constructor sets correct default parameters")
    void testDefaultConstructorAndGetters() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner();
        assertThat(planner.maxL0Tables()).isEqualTo(LeveledCompactionPlanner.DEFAULT_MAX_L0_TABLES);
        assertThat(planner.baseLevelSizeBytes()).isEqualTo(LeveledCompactionPlanner.DEFAULT_BASE_LEVEL_SIZE_BYTES);
        assertThat(planner.levelMultiplier()).isEqualTo(LeveledCompactionPlanner.DEFAULT_LEVEL_MULTIPLIER);
        assertThat(planner.maxLevels()).isEqualTo(LeveledCompactionPlanner.DEFAULT_MAX_LEVELS);
        assertThat(planner.targetFileSizeBytes()).isEqualTo(Compactor.DEFAULT_TARGET_FILE_SIZE);
    }

    @Test
    @DisplayName("Constructor parameter validation rejects invalid arguments")
    void testConstructorValidation() {
        assertThatThrownBy(() -> new LeveledCompactionPlanner(1, 1024L, 10, 7, 4096L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxL0Tables must be >= 2");

        assertThatThrownBy(() -> new LeveledCompactionPlanner(4, 0L, 10, 7, 4096L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("baseLevelSizeBytes must be positive");

        assertThatThrownBy(() -> new LeveledCompactionPlanner(4, 1024L, 1, 7, 4096L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("levelMultiplier must be >= 2");

        assertThatThrownBy(() -> new LeveledCompactionPlanner(4, 1024L, 10, 1, 4096L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxLevels must be >= 2");

        assertThatThrownBy(() -> new LeveledCompactionPlanner(4, 1024L, 10, 7, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetFileSizeBytes must be positive");
    }

    @Test
    @DisplayName("Level capacity computes exponential growth and guards against arithmetic overflow")
    void testLevelCapacity() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(4, 1000L, 10, 7, 500L);
        assertThat(planner.levelCapacityBytes(0)).isEqualTo(4 * 500L);
        assertThat(planner.levelCapacityBytes(1)).isEqualTo(1000L);
        assertThat(planner.levelCapacityBytes(2)).isEqualTo(10_000L);
        assertThat(planner.levelCapacityBytes(3)).isEqualTo(100_000L);
        assertThat(planner.levelCapacityBytes(4)).isEqualTo(1_000_000L);

        assertThatThrownBy(() -> planner.levelCapacityBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.levelCapacityBytes(7))
                .isInstanceOf(IllegalArgumentException.class);

        // Planner with large multiplier near overflow
        LeveledCompactionPlanner overflowPlanner = new LeveledCompactionPlanner(2, Long.MAX_VALUE / 2, 10, 5, 100L);
        assertThat(overflowPlanner.levelCapacityBytes(2)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("Assign level and levelOf correctly track table levels")
    void testAssignLevelAndLevelOf() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner();
        TableHandle t1 = stubTable(101L, 1L, 5L, 5L, 1000L, "a", "c");

        // Defaults to level 0
        assertThat(planner.levelOf(t1)).isEqualTo(0);

        planner.assignLevel(101L, 2);
        assertThat(planner.levelOf(t1)).isEqualTo(2);

        assertThatThrownBy(() -> planner.assignLevel(-1L, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.assignLevel(101L, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.assignLevel(101L, planner.maxLevels()))
                .isInstanceOf(IllegalArgumentException.class);

        // Custom resolver overrides internal map
        LeveledCompactionPlanner customPlanner = new LeveledCompactionPlanner(
                4, 1024L, 10, 7, 4096L, handle -> handle.id() == 101L ? 3 : null);
        assertThat(customPlanner.levelOf(t1)).isEqualTo(3);
    }

    @Test
    @DisplayName("Null and empty inputs are rejected or return Optional.empty")
    void testNullAndEmptyInputs() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner();

        assertThatThrownBy(() -> planner.plan(null)).isInstanceOf(NullPointerException.class);
        List<TableHandle> nullList = new ArrayList<>();
        nullList.add(null);
        assertThatThrownBy(() -> planner.plan(nullList)).isInstanceOf(NullPointerException.class);
        assertThat(planner.plan(List.of())).isEmpty();

        assertThatThrownBy(() -> planner.planFullCompaction(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> planner.planFullCompaction(nullList)).isInstanceOf(NullPointerException.class);
        assertThat(planner.planFullCompaction(List.of())).isEmpty();
    }

    @Test
    @DisplayName("L0 triggers when table count >= maxL0Tables, selecting L0 tables and overlapping L1 tables")
    void testL0TriggerWithKeyOverlap() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(4, 100_000L);

        // L0 tables (span key range "b" to "m")
        TableHandle l0_1 = stubTable(1L, 101L, 110L, 5L, 4000L, "b", "d");
        TableHandle l0_2 = stubTable(2L, 111L, 120L, 5L, 4000L, "c", "h");
        TableHandle l0_3 = stubTable(3L, 121L, 130L, 5L, 4000L, "f", "k");
        TableHandle l0_4 = stubTable(4L, 131L, 140L, 5L, 4000L, "i", "m");

        // L1 tables:
        // nonOverlapLeft: ["a", "a9"] -> does not overlap ["b", "m"]
        TableHandle l1_left = stubTable(10L, 1L, 10L, 5L, 4000L, "a", "a9");
        planner.assignLevel(10L, 1);

        // overlap1: ["c", "e"] -> overlaps ["b", "m"]
        TableHandle l1_mid1 = stubTable(11L, 11L, 20L, 5L, 4000L, "c", "e");
        planner.assignLevel(11L, 1);

        // overlap2: ["j", "n"] -> overlaps ["b", "m"]
        TableHandle l1_mid2 = stubTable(12L, 21L, 30L, 5L, 4000L, "j", "n");
        planner.assignLevel(12L, 1);

        // nonOverlapRight: ["p", "z"] -> does not overlap ["b", "m"]
        TableHandle l1_right = stubTable(13L, 31L, 40L, 5L, 4000L, "p", "z");
        planner.assignLevel(13L, 1);

        // With only 3 L0 tables, should NOT trigger
        assertThat(planner.plan(List.of(l0_1, l0_2, l0_3, l1_left, l1_mid1, l1_mid2, l1_right))).isEmpty();

        // With 4 L0 tables, triggers L0 -> L1 compaction
        List<TableHandle> allTables = List.of(l0_1, l0_2, l0_3, l0_4, l1_left, l1_mid1, l1_mid2, l1_right);
        Optional<CompactionPlan> planOpt = planner.plan(allTables);
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.tierIndex()).isEqualTo(0);
        // Must contain all 4 L0 tables + only overlapping L1 tables (l1_mid1, l1_mid2)
        assertThat(plan.selectedTables()).containsExactly(l1_mid1, l1_mid2, l0_1, l0_2, l0_3, l0_4);
        assertThat(plan.selectedTables()).doesNotContain(l1_left, l1_right);

        // Oldest table in DB is l1_left (seq 1..10), which is NOT in compaction -> cannot purge tombstones!
        assertThat(plan.includesBaseGeneration()).isFalse();
        assertThat(plan.retainTombstones()).isTrue();
    }

    @Test
    @DisplayName("Tombstone safety: base generation includesBaseGeneration=true allows purging tombstones")
    void testTombstoneSafetyBaseGeneration() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(2, 100_000L);

        // L0 tables spanning all keys
        TableHandle l0_1 = stubTable(1L, 101L, 110L, 5L, 4000L, "a", "m");
        TableHandle l0_2 = stubTable(2L, 111L, 120L, 5L, 4000L, "n", "z");

        // L1 tables: both overlap
        TableHandle l1_1 = stubTable(3L, 1L, 50L, 5L, 4000L, "a", "m");
        TableHandle l1_2 = stubTable(4L, 51L, 100L, 5L, 4000L, "n", "z");
        planner.assignLevel(3L, 1);
        planner.assignLevel(4L, 1);

        // In this scenario, all tables in the entire DB are included in the compaction
        Optional<CompactionPlan> planOpt = planner.plan(List.of(l0_1, l0_2, l1_1, l1_2));
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.includesBaseGeneration()).isTrue();
        assertThat(plan.retainTombstones()).isFalse();
    }

    @Test
    @DisplayName("Tombstone safety: deeper level L2 with older data prevents tombstone purging in L0->L1")
    void testTombstoneSafetyDeeperLevelPreventsPurge() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(2, 100_000L);

        TableHandle l0_1 = stubTable(1L, 201L, 210L, 5L, 4000L, "a", "m");
        TableHandle l0_2 = stubTable(2L, 211L, 220L, 5L, 4000L, "n", "z");

        TableHandle l1_1 = stubTable(3L, 101L, 150L, 5L, 4000L, "a", "z");
        planner.assignLevel(3L, 1);

        // L2 contains the oldest data in the database
        TableHandle l2_oldest = stubTable(4L, 1L, 100L, 10L, 50_000L, "a", "z");
        planner.assignLevel(4L, 2);

        Optional<CompactionPlan> planOpt = planner.plan(List.of(l0_1, l0_2, l1_1, l2_oldest));
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.selectedTables()).containsExactly(l1_1, l0_1, l0_2);
        assertThat(plan.selectedTables()).doesNotContain(l2_oldest);
        // Crucial LSM invariant: L2 has older data not in compaction -> retain tombstones!
        assertThat(plan.includesBaseGeneration()).isFalse();
        assertThat(plan.retainTombstones()).isTrue();
    }

    @Test
    @DisplayName("Higher level compaction triggers when level byte size exceeds capacity")
    void testHigherLevelCompactionTrigger() {
        // L1 capacity = 10_000 bytes; L0 trigger = 4
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(4, 10_000L);

        // L0 has only 1 table (does not trigger L0)
        TableHandle l0 = stubTable(1L, 1000L, 1010L, 5L, 2000L, "a", "z");

        // L1 has 3 tables totaling 15_000 bytes (> 10_000 capacity)
        TableHandle l1_a = stubTable(2L, 101L, 200L, 10L, 4000L, "a", "h");
        TableHandle l1_b = stubTable(3L, 201L, 300L, 10L, 7000L, "i", "p"); // largest in L1
        TableHandle l1_c = stubTable(4L, 301L, 400L, 10L, 4000L, "q", "z");
        planner.assignLevel(2L, 1);
        planner.assignLevel(3L, 1);
        planner.assignLevel(4L, 1);

        // L2 has tables:
        TableHandle l2_left = stubTable(5L, 1L, 50L, 10L, 5000L, "a", "g");
        TableHandle l2_overlap = stubTable(6L, 51L, 99L, 10L, 5000L, "j", "m"); // overlaps l1_b ["i", "p"]
        TableHandle l2_right = stubTable(7L, 100L, 100L, 10L, 5000L, "s", "z");
        planner.assignLevel(5L, 2);
        planner.assignLevel(6L, 2);
        planner.assignLevel(7L, 2);

        List<TableHandle> allTables = List.of(l0, l1_a, l1_b, l1_c, l2_left, l2_overlap, l2_right);
        Optional<CompactionPlan> planOpt = planner.plan(allTables);
        assertThat(planOpt).isPresent();

        CompactionPlan plan = planOpt.get();
        assertThat(plan.tierIndex()).isEqualTo(1); // L1 -> L2
        assertThat(plan.selectedTables()).contains(l1_b, l2_overlap);
        assertThat(plan.selectedTables()).doesNotContain(l2_left, l2_right);
    }

    @Test
    @DisplayName("recordCompaction removes inputs and updates output tables to next level")
    void testRecordCompaction() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(2, 1000L);

        TableHandle in1 = stubTable(1L, 1L, 10L, 5L, 100L, "a", "k");
        TableHandle in2 = stubTable(2L, 11L, 20L, 5L, 100L, "l", "z");
        planner.assignLevel(1L, 0);
        planner.assignLevel(2L, 0);

        CompactionPlan plan = new CompactionPlan(List.of(in1, in2), 4096L, false, true, 0);

        TableHandle out1 = stubTable(10L, 1L, 15L, 5L, 150L, "a", "m");
        TableHandle out2 = stubTable(11L, 16L, 20L, 5L, 150L, "n", "z");

        planner.recordCompaction(plan, List.of(out1, out2));

        // Old tables are removed
        assertThat(planner.levelOf(in1)).isEqualTo(0); // defaults back to 0 if re-queried
        // New tables advanced to level 1 (tierIndex 0 + 1)
        assertThat(planner.levelOf(out1)).isEqualTo(1);
        assertThat(planner.levelOf(out2)).isEqualTo(1);
    }

    @Test
    @DisplayName("planFullCompaction includes all live tables and purges tombstones")
    void testPlanFullCompaction() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner();
        TableHandle t1 = stubTable(1L, 1L, 10L, 5L, 100L, "a", "k");
        TableHandle t2 = stubTable(2L, 11L, 20L, 5L, 100L, "l", "z");

        Optional<CompactionPlan> fullPlanOpt = planner.planFullCompaction(List.of(t2, t1));
        assertThat(fullPlanOpt).isPresent();

        CompactionPlan fullPlan = fullPlanOpt.get();
        assertThat(fullPlan.selectedTables()).containsExactly(t1, t2); // sorted chronologically
        assertThat(fullPlan.includesBaseGeneration()).isTrue();
        assertThat(fullPlan.retainTombstones()).isFalse();
        assertThat(fullPlan.tierIndex()).isEqualTo(0);
    }

    @Test
    @DisplayName("currentLevelSizeBytes and levelCompactionScore accurately compute metrics")
    void testMetricsCalculation() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(4, 5000L);
        TableHandle t1 = stubTable(1L, 1L, 10L, 5L, 1500L, "a", "b");
        TableHandle t2 = stubTable(2L, 11L, 20L, 5L, 2500L, "c", "d");
        planner.assignLevel(1L, 1);
        planner.assignLevel(2L, 1);

        long sizeL1 = planner.currentLevelSizeBytes(1, List.of(t1, t2));
        assertThat(sizeL1).isEqualTo(4000L);

        double scoreL1 = planner.levelCompactionScore(1, List.of(t1, t2));
        assertThat(scoreL1).isEqualTo(4000.0 / 5000.0);
    }

    @Test
    @DisplayName("Fallback for handles without explicit key ranges treats them conservatively as overlapping")
    void testHandlesWithoutKeyBounds() {
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(2, 1000L);

        TableHandle t1 = stubTable(1L, 1L, 10L, 5L, 100L, null, null);
        TableHandle t2 = stubTable(2L, 11L, 20L, 5L, 100L, null, null);

        Optional<CompactionPlan> planOpt = planner.plan(List.of(t1, t2));
        assertThat(planOpt).isPresent();
        assertThat(planOpt.get().selectedTables()).containsExactly(t1, t2);
    }

    @Test
    @DisplayName("End-to-end integration: LeveledCompactionPlanner + DefaultCompactor produces compacted tables")
    void testEndToEndCompactionWithDefaultCompactor() throws Exception {
        // Build 2 tables using real flusher on disk
        TableMeta meta1 = flusher.build(1L, new ListCursor(List.of(
                put("user1", "alice", 1L),
                put("user2", "bob", 2L)
        )));
        TableMeta meta2 = flusher.build(2L, new ListCursor(List.of(
                put("user1", "alice_updated", 3L),
                del("user2", 4L)
        )));

        Path f1 = tempDir.resolve("000001.sst");
        Path f2 = tempDir.resolve("000002.sst");

        try (TableHandle h1 = DefaultTableHandle.open(storageIO, f1, meta1);
             TableHandle h2 = DefaultTableHandle.open(storageIO, f2, meta2)) {

            assertThat(h1.firstKey()).isPresent();
            assertThat(h1.lastKey()).isPresent();
            assertThat(h1.firstKey().get()).isEqualTo(ByteKey.copyOf("user1".getBytes(StandardCharsets.UTF_8)));
            assertThat(h1.lastKey().get()).isEqualTo(ByteKey.copyOf("user2".getBytes(StandardCharsets.UTF_8)));

            LeveledCompactionPlanner planner = new LeveledCompactionPlanner(2, 100_000L);
            Optional<CompactionPlan> planOpt = planner.plan(List.of(h1, h2));
            assertThat(planOpt).isPresent();

            CompactionPlan plan = planOpt.get();
            assertThat(plan.includesBaseGeneration()).isTrue();
            assertThat(plan.retainTombstones()).isFalse();

            List<TableMeta> splitResults = compactor.buildSplit(
                    new java.util.concurrent.atomic.AtomicLong(10L)::incrementAndGet,
                    plan.selectedTables(),
                    plan.retainTombstones(),
                    plan.targetFileSizeBytes()
            );

            assertThat(splitResults).hasSize(1);
            TableMeta compactedMeta = splitResults.getFirst();
            assertThat(compactedMeta.recordCount()).isEqualTo(1L); // user2 tombstone purged, user1 deduplicated

            Path compactedFile = tempDir.resolve(String.format("%06d.sst", compactedMeta.tableId()));
            try (TableHandle compactedHandle = DefaultTableHandle.open(storageIO, compactedFile, compactedMeta)) {
                assertThat(compactedHandle.firstKey()).isPresent();
                assertThat(compactedHandle.firstKey().get()).isEqualTo(ByteKey.copyOf("user1".getBytes(StandardCharsets.UTF_8)));
                assertThat(compactedHandle.get(ByteKey.copyOf("user1".getBytes(StandardCharsets.UTF_8)))).isPresent();
                assertThat(compactedHandle.get(ByteKey.copyOf("user2".getBytes(StandardCharsets.UTF_8)))).isEmpty();
            }
        }
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

    private static TableHandle stubTable(
            long id,
            long minSeq,
            long maxSeq,
            long recordCount,
            long fileSize,
            String firstKey,
            String lastKey
    ) {
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
            @Override public Optional<ByteKey> firstKey() {
                return firstKey == null ? Optional.empty() : Optional.of(ByteKey.copyOf(firstKey.getBytes(StandardCharsets.UTF_8)));
            }
            @Override public Optional<ByteKey> lastKey() {
                return lastKey == null ? Optional.empty() : Optional.of(ByteKey.copyOf(lastKey.getBytes(StandardCharsets.UTF_8)));
            }
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
