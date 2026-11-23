package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import io.jlsm.api.WriteBatch;
import io.jlsm.internal.io.FaultInjectingStorage;
import io.jlsm.internal.io.StandardStorageIO;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import io.jlsm.wal.DefaultWalWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultStorageEngineTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Basic put, get, overwrite, and delete operations")
    void testBasicCrud(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults();
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            assertThat(db.get(bytes("k1"))).isNull();

            db.put(bytes("k1"), bytes("v1"));
            assertThat(asString(db.get(bytes("k1")))).isEqualTo("v1");

            db.put(bytes("k2"), bytes("v2"));
            assertThat(asString(db.get(bytes("k2")))).isEqualTo("v2");

            db.put(bytes("k1"), bytes("v1_updated"));
            assertThat(asString(db.get(bytes("k1")))).isEqualTo("v1_updated");

            db.delete(bytes("k1"));
            assertThat(db.get(bytes("k1"))).isNull();
            assertThat(asString(db.get(bytes("k2")))).isEqualTo("v2");

            db.delete(bytes("non_existent"));
            assertThat(db.get(bytes("non_existent"))).isNull();
        }
    }

    @Test
    @DisplayName("Validation for null keys/values and size boundaries")
    void testValidation(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            assertThatThrownBy(() -> db.put(null, bytes("v")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.put(bytes("k"), null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.get(null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.delete(null))
                    .isInstanceOf(IllegalArgumentException.class);

            byte[] maxKey = new byte[100];
            byte[] maxValue = new byte[DefaultWalWriter.MAX_KEY_VALUE_TOTAL_BYTES - 100];
            db.put(maxKey, maxValue);
            assertThat(db.get(maxKey)).isEqualTo(maxValue);

            byte[] oversizedValue = new byte[DefaultWalWriter.MAX_KEY_VALUE_TOTAL_BYTES - 100 + 1];
            assertThatThrownBy(() -> db.put(maxKey, oversizedValue))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("exceeds max limit");
        }
    }

    @Test
    @DisplayName("Unbounded and bounded scan operations")
    void testScan(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("a"), bytes("1"));
            db.put(bytes("b"), bytes("2"));
            db.put(bytes("c"), bytes("3"));
            db.put(bytes("d"), bytes("4"));
            db.put(bytes("e"), bytes("5"));
            db.delete(bytes("c"));

            List<String> keys = new ArrayList<>();
            List<String> values = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(null, null)) {
                while (it.hasNext()) {
                    Entry entry = it.next();
                    keys.add(asString(entry.key()));
                    values.add(asString(entry.value()));
                }
                assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
            }

            assertThat(keys).containsExactly("a", "b", "d", "e");
            assertThat(values).containsExactly("1", "2", "4", "5");

            List<String> boundedKeys = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(bytes("b"), bytes("e"))) {
                while (it.hasNext()) {
                    boundedKeys.add(asString(it.next().key()));
                }
            }
            assertThat(boundedKeys).containsExactly("b", "d");

            try (ClosableIterator<Entry> it = db.scan(bytes("z"), bytes("a"))) {
                assertThat(it.hasNext()).isFalse();
                assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
            }
        }
    }

    @Test
    @DisplayName("Enforces concurrent open scans limit")
    void testScanLimitExceeded(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                defaults.memtableBytes(),
                defaults.compactionTrigger(),
                2,
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            db.put(bytes("k"), bytes("v"));

            ClosableIterator<Entry> scan1 = db.scan(null, null);
            ClosableIterator<Entry> scan2 = db.scan(null, null);

            assertThatThrownBy(() -> db.scan(null, null))
                    .isInstanceOf(StorageException.class)
                    .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.LIMIT_EXCEEDED));

            scan1.close();
            ClosableIterator<Entry> scan3 = db.scan(null, null);
            assertThat(scan3).isNotNull();

            scan2.close();
            scan3.close();
        }
    }

    @Test
    @DisplayName("Data persists across close and reopen")
    void testPersistenceAcrossReopen(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults();

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            db.put(bytes("persist_k1"), bytes("v1"));
            db.put(bytes("persist_k2"), bytes("v2"));
            db.put(bytes("persist_k3"), bytes("v3"));
            db.delete(bytes("persist_k2"));
            db.sync();
        }

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            assertThat(asString(db.get(bytes("persist_k1")))).isEqualTo("v1");
            assertThat(db.get(bytes("persist_k2"))).isNull();
            assertThat(asString(db.get(bytes("persist_k3")))).isEqualTo("v3");

            db.put(bytes("persist_k4"), bytes("v4"));
            db.sync();
        }

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            assertThat(asString(db.get(bytes("persist_k1")))).isEqualTo("v1");
            assertThat(db.get(bytes("persist_k2"))).isNull();
            assertThat(asString(db.get(bytes("persist_k3")))).isEqualTo("v3");
            assertThat(asString(db.get(bytes("persist_k4")))).isEqualTo("v4");
        }
    }

    @Test
    @DisplayName("Operations after database close throw CLOSED error code")
    void testOperationsAfterClose(@TempDir Path tempDir) throws Exception {
        KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults());
        db.put(bytes("k"), bytes("v"));
        db.close();

        // Idempotent close
        db.close();

        assertThatThrownBy(() -> db.put(bytes("k"), bytes("v2")))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));

        assertThatThrownBy(() -> db.get(bytes("k")))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));

        assertThatThrownBy(() -> db.delete(bytes("k")))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));

        assertThatThrownBy(() -> db.scan(null, null))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));

        assertThatThrownBy(db::sync)
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));
    }

    @Test
    @DisplayName("Concurrent database open on same directory is locked")
    void testLockExclusion(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults();
        try (KVDatabase db1 = KVDatabase.open(tempDir, options)) {
            assertThatThrownBy(() -> KVDatabase.open(tempDir, options))
                    .isInstanceOf(StorageException.class)
                    .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.LOCKED));
        }

        try (KVDatabase db2 = KVDatabase.open(tempDir, options)) {
            assertThat(db2).isNotNull();
        }
    }

    @Test
    @DisplayName("Automatic MemTable flush when memtableBytes threshold is exceeded")
    void testAutomaticFlushOnThreshold(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        // 512 bytes threshold to trigger flush quickly
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                512L,
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            assertThat(engine.liveTables()).isEmpty();

            // Insert records until threshold is crossed
            for (int i = 0; i < 20; i++) {
                db.put(bytes("key_" + i), bytes("value_payload_for_testing_" + i));
            }

            // Flush should have produced at least 1 live SSTable
            assertThat(engine.liveTables()).isNotEmpty();
            assertThat(engine.immutableMemTable()).isNull();

            // Verify all data is readable through get() from SSTable
            for (int i = 0; i < 20; i++) {
                assertThat(asString(db.get(bytes("key_" + i)))).isEqualTo("value_payload_for_testing_" + i);
            }

            // Flushed WAL 000001.wal should have been deleted from disk
            assertThat(Files.exists(tempDir.resolve("000001.wal"))).isFalse();
        }

        // Verify persistence after reopening
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            for (int i = 0; i < 20; i++) {
                assertThat(asString(db.get(bytes("key_" + i)))).isEqualTo("value_payload_for_testing_" + i);
            }
        }
    }

    @Test
    @DisplayName("Scan merges flushed SSTable data with active MemTable updates")
    void testScanAcrossFlushedSSTableAndMemTable(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                512L,
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            // First batch triggers flush into SSTable
            db.put(bytes("k1"), bytes("v1_old"));
            db.put(bytes("k2"), bytes("v2"));
            for (int i = 0; i < 15; i++) {
                db.put(bytes("filler_" + i), bytes("filler_val_" + i));
            }

            // Update in new active MemTable
            db.put(bytes("k1"), bytes("v1_new"));
            db.delete(bytes("k2"));
            db.put(bytes("k3"), bytes("v3"));

            assertThat(asString(db.get(bytes("k1")))).isEqualTo("v1_new");
            assertThat(db.get(bytes("k2"))).isNull();
            assertThat(asString(db.get(bytes("k3")))).isEqualTo("v3");

            // Range scan should properly merge and prioritize MemTable over SSTable
            List<String> scannedKeys = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(bytes("k1"), bytes("k4"))) {
                while (it.hasNext()) {
                    Entry entry = it.next();
                    scannedKeys.add(asString(entry.key()) + "=" + asString(entry.value()));
                }
            }

            assertThat(scannedKeys).containsExactly("k1=v1_new", "k3=v3");
        }
    }

    @Test
    @DisplayName("Automatic compaction triggers when live tables reach compactionTrigger")
    void testAutomaticCompactionOnTrigger(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                1024L,
                4,
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;

            // Trigger exactly 4 flushes into 4 live SSTables
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 25; i++) {
                    db.put(bytes("b" + batch + "_k" + i), bytes("v" + batch + "_" + i));
                }
                engine.rotateMemTable();
                engine.flushImmutableMemTable();
            }

            // Wait for background worker to complete compaction
            long deadline = System.currentTimeMillis() + 5000;
            while (engine.liveTables().size() > 1 && System.currentTimeMillis() < deadline) {
                engine.awaitBackgroundTasks();
                Thread.sleep(50);
            }

            assertThat(engine.liveTables()).hasSize(1);

            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 25; i++) {
                    assertThat(asString(db.get(bytes("b" + batch + "_k" + i))))
                            .isEqualTo("v" + batch + "_" + i);
                }
            }

            assertThat(Files.exists(tempDir.resolve("000001.sst"))).isFalse();
            assertThat(Files.exists(tempDir.resolve("000002.sst"))).isFalse();
            assertThat(Files.exists(tempDir.resolve("000003.sst"))).isFalse();
            assertThat(Files.exists(tempDir.resolve("000004.sst"))).isFalse();
        }

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            assertThat(engine.liveTables()).hasSize(1);
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 25; i++) {
                    assertThat(asString(db.get(bytes("b" + batch + "_k" + i))))
                            .isEqualTo("v" + batch + "_" + i);
                }
            }
        }
    }

    @Test
    @DisplayName("Compaction purges tombstones when consolidating all live tables")
    void testCompactionPurgesTombstones(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                256L,
                2,
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;

            db.put(bytes("k1"), bytes("v1"));
            db.put(bytes("k2"), bytes("v2"));
            db.put(bytes("k3"), bytes("v3"));
            for (int i = 0; i < 10; i++) {
                db.put(bytes("filler1_" + i), bytes("f1_" + i));
            }

            db.delete(bytes("k1"));
            db.put(bytes("k2"), bytes("v2_new"));
            db.delete(bytes("k3"));
            for (int i = 0; i < 10; i++) {
                db.put(bytes("filler2_" + i), bytes("f2_" + i));
            }

            long deadline = System.currentTimeMillis() + 5000;
            while (engine.liveTables().size() > 1 && System.currentTimeMillis() < deadline) {
                engine.awaitBackgroundTasks();
                Thread.sleep(50);
            }

            assertThat(engine.liveTables()).hasSize(1);
            assertThat(db.get(bytes("k1"))).isNull();
            assertThat(asString(db.get(bytes("k2")))).isEqualTo("v2_new");
            assertThat(db.get(bytes("k3"))).isNull();

            List<String> keys = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(bytes("k1"), bytes("k4"))) {
                while (it.hasNext()) {
                    keys.add(asString(it.next().key()));
                }
            }
            assertThat(keys).containsExactly("k2");
        }
    }

    @Test
    @DisplayName("Active scan prevents deletion of obsolete SSTables until closed")
    void testActiveScanPinsSSTablesDuringCompaction(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                256L,
                10,
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;

            for (int i = 0; i < 10; i++) {
                db.put(bytes("a_" + i), bytes("val_a_" + i));
            }
            for (int i = 0; i < 10; i++) {
                db.put(bytes("b_" + i), bytes("val_b_" + i));
            }

            engine.rotateMemTable();
            engine.flushImmutableMemTable();

            assertThat(engine.liveTables().size()).isGreaterThanOrEqualTo(2);

            ClosableIterator<Entry> activeScan = db.scan(null, null);

            engine.triggerCompactionSync();

            assertThat(engine.liveTables()).hasSize(1);
            assertThat(engine.obsoleteTablePaths()).isNotEmpty();
            for (Path obsolete : engine.obsoleteTablePaths()) {
                assertThat(Files.exists(obsolete)).isTrue();
            }

            int count = 0;
            while (activeScan.hasNext()) {
                activeScan.next();
                count++;
            }
            assertThat(count).isGreaterThan(0);

            activeScan.close();

            for (Path obsolete : engine.obsoleteTablePaths()) {
                assertThat(Files.exists(obsolete)).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Compaction when all keys are deleted results in 0 tables")
    void testCompactionWhenAllRecordsArePurgedResultsInZeroTables(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                256L,
                2,
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;

            db.put(bytes("only_key"), bytes("only_val"));
            db.delete(bytes("only_key"));

            engine.rotateMemTable();
            engine.flushImmutableMemTable();
            engine.rotateMemTable();
            engine.flushImmutableMemTable();

            engine.triggerCompactionSync();

            assertThat(engine.liveTables()).isEmpty();
            assertThat(db.get(bytes("only_key"))).isNull();
        }

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            assertThat(engine.liveTables()).isEmpty();
            assertThat(db.get(bytes("only_key"))).isNull();
        }
    }

    @Test
    @DisplayName("Repeated overwrites of the same key do not exhaust memory budget")
    void testRepeatedOverwritesDoNotStallBudget(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                4096L, // 4KB budget
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            byte[] key = bytes("fixed_key");
            byte[] value = bytes("fixed_value");
            // 15,000 overwrites would exceed 4KB many times if delta wasn't accounted for
            for (int i = 0; i < 15_000; i++) {
                db.put(key, value);
            }
            assertThat(asString(db.get(key))).isEqualTo("fixed_value");
        }
    }

    @Test
    @DisplayName("WAL files replayed during recovery are deleted when memtable is flushed")
    void testRecoveryWalDeletedAfterSubsequentFlush(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.SYNC,
                1024L * 1024L,
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            db.put(bytes("k1"), bytes("v1"));
            // no flush, leave WAL 1
        }

        Path wal1 = tempDir.resolve("000001.wal");
        assertThat(Files.exists(wal1)).isTrue();

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            assertThat(asString(db.get(bytes("k1")))).isEqualTo("v1");
            db.put(bytes("k2"), bytes("v2"));

            // Flush recovered data + new data
            engine.rotateMemTable();
            engine.flushImmutableMemTable();

            // Replayed WAL 000001.wal should now be deleted from disk
            assertThat(Files.exists(wal1)).isFalse();
        }
    }

    @Test
    @DisplayName("Manifest compaction executes periodically without corrupting metadata")
    void testPeriodicManifestCompaction(@TempDir Path tempDir) throws Exception {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                defaults.durabilityMode(),
                256L,
                2,
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            for (int i = 0; i < 60; i++) {
                db.put(bytes("key" + i), bytes("value" + i));
                engine.rotateMemTable();
                engine.flushImmutableMemTable();
            }
        }

        // Reopen to verify manifest is consistent after compactions
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            for (int i = 0; i < 60; i++) {
                assertThat(asString(db.get(bytes("key" + i)))).isEqualTo("value" + i);
            }
        }
    }

    // ---- Grader hypothesis tests (midterm feedback) ----

    /** Opens the engine the way KVDatabase.open does, but typed, so tests reach the package-private hooks without a cast. */
    private static DefaultStorageEngine openEngine(Path dir, DatabaseOptions options) throws Exception {
        StandardStorageIO storage = new StandardStorageIO();
        RecoveryResult recovered = new RecoveryManager(storage, dir, options).recover();
        return new DefaultStorageEngine(storage, dir, options, recovered);
    }

    @Test
    @DisplayName("Grader hypothesis: a value overwritten and flushed after a reopen stays overwritten after the next reopen")
    void graderOverwriteSurvivesSecondReopen(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults();
        try (DefaultStorageEngine db = openEngine(tempDir, options)) {
            db.put(bytes("k"), bytes("v1"));
        }
        try (DefaultStorageEngine db = openEngine(tempDir, options)) {
            db.put(bytes("k"), bytes("v2"));
            db.rotateMemTable();
            db.flushImmutableMemTable();
            assertThat(asString(db.get(bytes("k")))).isEqualTo("v2");
        }
        try (DefaultStorageEngine db = openEngine(tempDir, options)) {
            assertThat(asString(db.get(bytes("k")))).isEqualTo("v2");
        }
    }

    @Test
    @DisplayName("Grader hypothesis: a key deleted and flushed after a reopen stays deleted after the next reopen")
    void graderDeleteSurvivesSecondReopen(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults();
        try (DefaultStorageEngine db = openEngine(tempDir, options)) {
            db.put(bytes("k"), bytes("v1"));
        }
        try (DefaultStorageEngine db = openEngine(tempDir, options)) {
            db.delete(bytes("k"));
            db.rotateMemTable();
            db.flushImmutableMemTable();
            assertThat(db.get(bytes("k"))).isNull();
        }
        try (DefaultStorageEngine db = openEngine(tempDir, options)) {
            assertThat(db.get(bytes("k"))).isNull();
        }
    }

    @Test
    @DisplayName("Grader hypothesis: overwriting one key 20,000 times does not block put()")
    void graderRepeatedOverwritesDoNotBlock(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults().withDurabilityMode(DurabilityMode.BUFFERED);
        byte[] value = new byte[1000];
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
                for (int i = 0; i < 20_000; i++) {
                    db.put(bytes("counter"), value);
                }
            });
            assertThat(db.get(bytes("counter"))).isEqualTo(value);
        }
    }

    @Test
    @DisplayName("Grader hypothesis: after one failed flush, later puts complete once the disk works again")
    void graderPutsContinueAfterFailedFlush(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = DatabaseOptions.defaults()
                .withDurabilityMode(DurabilityMode.BUFFERED)
                .withMemtableBytes(2048);
        FaultInjectingStorage storage = new FaultInjectingStorage();
        RecoveryResult recovered = new RecoveryManager(storage, tempDir, options).recover();
        try (DefaultStorageEngine db = new DefaultStorageEngine(storage, tempDir, options, recovered)) {
            int i = 0;
            while (db.metrics().activeMemTableBytes() < options.memtableBytes()) {
                db.put(bytes("a" + (i++)), bytes("x".repeat(50)));
            }
            // The rotation (new WAL header + one MANIFEST frame) fits in 300 bytes; the SSTable does not.
            storage.failAfterBytesWritten(300);
            assertThatThrownBy(() -> db.put(bytes("trigger"), bytes("t")))
                    .isInstanceOf(StorageException.class);
            storage.reset();

            assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
                for (int k = 0; k < 200; k++) {
                    db.put(bytes("b" + k), bytes("y".repeat(50)));
                }
            });
            assertThat(asString(db.get(bytes("a0")))).isEqualTo("x".repeat(50));
            assertThat(asString(db.get(bytes("b199")))).isEqualTo("y".repeat(50));
        }
    }

    @Test
    @DisplayName("Grader hypothesis: an open scan does not see a key written after the scan was opened")
    void graderScanIsIsolatedFromLaterWrites(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            for (String k : List.of("a", "b", "c", "d", "e")) {
                db.put(bytes(k), bytes("1"));
            }
            List<String> keys = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(null, null)) {
                keys.add(asString(it.next().key()));
                db.put(bytes("z"), bytes("2"));
                while (it.hasNext()) {
                    keys.add(asString(it.next().key()));
                }
            }
            assertThat(keys).containsExactly("a", "b", "c", "d", "e");
        }
    }

    @Test
    @DisplayName("WriteBatch executes puts and deletes atomically in the engine")
    void testWriteBatchAtomicExecution(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k1"), bytes("v1_orig"));
            db.put(bytes("k2"), bytes("v2_orig"));
            db.put(bytes("k3"), bytes("v3_orig"));

            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("k1"), bytes("v1_batch"));
                batch.delete(bytes("k2"));
                batch.put(bytes("k4"), bytes("v4_new"));
                db.write(batch);
            }

            assertThat(asString(db.get(bytes("k1")))).isEqualTo("v1_batch");
            assertThat(db.get(bytes("k2"))).isNull();
            assertThat(asString(db.get(bytes("k3")))).isEqualTo("v3_orig");
            assertThat(asString(db.get(bytes("k4")))).isEqualTo("v4_new");
        }
    }

    @Test
    @DisplayName("WriteBatch updates are immediately visible in scan")
    void testWriteBatchScanVisibility(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("user:1"), bytes("Alice"));
                batch.put(bytes("user:2"), bytes("Bob"));
                batch.put(bytes("user:3"), bytes("Charlie"));
                db.write(batch);
            }

            List<String> values = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(bytes("user:1"), bytes("user:4"))) {
                while (it.hasNext()) {
                    values.add(asString(it.next().value()));
                }
            }

            assertThat(values).containsExactly("Alice", "Bob", "Charlie");
        }
    }

    @Test
    @DisplayName("WriteBatch validates input parameters: null batch, empty batch, and closed database")
    void testWriteBatchValidations(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            assertThatThrownBy(() -> db.write(null))
                    .isInstanceOf(IllegalArgumentException.class);

            try (WriteBatch empty = WriteBatch.create()) {
                assertThatThrownBy(() -> db.write(empty))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }

        KVDatabase closedDb = KVDatabase.open(tempDir, DatabaseOptions.defaults());
        closedDb.close();

        try (WriteBatch batch = WriteBatch.create()) {
            batch.put(bytes("k"), bytes("v"));
            assertThatThrownBy(() -> closedDb.write(batch))
                    .isInstanceOf(StorageException.class)
                    .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));
        }
    }

    @Test
    @DisplayName("WriteBatch updates storage metrics for put and delete counts")
    void testWriteBatchMetrics(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            long putsBefore = db.metrics().putsCount();
            long delsBefore = db.metrics().deletesCount();

            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("k1"), bytes("v1"));
                batch.put(bytes("k2"), bytes("v2"));
                batch.delete(bytes("k3"));
                db.write(batch);
            }

            assertThat(db.metrics().putsCount()).isEqualTo(putsBefore + 2);
            assertThat(db.metrics().deletesCount()).isEqualTo(delsBefore + 1);
        }
    }

    @Test
    @DisplayName("Batch written data survives database restart and WAL replay")
    void testWriteBatchSurvivesRestart(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("alpha"), bytes("valAlpha"));
                batch.put(bytes("beta"), bytes("valBeta"));
                batch.delete(bytes("gamma"));
                db.write(batch);
            }
        }

        try (KVDatabase reopened = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            assertThat(asString(reopened.get(bytes("alpha")))).isEqualTo("valAlpha");
            assertThat(asString(reopened.get(bytes("beta")))).isEqualTo("valBeta");
            assertThat(reopened.get(bytes("gamma"))).isNull();
        }
    }
}
