package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.Entry;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
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
}
