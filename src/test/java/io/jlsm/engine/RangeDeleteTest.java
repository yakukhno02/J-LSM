package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.Snapshot;
import io.jlsm.api.StorageException;
import io.jlsm.api.WriteBatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeDeleteTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Range delete in MemTable masks keys in [fromKey, toKey) while preserving toKey")
    void testBasicMemTableRangeDelete(@TempDir Path tempDir) throws StorageException {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("key1"), bytes("val1"));
            db.put(bytes("key2"), bytes("val2"));
            db.put(bytes("key3"), bytes("val3"));
            db.put(bytes("key4"), bytes("val4"));

            // Delete range [key2, key4)
            db.deleteRange(bytes("key2"), bytes("key4"));

            assertThat(asString(db.get(bytes("key1")))).isEqualTo("val1");
            assertThat(db.get(bytes("key2"))).isNull();
            assertThat(db.get(bytes("key3"))).isNull();
            assertThat(asString(db.get(bytes("key4")))).isEqualTo("val4");
        }
    }

    @Test
    @DisplayName("Point mutation written after range delete has higher sequence and is visible")
    void testPointMutationAfterRangeDelete(@TempDir Path tempDir) throws StorageException {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k1"), bytes("v1"));
            db.put(bytes("k2"), bytes("v2"));

            db.deleteRange(bytes("k0"), bytes("k9"));

            assertThat(db.get(bytes("k1"))).isNull();
            assertThat(db.get(bytes("k2"))).isNull();

            // Re-insert k2 after range delete
            db.put(bytes("k2"), bytes("v2_new"));

            assertThat(db.get(bytes("k1"))).isNull();
            assertThat(asString(db.get(bytes("k2")))).isEqualTo("v2_new");
        }
    }

    @Test
    @DisplayName("Range delete in WriteBatch applies atomically alongside puts and deletes")
    void testRangeDeleteInWriteBatch(@TempDir Path tempDir) throws StorageException {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("a"), bytes("valA"));
            db.put(bytes("b"), bytes("valB"));
            db.put(bytes("c"), bytes("valC"));

            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("d"), bytes("valD"));
                batch.deleteRange(bytes("a"), bytes("c")); // deletes 'a' and 'b'
                batch.put(bytes("b"), bytes("valB_resurrected"));
                db.write(batch);
            }

            assertThat(db.get(bytes("a"))).isNull();
            assertThat(asString(db.get(bytes("b")))).isEqualTo("valB_resurrected");
            assertThat(asString(db.get(bytes("c")))).isEqualTo("valC");
            assertThat(asString(db.get(bytes("d")))).isEqualTo("valD");
        }
    }

    @Test
    @DisplayName("Active MemTable range tombstone masks keys residing in on-disk SSTables")
    void testRangeDeleteMaskingDiskSSTables(@TempDir Path tempDir) throws Exception {
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
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            for (int i = 0; i < 20; i++) {
                db.put(bytes(String.format("k_%02d", i)), bytes("val_" + i));
            }
            assertThat(engine.liveTables()).isNotEmpty();

            // Active memtable performs range delete over flushed keys k_05 to k_15
            db.deleteRange(bytes("k_05"), bytes("k_15"));

            assertThat(asString(db.get(bytes("k_04")))).isEqualTo("val_4");
            assertThat(db.get(bytes("k_05"))).isNull();
            assertThat(db.get(bytes("k_10"))).isNull();
            assertThat(db.get(bytes("k_14"))).isNull();
            assertThat(asString(db.get(bytes("k_15")))).isEqualTo("val_15");

            // Scan should also mask shadowed records
            List<String> keys = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(bytes("k_00"), bytes("k_20"))) {
                while (it.hasNext()) {
                    keys.add(new String(it.next().key(), StandardCharsets.UTF_8));
                }
            }
            assertThat(keys).doesNotContain("k_05", "k_06", "k_07", "k_08", "k_09", "k_10", "k_11", "k_12", "k_13", "k_14");
            assertThat(keys).contains("k_04", "k_15");
        }
    }

    @Test
    @DisplayName("Range deletes flushed into SSTables mask keys in older SSTables")
    void testFlushedRangeTombstoneAcrossSSTables(@TempDir Path tempDir) throws Exception {
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
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            for (int i = 0; i < 20; i++) {
                db.put(bytes(String.format("rec_%02d", i)), bytes("data_" + i));
            }
            engine.flushImmutableMemTable();
            int tablesBefore = engine.liveTables().size();

            // Insert range deletes and trigger another flush
            db.deleteRange(bytes("rec_02"), bytes("rec_18"));
            for (int i = 0; i < 15; i++) {
                db.put(bytes(String.format("filler_%02d", i)), bytes("filler_payload_long_" + i));
            }
            engine.flushImmutableMemTable();
            assertThat(engine.liveTables().size()).isGreaterThan(tablesBefore);

            assertThat(asString(db.get(bytes("rec_01")))).isEqualTo("data_1");
            assertThat(db.get(bytes("rec_02"))).isNull();
            assertThat(db.get(bytes("rec_10"))).isNull();
            assertThat(db.get(bytes("rec_17"))).isNull();
            assertThat(asString(db.get(bytes("rec_18")))).isEqualTo("data_18");
        }
    }

    @Test
    @DisplayName("Consistent read snapshot taken before range delete preserves pre-delete state")
    void testSnapshotIsolationWithRangeDelete(@TempDir Path tempDir) throws StorageException {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("item1"), bytes("data1"));
            db.put(bytes("item2"), bytes("data2"));
            db.put(bytes("item3"), bytes("data3"));

            try (Snapshot snapshotBefore = db.snapshot()) {
                // Delete range after snapshot was taken
                db.deleteRange(bytes("item1"), bytes("item3"));

                // Live database reflects range delete
                assertThat(db.get(bytes("item1"))).isNull();
                assertThat(db.get(bytes("item2"))).isNull();
                assertThat(asString(db.get(bytes("item3")))).isEqualTo("data3");

                // Snapshot before range delete still reads pre-delete data
                assertThat(asString(snapshotBefore.get(bytes("item1")))).isEqualTo("data1");
                assertThat(asString(snapshotBefore.get(bytes("item2")))).isEqualTo("data2");
                assertThat(asString(snapshotBefore.get(bytes("item3")))).isEqualTo("data3");

                // Snapshot scan also sees pre-delete data
                List<String> snapKeys = new ArrayList<>();
                try (ClosableIterator<Entry> it = snapshotBefore.scan(null, null)) {
                    while (it.hasNext()) {
                        snapKeys.add(new String(it.next().key(), StandardCharsets.UTF_8));
                    }
                }
                assertThat(snapKeys).containsExactly("item1", "item2", "item3");
            }
        }
    }

    @Test
    @DisplayName("Crash recovery: replaying WAL faithfully recovers range deletes")
    void testCrashRecoveryReplayOfRangeDeletes(@TempDir Path tempDir) throws StorageException {
        DatabaseOptions defaults = DatabaseOptions.defaults();

        try (KVDatabase db = KVDatabase.open(tempDir, defaults)) {
            db.put(bytes("recA"), bytes("valA"));
            db.put(bytes("recB"), bytes("valB"));
            db.put(bytes("recC"), bytes("valC"));
            db.deleteRange(bytes("recA"), bytes("recC")); // removes recA and recB
            db.put(bytes("recD"), bytes("valD"));
            // Close without explicit flush: state is preserved in WAL
        }

        // Reopen database from disk directory
        try (KVDatabase db = KVDatabase.open(tempDir, defaults)) {
            assertThat(db.get(bytes("recA"))).isNull();
            assertThat(db.get(bytes("recB"))).isNull();
            assertThat(asString(db.get(bytes("recC")))).isEqualTo("valC");
            assertThat(asString(db.get(bytes("recD")))).isEqualTo("valD");

            List<String> keys = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(null, null)) {
                while (it.hasNext()) {
                    keys.add(new String(it.next().key(), StandardCharsets.UTF_8));
                }
            }
            assertThat(keys).containsExactly("recC", "recD");
        }
    }

    @Test
    @DisplayName("Range delete validation rejects invalid parameters")
    void testParameterValidation(@TempDir Path tempDir) throws StorageException {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            assertThatThrownBy(() -> db.deleteRange(null, bytes("b")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.deleteRange(bytes("a"), null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.deleteRange(new byte[0], bytes("b")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.deleteRange(bytes("b"), bytes("a")))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> db.deleteRange(bytes("a"), bytes("a")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
