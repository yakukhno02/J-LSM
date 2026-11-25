package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.Snapshot;
import io.jlsm.api.WriteBatch;
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

class DefaultSnapshotTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? "null" : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Snapshot provides isolated point-in-time reads against later puts and deletes")
    void testSnapshotPointLookupIsolation(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k1"), bytes("v1_orig"));
            db.put(bytes("k2"), bytes("v2_orig"));

            try (Snapshot snap1 = db.snapshot()) {
                assertThat(snap1.sequenceNumber()).isGreaterThan(0L);

                // Mutate the database after snap1 was captured
                db.put(bytes("k1"), bytes("v1_updated"));
                db.delete(bytes("k2"));
                db.put(bytes("k3"), bytes("v3_new"));

                // Database observes the latest mutations
                assertThat(asString(db.get(bytes("k1")))).isEqualTo("v1_updated");
                assertThat(db.get(bytes("k2"))).isNull();
                assertThat(asString(db.get(bytes("k3")))).isEqualTo("v3_new");

                // Snapshot 1 observes the point-in-time state
                assertThat(asString(snap1.get(bytes("k1")))).isEqualTo("v1_orig");
                assertThat(asString(snap1.get(bytes("k2")))).isEqualTo("v2_orig");
                assertThat(snap1.get(bytes("k3"))).isNull();

                // Snapshot 2 captured now observes the latest state
                try (Snapshot snap2 = db.snapshot()) {
                    assertThat(snap2.sequenceNumber()).isGreaterThan(snap1.sequenceNumber());
                    assertThat(asString(snap2.get(bytes("k1")))).isEqualTo("v1_updated");
                    assertThat(snap2.get(bytes("k2"))).isNull();
                    assertThat(asString(snap2.get(bytes("k3")))).isEqualTo("v3_new");
                }
            }
        }
    }

    @Test
    @DisplayName("Snapshot scan is isolated from later modifications")
    void testSnapshotScanIsolation(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("a"), bytes("1"));
            db.put(bytes("b"), bytes("2"));
            db.put(bytes("c"), bytes("3"));

            try (Snapshot snap = db.snapshot()) {
                db.put(bytes("d"), bytes("4"));
                db.delete(bytes("b"));
                db.put(bytes("a"), bytes("1_modified"));

                List<String> snapEntries = new ArrayList<>();
                try (ClosableIterator<Entry> it = snap.scan(null, null)) {
                    while (it.hasNext()) {
                        Entry e = it.next();
                        snapEntries.add(asString(e.key()) + "=" + asString(e.value()));
                    }
                }
                assertThat(snapEntries).containsExactly("a=1", "b=2", "c=3");

                List<String> dbEntries = new ArrayList<>();
                try (ClosableIterator<Entry> it = db.scan(null, null)) {
                    while (it.hasNext()) {
                        Entry e = it.next();
                        dbEntries.add(asString(e.key()) + "=" + asString(e.value()));
                    }
                }
                assertThat(dbEntries).containsExactly("a=1_modified", "c=3", "d=4");
            }
        }
    }

    @Test
    @DisplayName("Snapshot correctly reads across memtable flushes and SSTable compaction")
    void testSnapshotAcrossFlushAndCompaction(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.SYNC,
                1024L, // small memtable: 1 KB
                2,     // compaction trigger: 2 tables
                4,
                8L * 1024 * 1024,
                32L * 1024 * 1024,
                16
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            // Write records to force flushes
            for (int i = 0; i < 20; i++) {
                db.put(bytes(String.format("k_%03d", i)), bytes(String.format("v_orig_%03d", i)));
            }

            try (Snapshot snap = db.snapshot()) {
                // Overwrite all records and add more to trigger compaction
                for (int i = 0; i < 20; i++) {
                    db.put(bytes(String.format("k_%03d", i)), bytes(String.format("v_new_%03d", i)));
                }
                for (int i = 20; i < 40; i++) {
                    db.put(bytes(String.format("k_%03d", i)), bytes(String.format("v_new_%03d", i)));
                }

                ((DefaultStorageEngine) db).triggerCompactionSync();

                // Check point reads on snapshot
                for (int i = 0; i < 20; i++) {
                    assertThat(asString(snap.get(bytes(String.format("k_%03d", i)))))
                            .isEqualTo(String.format("v_orig_%03d", i));
                }
                for (int i = 20; i < 40; i++) {
                    assertThat(snap.get(bytes(String.format("k_%03d", i)))).isNull();
                }

                // Check bounded scan on snapshot
                List<String> rangeResults = new ArrayList<>();
                try (ClosableIterator<Entry> it = snap.scan(bytes("k_005"), bytes("k_010"))) {
                    while (it.hasNext()) {
                        Entry e = it.next();
                        rangeResults.add(asString(e.key()) + "=" + asString(e.value()));
                    }
                }
                assertThat(rangeResults).containsExactly(
                        "k_005=v_orig_005",
                        "k_006=v_orig_006",
                        "k_007=v_orig_007",
                        "k_008=v_orig_008",
                        "k_009=v_orig_009"
                );
            }
        }
    }

    @Test
    @DisplayName("Active snapshot pins SSTables and prevents deletion until closed")
    void testActiveSnapshotPreventsObsoleteTableDeletion(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.SYNC,
                512L,
                2,
                4,
                8L * 1024 * 1024,
                32L * 1024 * 1024,
                16
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;

            for (int i = 0; i < 15; i++) {
                db.put(bytes("key_" + i), bytes("val_" + i));
            }
            engine.flushImmutableMemTable();

            Snapshot activeSnap = db.snapshot();

            // Trigger compaction
            for (int i = 15; i < 30; i++) {
                db.put(bytes("key_" + i), bytes("val_" + i));
            }
            engine.flushImmutableMemTable();
            engine.triggerCompactionSync();

            // Obsolete table files must still exist on disk because activeSnap holds pins
            List<Path> obsoletePaths = engine.obsoleteTablePaths();
            assertThat(obsoletePaths).isNotEmpty();
            for (Path path : obsoletePaths) {
                assertThat(Files.exists(path)).isTrue();
            }

            // Closing snapshot releases pins, enabling cleanup
            activeSnap.close();

            // Perform another compaction to trigger cleanObsoleteTables or check files deleted
            engine.triggerCompactionSync();
            for (Path path : obsoletePaths) {
                assertThat(Files.exists(path)).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Closed snapshot throws IllegalStateException on get and scan, and close is idempotent")
    void testClosedSnapshotLifecycle(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k"), bytes("v"));
            Snapshot snap = db.snapshot();
            assertThat(snap.get(bytes("k"))).isNotNull();

            snap.close();
            snap.close(); // idempotent

            assertThatThrownBy(() -> snap.get(bytes("k")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");

            assertThatThrownBy(() -> snap.scan(null, null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Test
    @DisplayName("Closing the database closes all open snapshots and invalidates further reads")
    void testDatabaseCloseInvalidatesSnapshots(@TempDir Path tempDir) throws Exception {
        Snapshot snap;
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k"), bytes("v"));
            snap = db.snapshot();
            assertThat(snap.get(bytes("k"))).isNotNull();
        }

        assertThatThrownBy(() -> snap.get(bytes("k")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    @DisplayName("Snapshot input validation: invalid keys and inverted ranges")
    void testSnapshotInputValidation(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            try (Snapshot snap = db.snapshot()) {
                assertThatThrownBy(() -> snap.get(null))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> snap.get(new byte[0]))
                        .isInstanceOf(IllegalArgumentException.class);

                try (ClosableIterator<Entry> it = snap.scan(bytes("z"), bytes("a"))) {
                    assertThat(it.hasNext()).isFalse();
                    assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
                }
            }
        }
    }

    @Test
    @DisplayName("WriteBatch updates are atomically visible to snapshot captured after batch")
    void testSnapshotWithWriteBatch(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k1"), bytes("v1"));

            Snapshot beforeBatch = db.snapshot();

            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("k1"), bytes("v1_batched"));
                batch.put(bytes("k2"), bytes("v2_batched"));
                batch.delete(bytes("k3"));
                db.write(batch);
            }

            Snapshot afterBatch = db.snapshot();

            assertThat(asString(beforeBatch.get(bytes("k1")))).isEqualTo("v1");
            assertThat(beforeBatch.get(bytes("k2"))).isNull();

            assertThat(asString(afterBatch.get(bytes("k1")))).isEqualTo("v1_batched");
            assertThat(asString(afterBatch.get(bytes("k2")))).isEqualTo("v2_batched");

            beforeBatch.close();
            afterBatch.close();
        }
    }
}
