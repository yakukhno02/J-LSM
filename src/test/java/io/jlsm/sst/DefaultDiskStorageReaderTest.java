package io.jlsm.sst;

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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultDiskStorageReaderTest {

    @TempDir
    Path tempDir;

    private StandardStorageIO storageIO;
    private DefaultSSTableFlusher flusher;
    private DefaultDiskStorageReader reader;

    @BeforeEach
    void setUp() {
        storageIO = new StandardStorageIO();
        flusher = new DefaultSSTableFlusher(tempDir, storageIO);
        reader = new DefaultDiskStorageReader();
    }

    private static InternalRecord put(String key, String val, long seq) {
        return new InternalRecord(
                ByteKey.copyOf(key.getBytes(StandardCharsets.UTF_8)),
                val.getBytes(StandardCharsets.UTF_8),
                RecordKind.PUT,
                seq
        );
    }

    private static InternalRecord del(String key, long seq) {
        return new InternalRecord(
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
    @DisplayName("Point search picks record with highest sequence across multiple SSTables")
    void testPointSearchHighestSequenceWins() throws Exception {
        // Table 1 (older): "key1"@10 (PUT, "old"), "key2"@10 (PUT, "val2")
        TableMeta meta1 = flusher.build(1L, new ListCursor(List.of(
                put("key1", "old", 10L),
                put("key2", "val2", 10L)
        )));

        // Table 2 (newer): "key1"@25 (PUT, "updated"), "key3"@25 (PUT, "val3")
        TableMeta meta2 = flusher.build(2L, new ListCursor(List.of(
                put("key1", "updated", 25L),
                put("key3", "val3", 25L)
        )));

        // Table 3 (newest): "key2"@40 (DELETE)
        TableMeta meta3 = flusher.build(3L, new ListCursor(List.of(
                del("key2", 40L)
        )));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), meta1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), meta2);
             DefaultTableHandle h3 = DefaultTableHandle.open(storageIO, flusher.sstPath(3L), meta3)) {

            List<TableHandle> tables = List.of(h1, h2, h3);

            // key1: winner is table 2 (@ 25)
            Optional<InternalRecord> r1 = reader.search(tables, key("key1"));
            assertThat(r1).isPresent();
            assertThat(r1.get().sequence()).isEqualTo(25L);
            assertThat(r1.get().value()).isEqualTo("updated".getBytes(StandardCharsets.UTF_8));

            // key2: winner is table 3 (@ 40, DELETE tombstone)
            Optional<InternalRecord> r2 = reader.search(tables, key("key2"));
            assertThat(r2).isPresent();
            assertThat(r2.get().sequence()).isEqualTo(40L);
            assertThat(r2.get().kind()).isEqualTo(RecordKind.DELETE);

            // key3: from table 2 (@ 25)
            Optional<InternalRecord> r3 = reader.search(tables, key("key3"));
            assertThat(r3).isPresent();
            assertThat(r3.get().sequence()).isEqualTo(25L);

            // keyNotFound
            assertThat(reader.search(tables, key("nonExistent"))).isEmpty();
        }
    }

    @Test
    @DisplayName("Point search sequence resolution works regardless of table list ordering")
    void testPointSearchOrderIndependent() throws Exception {
        TableMeta meta1 = flusher.build(1L, new ListCursor(List.of(put("k", "v1", 10L))));
        TableMeta meta2 = flusher.build(2L, new ListCursor(List.of(put("k", "v2", 20L))));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), meta1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), meta2)) {

            // Pass in ascending order [h1, h2]
            Optional<InternalRecord> rAsc = reader.search(List.of(h1, h2), key("k"));
            assertThat(rAsc).isPresent();
            assertThat(rAsc.get().sequence()).isEqualTo(20L);

            // Pass in descending order [h2, h1]
            Optional<InternalRecord> rDesc = reader.search(List.of(h2, h1), key("k"));
            assertThat(rDesc).isPresent();
            assertThat(rDesc.get().sequence()).isEqualTo(20L);
        }
    }

    @Test
    @DisplayName("scanDisk merges multiple SSTables, deduplicates versions, and filters tombstones")
    void testScanDiskKWayMergeAndTombstoneFiltering() throws Exception {
        // Table 1: "a"@10, "c"@10, "d"@10 (PUT)
        TableMeta m1 = flusher.build(1L, new ListCursor(List.of(
                put("a", "a1", 10L),
                put("c", "c1", 10L),
                put("d", "d1", 10L)
        )));

        // Table 2: "b"@20, "c"@20 (PUT, newer version of c)
        TableMeta m2 = flusher.build(2L, new ListCursor(List.of(
                put("b", "b2", 20L),
                put("c", "c2", 20L)
        )));

        // Table 3: "a"@30 (DELETE tombstone)
        TableMeta m3 = flusher.build(3L, new ListCursor(List.of(
                del("a", 30L)
        )));

        try (DefaultTableHandle h1 = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), m1);
             DefaultTableHandle h2 = DefaultTableHandle.open(storageIO, flusher.sstPath(2L), m2);
             DefaultTableHandle h3 = DefaultTableHandle.open(storageIO, flusher.sstPath(3L), m3)) {

            List<TableHandle> tables = List.of(h1, h2, h3);

            // Scan full disk range:
            // "a" was deleted @ 30 -> filtered out!
            // "b" is live @ 20
            // "c" is live @ 20 (version 20 wins over 10)
            // "d" is live @ 10
            try (RecordCursor cursor = reader.scanDisk(tables, null, null)) {
                List<InternalRecord> records = new ArrayList<>();
                while (cursor.advance()) {
                    records.add(cursor.current());
                }

                assertThat(records).hasSize(3);

                assertThat(records.get(0).key()).isEqualTo(key("b"));
                assertThat(records.get(0).sequence()).isEqualTo(20L);

                assertThat(records.get(1).key()).isEqualTo(key("c"));
                assertThat(records.get(1).sequence()).isEqualTo(20L);
                assertThat(records.get(1).value()).isEqualTo("c2".getBytes(StandardCharsets.UTF_8));

                assertThat(records.get(2).key()).isEqualTo(key("d"));
                assertThat(records.get(2).sequence()).isEqualTo(10L);
            }

            // Bounded scan: [b, d) -> only b and c
            try (RecordCursor cursor = reader.scanDisk(tables, key("b"), key("d"))) {
                List<String> keys = new ArrayList<>();
                while (cursor.advance()) {
                    keys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
                }
                assertThat(keys).containsExactly("b", "c");
            }
        }
    }

    @Test
    @DisplayName("Empty tables list returns empty search and empty scan")
    void testEmptyTables() throws Exception {
        assertThat(reader.search(List.of(), key("anything"))).isEmpty();

        try (RecordCursor cursor = reader.scanDisk(List.of(), null, null)) {
            assertThat(cursor.advance()).isFalse();
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
