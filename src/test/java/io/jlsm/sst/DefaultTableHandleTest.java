package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultTableHandleTest {

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
    @DisplayName("Opens fixture sstable-minimal.bin and performs successful point lookups")
    void testOpenFixtureAndPointLookup() throws Exception {
        Path fixturePath = tempDir.resolve("000001.sst");
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            Files.copy(in, fixturePath);
        }

        try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, fixturePath)) {
            assertThat(handle.id()).isEqualTo(1L);
            assertThat(handle.recordCount()).isEqualTo(2L);
            assertThat(handle.fileSize()).isEqualTo(4196L);

            // Point lookup "key1" -> PUT "value1" @ 1
            Optional<InternalRecord> r1 = handle.get(key("key1"));
            assertThat(r1).isPresent();
            assertThat(r1.get().kind()).isEqualTo(RecordKind.PUT);
            assertThat(r1.get().value()).isEqualTo("value1".getBytes(StandardCharsets.UTF_8));
            assertThat(r1.get().sequence()).isEqualTo(1L);

            // Point lookup "key2" -> DELETE @ 2
            Optional<InternalRecord> r2 = handle.get(key("key2"));
            assertThat(r2).isPresent();
            assertThat(r2.get().kind()).isEqualTo(RecordKind.DELETE);
            assertThat(r2.get().sequence()).isEqualTo(2L);

            // Point lookup non-existent keys
            assertThat(handle.get(key("key0"))).isEmpty(); // before first key
            assertThat(handle.get(key("key15"))).isEmpty(); // between keys
            assertThat(handle.get(key("key3"))).isEmpty(); // after last key
        }
    }

    @Test
    @DisplayName("Point lookup and range scan across multi-block SSTable")
    void testMultiBlockLookupsAndScan() throws Exception {
        List<InternalRecord> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            records.add(put(String.format("k-%04d", i), String.format("val-%04d", i), 1000L + i));
        }

        TableMeta meta = flusher.build(10L, new ListCursor(records));

        try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, flusher.sstPath(10L), meta)) {
            assertThat(handle.id()).isEqualTo(10L);
            assertThat(handle.minSequence()).isEqualTo(1000L);
            assertThat(handle.maxSequence()).isEqualTo(1199L);
            assertThat(handle.recordCount()).isEqualTo(200L);

            // Point lookups
            Optional<InternalRecord> hit = handle.get(key("k-0050"));
            assertThat(hit).isPresent();
            assertThat(hit.get().sequence()).isEqualTo(1050L);

            assertThat(handle.get(key("k-9999"))).isEmpty();
            assertThat(handle.get(key("a-0000"))).isEmpty();

            // Range scan [k-0050, k-0055)
            try (RecordCursor cursor = handle.scan(key("k-0050"), key("k-0055"))) {
                List<String> foundKeys = new ArrayList<>();
                while (cursor.advance()) {
                    foundKeys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
                }
                assertThat(foundKeys).containsExactly(
                        "k-0050", "k-0051", "k-0052", "k-0053", "k-0054"
                );
            }

            // Prefix scan up to k-0003
            try (RecordCursor cursor = handle.scan(null, key("k-0003"))) {
                List<String> foundKeys = new ArrayList<>();
                while (cursor.advance()) {
                    foundKeys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
                }
                assertThat(foundKeys).containsExactly("k-0000", "k-0001", "k-0002");
            }

            // Suffix scan from k-0197 to end
            try (RecordCursor cursor = handle.scan(key("k-0197"), null)) {
                List<String> foundKeys = new ArrayList<>();
                while (cursor.advance()) {
                    foundKeys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
                }
                assertThat(foundKeys).containsExactly("k-0197", "k-0198", "k-0199");
            }
        }
    }

    @Test
    @DisplayName("Pinning refCount keeps handle open while cursor is active")
    void testPinningKeepsHandleOpen() throws Exception {
        TableMeta meta = flusher.build(20L, new ListCursor(List.of(put("a", "1", 1L))));
        DefaultTableHandle handle = DefaultTableHandle.open(storageIO, flusher.sstPath(20L), meta);

        RecordCursor cursor = handle.scan(null, null);

        // Close table handle: refCount was 2 (handle + cursor), now 1. Channel should still be open!
        handle.close();
        assertThat(handle.isClosed()).isFalse();

        assertThat(cursor.advance()).isTrue();
        assertThat(cursor.current().key()).isEqualTo(key("a"));

        // Now close cursor: refCount drops to 0 and closes channel.
        cursor.close();
        assertThat(handle.isClosed()).isTrue();

        assertThatThrownBy(() -> handle.get(key("a")))
                .isInstanceOfSatisfying(StorageException.class, e -> assertThat(e.code()).isEqualTo(ErrorCode.CLOSED));
    }

    @Test
    @DisplayName("Open fails with CORRUPTION when SSTable file is truncated")
    void testOpenTruncatedFile() throws Exception {
        Path badPath = tempDir.resolve("bad.sst");
        Files.write(badPath, new byte[50]); // less than 72 bytes

        assertThatThrownBy(() -> DefaultTableHandle.open(storageIO, badPath))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("smaller than required");
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
