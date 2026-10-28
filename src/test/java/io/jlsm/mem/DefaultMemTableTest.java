package io.jlsm.mem;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultMemTableTest {

    private static ByteKey key(String s) {
        return ByteKey.copyOf(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static InternalRecord putRecord(String k, String v, long seq) {
        return new InternalRecord(key(k), bytes(v), RecordKind.PUT, seq);
    }

    private static InternalRecord deleteRecord(String k, long seq) {
        return new InternalRecord(key(k), null, RecordKind.DELETE, seq);
    }

    @Test
    @DisplayName("Empty MemTable reports correct initial state")
    void testEmptyState() throws StorageException {
        DefaultMemTable memTable = new DefaultMemTable();
        assertThat(memTable.isEmpty()).isTrue();
        assertThat(memTable.byteSize()).isZero();
        assertThat(memTable.get(key("absent"))).isEmpty();

        try (RecordCursor cursor = memTable.scan(null, null)) {
            assertThat(cursor.advance()).isFalse();
            assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @DisplayName("Insert and point get operations return latest value")
    void testInsertAndGet() {
        DefaultMemTable memTable = new DefaultMemTable();
        InternalRecord rec1 = putRecord("keyA", "val1", 1L);
        memTable.insert(key("keyA"), rec1);

        assertThat(memTable.isEmpty()).isFalse();
        Optional<InternalRecord> fetched = memTable.get(key("keyA"));
        assertThat(fetched).isPresent().contains(rec1);

        // Overwrite with newer version
        InternalRecord rec2 = putRecord("keyA", "val2", 2L);
        memTable.insert(key("keyA"), rec2);

        Optional<InternalRecord> fetched2 = memTable.get(key("keyA"));
        assertThat(fetched2).isPresent().contains(rec2);
    }

    @Test
    @DisplayName("Byte size tracks inserts, updates and deletions accurately")
    void testByteSizeTracking() {
        DefaultMemTable memTable = new DefaultMemTable();
        ByteKey k = key("key1"); // length = 4
        byte[] v1 = bytes("value1"); // length = 6
        InternalRecord rec1 = new InternalRecord(k, v1, RecordKind.PUT, 1L);

        // overhead(32) + key(4) + val(6) = 42
        memTable.insert(k, rec1);
        assertThat(memTable.byteSize()).isEqualTo(42L);

        // Update with longer value: overhead(32) + key(4) + val(11) = 47
        byte[] v2 = bytes("valueLonger"); // length = 11
        InternalRecord rec2 = new InternalRecord(k, v2, RecordKind.PUT, 2L);
        memTable.insert(k, rec2);
        assertThat(memTable.byteSize()).isEqualTo(32L + 4L + 11L);

        // Update with tombstone (null value): overhead(32) + key(4) + val(0) = 36
        InternalRecord tombstone = deleteRecord("key1", 3L);
        memTable.insert(k, tombstone);
        assertThat(memTable.byteSize()).isEqualTo(36L);

        // Clear resets everything
        memTable.clear();
        assertThat(memTable.isEmpty()).isTrue();
        assertThat(memTable.byteSize()).isZero();
    }

    @Test
    @DisplayName("Scan iterates in unsigned lexicographical key order")
    void testScanFullRange() throws StorageException {
        DefaultMemTable memTable = new DefaultMemTable();
        memTable.insert(key("b"), putRecord("b", "vb", 1L));
        memTable.insert(key("a"), putRecord("a", "va", 2L));
        memTable.insert(key("c"), putRecord("c", "vc", 3L));

        List<String> keys = new ArrayList<>();
        try (RecordCursor cursor = memTable.scan(null, null)) {
            while (cursor.advance()) {
                keys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
            }
        }

        assertThat(keys).containsExactly("a", "b", "c");
    }

    @Test
    @DisplayName("Bounded scans [fromKey, toKey) enforce correct bounds")
    void testScanBoundedRange() throws StorageException {
        DefaultMemTable memTable = new DefaultMemTable();
        memTable.insert(key("10"), putRecord("10", "v10", 1L));
        memTable.insert(key("20"), putRecord("20", "v20", 2L));
        memTable.insert(key("30"), putRecord("30", "v30", 3L));
        memTable.insert(key("40"), putRecord("40", "v40", 4L));

        // [20, 40) should contain 20, 30
        List<String> keys = new ArrayList<>();
        try (RecordCursor cursor = memTable.scan(key("20"), key("40"))) {
            while (cursor.advance()) {
                keys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
            }
        }
        assertThat(keys).containsExactly("20", "30");

        // headMap: null to 30 -> 10, 20
        keys.clear();
        try (RecordCursor cursor = memTable.scan(null, key("30"))) {
            while (cursor.advance()) {
                keys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
            }
        }
        assertThat(keys).containsExactly("10", "20");

        // tailMap: 30 to null -> 30, 40
        keys.clear();
        try (RecordCursor cursor = memTable.scan(key("30"), null)) {
            while (cursor.advance()) {
                keys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
            }
        }
        assertThat(keys).containsExactly("30", "40");

        // inverted range fromKey > toKey -> empty
        try (RecordCursor cursor = memTable.scan(key("40"), key("10"))) {
            assertThat(cursor.advance()).isFalse();
            assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @DisplayName("RecordCursor contract: current before advance, advance after EOF, and close")
    void testCursorContract() throws StorageException {
        DefaultMemTable memTable = new DefaultMemTable();
        memTable.insert(key("k1"), putRecord("k1", "v1", 1L));

        RecordCursor cursor = memTable.scan(null, null);
        assertThatThrownBy(cursor::current)
                .isInstanceOf(IllegalStateException.class);

        assertThat(cursor.advance()).isTrue();
        assertThat(cursor.current().key()).isEqualTo(key("k1"));

        assertThat(cursor.advance()).isFalse();
        assertThatThrownBy(cursor::current)
                .isInstanceOf(IllegalStateException.class);

        cursor.close();
        assertThat(cursor.advance()).isFalse();
        assertThatThrownBy(cursor::current)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Null safety validation on arguments")
    void testNullValidation() {
        DefaultMemTable memTable = new DefaultMemTable();

        assertThatThrownBy(() -> memTable.insert(null, putRecord("a", "b", 1L)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> memTable.insert(key("a"), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> memTable.get(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Concurrent insertions maintain data consistency")
    void testConcurrentInserts() throws InterruptedException, StorageException {
        DefaultMemTable memTable = new DefaultMemTable();
        int threads = 4;
        int perThread = 250;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        String k = String.format("key_%02d_%04d", threadId, i);
                        memTable.insert(key(k), putRecord(k, "val", i));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertThat(memTable.byteSize()).isGreaterThan(0L);

        int count = 0;
        try (RecordCursor cursor = memTable.scan(null, null)) {
            while (cursor.advance()) {
                count++;
            }
        }
        assertThat(count).isEqualTo(threads * perThread);
    }
}
