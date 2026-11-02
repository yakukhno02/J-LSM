package io.jlsm.merge;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultMergedIteratorTest {

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

    @Test
    @DisplayName("K-way merge preserves key sorting across multiple streams")
    void testKWayMergeSorting() throws Exception {
        RecordCursor c1 = new TestListCursor(List.of(put("a", "1", 1L), put("d", "4", 4L)));
        RecordCursor c2 = new TestListCursor(List.of(put("b", "2", 2L), put("e", "5", 5L)));
        RecordCursor c3 = new TestListCursor(List.of(put("c", "3", 3L), put("f", "6", 6L)));

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2, c3), false)) {
            List<String> keys = new ArrayList<>();
            while (iter.advance()) {
                keys.add(new String(iter.current().key().bytes(), StandardCharsets.UTF_8));
            }
            assertThat(keys).containsExactly("a", "b", "c", "d", "e", "f");
        }
    }

    @Test
    @DisplayName("Deduplication picks latest sequence version and discards older versions")
    void testDeduplicationLatestSequenceWins() throws Exception {
        // Stream 1: "key1"@10 (PUT), "key2"@15 (PUT)
        RecordCursor c1 = new TestListCursor(List.of(put("key1", "old1", 10L), put("key2", "v2", 15L)));
        // Stream 2: "key1"@50 (PUT), "key3"@5 (PUT)
        RecordCursor c2 = new TestListCursor(List.of(put("key1", "newest1", 50L), put("key3", "v3", 5L)));
        // Stream 3: "key1"@30 (PUT)
        RecordCursor c3 = new TestListCursor(List.of(put("key1", "mid1", 30L)));

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2, c3), false)) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            assertThat(records).hasSize(3);
            // key1: winner must be sequence 50
            assertThat(records.get(0).sequence()).isEqualTo(50L);
            assertThat(records.get(0).value()).isEqualTo("newest1".getBytes(StandardCharsets.UTF_8));

            // key2: sequence 15
            assertThat(records.get(1).sequence()).isEqualTo(15L);

            // key3: sequence 5
            assertThat(records.get(2).sequence()).isEqualTo(5L);
        }
    }

    @Test
    @DisplayName("Tombstone filtering: when retainTombstones = false, tombstones purge older records and are skipped")
    void testTombstoneFiltering() throws Exception {
        // Table 1 (older): key "alpha"@10 PUT
        RecordCursor c1 = new TestListCursor(List.of(put("alpha", "old", 10L), put("beta", "stay", 10L)));
        // Table 2 (newer): key "alpha"@20 DELETE
        RecordCursor c2 = new TestListCursor(List.of(del("alpha", 20L)));

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), false)) {
            List<String> keys = new ArrayList<>();
            while (iter.advance()) {
                keys.add(new String(iter.current().key().bytes(), StandardCharsets.UTF_8));
            }
            // "alpha" must be filtered out! Only "beta" remains.
            assertThat(keys).containsExactly("beta");
        }
    }

    @Test
    @DisplayName("Tombstone retention: when retainTombstones = true, tombstones are yielded to caller")
    void testTombstoneRetention() throws Exception {
        RecordCursor c1 = new TestListCursor(List.of(put("alpha", "old", 10L)));
        RecordCursor c2 = new TestListCursor(List.of(del("alpha", 20L)));

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), true)) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            assertThat(records).hasSize(1);
            assertThat(records.get(0).kind()).isEqualTo(RecordKind.DELETE);
            assertThat(records.get(0).sequence()).isEqualTo(20L);
        }
    }

    @Test
    @DisplayName("Handles empty inputs gracefully")
    void testEmptyInputs() throws Exception {
        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(), false)) {
            assertThat(iter.advance()).isFalse();
            assertThatThrownBy(iter::current).isInstanceOf(IllegalStateException.class);
        }

        RecordCursor c1 = new TestListCursor(List.of());
        RecordCursor c2 = new TestListCursor(List.of());
        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), false)) {
            assertThat(iter.advance()).isFalse();
        }
    }

    @Test
    @DisplayName("Closing merged iterator closes all input cursors")
    void testClosingClosesAllInputs() throws Exception {
        TestListCursor c1 = new TestListCursor(List.of(put("a", "1", 1L)));
        TestListCursor c2 = new TestListCursor(List.of(put("b", "2", 2L)));

        DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), false);
        iter.close();

        assertThat(c1.closed).isTrue();
        assertThat(c2.closed).isTrue();

        assertThatThrownBy(iter::advance)
                .isInstanceOfSatisfying(StorageException.class, e -> assertThat(e.code()).isEqualTo(ErrorCode.CLOSED));
    }

    private static final class TestListCursor implements RecordCursor {
        private final Iterator<InternalRecord> it;
        private InternalRecord cur;
        boolean closed;

        TestListCursor(List<InternalRecord> list) {
            this.it = list.iterator();
        }

        @Override
        public boolean advance() throws StorageException {
            if (closed) throw new StorageException(ErrorCode.CLOSED, "closed");
            if (it.hasNext()) {
                cur = it.next();
                return true;
            }
            cur = null;
            return false;
        }

        @Override
        public InternalRecord current() {
            if (cur == null) throw new IllegalStateException("no current");
            return cur;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
