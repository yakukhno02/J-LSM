package io.jlsm.merge;

import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RangeTombstone;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultMergedIteratorTest {

    private static ByteKey key(String s) {
        return ByteKey.copyOf(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] val(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static InternalRecord put(String key, String val, long seq) {
        return new InternalRecord.Put(key(key), val(val), seq);
    }

    private static InternalRecord del(String key, long seq) {
        return new InternalRecord.Delete(key(key), seq);
    }

    @Test
    @DisplayName("Merges multiple cursors in lexicographical key order and deduplicates identical keys")
    void testMergeAndDeduplicate() throws Exception {
        // Stream 1: (apple, 10), (cherry, 5)
        List<InternalRecord> s1 = List.of(
                put("apple", "val1", 10L),
                put("cherry", "val3", 5L)
        );
        // Stream 2: (apple, 20), (banana, 15)  -> (apple, 20) should win over (apple, 10)
        List<InternalRecord> s2 = List.of(
                put("apple", "val1-updated", 20L),
                put("banana", "val2", 15L)
        );

        RecordCursor c1 = new ListRecordCursor(s1);
        RecordCursor c2 = new ListRecordCursor(s2);

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), false)) {
            List<InternalRecord> merged = new ArrayList<>();
            while (iter.advance()) {
                merged.add(iter.current());
            }

            assertThat(merged).hasSize(3);

            // 1. apple with seq 20
            assertThat(merged.getFirst().key()).isEqualTo(key("apple"));
            assertThat(merged.get(0).sequence()).isEqualTo(20L);
            assertThat(merged.get(0).value()).isEqualTo(val("val1-updated"));

            // 2. banana with seq 15
            assertThat(merged.get(1).key()).isEqualTo(key("banana"));
            assertThat(merged.get(1).sequence()).isEqualTo(15L);

            // 3. cherry with seq 5
            assertThat(merged.get(2).key()).isEqualTo(key("cherry"));
            assertThat(merged.get(2).sequence()).isEqualTo(5L);
        }
    }

    @Test
    @DisplayName("Filters out tombstones when retainTombstones = false")
    void testFilterTombstones() throws Exception {
        // Stream 1: (alpha, 10, PUT), (beta, 30, DELETE)
        List<InternalRecord> s1 = List.of(
                put("alpha", "v1", 10L),
                del("beta", 30L)
        );
        // Stream 2: (alpha, 20, DELETE) -> alpha should be deleted and skipped
        List<InternalRecord> s2 = List.of(
                del("alpha", 20L)
        );

        RecordCursor c1 = new ListRecordCursor(s1);
        RecordCursor c2 = new ListRecordCursor(s2);

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), false)) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            // Both alpha (deleted by seq 20) and beta (deleted by seq 30) are tombstones -> 0 records
            assertThat(records).isEmpty();
        }
    }

    @Test
    @DisplayName("Retains tombstones when retainTombstones = true")
    void testRetainTombstones() throws Exception {
        List<InternalRecord> s1 = List.of(put("alpha", "v1", 10L));
        List<InternalRecord> s2 = List.of(del("alpha", 20L));

        RecordCursor c1 = new ListRecordCursor(s1);
        RecordCursor c2 = new ListRecordCursor(s2);

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1, c2), true)) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            assertThat(records).hasSize(1);
            assertThat(records.getFirst().kind()).isEqualTo(RecordKind.DELETE);
            assertThat(records.getFirst().sequence()).isEqualTo(20L);
        }
    }

    @Test
    @DisplayName("Handles empty inputs gracefully")
    void testEmptyInputs() throws Exception {
        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(), false)) {
            assertThat(iter.advance()).isFalse();
            assertThatThrownBy(iter::current).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @DisplayName("Throws on operations after close")
    void testCloseBehavior() throws Exception {
        RecordCursor c = new ListRecordCursor(List.of(put("k", "v", 1L)));
        DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c), false);
        iter.close();

        assertThatThrownBy(iter::advance).isInstanceOf(StorageException.class);
        assertThatThrownBy(iter::current).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Range tombstone shadows older point records in DefaultMergedIterator")
    void testRangeTombstoneShadowsInMergedIterator() throws Exception {
        RecordCursor c1 = new ListRecordCursor(List.of(
                put("b", "val-b", 5L),
                put("m", "val-m", 5L),
                put("z", "val-z", 5L)
        ));

        RangeTombstone rt = new RangeTombstone(key("c"), key("p"), 10L);

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1), false, List.of(rt))) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            assertThat(records).hasSize(2);
            assertThat(records.get(0).key()).isEqualTo(key("b"));
            assertThat(records.get(1).key()).isEqualTo(key("z"));
        }
    }

    @Test
    @DisplayName("Range tombstone does not shadow records with higher sequence")
    void testRangeTombstoneDoesNotShadowNewerRecord() throws Exception {
        RecordCursor c1 = new ListRecordCursor(List.of(
                put("m", "val-m-new", 15L)
        ));

        RangeTombstone rt = new RangeTombstone(key("c"), key("p"), 10L);

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1), false, List.of(rt))) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            assertThat(records).hasSize(1);
            assertThat(records.getFirst().key()).isEqualTo(key("m"));
        }
    }

    @Test
    @DisplayName("Dynamic range delete in stream shadows subsequent older records in stream")
    void testDynamicRangeDeleteInStream() throws Exception {
        InternalRecord.RangeDelete rd = new InternalRecord.RangeDelete(key("c"), key("p"), 10L);
        InternalRecord p1 = put("m", "val-m", 5L);

        RecordCursor c1 = new ListRecordCursor(List.of(rd, p1));

        try (DefaultMergedIterator iter = new DefaultMergedIterator(List.of(c1), false)) {
            List<InternalRecord> records = new ArrayList<>();
            while (iter.advance()) {
                records.add(iter.current());
            }

            assertThat(records).isEmpty();
        }
    }

    private static final class ListRecordCursor implements RecordCursor {
        private final List<InternalRecord> records;
        private int index = -1;

        ListRecordCursor(List<InternalRecord> records) {
            this.records = records;
        }

        @Override
        public boolean advance() {
            index++;
            return index < records.size();
        }

        @Override
        public InternalRecord current() {
            return records.get(index);
        }

        @Override
        public void close() {
        }
    }
}
