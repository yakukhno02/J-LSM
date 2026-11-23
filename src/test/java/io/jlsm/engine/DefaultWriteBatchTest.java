package io.jlsm.engine;

import io.jlsm.api.WriteBatch;
import io.jlsm.internal.model.BatchOperation;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultWriteBatchTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Empty write batch initializes with 0 operations and 0 byte size")
    void testEmptyBatch() {
        try (WriteBatch batch = WriteBatch.create()) {
            assertThat(batch.isEmpty()).isTrue();
            assertThat(batch.operationCount()).isEqualTo(0);
            assertThat(batch.byteSize()).isEqualTo(0L);
        }
    }

    @Test
    @DisplayName("Batch preserves sequential order of puts and deletes")
    void testOrderPreservation() {
        try (DefaultWriteBatch batch = new DefaultWriteBatch()) {
            batch.put(bytes("key1"), bytes("value1"));
            batch.delete(bytes("key1"));
            batch.put(bytes("key2"), bytes("value2"));

            assertThat(batch.isEmpty()).isFalse();
            assertThat(batch.operationCount()).isEqualTo(3);
            assertThat(batch.byteSize()).isGreaterThan(0L);

            List<BatchOperation> ops = batch.operations();
            assertThat(ops).hasSize(3);

            assertThat(ops.get(0).kind()).isEqualTo(RecordKind.PUT);
            assertThat(asString(ops.get(0).key())).isEqualTo("key1");
            assertThat(asString(ops.get(0).value())).isEqualTo("value1");

            assertThat(ops.get(1).kind()).isEqualTo(RecordKind.DELETE);
            assertThat(asString(ops.get(1).key())).isEqualTo("key1");
            assertThat(ops.get(1).value()).isNull();

            assertThat(ops.get(2).kind()).isEqualTo(RecordKind.PUT);
            assertThat(asString(ops.get(2).key())).isEqualTo("key2");
            assertThat(asString(ops.get(2).value())).isEqualTo("value2");
        }
    }

    @Test
    @DisplayName("Defensive copying protects queued operations from subsequent external array mutations")
    void testDefensiveCopying() {
        byte[] key = bytes("originalKey");
        byte[] val = bytes("originalValue");

        try (DefaultWriteBatch batch = new DefaultWriteBatch()) {
            batch.put(key, val);

            // Mutate source arrays
            key[0] = 'X';
            val[0] = 'Y';

            BatchOperation op = batch.operations().get(0);
            assertThat(asString(op.key())).isEqualTo("originalKey");
            assertThat(asString(op.value())).isEqualTo("originalValue");

            // Mutate getter returned arrays
            byte[] returnedKey = op.key();
            returnedKey[0] = 'Z';
            assertThat(asString(op.key())).isEqualTo("originalKey");
        }
    }

    @Test
    @DisplayName("Empty byte array value (byte[0]) is allowed for PUT")
    void testEmptyValuePut() {
        try (DefaultWriteBatch batch = new DefaultWriteBatch()) {
            batch.put(bytes("k"), new byte[0]);
            BatchOperation op = batch.operations().get(0);
            assertThat(op.value()).isNotNull().isEmpty();
        }
    }

    @Test
    @DisplayName("Batch rejects null keys, empty keys, and null PUT values")
    void testValidationConstraints() {
        try (WriteBatch batch = WriteBatch.create()) {
            assertThatThrownBy(() -> batch.put(null, bytes("v")))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> batch.put(bytes("k"), null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> batch.put(new byte[0], bytes("v")))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> batch.delete(null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> batch.delete(new byte[0]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("Clear resets batch to empty state")
    void testClear() {
        try (WriteBatch batch = WriteBatch.create()) {
            batch.put(bytes("k1"), bytes("v1"));
            batch.delete(bytes("k2"));
            assertThat(batch.operationCount()).isEqualTo(2);

            batch.clear();
            assertThat(batch.isEmpty()).isTrue();
            assertThat(batch.operationCount()).isEqualTo(0);
            assertThat(batch.byteSize()).isEqualTo(0L);
        }
    }

    @Test
    @DisplayName("Operations on closed batch throw IllegalStateException")
    void testClosedBatchThrows() {
        WriteBatch batch = WriteBatch.create();
        batch.put(bytes("k"), bytes("v"));
        batch.close();

        assertThatThrownBy(() -> batch.put(bytes("k2"), bytes("v2")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> batch.delete(bytes("k2")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(batch::operationCount)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(batch::byteSize)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(batch::isEmpty)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(batch::clear)
                .isInstanceOf(IllegalStateException.class);
    }
}
