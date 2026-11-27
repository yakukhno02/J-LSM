package io.jlsm.internal.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalRecordTest {

    @Test
    @DisplayName("InternalRecord preserves defensive copying of value")
    void testDefensiveCopying() {
        byte[] val = new byte[]{1, 2, 3};
        ByteKey key = ByteKey.copyOf(new byte[]{10});
        InternalRecord record = InternalRecord.of(key, val, RecordKind.PUT, 42L);

        val[0] = 99;
        assertThat(record.value()[0]).isEqualTo((byte) 1);

        byte[] returnedVal = record.value();
        returnedVal[0] = 88;
        assertThat(record.value()[0]).isEqualTo((byte) 1);
    }

    @Test
    @DisplayName("InternalRecord tombstone check")
    void testTombstone() {
        ByteKey key = ByteKey.copyOf(new byte[]{10});
        InternalRecord putRecord = InternalRecord.of(key, new byte[]{1}, RecordKind.PUT, 1L);
        InternalRecord deleteRecord = InternalRecord.of(key, null, RecordKind.DELETE, 2L);

        assertThat(putRecord.isTombstone()).isFalse();
        assertThat(deleteRecord.isTombstone()).isTrue();
        assertThat(deleteRecord.value()).isNull();
    }

    @Test
    @DisplayName("InternalRecord equals and hashCode value comparison")
    void testEqualsAndHashCode() {
        ByteKey key = ByteKey.copyOf(new byte[]{10});
        InternalRecord r1 = InternalRecord.of(key, new byte[]{1, 2}, RecordKind.PUT, 100L);
        InternalRecord r2 = InternalRecord.of(key, new byte[]{1, 2}, RecordKind.PUT, 100L);
        InternalRecord r3DiffSeq = InternalRecord.of(key, new byte[]{1, 2}, RecordKind.PUT, 101L);
        InternalRecord r4DiffVal = InternalRecord.of(key, new byte[]{1, 3}, RecordKind.PUT, 100L);
        InternalRecord r5Delete = InternalRecord.of(key, null, RecordKind.DELETE, 100L);
        InternalRecord r6DeleteSame = InternalRecord.of(key, null, RecordKind.DELETE, 100L);

        assertThat(r1).isEqualTo(r2);
        assertThat(r1.hashCode()).isEqualTo(r2.hashCode());

        assertThat(r1).isNotEqualTo(r3DiffSeq);
        assertThat(r1).isNotEqualTo(r4DiffVal);
        assertThat(r1).isNotEqualTo(r5Delete);

        assertThat(r5Delete).isEqualTo(r6DeleteSame);
        assertThat(r5Delete.hashCode()).isEqualTo(r6DeleteSame.hashCode());
    }

    @Test
    @DisplayName("Sealed hierarchy: Put rejects null value, accepts empty byte array")
    void testPutRecordValidation() {
        ByteKey key = ByteKey.copyOf(new byte[]{1});
        assertThatThrownBy(() -> new InternalRecord.Put(key, null, 1L))
                .isInstanceOf(NullPointerException.class);

        InternalRecord.Put emptyValPut = new InternalRecord.Put(key, new byte[0], 2L);
        assertThat(emptyValPut.value()).isEqualTo(new byte[0]);
        assertThat(emptyValPut.isTombstone()).isFalse();
        assertThat(emptyValPut.kind()).isEqualTo(RecordKind.PUT);
    }

    @Test
    @DisplayName("RangeDelete validation and behaviour")
    void testRangeDelete() {
        ByteKey k1 = ByteKey.copyOf(new byte[]{1});
        ByteKey k2 = ByteKey.copyOf(new byte[]{5});

        InternalRecord.RangeDelete rd = InternalRecord.rangeDelete(k1, k2, 10L);
        assertThat(rd.isTombstone()).isTrue();
        assertThat(rd.kind()).isEqualTo(RecordKind.RANGE_DELETE);
        assertThat(rd.key()).isEqualTo(k1);
        assertThat(rd.fromKey()).isEqualTo(k1);
        assertThat(rd.toKey()).isEqualTo(k2);
        assertThat(rd.covers(ByteKey.copyOf(new byte[]{3}), 5L)).isTrue();
        assertThat(rd.covers(ByteKey.copyOf(new byte[]{3}), 15L)).isFalse();
        assertThat(rd.covers(ByteKey.copyOf(new byte[]{5}), 5L)).isFalse();

        assertThatThrownBy(() -> InternalRecord.rangeDelete(k2, k1, 10L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Sealed hierarchy: Pattern matching exhaustive switch")
    void testSealedPatternMatching() {
        ByteKey key = ByteKey.copyOf(new byte[]{1});
        ByteKey key2 = ByteKey.copyOf(new byte[]{2});
        InternalRecord put = new InternalRecord.Put(key, new byte[]{42}, 10L);
        InternalRecord del = new InternalRecord.Delete(key, 20L);
        InternalRecord rangeDel = new InternalRecord.RangeDelete(key, key2, 30L);

        String putDesc = switch (put) {
            case InternalRecord.Put p -> "PUT:" + p.value()[0];
            case InternalRecord.Delete d -> "DEL";
            case InternalRecord.RangeDelete rd -> "RANGE_DEL";
        };
        String delDesc = switch (del) {
            case InternalRecord.Put p -> "PUT:" + p.value()[0];
            case InternalRecord.Delete d -> "DEL";
            case InternalRecord.RangeDelete rd -> "RANGE_DEL";
        };
        String rangeDelDesc = switch (rangeDel) {
            case InternalRecord.Put p -> "PUT:" + p.value()[0];
            case InternalRecord.Delete d -> "DEL";
            case InternalRecord.RangeDelete rd -> "RANGE_DEL";
        };

        assertThat(putDesc).isEqualTo("PUT:42");
        assertThat(delDesc).isEqualTo("DEL");
        assertThat(rangeDelDesc).isEqualTo("RANGE_DEL");
    }
}
