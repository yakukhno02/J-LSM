package io.jlsm.internal.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InternalRecordTest {

    @Test
    @DisplayName("InternalRecord preserves defensive copying of value")
    void testDefensiveCopying() {
        byte[] val = new byte[]{1, 2, 3};
        ByteKey key = ByteKey.copyOf(new byte[]{10});
        InternalRecord record = new InternalRecord(key, val, RecordKind.PUT, 42L);

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
        InternalRecord putRecord = new InternalRecord(key, new byte[]{1}, RecordKind.PUT, 1L);
        InternalRecord deleteRecord = new InternalRecord(key, null, RecordKind.DELETE, 2L);

        assertThat(putRecord.isTombstone()).isFalse();
        assertThat(deleteRecord.isTombstone()).isTrue();
        assertThat(deleteRecord.value()).isNull();
    }

    @Test
    @DisplayName("InternalRecord equals and hashCode value comparison")
    void testEqualsAndHashCode() {
        ByteKey key = ByteKey.copyOf(new byte[]{10});
        InternalRecord r1 = new InternalRecord(key, new byte[]{1, 2}, RecordKind.PUT, 100L);
        InternalRecord r2 = new InternalRecord(key, new byte[]{1, 2}, RecordKind.PUT, 100L);
        InternalRecord r3DiffSeq = new InternalRecord(key, new byte[]{1, 2}, RecordKind.PUT, 101L);
        InternalRecord r4DiffVal = new InternalRecord(key, new byte[]{1, 3}, RecordKind.PUT, 100L);
        InternalRecord r5Delete = new InternalRecord(key, null, RecordKind.DELETE, 100L);
        InternalRecord r6DeleteSame = new InternalRecord(key, null, RecordKind.DELETE, 100L);

        assertThat(r1).isEqualTo(r2);
        assertThat(r1.hashCode()).isEqualTo(r2.hashCode());

        assertThat(r1).isNotEqualTo(r3DiffSeq);
        assertThat(r1).isNotEqualTo(r4DiffVal);
        assertThat(r1).isNotEqualTo(r5Delete);

        assertThat(r5Delete).isEqualTo(r6DeleteSame);
        assertThat(r5Delete.hashCode()).isEqualTo(r6DeleteSame.hashCode());
    }
}
