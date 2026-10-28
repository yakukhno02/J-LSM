package io.jlsm.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EntryTest {

    @Test
    @DisplayName("Entry preserves defensive copying of key and value")
    void testDefensiveCopying() {
        byte[] k = new byte[]{1, 2};
        byte[] v = new byte[]{3, 4};
        Entry entry = new Entry(k, v, false);

        k[0] = 99;
        v[0] = 88;
        assertThat(entry.key()[0]).isEqualTo((byte) 1);
        assertThat(entry.value()[0]).isEqualTo((byte) 3);

        byte[] returnedKey = entry.key();
        returnedKey[0] = 77;
        assertThat(entry.key()[0]).isEqualTo((byte) 1);
    }

    @Test
    @DisplayName("Entry handles null key and value gracefully")
    void testNullHandling() {
        Entry nullKey = new Entry(null, null, true);
        assertThat(nullKey.key()).isEmpty();
        assertThat(nullKey.value()).isNull();
        assertThat(nullKey.isTombstone()).isTrue();
    }

    @Test
    @DisplayName("Entry equals and hashCode contract")
    void testEqualsAndHashCode() {
        Entry e1 = new Entry(new byte[]{1}, new byte[]{2}, false);
        Entry e2 = new Entry(new byte[]{1}, new byte[]{2}, false);
        Entry e3Tombstone = new Entry(new byte[]{1}, new byte[]{2}, true);
        Entry e4DiffVal = new Entry(new byte[]{1}, new byte[]{3}, false);

        assertThat(e1).isEqualTo(e2);
        assertThat(e1.hashCode()).isEqualTo(e2.hashCode());
        assertThat(e1).isNotEqualTo(e3Tombstone);
        assertThat(e1).isNotEqualTo(e4DiffVal);
        assertThat(e1.equals("other")).isFalse();
        assertThat(e1.equals(null)).isFalse();
    }
}
