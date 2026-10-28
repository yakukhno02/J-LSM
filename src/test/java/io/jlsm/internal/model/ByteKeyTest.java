package io.jlsm.internal.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class ByteKeyTest {

    @Test
    @DisplayName("ByteKey compareTo must use unsigned byte ordering")
    void testUnsignedByteOrdering() {
        // In signed byte, 0x80 is -128 (< 0x7F = 127).
        // In unsigned byte comparison, 0x80 is 128 (> 0x7F = 127).
        ByteKey key7F = ByteKey.copyOf(new byte[]{(byte) 0x7F});
        ByteKey key80 = ByteKey.copyOf(new byte[]{(byte) 0x80});
        ByteKey keyFF = ByteKey.copyOf(new byte[]{(byte) 0xFF});
        ByteKey key00 = ByteKey.copyOf(new byte[]{(byte) 0x00});

        assertThat(key00.compareTo(key7F)).isNegative();
        assertThat(key7F.compareTo(key80)).isNegative();
        assertThat(key80.compareTo(keyFF)).isNegative();
        assertThat(keyFF.compareTo(key00)).isPositive();
    }

    @Test
    @DisplayName("ByteKey compareTo should handle prefixes correctly")
    void testPrefixComparison() {
        ByteKey kA = ByteKey.copyOf(new byte[]{1, 2});
        ByteKey kAB = ByteKey.copyOf(new byte[]{1, 2, 3});
        ByteKey kIdentical = ByteKey.copyOf(new byte[]{1, 2});

        assertThat(kA.compareTo(kAB)).isNegative();
        assertThat(kAB.compareTo(kA)).isPositive();
        assertThat(kA.compareTo(kIdentical)).isZero();
    }

    @Test
    @DisplayName("ByteKey must guarantee defensive copying")
    void testDefensiveCopying() {
        byte[] original = new byte[]{10, 20, 30};
        ByteKey key = ByteKey.copyOf(original);

        // Mutating original array must not mutate ByteKey
        original[0] = 99;
        assertThat(key.bytes()[0]).isEqualTo((byte) 10);

        // Mutating bytes() return array must not mutate ByteKey
        byte[] copy = key.bytes();
        copy[0] = 77;
        assertThat(key.bytes()[0]).isEqualTo((byte) 10);
    }

    @Test
    @DisplayName("ByteKey.copyOf with null returns empty ByteKey")
    void testNullCopyOf() {
        ByteKey empty = ByteKey.copyOf(null);
        assertThat(empty.length()).isZero();
        assertThat(empty.bytes()).isEmpty();
    }

    @Test
    @DisplayName("ByteKey equals and hashCode contract")
    void testEqualsAndHashCode() {
        ByteKey k1 = ByteKey.copyOf(new byte[]{1, 2, 3});
        ByteKey k2 = ByteKey.copyOf(new byte[]{1, 2, 3});
        ByteKey k3 = ByteKey.copyOf(new byte[]{1, 2, 4});

        assertThat(k1).isEqualTo(k2);
        assertThat(k1.hashCode()).isEqualTo(k2.hashCode());
        assertThat(k1).isNotEqualTo(k3);
        assertThat(k1.equals("different type")).isFalse();
        assertThat(k1.equals(null)).isFalse();
    }

    @Test
    @DisplayName("ByteKey toString displays byte contents")
    void testToString() {
        ByteKey key = ByteKey.copyOf(new byte[]{1, 2});
        assertThat(key.toString()).isEqualTo(Arrays.toString(new byte[]{1, 2}));
    }
}
