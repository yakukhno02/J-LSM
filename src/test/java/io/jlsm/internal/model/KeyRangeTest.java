package io.jlsm.internal.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyRangeTest {

    @Test
    @DisplayName("KeyRange constructor validates fromKey <= toKey")
    void testConstructorValidation() {
        ByteKey k1 = ByteKey.copyOf(new byte[]{1});
        ByteKey k2 = ByteKey.copyOf(new byte[]{2});

        assertThatThrownBy(() -> new KeyRange(k2, k1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fromKey must be <= toKey");

        // Equal keys are valid: [k1, k1) represents an empty range
        KeyRange emptyRange = new KeyRange(k1, k1);
        assertThat(emptyRange.contains(k1)).isFalse();
    }

    @Test
    @DisplayName("KeyRange contains logic for [fromKey, toKey)")
    void testBoundedRange() {
        ByteKey k10 = ByteKey.copyOf(new byte[]{10});
        ByteKey k20 = ByteKey.copyOf(new byte[]{20});
        KeyRange range = new KeyRange(k10, k20);

        assertThat(range.contains(ByteKey.copyOf(new byte[]{9}))).isFalse();
        assertThat(range.contains(ByteKey.copyOf(new byte[]{10}))).isTrue(); // inclusive start
        assertThat(range.contains(ByteKey.copyOf(new byte[]{15}))).isTrue();
        assertThat(range.contains(ByteKey.copyOf(new byte[]{20}))).isFalse(); // exclusive end
        assertThat(range.contains(ByteKey.copyOf(new byte[]{21}))).isFalse();
    }

    @Test
    @DisplayName("KeyRange contains logic for unbounded ranges")
    void testUnboundedRanges() {
        ByteKey k10 = ByteKey.copyOf(new byte[]{10});

        KeyRange fromOnly = new KeyRange(k10, null);
        assertThat(fromOnly.contains(ByteKey.copyOf(new byte[]{9}))).isFalse();
        assertThat(fromOnly.contains(ByteKey.copyOf(new byte[]{10}))).isTrue();
        assertThat(fromOnly.contains(ByteKey.copyOf(new byte[]{100}))).isTrue();

        KeyRange toOnly = new KeyRange(null, k10);
        assertThat(toOnly.contains(ByteKey.copyOf(new byte[]{0}))).isTrue();
        assertThat(toOnly.contains(ByteKey.copyOf(new byte[]{9}))).isTrue();
        assertThat(toOnly.contains(ByteKey.copyOf(new byte[]{10}))).isFalse();

        KeyRange completelyUnbounded = new KeyRange(null, null);
        assertThat(completelyUnbounded.contains(ByteKey.copyOf(new byte[]{0}))).isTrue();
        assertThat(completelyUnbounded.contains(ByteKey.copyOf(new byte[]{(byte) 0xFF}))).isTrue();
    }

    @Test
    @DisplayName("KeyRange contains throws NullPointerException when key is null")
    void testNullKey() {
        KeyRange range = new KeyRange(null, null);
        assertThatThrownBy(() -> range.contains(null))
                .isInstanceOf(NullPointerException.class);
    }
}
