package io.jlsm.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseOptionsTest {

    @Test
    @DisplayName("DatabaseOptions defaults adhere to design specification")
    void testDefaults() {
        DatabaseOptions options = DatabaseOptions.defaults();

        assertThat(options.durabilityMode()).isEqualTo(DurabilityMode.SYNC);
        assertThat(options.memtableBytes()).isEqualTo(4L * 1024 * 1024);
        assertThat(options.compactionTrigger()).isEqualTo(4);
        assertThat(options.maxOpenScans()).isEqualTo(4);
        assertThat(options.maxSnapshotBytes()).isEqualTo(8L * 1024 * 1024);
        assertThat(options.maxIndexBytes()).isEqualTo(32L * 1024 * 1024);
        assertThat(options.maxLiveTables()).isEqualTo(16);
    }

    @Test
    @DisplayName("Validation enforces positive bounds on thresholds")
    void testValidation() {
        DatabaseOptions d = DatabaseOptions.defaults();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new DatabaseOptions(null, 100, 4, 4, 100, 100, 16))
                .isInstanceOf(NullPointerException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> d.withMemtableBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> d.withCompactionTrigger(1))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> d.withMaxOpenScans(0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> d.withMaxSnapshotBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> d.withMaxIndexBytes(0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> d.withMaxLiveTables(2).withCompactionTrigger(4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Wither methods create modified copies correctly")
    void testWithers() {
        DatabaseOptions d = DatabaseOptions.defaults()
                .withDurabilityMode(DurabilityMode.BUFFERED)
                .withMemtableBytes(1024)
                .withCompactionTrigger(5)
                .withMaxOpenScans(10)
                .withMaxSnapshotBytes(2048)
                .withMaxIndexBytes(4096)
                .withMaxLiveTables(20);

        assertThat(d.durabilityMode()).isEqualTo(DurabilityMode.BUFFERED);
        assertThat(d.memtableBytes()).isEqualTo(1024L);
        assertThat(d.compactionTrigger()).isEqualTo(5);
        assertThat(d.maxOpenScans()).isEqualTo(10);
        assertThat(d.maxSnapshotBytes()).isEqualTo(2048L);
        assertThat(d.maxIndexBytes()).isEqualTo(4096L);
        assertThat(d.maxLiveTables()).isEqualTo(20);
    }
}
