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
}
