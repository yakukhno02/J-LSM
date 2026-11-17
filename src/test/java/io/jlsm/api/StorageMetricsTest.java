package io.jlsm.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class StorageMetricsTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Metrics record operational counters, memory usage, and formatted summary")
    void testMetricsCalculationAndFormatting(@TempDir Path tempDir) throws Exception {
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k1"), bytes("v1"));
            db.put(bytes("k2"), bytes("v2"));
            db.get(bytes("k1"));
            db.delete(bytes("k2"));

            StorageMetrics metrics = db.metrics();
            assertThat(metrics).isNotNull();
            assertThat(metrics.putsCount()).isEqualTo(2L);
            assertThat(metrics.getsCount()).isEqualTo(1L);
            assertThat(metrics.deletesCount()).isEqualTo(1L);
            assertThat(metrics.activeMemTableBytes()).isGreaterThan(0L);
            assertThat(metrics.memoryUtilization()).isBetween(0.0, 1.0);
            assertThat(metrics.toSummaryString()).contains("StorageMetrics");
        }
    }
}
