package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.WriteBatch;
import io.jlsm.internal.io.StandardStorageIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WriteBatchCrashRecoveryTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("R-03: Subprocess kill -9 mid-flight crash simulation validates WriteBatch all-or-nothing recovery")
    void testBatchCrashSimulationWithKill9AndRecovery(@TempDir Path tempDir) throws Exception {
        String javaHome = System.getProperty("java.home");
        String javaBin = Path.of(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        String projectClasses = Path.of("target/classes").toAbsolutePath().toString();
        String testClasses = Path.of("target/test-classes").toAbsolutePath().toString();
        String fullClasspath = projectClasses + File.pathSeparator + testClasses + File.pathSeparator + classpath;

        List<String> command = List.of(
                javaBin,
                "-cp",
                fullClasspath,
                BatchCrashWriterWorker.class.getName(),
                tempDir.toAbsolutePath().toString()
        );

        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = pb.start();

        int lastAckedBatch = -1;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("BATCH_ACK:")) {
                    lastAckedBatch = Integer.parseInt(line.substring(10).trim());
                    if (lastAckedBatch >= 250) {
                        process.destroyForcibly();
                        break;
                    }
                }
            }
        }

        process.waitFor();
        assertThat(process.isAlive()).isFalse();
        assertThat(lastAckedBatch).isGreaterThanOrEqualTo(250);

        IntegrityReport report = StorageDiagnostics.inspect(new StandardStorageIO(), tempDir);
        assertThat(report.isHealthy()).isTrue();
        assertThat(report.hasErrors()).isFalse();

        DatabaseOptions options = DatabaseOptions.defaults().withDurabilityMode(DurabilityMode.SYNC);
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            for (int b = 0; b <= lastAckedBatch; b++) {
                for (int k = 0; k < 10; k++) {
                    byte[] key = String.format("b_%05d_k_%02d", b, k).getBytes(StandardCharsets.UTF_8);
                    byte[] expectedVal = String.format("val_%05d_k_%02d", b, k).getBytes(StandardCharsets.UTF_8);
                    byte[] actualVal = db.get(key);
                    assertThat(actualVal)
                            .as("Acknowledged batch key %s must be present after crash", asString(key))
                            .isEqualTo(expectedVal);
                }
            }

            int inFlightBatch = lastAckedBatch + 1;
            int foundInFlightKeys = 0;
            for (int k = 0; k < 10; k++) {
                byte[] key = String.format("b_%05d_k_%02d", inFlightBatch, k).getBytes(StandardCharsets.UTF_8);
                if (db.get(key) != null) {
                    foundInFlightKeys++;
                }
            }

            assertThat(foundInFlightKeys)
                    .as("In-flight batch must adhere to all-or-nothing invariant: either 0 or 10 keys visible")
                    .isIn(0, 10);

            try (WriteBatch postCrashBatch = WriteBatch.create()) {
                postCrashBatch.put(bytes("post_crash_k1"), bytes("v1"));
                postCrashBatch.put(bytes("post_crash_k2"), bytes("v2"));
                db.write(postCrashBatch);
            }

            assertThat(asString(db.get(bytes("post_crash_k1")))).isEqualTo("v1");
            assertThat(asString(db.get(bytes("post_crash_k2")))).isEqualTo("v2");
        }

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            assertThat(asString(db.get(bytes("post_crash_k1")))).isEqualTo("v1");
            assertThat(asString(db.get(bytes("post_crash_k2")))).isEqualTo("v2");
        }
    }

    public static final class BatchCrashWriterWorker {
        public static void main(String[] args) {
            try {
                Path dbPath = Path.of(args[0]);
                DatabaseOptions options = new DatabaseOptions(
                        DurabilityMode.SYNC,
                        64L * 1024,
                        4,
                        4,
                        8L * 1024 * 1024,
                        32L * 1024 * 1024,
                        16
                );

                try (KVDatabase db = KVDatabase.open(dbPath, options)) {
                    for (int b = 0; b < 10_000; b++) {
                        try (WriteBatch batch = WriteBatch.create()) {
                            for (int k = 0; k < 10; k++) {
                                byte[] key = String.format("b_%05d_k_%02d", b, k).getBytes(StandardCharsets.UTF_8);
                                byte[] val = String.format("val_%05d_k_%02d", b, k).getBytes(StandardCharsets.UTF_8);
                                batch.put(key, val);
                            }
                            db.write(batch);
                        }

                        System.out.println("BATCH_ACK:" + b);
                        System.out.flush();
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        }
    }
}
