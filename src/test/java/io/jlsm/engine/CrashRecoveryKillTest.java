package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.KVDatabase;
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

class CrashRecoveryKillTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Definition of Done: Abrupt kill -9 mid-flight crash simulation and recovery demonstration")
    void testCrashSimulationWithKill9AndRecovery(@TempDir Path tempDir) throws Exception {
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
                CrashWriterWorker.class.getName(),
                tempDir.toAbsolutePath().toString()
        );

        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = pb.start();

        int acknowledgedWrites = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("ACK:")) {
                    acknowledgedWrites = Integer.parseInt(line.substring(4).trim());
                    if (acknowledgedWrites >= 2000) {
                        // Kill child process abruptly mid-operation with SIGKILL (kill -9)
                        process.destroyForcibly();
                        break;
                    }
                }
            }
        }

        process.waitFor();
        assertThat(process.isAlive()).isFalse();
        assertThat(acknowledgedWrites).isGreaterThanOrEqualTo(2000);

        // Run structural diagnostics on crashed directory
        IntegrityReport report = StorageDiagnostics.inspect(new StandardStorageIO(), tempDir);
        assertThat(report.isHealthy()).isTrue();
        assertThat(report.hasErrors()).isFalse();

        // Demonstrate recovery via KVDatabase.open()
        DatabaseOptions options = DatabaseOptions.defaults();
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            // Verify all acknowledged writes are recoverable
            for (int i = 0; i < acknowledgedWrites; i++) {
                byte[] key = String.format("crash_key_%06d", i).getBytes(StandardCharsets.UTF_8);
                byte[] expectedVal = String.format("crash_val_%06d", i).getBytes(StandardCharsets.UTF_8);
                byte[] actualVal = db.get(key);
                assertThat(actualVal)
                        .as("Key %s should be preserved after crash recovery", asString(key))
                        .isEqualTo(expectedVal);
            }

            // Verify database can continue normal operation after crash recovery
            db.put(bytes("post_crash_key"), bytes("post_crash_value"));
            assertThat(asString(db.get(bytes("post_crash_key")))).isEqualTo("post_crash_value");
        }

        // Reopen again cleanly
        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            assertThat(asString(db.get(bytes("post_crash_key")))).isEqualTo("post_crash_value");
        }
    }

    public static final class CrashWriterWorker {
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
                    for (int i = 0; i < 50_000; i++) {
                        byte[] key = String.format("crash_key_%06d", i).getBytes(StandardCharsets.UTF_8);
                        byte[] val = String.format("crash_val_%06d", i).getBytes(StandardCharsets.UTF_8);
                        db.put(key, val);

                        if (i > 0 && i % 500 == 0) {
                            System.out.println("ACK:" + i);
                            System.out.flush();
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        }
    }
}
