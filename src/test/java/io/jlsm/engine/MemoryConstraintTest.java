package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.KVDatabase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MemoryConstraintTest {

    @Test
    @DisplayName("Stage 5 (M6): 500 MB write stream under strict JVM heap constraint (-Xmx32m) without OutOfMemoryError")
    void test500MbUnderStrictMemoryConstraint(@TempDir Path tempDir) throws Exception {
        String javaHome = System.getProperty("java.home");
        String javaBin = Path.of(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        String projectClasses = Path.of("target/classes").toAbsolutePath().toString();
        String testClasses = Path.of("target/test-classes").toAbsolutePath().toString();
        String fullClasspath = projectClasses + java.io.File.pathSeparator + testClasses + java.io.File.pathSeparator + classpath;

        List<String> command = List.of(
                javaBin,
                "-Xmx32m",
                "-cp",
                fullClasspath,
                MemoryConstraintWorker.class.getName(),
                tempDir.toAbsolutePath().toString()
        );

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        List<String> outputLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                outputLines.add(line);
            }
        }

        int exitCode = process.waitFor();
        assertThat(exitCode)
                .as("Worker process terminated with non-zero exit code: %s", String.join("\n", outputLines))
                .isEqualTo(0);

        // Verify database is completely valid and readable after reopening
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.BUFFERED,
                1024L * 1024,
                4,
                4,
                4L * 1024 * 1024,
                16L * 1024 * 1024,
                16
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            // Verify spot checks across key space
            byte[] val0 = db.get(String.format("key_%06d", 0).getBytes(StandardCharsets.UTF_8));
            assertThat(val0).isNotNull();
            assertThat(val0).hasSize(2000);

            byte[] valMid = db.get(String.format("key_%06d", 125_000).getBytes(StandardCharsets.UTF_8));
            assertThat(valMid).isNotNull();
            assertThat(valMid).hasSize(2000);

            byte[] valLast = db.get(String.format("key_%06d", 249_999).getBytes(StandardCharsets.UTF_8));
            assertThat(valLast).isNotNull();
            assertThat(valLast).hasSize(2000);
        }
    }

    public static final class MemoryConstraintWorker {
        public static void main(String[] args) {
            try {
                Path dbPath = Path.of(args[0]);

                DatabaseOptions options = new DatabaseOptions(
                        DurabilityMode.BUFFERED,
                        1024L * 1024, // 1MB MemTable
                        4,
                        4,
                        4L * 1024 * 1024,
                        16L * 1024 * 1024,
                        16
                );

                byte[] payload = new byte[2000]; // 2 KB payload per record (within 4063B block limit)
                Arrays.fill(payload, (byte) 'X');

                int totalRecords = 250_000; // 250,000 * 2,000 B = 500,000,000 bytes (~500 MB)

                try (KVDatabase db = KVDatabase.open(dbPath, options)) {
                    for (int i = 0; i < totalRecords; i++) {
                        byte[] key = String.format("key_%06d", i).getBytes(StandardCharsets.UTF_8);
                        db.put(key, payload);
                    }
                    db.sync();
                }

                System.out.println("SUCCESS: Wrote 500 MB without OutOfMemoryError");
                System.exit(0);
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        }
    }
}
