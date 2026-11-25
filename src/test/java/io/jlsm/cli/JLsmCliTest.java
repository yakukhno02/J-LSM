package io.jlsm.cli;

import io.jlsm.api.DurabilityMode;
import io.jlsm.api.StorageException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JLsmCliTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Interactive REPL executes CRUD, scan, metrics, inspect, backup, restore, and exit commands")
    void testInteractiveReplWorkflow() throws Exception {
        Path dbDir = tempDir.resolve("repl-db");
        Path backupDir = tempDir.resolve("repl-backup");
        Path restoreDir = tempDir.resolve("repl-restore");

        String script = String.join("\n",
                "help",
                "put user:1 \"Alice Admin\"",
                "put user:2 \"Bob Builder\"",
                "put user:3 \"Charlie\"",
                "get user:1",
                "get unknown",
                "delete user:3",
                "get user:3",
                "batch put:b1=val1 put:b2=val2 del:user:2",
                "get b1",
                "get user:2",
                "snap b1 b9",
                "scan user:1 user:9",
                "scan",
                "sync",
                "metrics",
                "inspect",
                "backup " + backupDir,
                "verify-backup " + backupDir,
                "restore " + backupDir + " " + restoreDir,
                "unknown-cmd",
                "exit"
        ) + "\n";

        ByteArrayInputStream in = new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);

        int exitCode = JLsmCli.run(new String[]{"--dir", dbDir.toString(), "--memtable-kb", "64"}, in, out);
        String output = outBytes.toString(StandardCharsets.UTF_8);

        assertThat(exitCode).isZero();
        assertThat(output)
                .contains("Available J-LSM Commands:")
                .contains("\"Alice Admin\"")
                .contains("(nil)")
                .contains("OK [tombstone written]")
                .contains("OK [Atomic WriteBatch committed: 3 ops")
                .contains("[Snapshot seq=")
                .contains("\"user:1\" => \"Alice Admin\"")
                .contains("OK [WAL synced to disk]")
                .contains("J-LSM REAL-TIME TELEMETRY")
                .contains("J-LSM STORAGE DIAGNOSTICS REPORT")
                .contains("OK [Backup verified: all CRC32 checksums match]")
                .contains("OK [Backup restored")
                .contains("Unknown command: 'unknown-cmd'")
                .contains("Closing database and exiting J-LSM shell...");
    }

    @Test
    @DisplayName("CLI guided M7 demo executes flush, compaction, SSTable dump, and torn-WAL crash recovery")
    void testGuidedM7DemoSubcommand() {
        Path demoDir = tempDir.resolve("guided-demo");
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);

        int exitCode = JLsmCli.run(
                new String[]{"--dir", demoDir.toString(), "demo"},
                new ByteArrayInputStream(new byte[0]),
                out
        );

        String output = outBytes.toString(StandardCharsets.UTF_8);
        assertThat(exitCode).isZero();
        assertThat(output)
                .contains("MILESTONE M7: GUIDED ARCHITECTURE & CRASH DEMO")
                .contains("[Step 1]")
                .contains("[Step 2]")
                .contains("[Step 3]")
                .contains("[Step 4]")
                .contains("SSTABLE BINARY STRUCTURE DUMP")
                .contains("[Step 5]")
                .contains("RecoveryManager truncated torn tail")
                .contains("M7 GUIDED DEMONSTRATION COMPLETED SUCCESSFULLY");
    }

    @Test
    @DisplayName("CLI benchmark subcommand measures PUT, GET, SCAN, and DELETE performance")
    void testBenchmarkSubcommand() {
        Path benchDir = tempDir.resolve("bench-db");
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);

        int exitCode = JLsmCli.run(
                new String[]{
                        "--dir", benchDir.toString(),
                        "--durability", "BUFFERED",
                        "--memtable-kb", "2",
                        "--compaction-trigger", "3",
                        "benchmark", "80", "48"
                },
                new ByteArrayInputStream(new byte[0]),
                out
        );

        String output = outBytes.toString(StandardCharsets.UTF_8);
        assertThat(exitCode).isZero();
        assertThat(output)
                .contains("J-LSM PERFORMANCE BENCHMARK (M7)")
                .contains("Sequential PUT")
                .contains("Point GET")
                .contains("Range SCAN")
                .contains("Tombstone DELETE")
                .contains("Engine Telemetry");
    }

    @Test
    @DisplayName("SSTableInspector dumps sstable-minimal.bin fixture structure and rejects truncated files")
    void testSSTableInspectorAndDumpSubcommand() throws Exception {
        Path fixtureCopy = tempDir.resolve("000001.sst");
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            Files.copy(in, fixtureCopy);
        }

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);

        int exitCode = JLsmCli.run(
                new String[]{"--dir", tempDir.toString(), "dump-sst", "000001.sst"},
                new ByteArrayInputStream(new byte[0]),
                out
        );

        String output = outBytes.toString(StandardCharsets.UTF_8);
        assertThat(exitCode).isZero();
        assertThat(output)
                .contains("SSTABLE BINARY STRUCTURE DUMP")
                .contains("Magic=0x4A535354 (JSST)")
                .contains("Magic=0x4A4C534D (JLSM)")
                .contains("Data Block #0")
                .contains("key=\"key1\" -> \"value1\"")
                .contains("key=\"key2\" -> <TOMBSTONE>");

        Path tooSmall = tempDir.resolve("small.sst");
        Files.write(tooSmall, new byte[16]);
        assertThatThrownBy(() -> SSTableInspector.dumpSSTable(tooSmall, out))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("too small");
    }

    @Test
    @DisplayName("CLI --help flag and direct single subcommands work cleanly")
    void testHelpFlagAndSingleSubcommands() {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);

        assertThat(JLsmCli.run(new String[]{"--help"}, new ByteArrayInputStream(new byte[0]), out)).isZero();
        assertThat(outBytes.toString(StandardCharsets.UTF_8)).contains("Global CLI Options:");

        Path singleDir = tempDir.resolve("single-cmd");
        outBytes.reset();
        assertThat(JLsmCli.run(new String[]{"--dir", singleDir.toString(), "put", "k1", "v1"},
                new ByteArrayInputStream(new byte[0]), out)).isZero();
        assertThat(outBytes.toString(StandardCharsets.UTF_8)).contains("OK");

        outBytes.reset();
        assertThat(JLsmCli.run(new String[]{"--dir", singleDir.toString(), "get", "k1"},
                new ByteArrayInputStream(new byte[0]), out)).isZero();
        assertThat(outBytes.toString(StandardCharsets.UTF_8)).contains("\"v1\"");

        outBytes.reset();
        assertThat(JLsmCli.run(new String[]{"--dir", singleDir.toString(), "inspect"},
                new ByteArrayInputStream(new byte[0]), out)).isZero();
        assertThat(outBytes.toString(StandardCharsets.UTF_8)).contains("J-LSM STORAGE DIAGNOSTICS REPORT");
    }

    @Test
    @DisplayName("StorageBenchmark validates input arguments")
    void testBenchmarkValidation() {
        PrintStream out = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertThatThrownBy(() -> StorageBenchmark.runBenchmark(tempDir, 0, 32, DurabilityMode.SYNC, 4096L, out))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StorageBenchmark.runBenchmark(tempDir, 10, -1, DurabilityMode.SYNC, 4096L, out))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
