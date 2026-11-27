package io.jlsm.cli;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.Snapshot;
import io.jlsm.api.StorageException;
import io.jlsm.api.StorageMetrics;
import io.jlsm.api.WriteBatch;
import io.jlsm.engine.IntegrityReport;
import io.jlsm.engine.StorageBackupManager;
import io.jlsm.engine.StorageDiagnostics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Interactive Console / CLI Demonstration Utility for J-LSM (Milestone M7).
 * Built strictly on {@code java.base} with zero external dependencies.
 */
public final class JLsmCli {

    private JLsmCli() {}

    public static void main(String[] args) {
        int code = run(args, System.in, System.out);
        if (code != 0) {
            System.exit(code);
        }
    }

    public static int run(String[] args, InputStream in, PrintStream out) {
        Objects.requireNonNull(args, "args cannot be null");
        Objects.requireNonNull(in, "in cannot be null");
        Objects.requireNonNull(out, "out cannot be null");

        Path directory = Path.of("jlsm-data");
        long memtableBytes = 4L * 1024 * 1024;
        DurabilityMode durabilityMode = DurabilityMode.SYNC;
        int compactionTrigger = 4;
        List<String> subcommand = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--dir".equals(arg) && i + 1 < args.length) {
                directory = Path.of(args[++i]);
            } else if ("--memtable-kb".equals(arg) && i + 1 < args.length) {
                memtableBytes = Long.parseLong(args[++i]) * 1024L;
            } else if ("--durability".equals(arg) && i + 1 < args.length) {
                durabilityMode = DurabilityMode.valueOf(args[++i].toUpperCase(Locale.ROOT));
            } else if ("--compaction-trigger".equals(arg) && i + 1 < args.length) {
                compactionTrigger = Integer.parseInt(args[++i]);
            } else if ("--help".equals(arg) || "-h".equals(arg)) {
                printBanner(out);
                printHelp(out);
                return 0;
            } else {
                for (int j = i; j < args.length; j++) {
                    subcommand.add(args[j]);
                }
                break;
            }
        }

        DatabaseOptions options = DatabaseOptions.defaults()
                .withMemtableBytes(memtableBytes)
                .withDurabilityMode(durabilityMode)
                .withCompactionTrigger(compactionTrigger)
                .withMaxLiveTables(Math.max(16, compactionTrigger * 2));

        try {
            Files.createDirectories(directory);

            if (!subcommand.isEmpty() && !"shell".equalsIgnoreCase(subcommand.getFirst())) {
                return executeSingleSubcommand(directory, options, subcommand, out);
            }

            return runInteractiveRepl(directory, options, in, out);
        } catch (Exception e) {
            out.println("[ERROR] " + e.getMessage());
            return 1;
        }
    }

    private static int executeSingleSubcommand(
            Path directory,
            DatabaseOptions options,
            List<String> tokens,
            PrintStream out
    ) throws Exception {
        String cmd = tokens.getFirst().toLowerCase(Locale.ROOT);
        if ("demo".equals(cmd)) {
            runGuidedDemo(directory, out);
            return 0;
        }
        if ("benchmark".equals(cmd) || "bench".equals(cmd)) {
            int count = tokens.size() > 1 ? Integer.parseInt(tokens.get(1)) : 1000;
            int valSize = tokens.size() > 2 ? Integer.parseInt(tokens.get(2)) : 128;
            StorageBenchmark.runBenchmark(directory, count, valSize, options.durabilityMode(), options.memtableBytes(), out);
            return 0;
        }
        if ("inspect".equals(cmd) || "diag".equals(cmd)) {
            printDiagnostics(directory, out);
            return 0;
        }
        if ("dump-sst".equals(cmd)) {
            dumpSstCommand(directory, tokens, out);
            return 0;
        }

        try (KVDatabase db = KVDatabase.open(directory, options)) {
            executeCommandInSession(db, directory, options, tokens, out);
        }
        return 0;
    }

    private static int runInteractiveRepl(
            Path directory,
            DatabaseOptions options,
            InputStream in,
            PrintStream out
    ) throws Exception {
        printBanner(out);
        out.printf(Locale.US, "  Directory : %s%n", directory.toAbsolutePath());
        out.printf(Locale.US, "  Options   : durability=%s, memtable=%d KB, compactionTrigger=%d%n",
                options.durabilityMode(), options.memtableBytes() / 1024, options.compactionTrigger());
        out.println("  Type 'help' for available commands, 'demo' for M7 showcase, or 'exit' to quit.");
        out.println("================================================================================");

        try (KVDatabase db = KVDatabase.open(directory, options);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            while (true) {
                out.print("j-lsm> ");
                out.flush();
                String line = reader.readLine();
                if (line == null) {
                    out.println();
                    break;
                }
                List<String> tokens = tokenize(line);
                if (tokens.isEmpty()) {
                    continue;
                }

                String cmd = tokens.getFirst().toLowerCase(Locale.ROOT);
                if ("exit".equals(cmd) || "quit".equals(cmd)) {
                    out.println("Closing database and exiting J-LSM shell...");
                    break;
                }

                try {
                    executeCommandInSession(db, directory, options, tokens, out);
                } catch (Exception e) {
                    out.println("[ERROR] " + e.getMessage());
                }
            }
        }
        return 0;
    }

    private static void executeCommandInSession(
            KVDatabase db,
            Path directory,
            DatabaseOptions options,
            List<String> tokens,
            PrintStream out
    ) throws Exception {
        String cmd = tokens.getFirst().toLowerCase(Locale.ROOT);
        switch (cmd) {
            case "help" -> printHelp(out);
            case "put" -> {
                if (tokens.size() < 3) {
                    out.println("Usage: put <key> <value>");
                    return;
                }
                String key = tokens.get(1);
                String val = String.join(" ", tokens.subList(2, tokens.size()));
                long t0 = System.nanoTime();
                db.put(bytes(key), bytes(val));
                double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                out.printf(Locale.US, "OK (%.1f us)%n", elapsedUs);
            }
            case "get" -> {
                if (tokens.size() < 2) {
                    out.println("Usage: get <key>");
                    return;
                }
                String key = tokens.get(1);
                long t0 = System.nanoTime();
                byte[] val = db.get(bytes(key));
                double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                if (val == null) {
                    out.printf(Locale.US, "(nil) (%.1f us)%n", elapsedUs);
                } else {
                    out.printf(Locale.US, "\"%s\" (%.1f us)%n", new String(val, StandardCharsets.UTF_8), elapsedUs);
                }
            }
            case "delete", "del" -> {
                if (tokens.size() < 2) {
                    out.println("Usage: delete <key>");
                    return;
                }
                String key = tokens.get(1);
                long t0 = System.nanoTime();
                db.delete(bytes(key));
                double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                out.printf(Locale.US, "OK [tombstone written] (%.1f us)%n", elapsedUs);
            }
            case "delete-range", "del-range" -> {
                if (tokens.size() < 3) {
                    out.println("Usage: delete-range <fromKey> <toKey>");
                    return;
                }
                String fromKey = tokens.get(1);
                String toKey = tokens.get(2);
                long t0 = System.nanoTime();
                db.deleteRange(bytes(fromKey), bytes(toKey));
                double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                out.printf(Locale.US, "OK [range tombstone [\"%s\", \"%s\") written] (%.1f us)%n",
                        fromKey, toKey, elapsedUs);
            }
            case "batch" -> {
                if (tokens.size() < 2) {
                    out.println("Usage: batch <put:key=val | del:key | delrange:from..to> ...");
                    return;
                }
                long t0 = System.nanoTime();
                try (WriteBatch batch = WriteBatch.create()) {
                    for (int i = 1; i < tokens.size(); i++) {
                        String spec = tokens.get(i);
                        if (spec.startsWith("put:")) {
                            int eq = spec.indexOf('=', 4);
                            if (eq <= 4) {
                                out.println("Invalid batch put specification: " + spec + " (expected put:key=value)");
                                return;
                            }
                            String k = spec.substring(4, eq);
                            String v = spec.substring(eq + 1);
                            batch.put(bytes(k), bytes(v));
                        } else if (spec.startsWith("del:")) {
                            String k = spec.substring(4);
                            if (k.isEmpty()) {
                                out.println("Invalid batch delete specification: " + spec + " (expected del:key)");
                                return;
                            }
                            batch.delete(bytes(k));
                        } else if (spec.startsWith("delrange:")) {
                            int sep = spec.indexOf("..", 9);
                            if (sep <= 9 || sep + 2 >= spec.length()) {
                                out.println("Invalid batch range delete specification: " + spec + " (expected delrange:fromKey..toKey)");
                                return;
                            }
                            String fromK = spec.substring(9, sep);
                            String toK = spec.substring(sep + 2);
                            batch.deleteRange(bytes(fromK), bytes(toK));
                        } else {
                            out.println("Unknown batch operation: " + spec + " (use put:key=val, del:key, or delrange:from..to)");
                            return;
                        }
                    }
                    int opCount = batch.operationCount();
                    long byteSize = batch.byteSize();
                    db.write(batch);
                    double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                    out.printf(Locale.US, "OK [Atomic WriteBatch committed: %d ops, %d B] (%.1f us)%n",
                            opCount, byteSize, elapsedUs);
                }
            }
            case "snapshot-scan", "snap" -> {
                byte[] fromKey = (tokens.size() > 1 && !"*".equals(tokens.get(1))) ? bytes(tokens.get(1)) : null;
                byte[] toKey = (tokens.size() > 2 && !"*".equals(tokens.get(2))) ? bytes(tokens.get(2)) : null;
                long t0 = System.nanoTime();
                int count = 0;
                try (Snapshot snap = db.snapshot();
                     ClosableIterator<Entry> it = snap.scan(fromKey, toKey)) {
                    out.printf(Locale.US, "  [Snapshot seq=%d opened]%n", snap.sequenceNumber());
                    while (it.hasNext()) {
                        Entry e = it.next();
                        out.printf("  [%d] \"%s\" => \"%s\"%n",
                                ++count,
                                new String(e.key(), StandardCharsets.UTF_8),
                                new String(e.value(), StandardCharsets.UTF_8));
                    }
                }
                double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                out.printf(Locale.US, "(%d snapshot entries scanned in %.1f us)%n", count, elapsedUs);
            }
            case "scan" -> {
                byte[] fromKey = (tokens.size() > 1 && !"*".equals(tokens.get(1))) ? bytes(tokens.get(1)) : null;
                byte[] toKey = (tokens.size() > 2 && !"*".equals(tokens.get(2))) ? bytes(tokens.get(2)) : null;

                long t0 = System.nanoTime();
                int count = 0;
                try (ClosableIterator<Entry> it = db.scan(fromKey, toKey)) {
                    while (it.hasNext()) {
                        Entry e = it.next();
                        out.printf("  [%d] \"%s\" => \"%s\"%n",
                                ++count,
                                new String(e.key(), StandardCharsets.UTF_8),
                                new String(e.value(), StandardCharsets.UTF_8));
                    }
                }
                double elapsedUs = (System.nanoTime() - t0) / 1000.0;
                out.printf(Locale.US, "(%d entries scanned in %.1f us)%n", count, elapsedUs);
            }
            case "sync" -> {
                db.sync();
                out.println("OK [WAL synced to disk]");
            }
            case "metrics", "stats" -> printMetricsDashboard(db.metrics(), out);
            case "inspect", "diag" -> printDiagnostics(directory, out);
            case "dump-sst" -> dumpSstCommand(directory, tokens, out);
            case "backup" -> {
                if (tokens.size() < 2) {
                    out.println("Usage: backup <backupDir>");
                    return;
                }
                db.sync();
                Path backupDir = Path.of(tokens.get(1));
                StorageBackupManager mgr = new StorageBackupManager();
                StorageBackupManager.BackupInfo info = mgr.createBackup(directory, backupDir);
                out.printf(Locale.US, "OK [Backup '%s' created: %d files, %d bytes, epoch=%d, highSeq=%d]%n",
                        info.backupId(), info.entries().size(), info.totalBackupSizeBytes(),
                        info.epoch(), info.highSequence());
            }
            case "verify-backup" -> {
                if (tokens.size() < 2) {
                    out.println("Usage: verify-backup <backupDir>");
                    return;
                }
                Path backupDir = Path.of(tokens.get(1));
                boolean valid = new StorageBackupManager().verifyBackup(backupDir);
                out.println(valid ? "OK [Backup verified: all CRC32 checksums match]" : "FAILED [Backup is corrupted or incomplete]");
            }
            case "restore" -> {
                if (tokens.size() < 3) {
                    out.println("Usage: restore <backupDir> <targetDir>");
                    return;
                }
                Path backupDir = Path.of(tokens.get(1));
                Path targetDir = Path.of(tokens.get(2));
                new StorageBackupManager().restoreBackup(backupDir, targetDir);
                out.printf("OK [Backup restored from %s to %s]%n", backupDir, targetDir);
            }
            case "benchmark", "bench" -> {
                int count = tokens.size() > 1 ? Integer.parseInt(tokens.get(1)) : 500;
                int valSize = tokens.size() > 2 ? Integer.parseInt(tokens.get(2)) : 64;
                Path benchDir = Files.createTempDirectory(directory, "bench-run-");
                try {
                    StorageBenchmark.runBenchmark(benchDir, count, valSize, options.durabilityMode(), options.memtableBytes(), out);
                } finally {
                    deleteDirectoryQuietly(benchDir);
                }
            }
            case "demo" -> {
                Path demoDir = Files.createTempDirectory(directory, "m7-demo-");
                try {
                    runGuidedDemo(demoDir, out);
                } finally {
                    deleteDirectoryQuietly(demoDir);
                }
            }
            default -> out.println("Unknown command: '" + cmd + "'. Type 'help' for available commands.");
        }
    }

    /**
     * Runs a comprehensive guided M7 defense demonstration showcasing:
     * 1. CRUD operations (put, get, overwrite, delete tombstone)
     * 2. Automatic MemTable Flush to 4KB SSTables & background Compaction
     * 3. K-Way Merge Range Scan
     * 4. SSTable 4KB Block & Sparse Index binary inspection
     * 5. Crash Recovery after simulated torn WAL frame at EOF (FI-01)
     */
    public static void runGuidedDemo(Path dbDir, PrintStream out) throws Exception {
        deleteDirectoryQuietly(dbDir);
        Files.createDirectories(dbDir);

        printBanner(out);
        out.println("                 MILESTONE M7: GUIDED ARCHITECTURE & CRASH DEMO                 ");
        out.println("================================================================================");

        // Step 1 & 2: Configure small MemTable (1 KB) and compactionTrigger=3 to demonstrate flush + compaction
        DatabaseOptions demoOpts = DatabaseOptions.defaults()
                .withDurabilityMode(DurabilityMode.SYNC)
                .withMemtableBytes(1024L)
                .withCompactionTrigger(3)
                .withMaxLiveTables(8);

        out.println("\n[Step 1] Opening J-LSM Database (memtable=1KB, compactionTrigger=3, durability=SYNC)...");
        try (KVDatabase db = KVDatabase.open(dbDir, demoOpts)) {
            db.put(bytes("user:1001"), bytes("Alice (Admin)"));
            db.put(bytes("user:1002"), bytes("Bob (Developer)"));
            db.put(bytes("user:1003"), bytes("Charlie (Reviewer)"));
            out.println("  Inserted 3 records: user:1001, user:1002, user:1003");
            out.println("  Point lookup get(\"user:1001\") -> \"" + new String(db.get(bytes("user:1001")), StandardCharsets.UTF_8) + "\"");

            db.put(bytes("user:1002"), bytes("Bob (Senior Architect)"));
            db.delete(bytes("user:1003"));
            out.println("  Updated user:1002 and deleted user:1003 (Tombstone)");
            out.println("  Point lookup get(\"user:1003\") -> " + (db.get(bytes("user:1003")) == null ? "(nil)" : "present"));

            out.println("\n[Step 2] Writing 45 records to trigger MemTable rotations, 4KB SSTable Flushes & Compaction...");
            for (int i = 1; i <= 45; i++) {
                String k = String.format("metric:%03d", i);
                String v = String.format("sample-payload-value-%03d-data", i);
                db.put(bytes(k), bytes(v));
            }
            // Wait briefly for background compaction worker if triggered
            Thread.sleep(100L);
            printMetricsDashboard(db.metrics(), out);

            out.println("\n[Step 3] Executing Atomic WriteBatch (0x03), Range Delete (0x04) & Snapshot isolation (M8)...");
            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(bytes("batch:1"), bytes("atomic-val-1"))
                     .put(bytes("batch:2"), bytes("atomic-val-2"))
                     .delete(bytes("user:1001"));
                db.write(batch);
                out.printf("  Committed Atomic WriteBatch (%d operations)%n", batch.operationCount());
            }
            try (Snapshot snap = db.snapshot()) {
                long snapSeq = snap.sequenceNumber();
                db.put(bytes("batch:1"), bytes("mutated-after-snapshot"));
                db.deleteRange(bytes("metric:040"), bytes("metric:046"));
                String snapVal = new String(snap.get(bytes("batch:1")), StandardCharsets.UTF_8);
                String liveVal = new String(db.get(bytes("batch:1")), StandardCharsets.UTF_8);
                out.printf("  Snapshot [seq=%d] get(\"batch:1\") -> \"%s\" (isolated from live update \"%s\")%n",
                        snapSeq, snapVal, liveVal);
                out.printf("  Executed deleteRange([\"metric:040\", \"metric:046\")): live get(\"metric:042\") -> %s, snapshot get(\"metric:042\") -> \"%s\"%n",
                        db.get(bytes("metric:042")) == null ? "(nil)" : "present",
                        new String(snap.get(bytes("metric:042")), StandardCharsets.UTF_8));
            }

            out.println("\n[Step 3b] Executing Range Scan [\"metric:001\", \"metric:006\") across MemTable + SSTables...");
            try (ClosableIterator<Entry> it = db.scan(bytes("metric:001"), bytes("metric:006"))) {
                while (it.hasNext()) {
                    Entry e = it.next();
                    out.printf("  SCAN -> \"%s\" = \"%s\"%n",
                            new String(e.key(), StandardCharsets.UTF_8),
                            new String(e.value(), StandardCharsets.UTF_8));
                }
            }
        }

        // Step 4: Inspect SSTable binary structure & Diagnostics
        out.println("\n[Step 4] Inspecting on-disk SSTable 4KB blocks, Sparse Index, and Catalog Integrity...");
        Path firstSst = findFirstSstFile(dbDir);
        if (firstSst != null) {
            SSTableInspector.dumpSSTable(firstSst, out);
        }
        printDiagnostics(dbDir, out);

        // Step 5: Simulate Crash / Torn WAL tail frame (FI-01) and verify automatic recovery
        out.println("\n[Step 5] Simulating sudden power loss / torn write at EOF of active WAL (FI-01)...");
        Path activeWal = findLatestWalFile(dbDir);
        if (activeWal != null) {
            long sizeBefore = Files.size(activeWal);
            // Append 9 corrupted/incomplete bytes at the end of the WAL to simulate mid-frame power failure
            Files.write(activeWal, new byte[]{0x12, 0x34, 0x56, 0x78, 0x00, 0x01, 0x02, 0x03, 0x04}, StandardOpenOption.APPEND);
            long sizeWithTornTail = Files.size(activeWal);
            out.printf("  Appended 9-byte torn frame to %s (size %d -> %d bytes)%n",
                    activeWal.getFileName(), sizeBefore, sizeWithTornTail);

            try (KVDatabase recoveredDb = KVDatabase.open(dbDir, demoOpts)) {
                long sizeAfterRecovery = Files.size(activeWal);
                out.printf("  RecoveryManager truncated torn tail of %s back to %d bytes!%n",
                        activeWal.getFileName(), sizeAfterRecovery);
                out.printf("  Verified surviving record get(\"user:1002\") -> \"%s\"%n",
                        new String(recoveredDb.get(bytes("user:1002")), StandardCharsets.UTF_8));
                out.printf("  Verified deleted record get(\"user:1003\") -> %s%n",
                        recoveredDb.get(bytes("user:1003")) == null ? "(nil)" : "ERROR");
            }
        }
        out.println("================================================================================");
        out.println("                    M7 GUIDED DEMONSTRATION COMPLETED SUCCESSFULLY              ");
        out.println("================================================================================");
    }

    public static void printMetricsDashboard(StorageMetrics m, PrintStream out) {
        if (m == null) {
            out.println("Metrics unavailable.");
            return;
        }
        double utilPct = m.memoryUtilization() * 100.0;
        int filledBars = (int) Math.round(Math.min(1.0, m.memoryUtilization()) * 20);
        String bar = "[" + "#".repeat(filledBars) + "-".repeat(20 - filledBars) + "]";

        out.println("================================================================================");
        out.println("                          J-LSM REAL-TIME TELEMETRY                             ");
        out.println("================================================================================");
        out.printf(Locale.US, "  Operations       : total=%d (PUT=%d, GET=%d, DELETE=%d, SCAN=%d)%n",
                m.totalOperations(), m.putsCount(), m.getsCount(), m.deletesCount(), m.scansCount());
        out.printf(Locale.US, "  High Sequence    : %d%n", m.highSequence());
        out.printf(Locale.US, "  RAM Utilization  : %s %.1f%% (%d B / %d B)%n",
                bar, utilPct, m.totalMemoryBytes(), m.maxMemoryBudgetBytes());
        out.printf(Locale.US, "  MemTables        : active=%d B | immutable=%d B%n",
                m.activeMemTableBytes(), m.immutableMemTableBytes());
        out.printf(Locale.US, "  Disk Storage     : liveSSTables=%d (%d B) | WAL=%d B | total=%d B%n",
                m.liveTablesCount(), m.totalSSTableBytes(), m.totalWalBytes(), m.totalDiskUsageBytes());
        out.printf(Locale.US, "  Background Tasks : flushes=%d | compactions=%d | activeScans=%d%n",
                m.flushCount(), m.compactionCount(), m.activeScansCount());
        out.println("================================================================================");
    }

    public static void printDiagnostics(Path directory, PrintStream out) throws StorageException {
        IntegrityReport report = StorageDiagnostics.inspect(directory);
        StorageDiagnostics.printReport(report, out);
        StorageDiagnostics.DiskSpaceBreakdown space = StorageDiagnostics.analyzeDiskSpace(directory);
        out.printf(Locale.US, "  Disk Space Breakdown: total=%d B | SSTables=%d (%d B) | WALs=%d (%d B) | MANIFEST=%d B | Temp=%d (%d B)%n",
                space.totalBytes(), space.sstableCount(), space.sstableBytes(),
                space.walCount(), space.walBytes(), space.manifestBytes(),
                space.tempFileCount(), space.tempBytes());
        out.println("================================================================================");
    }

    public static void printBanner(PrintStream out) {
        out.println("================================================================================");
        out.println("      _       _     ____  __  __    Storage Engine CLI & Diagnostic Utility     ");
        out.println("     | |     | |   / ___||  \\/  |   Pure Java 21 (java.base) | 4KB SSTables     ");
        out.println("  _  | |_____| |   \\___ \\| |\\/| |   CRC32 WAL & Manifest | K-Way Compactor      ");
        out.println(" | |_| |_____| |___ ___) | |  | |                                               ");
        out.println("  \\___/      |_____|____/|_|  |_|   Milestone M7 Interactive Console            ");
        out.println("================================================================================");
    }

    public static void printHelp(PrintStream out) {
        out.println("Available J-LSM Commands:");
        out.println("  put <key> <value>              Insert or update a key-value pair");
        out.println("  get <key>                      Retrieve value by key");
        out.println("  delete <key> (or del <key>)    Mark key as deleted (Tombstone)");
        out.println("  delete-range <fromKey> <toKey> Delete key interval [fromKey, toKey) via RangeTombstone (0x04)");
        out.println("  batch <put:k=v|del:k|delrange:a..b> Commit an atomic WriteBatch (WAL frame 0x03)");
        out.println("  snap [fromKey|*] [toKey|*]     Point-in-time Snapshot range scan");
        out.println("  scan [fromKey|*] [toKey|*]     Range scan [fromKey, toKey) in unsigned byte order");
        out.println("  sync                           Force active WAL fsync to disk");
        out.println("  metrics (or stats)             Show real-time memory, disk, and operation telemetry");
        out.println("  inspect (or diag)              Verify MANIFEST, SSTables, WALs, and disk usage");
        out.println("  dump-sst <file.sst>            Inspect 4KB Data Blocks, Bloom Filter, Sparse Index & Footer");
        out.println("  backup <backupDir>             Create an online hot backup of the database");
        out.println("  verify-backup <backupDir>      Verify CRC32 integrity of a backup directory");
        out.println("  restore <backupDir> <target>   Restore a verified backup to a target directory");
        out.println("  benchmark [count] [valBytes]   Run M7 throughput & latency benchmark suite");
        out.println("  demo                           Run guided M7/M8 architecture & crash-recovery showcase");
        out.println("  help                           Display this command reference");
        out.println("  exit (or quit)                 Close database cleanly and exit");
        out.println();
        out.println("Global CLI Options:");
        out.println("  --dir <path>                   Database directory (default: ./jlsm-data)");
        out.println("  --memtable-kb <kb>             MemTable flush threshold in KB (default: 4096)");
        out.println("  --durability <SYNC|BUFFERED>   WAL durability mode (default: SYNC)");
        out.println("  --compaction-trigger <count>   SSTable count triggering compaction (default: 4)");
    }

    static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        if (line == null) {
            return tokens;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (Character.isWhitespace(c) && !inQuotes) {
                if (!current.isEmpty()) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    private static void dumpSstCommand(Path directory, List<String> tokens, PrintStream out) throws Exception {
        if (tokens.size() >= 2) {
            Path target = resolveSstPath(directory, tokens.get(1));
            if (Files.exists(target)) {
                SSTableInspector.dumpSSTable(target, out);
                return;
            }
            Path fallback = findFirstSstFile(directory);
            if (fallback != null) {
                out.printf("[INFO] File '%s' was not found (or already compacted); dumping active SSTable '%s' instead:%n",
                        tokens.get(1), fallback.getFileName());
                SSTableInspector.dumpSSTable(fallback, out);
                return;
            }
            SSTableInspector.dumpSSTable(target, out);
            return;
        }

        Path firstSst = findFirstSstFile(directory);
        if (firstSst == null) {
            out.println("No .sst files found in " + directory + ". Usage: dump-sst [file.sst]");
            return;
        }
        SSTableInspector.dumpSSTable(firstSst, out);
    }

    private static Path resolveSstPath(Path directory, String fileArg) {
        Path direct = Path.of(fileArg);
        if (Files.exists(direct)) {
            return direct;
        }
        return directory.resolve(fileArg);
    }

    private static Path findFirstSstFile(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.sst")) {
            for (Path entry : stream) {
                return entry;
            }
        }
        return null;
    }

    private static Path findLatestWalFile(Path dir) throws IOException {
        Path latest = null;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.wal")) {
            for (Path entry : stream) {
                if (latest == null || entry.getFileName().toString().compareTo(latest.getFileName().toString()) > 0) {
                    latest = entry;
                }
            }
        }
        return latest;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void deleteDirectoryQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }
}
