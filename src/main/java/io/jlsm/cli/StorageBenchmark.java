package io.jlsm.cli;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import io.jlsm.api.StorageMetrics;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

/**
 * Milestone M7 performance measurement utility for J-LSM.
 * Measures throughput (ops/sec), average latency (us/op), P99 latency (us/op),
 * and flush/compaction telemetry across put, get, scan, and delete workloads.
 */
public final class StorageBenchmark {

    public record PhaseResult(
            String phaseName,
            int operations,
            long totalNanos,
            double opsPerSec,
            double avgLatencyMicros,
            double p99LatencyMicros
    ) {}

    public record BenchmarkReport(
            int recordCount,
            int valueSizeBytes,
            PhaseResult sequentialPut,
            PhaseResult pointGet,
            PhaseResult rangeScan,
            PhaseResult deletePhase,
            StorageMetrics finalMetrics
    ) {}

    private StorageBenchmark() {}

    public static BenchmarkReport runBenchmark(
            Path directory,
            int recordCount,
            int valueSizeBytes,
            DurabilityMode durabilityMode,
            long memtableBytes,
            PrintStream out
    ) throws StorageException {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(durabilityMode, "durabilityMode cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive: " + recordCount);
        }
        if (valueSizeBytes < 0 || valueSizeBytes > 3000) {
            throw new IllegalArgumentException("valueSizeBytes must be between 0 and 3000: " + valueSizeBytes);
        }

        DatabaseOptions options = DatabaseOptions.defaults()
                .withDurabilityMode(durabilityMode)
                .withMemtableBytes(memtableBytes);

        byte[][] keys = new byte[recordCount][];
        byte[][] values = new byte[recordCount][];
        String pad = "x".repeat(Math.max(0, valueSizeBytes));
        for (int i = 0; i < recordCount; i++) {
            keys[i] = String.format("bench-key-%08d", i).getBytes(StandardCharsets.UTF_8);
            values[i] = (pad + i).substring(0, valueSizeBytes).getBytes(StandardCharsets.UTF_8);
        }

        try (KVDatabase db = KVDatabase.open(directory, options)) {
            // 1. Sequential PUT phase
            long[] putLatencies = new long[recordCount];
            long putStart = System.nanoTime();
            for (int i = 0; i < recordCount; i++) {
                long t0 = System.nanoTime();
                db.put(keys[i], values[i]);
                putLatencies[i] = System.nanoTime() - t0;
            }
            db.sync();
            long putTotalNanos = System.nanoTime() - putStart;
            PhaseResult putResult = computePhase("Sequential PUT", recordCount, putTotalNanos, putLatencies);

            // 2. Point GET phase
            long[] getLatencies = new long[recordCount];
            long getStart = System.nanoTime();
            for (int i = 0; i < recordCount; i++) {
                int idx = (i * 31) % recordCount;
                long t0 = System.nanoTime();
                byte[] val = db.get(keys[idx]);
                getLatencies[i] = System.nanoTime() - t0;
                if (val == null) {
                    throw new IllegalStateException("Benchmark verification failed: missing key at index " + idx);
                }
            }
            long getTotalNanos = System.nanoTime() - getStart;
            PhaseResult getResult = computePhase("Point GET", recordCount, getTotalNanos, getLatencies);

            // 3. Full Range SCAN phase
            int scannedCount = 0;
            long scanStart = System.nanoTime();
            try (ClosableIterator<Entry> it = db.scan(null, null)) {
                while (it.hasNext()) {
                    it.next();
                    scannedCount++;
                }
            }
            long scanTotalNanos = System.nanoTime() - scanStart;
            PhaseResult scanResult = computePhase("Range SCAN (entries)", Math.max(1, scannedCount), scanTotalNanos,
                    new long[]{scanTotalNanos / Math.max(1, scannedCount)});

            // 4. Delete 20% of keys phase
            int deleteCount = Math.max(1, recordCount / 5);
            long[] delLatencies = new long[deleteCount];
            long delStart = System.nanoTime();
            for (int i = 0; i < deleteCount; i++) {
                long t0 = System.nanoTime();
                db.delete(keys[i]);
                delLatencies[i] = System.nanoTime() - t0;
            }
            db.sync();
            long delTotalNanos = System.nanoTime() - delStart;
            PhaseResult delResult = computePhase("Tombstone DELETE", deleteCount, delTotalNanos, delLatencies);

            StorageMetrics metrics = db.metrics();
            BenchmarkReport report = new BenchmarkReport(
                    recordCount, valueSizeBytes, putResult, getResult, scanResult, delResult, metrics);
            printBenchmarkReport(report, durabilityMode, memtableBytes, out);
            return report;
        }
    }

    public static void printBenchmarkReport(
            BenchmarkReport report,
            DurabilityMode mode,
            long memtableBytes,
            PrintStream out
    ) {
        out.println("================================================================================");
        out.println("                          J-LSM PERFORMANCE BENCHMARK (M7)                      ");
        out.println("================================================================================");
        out.printf(Locale.US, "  Records: %d | Value Size: %d B | Durability: %s | MemTable Limit: %d KB%n",
                report.recordCount(), report.valueSizeBytes(), mode, memtableBytes / 1024);
        out.println("--------------------------------------------------------------------------------");
        out.printf(Locale.US, "  %-22s | %10s | %12s | %10s | %10s%n",
                "Workload Phase", "Operations", "Throughput", "Avg (us)", "P99 (us)");
        out.println("--------------------------------------------------------------------------------");

        for (PhaseResult phase : ListOfPhases(report)) {
            out.printf(Locale.US, "  %-22s | %10d | %8.0f op/s | %10.2f | %10.2f%n",
                    phase.phaseName(),
                    phase.operations(),
                    phase.opsPerSec(),
                    phase.avgLatencyMicros(),
                    phase.p99LatencyMicros());
        }
        out.println("--------------------------------------------------------------------------------");
        if (report.finalMetrics() != null) {
            StorageMetrics m = report.finalMetrics();
            out.printf(Locale.US, "  Engine Telemetry : Flushes=%d | Compactions=%d | Live SSTables=%d | HighSeq=%d%n",
                    m.flushCount(), m.compactionCount(), m.liveTablesCount(), m.highSequence());
            out.printf(Locale.US, "  Disk Footprint   : SSTables=%.2f KB | WAL=%.2f KB | RAM Used=%.2f KB%n",
                    m.totalSSTableBytes() / 1024.0,
                    m.totalWalBytes() / 1024.0,
                    m.totalMemoryBytes() / 1024.0);
        }
        out.println("================================================================================");
    }

    private static PhaseResult[] ListOfPhases(BenchmarkReport r) {
        return new PhaseResult[]{r.sequentialPut(), r.pointGet(), r.rangeScan(), r.deletePhase()};
    }

    private static PhaseResult computePhase(String name, int ops, long totalNanos, long[] latenciesNanos) {
        long safeNanos = Math.max(1L, totalNanos);
        double opsPerSec = (ops * 1_000_000_000.0) / safeNanos;
        double avgMicros = (safeNanos / 1000.0) / ops;

        long[] sorted = latenciesNanos.clone();
        Arrays.sort(sorted);
        int p99Index = Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * 0.99) - 1);
        double p99Micros = sorted[Math.max(0, p99Index)] / 1000.0;

        return new PhaseResult(name, ops, safeNanos, opsPerSec, avgMicros, p99Micros);
    }
}
