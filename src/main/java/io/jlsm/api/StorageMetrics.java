package io.jlsm.api;

public record StorageMetrics(
        long putsCount,
        long getsCount,
        long deletesCount,
        long scansCount,
        long activeMemTableBytes,
        long immutableMemTableBytes,
        long totalMemoryBytes,
        long maxMemoryBudgetBytes,
        int liveTablesCount,
        long totalSSTableBytes,
        long totalWalBytes,
        int activeScansCount,
        long flushCount,
        long compactionCount,
        long backpressureWaitNanos,
        long highSequence
) {

    public double memoryUtilization() {
        if (maxMemoryBudgetBytes <= 0) {
            return 0.0;
        }
        return Math.min(1.0, (double) totalMemoryBytes / (double) maxMemoryBudgetBytes);
    }

    public long totalDiskUsageBytes() {
        return totalSSTableBytes + totalWalBytes;
    }

    public boolean isMemoryPressured() {
        return memoryUtilization() >= 0.80;
    }

    public long totalOperations() {
        return putsCount + getsCount + deletesCount + scansCount;
    }

    public double writeOpsRatio() {
        long total = totalOperations();
        if (total == 0L) {
            return 0.0;
        }
        return (double) (putsCount + deletesCount) / (double) total;
    }

    public double readOpsRatio() {
        long total = totalOperations();
        if (total == 0L) {
            return 0.0;
        }
        return (double) (getsCount + scansCount) / (double) total;
    }


    public String toSummaryString() {
        return String.format(
                "StorageMetrics[ops(put=%d, get=%d, del=%d, scan=%d), mem=%.2fMB/%.2fMB (%.1f%%), tables=%d (%.2fMB), wal=%.2fMB, flushes=%d, compactions=%d]",
                putsCount, getsCount, deletesCount, scansCount,
                totalMemoryBytes / (1024.0 * 1024.0),
                maxMemoryBudgetBytes / (1024.0 * 1024.0),
                memoryUtilization() * 100.0,
                liveTablesCount,
                totalSSTableBytes / (1024.0 * 1024.0),
                totalWalBytes / (1024.0 * 1024.0),
                flushCount,
                compactionCount
        );
    }
}
