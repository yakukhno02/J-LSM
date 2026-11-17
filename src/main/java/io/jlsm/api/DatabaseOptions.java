package io.jlsm.api;

import java.util.Objects;

public record DatabaseOptions(
        DurabilityMode durabilityMode,
        long memtableBytes,
        int compactionTrigger,
        int maxOpenScans,
        long maxSnapshotBytes,
        long maxIndexBytes,
        int maxLiveTables
) {

    public DatabaseOptions {
        Objects.requireNonNull(durabilityMode, "durabilityMode cannot be null");
        if (memtableBytes <= 0) {
            throw new IllegalArgumentException("memtableBytes must be positive: " + memtableBytes);
        }
        if (compactionTrigger < 2) {
            throw new IllegalArgumentException("compactionTrigger must be at least 2: " + compactionTrigger);
        }
        if (maxOpenScans <= 0) {
            throw new IllegalArgumentException("maxOpenScans must be positive: " + maxOpenScans);
        }
        if (maxSnapshotBytes <= 0) {
            throw new IllegalArgumentException("maxSnapshotBytes must be positive: " + maxSnapshotBytes);
        }
        if (maxIndexBytes <= 0) {
            throw new IllegalArgumentException("maxIndexBytes must be positive: " + maxIndexBytes);
        }
        if (maxLiveTables < compactionTrigger) {
            throw new IllegalArgumentException("maxLiveTables cannot be less than compactionTrigger");
        }
    }

    public static DatabaseOptions defaults() {
        return new DatabaseOptions(
                DurabilityMode.SYNC,
                4L * 1024 * 1024,
                4,
                4,
                8L * 1024 * 1024,
                32L * 1024 * 1024,
                16
        );
    }

    public DatabaseOptions withDurabilityMode(DurabilityMode durabilityMode) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public DatabaseOptions withMemtableBytes(long memtableBytes) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public DatabaseOptions withCompactionTrigger(int compactionTrigger) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public DatabaseOptions withMaxOpenScans(int maxOpenScans) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public DatabaseOptions withMaxLiveTables(int maxLiveTables) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public DatabaseOptions withMaxSnapshotBytes(long maxSnapshotBytes) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public DatabaseOptions withMaxIndexBytes(long maxIndexBytes) {
        return new DatabaseOptions(durabilityMode, memtableBytes, compactionTrigger, maxOpenScans, maxSnapshotBytes, maxIndexBytes, maxLiveTables);
    }

    public boolean isSyncDurability() {
        return durabilityMode == DurabilityMode.SYNC;
    }

    public long totalEstimatedMemoryUsage() {
        return (memtableBytes * 2) + maxSnapshotBytes + maxIndexBytes;
    }
}
