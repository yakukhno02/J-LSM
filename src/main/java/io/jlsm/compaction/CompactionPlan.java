package io.jlsm.compaction;

import io.jlsm.sst.TableHandle;

import java.util.List;
import java.util.Objects;

/**
 * Immutable execution plan selected by a {@link CompactionPlanner}, describing which subset
 * of live {@link TableHandle} instances to merge, the target output file size for SSTable splitting,
 * and whether tombstones can be safely purged.
 *
 * <p>Tombstone purging rule: tombstones may ONLY be purged ({@code retainTombstones == false})
 * when the compaction includes the oldest generation of SSTables in the database
 * ({@code includesBaseGeneration == true}), guaranteeing no shadowed older key versions exist
 * in uncompacted older tables.
 */
public record CompactionPlan(
        List<TableHandle> selectedTables,
        long targetFileSizeBytes,
        boolean retainTombstones,
        boolean includesBaseGeneration,
        int tierIndex
) {
    public CompactionPlan {
        Objects.requireNonNull(selectedTables, "selectedTables cannot be null");
        if (selectedTables.isEmpty()) {
            throw new IllegalArgumentException("selectedTables cannot be empty");
        }
        for (TableHandle handle : selectedTables) {
            Objects.requireNonNull(handle, "selectedTables cannot contain null TableHandle");
        }
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive: " + targetFileSizeBytes);
        }
        if (tierIndex < 0) {
            throw new IllegalArgumentException("tierIndex cannot be negative: " + tierIndex);
        }
        selectedTables = List.copyOf(selectedTables);
    }

    /**
     * Returns the sum of on-disk byte sizes across all selected input SSTables.
     */
    public long totalInputBytes() {
        long sum = 0L;
        for (TableHandle handle : selectedTables) {
            sum += handle.fileSize();
        }
        return sum;
    }

    /**
     * Returns the sum of record counts across all selected input SSTables.
     */
    public long totalInputRecords() {
        long sum = 0L;
        for (TableHandle handle : selectedTables) {
            sum += handle.recordCount();
        }
        return sum;
    }

    /**
     * Returns the minimum sequence number across all selected input SSTables.
     */
    public long minSequence() {
        long min = Long.MAX_VALUE;
        for (TableHandle handle : selectedTables) {
            if (handle.minSequence() < min) {
                min = handle.minSequence();
            }
        }
        return min == Long.MAX_VALUE ? 0L : min;
    }

    /**
     * Returns the maximum sequence number across all selected input SSTables.
     */
    public long maxSequence() {
        long max = Long.MIN_VALUE;
        for (TableHandle handle : selectedTables) {
            if (handle.maxSequence() > max) {
                max = handle.maxSequence();
            }
        }
        return max == Long.MIN_VALUE ? 0L : max;
    }
}
