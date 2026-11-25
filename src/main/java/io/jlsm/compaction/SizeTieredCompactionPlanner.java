package io.jlsm.compaction;

import io.jlsm.sst.TableHandle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Size-Tiered Compaction Strategy (STCS) planner that groups contiguous SSTables of comparable
 * byte sizes into buckets (tiers) and selects the best qualifying run for compaction.
 *
 * <h3>Algorithm Overview</h3>
 * <ol>
 *   <li>Live tables are inspected in chronological order (oldest first, by {@code minSequence} / {@code id})
 *       so that any compacted slice preserves strict sequence ordering without overlapping an uncompacted
 *       intermediate generation.</li>
 *   <li>Contiguous runs of similarly sized SSTables are evaluated against size-tier buckets:
 *       tables whose byte sizes are either both small ({@code <= minThresholdBytes}) or within
 *       {@code [avgSize * bucketLowRatio, avgSize * bucketHighRatio]} belong to the same size tier.</li>
 *   <li>Any contiguous run with {@code size >= minTablesPerTier} (up to {@code maxTablesPerTier})
 *       is a valid candidate. If no single size-similar run reaches {@code minTablesPerTier} but
 *       {@code liveTables.size() >= minTablesPerTier}, the planner falls back to compacting the
 *       oldest contiguous prefix of {@code min(maxTablesPerTier, liveTables.size())} tables.</li>
 *   <li><b>Tombstone Safety Invariant:</b> If the selected run starts at index 0 (includes the oldest
 *       SSTable generation on disk), {@code retainTombstones} is {@code false} (tombstones are purged).
 *       If older SSTables exist before the selected run, {@code retainTombstones} is {@code true} so
 *       that tombstones continue to mask older values in earlier tables.</li>
 * </ol>
 */
public final class SizeTieredCompactionPlanner implements CompactionPlanner {

    public static final int DEFAULT_MIN_TABLES = 4;
    public static final int DEFAULT_MAX_TABLES = 32;
    public static final long DEFAULT_MIN_THRESHOLD_BYTES = 64L * 1024; // 64 KiB
    public static final double DEFAULT_BUCKET_LOW_RATIO = 0.5;
    public static final double DEFAULT_BUCKET_HIGH_RATIO = 1.5;

    private final int minTablesPerTier;
    private final int maxTablesPerTier;
    private final long targetFileSizeBytes;
    private final long minThresholdBytes;
    private final double bucketLowRatio;
    private final double bucketHighRatio;

    public SizeTieredCompactionPlanner() {
        this(
                DEFAULT_MIN_TABLES,
                DEFAULT_MAX_TABLES,
                Compactor.DEFAULT_TARGET_FILE_SIZE,
                DEFAULT_MIN_THRESHOLD_BYTES,
                DEFAULT_BUCKET_LOW_RATIO,
                DEFAULT_BUCKET_HIGH_RATIO
        );
    }

    public SizeTieredCompactionPlanner(int minTablesPerTier, long targetFileSizeBytes) {
        this(
                minTablesPerTier,
                Math.max(minTablesPerTier, DEFAULT_MAX_TABLES),
                targetFileSizeBytes,
                DEFAULT_MIN_THRESHOLD_BYTES,
                DEFAULT_BUCKET_LOW_RATIO,
                DEFAULT_BUCKET_HIGH_RATIO
        );
    }

    public SizeTieredCompactionPlanner(
            int minTablesPerTier,
            int maxTablesPerTier,
            long targetFileSizeBytes,
            long minThresholdBytes,
            double bucketLowRatio,
            double bucketHighRatio
    ) {
        if (minTablesPerTier < 2) {
            throw new IllegalArgumentException("minTablesPerTier must be >= 2: " + minTablesPerTier);
        }
        if (maxTablesPerTier < minTablesPerTier) {
            throw new IllegalArgumentException(
                    "maxTablesPerTier (" + maxTablesPerTier + ") must be >= minTablesPerTier (" + minTablesPerTier + ")");
        }
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive: " + targetFileSizeBytes);
        }
        if (minThresholdBytes <= 0) {
            throw new IllegalArgumentException("minThresholdBytes must be positive: " + minThresholdBytes);
        }
        if (bucketLowRatio <= 0.0 || bucketLowRatio >= 1.0 || Double.isNaN(bucketLowRatio)) {
            throw new IllegalArgumentException("bucketLowRatio must be in (0.0, 1.0): " + bucketLowRatio);
        }
        if (bucketHighRatio <= 1.0 || Double.isNaN(bucketHighRatio) || Double.isInfinite(bucketHighRatio)) {
            throw new IllegalArgumentException("bucketHighRatio must be > 1.0: " + bucketHighRatio);
        }
        this.minTablesPerTier = minTablesPerTier;
        this.maxTablesPerTier = maxTablesPerTier;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.minThresholdBytes = minThresholdBytes;
        this.bucketLowRatio = bucketLowRatio;
        this.bucketHighRatio = bucketHighRatio;
    }

    public int minTablesPerTier() {
        return minTablesPerTier;
    }

    public int maxTablesPerTier() {
        return maxTablesPerTier;
    }

    public long targetFileSizeBytes() {
        return targetFileSizeBytes;
    }

    @Override
    public Optional<CompactionPlan> plan(List<TableHandle> liveTables) {
        Objects.requireNonNull(liveTables, "liveTables cannot be null");
        if (liveTables.size() < minTablesPerTier) {
            return Optional.empty();
        }
        for (TableHandle handle : liveTables) {
            Objects.requireNonNull(handle, "liveTables cannot contain null TableHandle");
        }

        List<TableHandle> ordered = sortChronologically(liveTables);
        List<TierBucket> buckets = buildContiguousSizeBuckets(ordered);

        TierBucket bestBucket = null;
        int bestTierIndex = -1;
        for (int i = 0; i < buckets.size(); i++) {
            TierBucket bucket = buckets.get(i);
            if (bucket.tables().size() >= minTablesPerTier) {
                if (bestBucket == null || isHigherPriority(bucket, bestBucket)) {
                    bestBucket = bucket;
                    bestTierIndex = i;
                }
            }
        }

        if (bestBucket != null) {
            List<TableHandle> selected = bestBucket.tables();
            if (selected.size() > maxTablesPerTier) {
                selected = selected.subList(0, maxTablesPerTier);
            }
            boolean includesBase = (bestBucket.startIndex() == 0);
            boolean retainTombstones = !includesBase;
            return Optional.of(new CompactionPlan(
                    selected,
                    targetFileSizeBytes,
                    retainTombstones,
                    includesBase,
                    bestTierIndex
            ));
        }

        // Fallback when total live tables exceed compaction trigger across heterogeneous tiers:
        // compact the oldest contiguous prefix so table count stays bounded and tombstones can be purged.
        int count = Math.min(maxTablesPerTier, ordered.size());
        List<TableHandle> prefix = ordered.subList(0, count);
        return Optional.of(new CompactionPlan(
                prefix,
                targetFileSizeBytes,
                false,
                true,
                0
        ));
    }

    @Override
    public Optional<CompactionPlan> planFullCompaction(List<TableHandle> liveTables) {
        Objects.requireNonNull(liveTables, "liveTables cannot be null");
        if (liveTables.isEmpty()) {
            return Optional.empty();
        }
        for (TableHandle handle : liveTables) {
            Objects.requireNonNull(handle, "liveTables cannot contain null TableHandle");
        }
        List<TableHandle> ordered = sortChronologically(liveTables);
        return Optional.of(new CompactionPlan(
                ordered,
                targetFileSizeBytes,
                false,
                true,
                0
        ));
    }

    /**
     * Partitions chronologically ordered tables into contiguous size-similar buckets.
     */
    List<TierBucket> buildContiguousSizeBuckets(List<TableHandle> orderedTables) {
        List<TierBucket> buckets = new ArrayList<>();
        if (orderedTables.isEmpty()) {
            return buckets;
        }

        int currentStart = 0;
        List<TableHandle> currentGroup = new ArrayList<>();
        long currentSumBytes = 0L;

        for (int i = 0; i < orderedTables.size(); i++) {
            TableHandle candidate = orderedTables.get(i);
            long candidateSize = Math.max(1L, candidate.fileSize());

            if (currentGroup.isEmpty()) {
                currentStart = i;
                currentGroup.add(candidate);
                currentSumBytes = candidateSize;
            } else {
                double avgSize = (double) currentSumBytes / currentGroup.size();
                if (belongsToBucket(candidateSize, avgSize) && currentGroup.size() < maxTablesPerTier) {
                    currentGroup.add(candidate);
                    currentSumBytes += candidateSize;
                } else {
                    buckets.add(new TierBucket(currentStart, List.copyOf(currentGroup), currentSumBytes));
                    currentStart = i;
                    currentGroup = new ArrayList<>();
                    currentGroup.add(candidate);
                    currentSumBytes = candidateSize;
                }
            }
        }

        if (!currentGroup.isEmpty()) {
            buckets.add(new TierBucket(currentStart, List.copyOf(currentGroup), currentSumBytes));
        }

        return buckets;
    }

    private boolean belongsToBucket(long fileSize, double bucketAverageSize) {
        if (fileSize <= minThresholdBytes && bucketAverageSize <= minThresholdBytes) {
            return true;
        }
        double low = bucketAverageSize * bucketLowRatio;
        double high = bucketAverageSize * bucketHighRatio;
        return fileSize >= low && fileSize <= high;
    }

    private static boolean isHigherPriority(TierBucket candidate, TierBucket currentBest) {
        // Prefer the bucket with more tables; break ties by smaller average table size (minor compaction first),
        // then by earlier start index (includes base generation).
        int sizeCmp = Integer.compare(candidate.tables().size(), currentBest.tables().size());
        if (sizeCmp != 0) {
            return sizeCmp > 0;
        }
        int bytesCmp = Long.compare(candidate.totalBytes(), currentBest.totalBytes());
        if (bytesCmp != 0) {
            return bytesCmp < 0;
        }
        return candidate.startIndex() < currentBest.startIndex();
    }

    private static List<TableHandle> sortChronologically(List<TableHandle> tables) {
        List<TableHandle> copy = new ArrayList<>(tables);
        copy.sort(Comparator
                .comparingLong(TableHandle::minSequence)
                .thenComparingLong(TableHandle::maxSequence)
                .thenComparingLong(TableHandle::id));
        return copy;
    }

    record TierBucket(int startIndex, List<TableHandle> tables, long totalBytes) {}
}
