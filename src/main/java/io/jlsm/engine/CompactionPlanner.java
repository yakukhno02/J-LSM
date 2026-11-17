package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.sst.TableHandle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class CompactionPlanner {

    public enum Urgency {
        NONE,
        LOW,
        HIGH,
        CRITICAL
    }

    public record CompactionPlan(
            Urgency urgency,
            double spaceAmplification,
            double overlapRatio,
            int candidateTableCount,
            long estimatedReclaimableBytes,
            String reason
    ) {
        public boolean shouldCompact() {
            return urgency != Urgency.NONE;
        }
    }

    private CompactionPlanner() {}

    public static CompactionPlan evaluate(List<TableHandle> tables, DatabaseOptions options) {
        Objects.requireNonNull(tables, "tables cannot be null");
        Objects.requireNonNull(options, "options cannot be null");

        if (tables.size() < 2) {
            return new CompactionPlan(Urgency.NONE, 1.0, 0.0, 0, 0L, "Less than 2 tables present");
        }

        int tableCount = tables.size();
        long totalBytes = 0L;
        long totalRecordCount = 0L;

        for (TableHandle handle : tables) {
            totalBytes += handle.fileSize();
            totalRecordCount += handle.recordCount();
        }

        double overlapRatio = calculateOverlapRatio(tables);
        double spaceAmplification = calculateSpaceAmplification(tables, totalBytes);
        long estimatedReclaimable = (long) (totalBytes * (overlapRatio * 0.5));

        if (tableCount >= options.compactionTrigger() * 2) {
            return new CompactionPlan(
                    Urgency.CRITICAL,
                    spaceAmplification,
                    overlapRatio,
                    tableCount,
                    estimatedReclaimable,
                    "Table count (" + tableCount + ") exceeds double compaction trigger threshold (" + (options.compactionTrigger() * 2) + ")"
            );
        }

        if (tableCount >= options.compactionTrigger()) {
            return new CompactionPlan(
                    Urgency.HIGH,
                    spaceAmplification,
                    overlapRatio,
                    tableCount,
                    estimatedReclaimable,
                    "Table count (" + tableCount + ") reached compaction trigger (" + options.compactionTrigger() + ")"
            );
        }

        if (overlapRatio > 0.70 && tableCount >= 3) {
            return new CompactionPlan(
                    Urgency.LOW,
                    spaceAmplification,
                    overlapRatio,
                    tableCount,
                    estimatedReclaimable,
                    "High sequence overlap (" + String.format("%.2f", overlapRatio * 100.0) + "%) across " + tableCount + " tables"
            );
        }

        return new CompactionPlan(Urgency.NONE, spaceAmplification, overlapRatio, 0, 0L, "Storage operates within optimal thresholds");
    }

    public static double calculateOverlapRatio(List<TableHandle> tables) {
        if (tables.size() < 2) {
            return 0.0;
        }

        int overlappingPairs = 0;
        int totalPairs = 0;

        for (int i = 0; i < tables.size(); i++) {
            TableHandle t1 = tables.get(i);
            for (int j = i + 1; j < tables.size(); j++) {
                TableHandle t2 = tables.get(j);
                totalPairs++;
                boolean overlaps = Math.max(t1.minSequence(), t2.minSequence()) <= Math.min(t1.maxSequence(), t2.maxSequence());
                if (overlaps) {
                    overlappingPairs++;
                }
            }
        }

        return totalPairs > 0 ? (double) overlappingPairs / (double) totalPairs : 0.0;
    }

    public static double calculateSpaceAmplification(List<TableHandle> tables, long totalBytes) {
        if (tables.isEmpty() || totalBytes <= 0) {
            return 1.0;
        }

        long uniqueKeyFootprintEstimate = 0L;
        for (TableHandle handle : tables) {
            long size = handle.fileSize();
            if (size > uniqueKeyFootprintEstimate) {
                uniqueKeyFootprintEstimate = size;
            }
        }

        if (uniqueKeyFootprintEstimate <= 0) {
            return 1.0;
        }

        return (double) totalBytes / (double) uniqueKeyFootprintEstimate;
    }

    public static List<TableHandle> selectCompactionCandidates(List<TableHandle> tables, int maxCandidates) {
        Objects.requireNonNull(tables, "tables cannot be null");
        if (tables.size() <= maxCandidates) {
            return new ArrayList<>(tables);
        }

        List<TableHandle> sorted = new ArrayList<>(tables);
        sorted.sort(Comparator.comparingLong(TableHandle::id));
        return sorted.subList(0, maxCandidates);
    }
}
