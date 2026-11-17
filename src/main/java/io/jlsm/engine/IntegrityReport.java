package io.jlsm.engine;

import java.util.List;

public record IntegrityReport(
        boolean isHealthy,
        long currentEpoch,
        long nextFileId,
        long highSequence,
        int verifiedTablesCount,
        int verifiedWalsCount,
        long verifiedWalRecordsCount,
        int orphanTablesCount,
        int orphanWalsCount,
        int tornFramesDetected,
        List<String> warnings,
        List<String> errors
) {

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    public int totalOrphanFiles() {
        return orphanTablesCount + orphanWalsCount;
    }

    public String toFormattedSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("IntegrityReport status=").append(isHealthy ? "HEALTHY" : "COMPROMISED").append("\n");
        sb.append(String.format("  Epoch: %d, NextFileId: %d, HighSequence: %d\n", currentEpoch, nextFileId, highSequence));
        sb.append(String.format("  Verified Tables: %d, Verified WALs: %d (records: %d)\n", verifiedTablesCount, verifiedWalsCount, verifiedWalRecordsCount));
        sb.append(String.format("  Orphan Tables: %d, Orphan WALs: %d, Torn Tail Frames: %d\n", orphanTablesCount, orphanWalsCount, tornFramesDetected));

        if (!warnings.isEmpty()) {
            sb.append("  Warnings (").append(warnings.size()).append("):\n");
            for (String w : warnings) {
                sb.append("    - ").append(w).append("\n");
            }
        }
        if (!errors.isEmpty()) {
            sb.append("  Errors (").append(errors.size()).append("):\n");
            for (String e : errors) {
                sb.append("    - ").append(e).append("\n");
            }
        }
        return sb.toString();
    }
}
