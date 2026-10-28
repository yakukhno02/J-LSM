package io.jlsm.api;

public record DatabaseOptions(
        DurabilityMode durabilityMode,
        long memtableBytes,
        int compactionTrigger,
        int maxOpenScans,
        long maxSnapshotBytes,
        long maxIndexBytes,
        int maxLiveTables
) {
    public static DatabaseOptions defaults() {
        return new DatabaseOptions(
                DurabilityMode.SYNC,
                4L * 1024 * 1024, // 4 MB
                4,                // compaction при 4 таблицях
                4,                // max concurrent scans
                8L * 1024 * 1024, // 8 MB RAM для snapshot scans
                32L * 1024 * 1024,// 32 MB для Sparse Index
                16                // max live SST files
        );
    }
}
