package io.jlsm.manifest;

public record TableMeta(
        long tableId,
        long minSeq,
        long maxSeq,
        long recordCount,
        long fileSize
) {}
