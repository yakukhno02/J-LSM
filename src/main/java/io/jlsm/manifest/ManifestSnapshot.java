package io.jlsm.manifest;

import java.util.List;
import java.util.Objects;

public record ManifestSnapshot(
        long epoch,
        long nextFileId,
        long highSequence,
        List<WalMeta> wals,
        List<TableMeta> tables
) {
    public ManifestSnapshot {
        Objects.requireNonNull(wals, "wals cannot be null");
        Objects.requireNonNull(tables, "tables cannot be null");
        wals = List.copyOf(wals);
        tables = List.copyOf(tables);
    }

    public static ManifestSnapshot empty() {
        return new ManifestSnapshot(0L, 1L, 0L, List.of(), List.of());
    }
}
