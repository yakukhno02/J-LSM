package io.jlsm.engine;

import io.jlsm.internal.io.LockHandle;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.ManifestStore;
import io.jlsm.mem.MemTable;
import io.jlsm.wal.WalWriter;

import java.util.Objects;

public record RecoveryResult(
        LockHandle lockHandle,
        ManifestStore manifestStore,
        ManifestSnapshot manifestSnapshot,
        MemTable activeMemTable,
        WalWriter activeWalWriter,
        long highSequence,
        long activeWalId
) {
    public RecoveryResult {
        Objects.requireNonNull(lockHandle, "lockHandle cannot be null");
        Objects.requireNonNull(manifestStore, "manifestStore cannot be null");
        Objects.requireNonNull(manifestSnapshot, "manifestSnapshot cannot be null");
        Objects.requireNonNull(activeMemTable, "activeMemTable cannot be null");
        Objects.requireNonNull(activeWalWriter, "activeWalWriter cannot be null");
    }
}
