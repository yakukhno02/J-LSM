package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.LockHandle;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.manifest.DefaultManifestStore;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.ManifestStore;
import io.jlsm.manifest.WalMeta;
import io.jlsm.mem.DefaultMemTable;
import io.jlsm.mem.MemTable;
import io.jlsm.wal.DefaultWalReader;
import io.jlsm.wal.DefaultWalWriter;
import io.jlsm.wal.WalWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public final class RecoveryManager {

    public static final String LOCK_FILE_NAME = "LOCK";
    public static final String MANIFEST_FILE_NAME = "MANIFEST";

    private final StorageIO storageIO;
    private final Path directory;
    private final DatabaseOptions options;

    public RecoveryManager(StorageIO storageIO, Path directory, DatabaseOptions options) {
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
        this.directory = Objects.requireNonNull(directory, "directory cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
    }

    public RecoveryResult recover() throws StorageException {
        LockHandle lockHandle = acquireLock();
        boolean success = false;
        ManifestStore manifestStore = null;

        try {
            manifestStore = openManifestStore();
            ManifestSnapshot snapshot = manifestStore.currentSnapshot();

            MemTable memTable = new DefaultMemTable();
            long maxRecoveredSeq = replayUnsealedWals(snapshot, memTable);
            long highSequence = Math.max(snapshot.highSequence(), maxRecoveredSeq);

            long activeWalId = snapshot.nextFileId();
            long nextFileId = activeWalId + 1;
            long nextEpoch = snapshot.epoch() + 1;

            WalWriter activeWalWriter = createActiveWal(activeWalId);

            List<WalMeta> updatedWals = new ArrayList<>();
            for (WalMeta wal : snapshot.wals()) {
                if (!wal.isSealed()) {
                    updatedWals.add(new WalMeta(wal.walId(), true));
                } else {
                    updatedWals.add(wal);
                }
            }
            updatedWals.add(new WalMeta(activeWalId, false));

            ManifestSnapshot nextSnapshot = new ManifestSnapshot(
                    nextEpoch,
                    nextFileId,
                    highSequence,
                    updatedWals,
                    snapshot.tables()
            );

            manifestStore.commit(nextSnapshot);
            storageIO.syncDirectory(directory);

            success = true;
            return new RecoveryResult(
                    lockHandle,
                    manifestStore,
                    nextSnapshot,
                    memTable,
                    activeWalWriter,
                    highSequence,
                    activeWalId
            );

        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "I/O failure during recovery in " + directory, e);
        } finally {
            if (!success) {
                if (manifestStore != null) {
                    try {
                        manifestStore.close();
                    } catch (Exception ignored) {
                    }
                }
                try {
                    lockHandle.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private LockHandle acquireLock() throws StorageException {
        Path lockPath = directory.resolve(LOCK_FILE_NAME);
        try {
            return storageIO.acquireLock(lockPath);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.LOCKED, "Could not acquire database lock on " + lockPath, e);
        }
    }

    private ManifestStore openManifestStore() throws StorageException {
        Path manifestPath = directory.resolve(MANIFEST_FILE_NAME);
        try {
            ChannelIO channel = storageIO.openReadWrite(manifestPath);
            return DefaultManifestStore.open(channel, manifestPath, ManifestSnapshot.empty());
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to open MANIFEST channel at " + manifestPath, e);
        }
    }

    private long replayUnsealedWals(ManifestSnapshot snapshot, MemTable memTable) throws StorageException {
        long highestSeq = 0L;

        List<WalMeta> unsealedWals = snapshot.wals().stream()
                .filter(w -> !w.isSealed())
                .sorted(Comparator.comparingLong(WalMeta::walId))
                .toList();

        for (WalMeta wal : unsealedWals) {
            Path walPath = directory.resolve(walFileName(wal.walId()));
            try {
                ChannelIO channel = storageIO.openReadWrite(walPath);
                try (DefaultWalReader reader = DefaultWalReader.open(channel, walPath)) {
                    reader.recoverAndTruncateTail();

                    try (RecordCursor cursor = reader.openCursor()) {
                        while (cursor.advance()) {
                            InternalRecord record = cursor.current();
                            memTable.insert(record.key(), record);
                            if (record.sequence() > highestSeq) {
                                highestSeq = record.sequence();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new StorageException(ErrorCode.IO, "Failed to replay WAL file: " + walPath, e);
            }
        }

        return highestSeq;
    }

    private WalWriter createActiveWal(long walId) throws StorageException, IOException {
        Path walPath = directory.resolve(walFileName(walId));
        ChannelIO channel = storageIO.createNew(walPath);
        return DefaultWalWriter.create(channel, walId, options.durabilityMode());
    }

    public static String walFileName(long fileId) {
        return String.format("%06d.wal", fileId);
    }
}
