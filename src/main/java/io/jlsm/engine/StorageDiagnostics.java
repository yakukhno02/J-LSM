package io.jlsm.engine;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.manifest.DefaultManifestStore;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.TableMeta;
import io.jlsm.manifest.WalMeta;
import io.jlsm.sst.DefaultTableHandle;
import io.jlsm.sst.TableHandle;
import io.jlsm.wal.DefaultWalReader;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StorageDiagnostics {

    private static final Pattern SST_PATTERN = Pattern.compile("^(\\d{6})\\.sst$");
    private static final Pattern WAL_PATTERN = Pattern.compile("^(\\d{6})\\.wal$");

    private StorageDiagnostics() {}

    public static IntegrityReport inspect(Path directory) throws StorageException {
        return inspect(new StandardStorageIO(), directory);
    }

    public static IntegrityReport inspect(StorageIO storageIO, Path directory) throws StorageException {
        Objects.requireNonNull(storageIO, "storageIO cannot be null");
        Objects.requireNonNull(directory, "directory cannot be null");

        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        if (!Files.exists(directory)) {
            errors.add("Database directory does not exist: " + directory);
            return new IntegrityReport(false, 0, 0, 0, 0, 0, 0, 0, 0, 0, warnings, errors);
        }

        Path manifestPath = directory.resolve(RecoveryManager.MANIFEST_FILE_NAME);
        if (!Files.exists(manifestPath)) {
            warnings.add("MANIFEST file does not exist yet (clean directory)");
            return new IntegrityReport(true, 0, 1, 0, 0, 0, 0, 0, 0, 0, warnings, errors);
        }

        ManifestSnapshot snapshot = ManifestSnapshot.empty();
        int tornFrames = 0;
        try (ChannelIO channel = storageIO.openReadWrite(manifestPath)) {
            try (DefaultManifestStore manifestStore = DefaultManifestStore.open(channel, manifestPath, snapshot)) {
                snapshot = manifestStore.currentSnapshot();
            }
        } catch (StorageException e) {
            errors.add("Failed to read MANIFEST catalog: " + e.getMessage());
            return new IntegrityReport(false, 0, 0, 0, 0, 0, 0, 0, 0, 0, warnings, errors);
        } catch (IOException e) {
            errors.add("I/O error opening MANIFEST: " + e.getMessage());
            return new IntegrityReport(false, 0, 0, 0, 0, 0, 0, 0, 0, 0, warnings, errors);
        }

        Set<Long> manifestTableIds = new HashSet<>();
        int verifiedTables = 0;
        for (TableMeta tableMeta : snapshot.tables()) {
            manifestTableIds.add(tableMeta.tableId());
            Path sstPath = directory.resolve(String.format("%06d.sst", tableMeta.tableId()));
            if (!Files.exists(sstPath)) {
                errors.add("Referenced SSTable file missing on disk: " + sstPath);
                continue;
            }

            try (TableHandle handle = DefaultTableHandle.open(storageIO, sstPath, tableMeta)) {
                if (handle.id() != tableMeta.tableId()) {
                    errors.add(String.format("SSTable %s header fileId (%d) does not match catalog ID (%d)",
                            sstPath, handle.id(), tableMeta.tableId()));
                } else {
                    verifiedTables++;
                }
            } catch (Exception e) {
                errors.add(String.format("Corrupted or unreadable SSTable %s: %s", sstPath, e.getMessage()));
            }
        }

        Set<Long> manifestWalIds = new HashSet<>();
        int verifiedWals = 0;
        long verifiedWalRecords = 0L;
        for (WalMeta walMeta : snapshot.wals()) {
            manifestWalIds.add(walMeta.walId());
            Path walPath = directory.resolve(RecoveryManager.walFileName(walMeta.walId()));
            if (!Files.exists(walPath)) {
                if (walMeta.isSealed()) {
                    warnings.add("Sealed WAL file missing on disk: " + walPath);
                } else {
                    errors.add("Active WAL file missing on disk: " + walPath);
                }
                continue;
            }

            try (ChannelIO channel = storageIO.openRead(walPath);
                 DefaultWalReader reader = DefaultWalReader.open(channel, walPath)) {
                long recordsInWal = 0L;
                try (RecordCursor cursor = reader.openCursor()) {
                    while (cursor.advance()) {
                        recordsInWal++;
                    }
                }
                verifiedWalRecords += recordsInWal;
                verifiedWals++;
            } catch (Exception e) {
                errors.add(String.format("Corrupted WAL file %s: %s", walPath, e.getMessage()));
            }
        }

        int orphanTables = 0;
        int orphanWals = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                Matcher sstMatcher = SST_PATTERN.matcher(fileName);
                if (sstMatcher.matches()) {
                    long id = Long.parseLong(sstMatcher.group(1));
                    if (!manifestTableIds.contains(id)) {
                        orphanTables++;
                        warnings.add("Orphan SSTable file detected (not in current manifest): " + fileName);
                    }
                }

                Matcher walMatcher = WAL_PATTERN.matcher(fileName);
                if (walMatcher.matches()) {
                    long id = Long.parseLong(walMatcher.group(1));
                    if (!manifestWalIds.contains(id)) {
                        orphanWals++;
                        warnings.add("Orphan WAL file detected (not in current manifest): " + fileName);
                    }
                }
            }
        } catch (IOException e) {
            warnings.add("Failed to scan directory for orphan files: " + e.getMessage());
        }

        boolean isHealthy = errors.isEmpty();

        return new IntegrityReport(
                isHealthy,
                snapshot.epoch(),
                snapshot.nextFileId(),
                snapshot.highSequence(),
                verifiedTables,
                verifiedWals,
                verifiedWalRecords,
                orphanTables,
                orphanWals,
                tornFrames,
                warnings,
                errors
        );
    }

    public static void printReport(IntegrityReport report, PrintStream out) {
        Objects.requireNonNull(report, "report cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        out.println("================================================================================");
        out.println("                         J-LSM STORAGE DIAGNOSTICS REPORT                       ");
        out.println("================================================================================");
        out.println(report.toFormattedSummary());
        out.println("================================================================================");
    }

    public static int cleanupOrphanFiles(Path directory) throws StorageException {
        return cleanupOrphanFiles(new StandardStorageIO(), directory);
    }

    public static int cleanupOrphanFiles(StorageIO storageIO, Path directory) throws StorageException {
        Objects.requireNonNull(storageIO, "storageIO cannot be null");
        Objects.requireNonNull(directory, "directory cannot be null");

        Path manifestPath = directory.resolve(RecoveryManager.MANIFEST_FILE_NAME);
        if (!Files.exists(manifestPath)) {
            return 0;
        }

        ManifestSnapshot snapshot;
        try (ChannelIO channel = storageIO.openRead(manifestPath);
             DefaultManifestStore store = DefaultManifestStore.open(channel, manifestPath, ManifestSnapshot.empty())) {
            snapshot = store.currentSnapshot();
        } catch (Exception e) {
            throw new StorageException(ErrorCode.IO, "Failed reading manifest during orphan cleanup", e);
        }

        Set<Long> liveTableIds = new HashSet<>();
        for (TableMeta t : snapshot.tables()) {
            liveTableIds.add(t.tableId());
        }

        Set<Long> liveWalIds = new HashSet<>();
        for (WalMeta w : snapshot.wals()) {
            liveWalIds.add(w.walId());
        }

        int deletedCount = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                if (fileName.endsWith(".tmp")) {
                    try {
                        storageIO.deleteIfExists(entry);
                        deletedCount++;
                    } catch (IOException ignored) {}
                    continue;
                }

                Matcher sstMatcher = SST_PATTERN.matcher(fileName);
                if (sstMatcher.matches()) {
                    long id = Long.parseLong(sstMatcher.group(1));
                    if (!liveTableIds.contains(id)) {
                        try {
                            storageIO.deleteIfExists(entry);
                            deletedCount++;
                        } catch (IOException ignored) {}
                    }
                    continue;
                }

                Matcher walMatcher = WAL_PATTERN.matcher(fileName);
                if (walMatcher.matches()) {
                    long id = Long.parseLong(walMatcher.group(1));
                    if (!liveWalIds.contains(id)) {
                        try {
                            storageIO.deleteIfExists(entry);
                            deletedCount++;
                        } catch (IOException ignored) {}
                    }
                }
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Error scanning directory for orphan cleanup", e);
        }

        return deletedCount;
    }

    public static boolean isHealthyDirectory(Path directory) {
        try {
            return inspect(directory).isHealthy();
        } catch (StorageException e) {
            return false;
        }
    }

    public static boolean isHealthyDirectory(StorageIO storageIO, Path directory) {
        try {
            return inspect(storageIO, directory).isHealthy();
        } catch (StorageException e) {
            return false;
        }
    }

    public record DiskSpaceBreakdown(
            long totalBytes,
            long sstableBytes,
            long walBytes,
            long manifestBytes,
            long tempBytes,
            int sstableCount,
            int walCount,
            int tempFileCount
    ) {}

    public static DiskSpaceBreakdown analyzeDiskSpace(Path directory) throws StorageException {
        Objects.requireNonNull(directory, "directory cannot be null");
        if (!Files.exists(directory)) {
            return new DiskSpaceBreakdown(0L, 0L, 0L, 0L, 0L, 0, 0, 0);
        }

        long total = 0L;
        long sst = 0L;
        long wal = 0L;
        long manifest = 0L;
        long temp = 0L;
        int sstCount = 0;
        int walCount = 0;
        int tempCount = 0;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                if (!Files.isRegularFile(entry)) {
                    continue;
                }
                long size = Files.size(entry);
                total += size;
                String name = entry.getFileName().toString();
                if (SST_PATTERN.matcher(name).matches()) {
                    sst += size;
                    sstCount++;
                } else if (WAL_PATTERN.matcher(name).matches()) {
                    wal += size;
                    walCount++;
                } else if (name.equals(RecoveryManager.MANIFEST_FILE_NAME) || name.startsWith("MANIFEST")) {
                    manifest += size;
                } else if (name.endsWith(".tmp")) {
                    temp += size;
                    tempCount++;
                }
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed analyzing directory disk space: " + directory, e);
        }

        return new DiskSpaceBreakdown(total, sst, wal, manifest, temp, sstCount, walCount, tempCount);
    }
}
