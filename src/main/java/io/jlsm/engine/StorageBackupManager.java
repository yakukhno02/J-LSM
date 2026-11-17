package io.jlsm.engine;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.manifest.DefaultManifestStore;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.TableMeta;
import io.jlsm.manifest.WalMeta;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

public final class StorageBackupManager {

    private static final String BACKUP_METADATA_FILE = "BACKUP_META";

    public record BackupEntry(String fileName, long fileSize, long crc32) {}

    public record BackupInfo(
            String backupId,
            long timestamp,
            long epoch,
            long highSequence,
            List<BackupEntry> entries
    ) {
        public long totalBackupSizeBytes() {
            long total = 0L;
            for (BackupEntry entry : entries) {
                total += entry.fileSize();
            }
            return total;
        }
    }

    private final StorageIO storageIO;

    public StorageBackupManager() {
        this(new StandardStorageIO());
    }

    public StorageBackupManager(StorageIO storageIO) {
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
    }

    public BackupInfo createBackup(Path sourceDir, Path backupDir) throws StorageException {
        Objects.requireNonNull(sourceDir, "sourceDir cannot be null");
        Objects.requireNonNull(backupDir, "backupDir cannot be null");

        if (!Files.exists(sourceDir)) {
            throw new StorageException(ErrorCode.IO, "Source directory does not exist: " + sourceDir);
        }

        try {
            Files.createDirectories(backupDir);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed creating backup directory: " + backupDir, e);
        }

        Path manifestPath = sourceDir.resolve(RecoveryManager.MANIFEST_FILE_NAME);
        if (!Files.exists(manifestPath)) {
            throw new StorageException(ErrorCode.CORRUPTION, "MANIFEST not found in source directory: " + sourceDir);
        }

        ManifestSnapshot snapshot;
        try (ChannelIO channel = storageIO.openRead(manifestPath);
             DefaultManifestStore store = DefaultManifestStore.open(channel, manifestPath, ManifestSnapshot.empty())) {
            snapshot = store.currentSnapshot();
        } catch (Exception e) {
            throw new StorageException(ErrorCode.IO, "Failed reading manifest during backup creation", e);
        }

        List<Path> filesToBackup = new ArrayList<>();
        filesToBackup.add(manifestPath);

        for (TableMeta table : snapshot.tables()) {
            Path sstPath = sourceDir.resolve(String.format("%06d.sst", table.tableId()));
            if (Files.exists(sstPath)) {
                filesToBackup.add(sstPath);
            } else {
                throw new StorageException(ErrorCode.CORRUPTION, "Referenced SSTable missing during backup: " + sstPath);
            }
        }

        for (WalMeta wal : snapshot.wals()) {
            Path walPath = sourceDir.resolve(RecoveryManager.walFileName(wal.walId()));
            if (Files.exists(walPath)) {
                filesToBackup.add(walPath);
            }
        }

        List<BackupEntry> entries = new ArrayList<>();
        for (Path src : filesToBackup) {
            String name = src.getFileName().toString();
            Path dst = backupDir.resolve(name);
            try {
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
                long size = Files.size(dst);
                long crc = calculateCrc32(dst);
                entries.add(new BackupEntry(name, size, crc));
            } catch (IOException e) {
                throw new StorageException(ErrorCode.IO, "Failed to copy backup file: " + src, e);
            }
        }

        String backupId = "backup-" + System.currentTimeMillis();
        BackupInfo info = new BackupInfo(backupId, System.currentTimeMillis(), snapshot.epoch(), snapshot.highSequence(), entries);
        writeMetadata(backupDir.resolve(BACKUP_METADATA_FILE), info);

        return info;
    }

    public boolean verifyBackup(Path backupDir) throws StorageException {
        Objects.requireNonNull(backupDir, "backupDir cannot be null");
        Path metaPath = backupDir.resolve(BACKUP_METADATA_FILE);
        if (!Files.exists(metaPath)) {
            return false;
        }

        BackupInfo info = readMetadata(metaPath);
        for (BackupEntry entry : info.entries()) {
            Path target = backupDir.resolve(entry.fileName());
            if (!Files.exists(target)) {
                return false;
            }
            try {
                if (Files.size(target) != entry.fileSize()) {
                    return false;
                }
                if (calculateCrc32(target) != entry.crc32()) {
                    return false;
                }
            } catch (IOException e) {
                return false;
            }
        }

        return true;
    }

    public void restoreBackup(Path backupDir, Path targetDir) throws StorageException {
        Objects.requireNonNull(backupDir, "backupDir cannot be null");
        Objects.requireNonNull(targetDir, "targetDir cannot be null");

        if (!verifyBackup(backupDir)) {
            throw new StorageException(ErrorCode.CORRUPTION, "Backup verification failed for: " + backupDir);
        }

        BackupInfo info = readMetadata(backupDir.resolve(BACKUP_METADATA_FILE));
        try {
            Files.createDirectories(targetDir);
            for (BackupEntry entry : info.entries()) {
                Path src = backupDir.resolve(entry.fileName());
                Path dst = targetDir.resolve(entry.fileName());
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed restoring backup to: " + targetDir, e);
        }
    }

    private static void writeMetadata(Path metaPath, BackupInfo info) throws StorageException {
        try (BufferedWriter writer = Files.newBufferedWriter(metaPath, StandardCharsets.UTF_8)) {
            writer.write(info.backupId() + "\n");
            writer.write(info.timestamp() + "\n");
            writer.write(info.epoch() + "\n");
            writer.write(info.highSequence() + "\n");
            writer.write(info.entries().size() + "\n");
            for (BackupEntry entry : info.entries()) {
                writer.write(entry.fileName() + "," + entry.fileSize() + "," + entry.crc32() + "\n");
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed writing backup metadata", e);
        }
    }

    public static BackupInfo readMetadata(Path metaPath) throws StorageException {
        try (BufferedReader reader = Files.newBufferedReader(metaPath, StandardCharsets.UTF_8)) {
            String backupId = reader.readLine();
            long timestamp = Long.parseLong(reader.readLine());
            long epoch = Long.parseLong(reader.readLine());
            long highSequence = Long.parseLong(reader.readLine());
            int count = Integer.parseInt(reader.readLine());

            List<BackupEntry> entries = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    entries.add(new BackupEntry(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2])));
                }
            }

            return new BackupInfo(backupId, timestamp, epoch, highSequence, entries);
        } catch (Exception e) {
            throw new StorageException(ErrorCode.CORRUPTION, "Failed reading backup metadata: " + metaPath, e);
        }
    }

    public List<BackupInfo> listBackups(Path backupRoot) throws StorageException {
        Objects.requireNonNull(backupRoot, "backupRoot cannot be null");
        List<BackupInfo> result = new ArrayList<>();
        if (!Files.exists(backupRoot) || !Files.isDirectory(backupRoot)) {
            return result;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(backupRoot)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    Path metaPath = entry.resolve(BACKUP_METADATA_FILE);
                    if (Files.exists(metaPath)) {
                        try {
                            result.add(readMetadata(metaPath));
                        } catch (Exception ignored) {}
                    }
                }
            }
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed listing backups in: " + backupRoot, e);
        }

        return result;
    }

    public boolean deleteBackup(Path backupDir) throws StorageException {
        Objects.requireNonNull(backupDir, "backupDir cannot be null");
        if (!Files.exists(backupDir)) {
            return false;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(backupDir)) {
            for (Path entry : stream) {
                Files.deleteIfExists(entry);
            }
            Files.deleteIfExists(backupDir);
            return true;
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed deleting backup directory: " + backupDir, e);
        }
    }


    private static long calculateCrc32(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }
}
