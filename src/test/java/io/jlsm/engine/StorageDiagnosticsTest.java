package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.KVDatabase;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.manifest.DefaultManifestStore;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.TableMeta;
import io.jlsm.manifest.WalMeta;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageDiagnosticsTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Inspect reports healthy status on valid database directory")
    void testInspectHealthyDatabase(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();

        // 1. Unopened / empty directory
        IntegrityReport emptyReport = StorageDiagnostics.inspect(storageIO, tempDir);
        assertThat(emptyReport.isHealthy()).isTrue();
        assertThat(emptyReport.hasWarnings()).isTrue();

        // 2. Populated database
        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k1"), bytes("v1"));
            db.put(bytes("k2"), bytes("v2"));
            db.sync();
        }

        IntegrityReport report = StorageDiagnostics.inspect(storageIO, tempDir);
        assertThat(report.isHealthy()).isTrue();
        assertThat(report.hasErrors()).isFalse();
        assertThat(report.verifiedWalsCount()).isGreaterThanOrEqualTo(1);
        assertThat(report.verifiedWalRecordsCount()).isGreaterThanOrEqualTo(2);
        assertThat(report.toFormattedSummary()).contains("status=HEALTHY");
    }

    @Test
    @DisplayName("Inspect detects orphan SSTables and cleanupOrphanFiles removes them")
    void testOrphanFileDetectionAndCleanup(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();

        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("k"), bytes("v"));
        }

        // Inject orphan files
        Path orphanSst = tempDir.resolve("999999.sst");
        Path orphanTmp = tempDir.resolve("temporary.tmp");
        Path orphanWal = tempDir.resolve("888888.wal");
        Files.write(orphanSst, new byte[100]);
        Files.write(orphanTmp, new byte[50]);
        Files.write(orphanWal, new byte[20]);

        IntegrityReport report = StorageDiagnostics.inspect(storageIO, tempDir);
        assertThat(report.orphanTablesCount()).isEqualTo(1);
        assertThat(report.orphanWalsCount()).isEqualTo(1);
        assertThat(report.hasWarnings()).isTrue();

        // Cleanup
        int cleaned = StorageDiagnostics.cleanupOrphanFiles(storageIO, tempDir);
        assertThat(cleaned).isGreaterThanOrEqualTo(3);
        assertThat(Files.exists(orphanSst)).isFalse();
        assertThat(Files.exists(orphanTmp)).isFalse();
        assertThat(Files.exists(orphanWal)).isFalse();
    }

    @Test
    @DisplayName("Inspect handles non-existent directory and null arguments")
    void testNullAndNonExistentDir(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        Path nonExistent = tempDir.resolve("sub_does_not_exist");

        IntegrityReport report = StorageDiagnostics.inspect(storageIO, nonExistent);
        assertThat(report.isHealthy()).isFalse();
        assertThat(report.hasErrors()).isTrue();
        assertThat(report.toFormattedSummary()).contains("status=COMPROMISED");

        assertThatThrownBy(() -> StorageDiagnostics.inspect(null, tempDir))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> StorageDiagnostics.inspect(storageIO, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Inspect detects missing or corrupted referenced files in manifest")
    void testMissingReferencedFiles(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        Path manifestPath = tempDir.resolve("MANIFEST");

        ManifestSnapshot snapshot = new ManifestSnapshot(
                1,
                10,
                100,
                List.of(new WalMeta(1, true), new WalMeta(2, false)),
                List.of(new TableMeta(3, 0, 10, 5, 2048))
        );

        try (ChannelIO channel = storageIO.createNew(manifestPath);
             DefaultManifestStore store = DefaultManifestStore.open(channel, manifestPath, ManifestSnapshot.empty())) {
            store.commit(snapshot);
        }

        // Neither WAL 1, WAL 2, nor SSTable 3 exist on disk
        IntegrityReport report = StorageDiagnostics.inspect(storageIO, tempDir);
        assertThat(report.hasWarnings()).isTrue(); // missing sealed wal is warning
        assertThat(report.hasErrors()).isTrue();   // missing active wal and missing sst are errors
        assertThat(report.errors()).anyMatch(e -> e.contains("Active WAL file missing"));
        assertThat(report.errors()).anyMatch(e -> e.contains("Referenced SSTable file missing"));
    }

    @Test
    @DisplayName("Analyze disk space calculates size and file counts")
    void testAnalyzeDiskSpace(@TempDir Path tempDir) throws Exception {
        StorageDiagnostics.DiskSpaceBreakdown empty = StorageDiagnostics.analyzeDiskSpace(tempDir);
        assertThat(empty.totalBytes()).isEqualTo(0L);

        try (KVDatabase db = KVDatabase.open(tempDir, DatabaseOptions.defaults())) {
            db.put(bytes("key"), bytes("val"));
            db.sync();
        }

        StorageDiagnostics.DiskSpaceBreakdown populated = StorageDiagnostics.analyzeDiskSpace(tempDir);
        assertThat(populated.totalBytes()).isGreaterThan(0L);
        assertThat(populated.walBytes()).isGreaterThan(0L);
        assertThat(StorageDiagnostics.isHealthyDirectory(tempDir)).isTrue();
    }
}
