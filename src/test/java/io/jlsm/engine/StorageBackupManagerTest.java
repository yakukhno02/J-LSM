package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.KVDatabase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageBackupManagerTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Create, verify, and restore hot backup")
    void testBackupAndRestore(@TempDir Path tempDir) throws Exception {
        Path dbDir = tempDir.resolve("db");
        Path backupDir = tempDir.resolve("backup");
        Path restoreDir = tempDir.resolve("restored");

        try (KVDatabase db = KVDatabase.open(dbDir, DatabaseOptions.defaults())) {
            db.put(bytes("key1"), bytes("value1"));
            db.put(bytes("key2"), bytes("value2"));
            db.sync();
        }

        StorageBackupManager manager = new StorageBackupManager();
        StorageBackupManager.BackupInfo info = manager.createBackup(dbDir, backupDir);

        assertThat(info.entries()).isNotEmpty();
        assertThat(info.totalBackupSizeBytes()).isGreaterThan(0L);
        assertThat(manager.verifyBackup(backupDir)).isTrue();

        manager.restoreBackup(backupDir, restoreDir);

        try (KVDatabase restoredDb = KVDatabase.open(restoreDir, DatabaseOptions.defaults())) {
            assertThat(restoredDb.get(bytes("key1"))).isEqualTo(bytes("value1"));
            assertThat(restoredDb.get(bytes("key2"))).isEqualTo(bytes("value2"));
        }

        List<StorageBackupManager.BackupInfo> list = manager.listBackups(tempDir);
        assertThat(list).hasSize(1);

        assertThat(manager.deleteBackup(backupDir)).isTrue();
        assertThat(manager.verifyBackup(backupDir)).isFalse();
    }

    @Test
    @DisplayName("Backup validation handles non-existent paths")
    void testBackupValidation(@TempDir Path tempDir) {
        StorageBackupManager manager = new StorageBackupManager();
        Path nonExistent = tempDir.resolve("missing");

        assertThatThrownBy(() -> manager.createBackup(nonExistent, tempDir.resolve("b")))
                .isInstanceOf(Exception.class);
    }
}
