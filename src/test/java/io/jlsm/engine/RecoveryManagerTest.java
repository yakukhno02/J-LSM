package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.WalMeta;
import io.jlsm.wal.DefaultWalWriter;
import io.jlsm.wal.WalWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryManagerTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ByteKey key(String s) {
        return ByteKey.copyOf(bytes(s));
    }

    @Test
    @DisplayName("Fresh directory recovery initializes empty manifest and first WAL")
    void testFreshDirectoryRecovery(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DatabaseOptions options = DatabaseOptions.defaults();
        RecoveryManager manager = new RecoveryManager(storageIO, tempDir, options);

        RecoveryResult result = manager.recover();
        try {
            assertThat(result.lockHandle()).isNotNull();
            assertThat(result.manifestStore()).isNotNull();
            assertThat(result.activeMemTable().isEmpty()).isTrue();
            assertThat(result.highSequence()).isZero();
            assertThat(result.activeWalId()).isEqualTo(1L);

            ManifestSnapshot snapshot = result.manifestSnapshot();
            assertThat(snapshot.epoch()).isEqualTo(1L);
            assertThat(snapshot.nextFileId()).isEqualTo(2L);
            assertThat(snapshot.wals()).hasSize(1);
            assertThat(snapshot.wals().getFirst()).isEqualTo(new WalMeta(1L, false));

            // Verify files created on disk
            assertThat(Files.exists(tempDir.resolve(RecoveryManager.LOCK_FILE_NAME))).isTrue();
            assertThat(Files.exists(tempDir.resolve(RecoveryManager.MANIFEST_FILE_NAME))).isTrue();
            assertThat(Files.exists(tempDir.resolve(RecoveryManager.walFileName(1L)))).isTrue();
        } finally {
            result.activeWalWriter().close();
            Assertions.assertNotNull(result.manifestStore());
            result.manifestStore().close();
            Assertions.assertNotNull(result.lockHandle());
            result.lockHandle().close();
        }
    }

    @Test
    @DisplayName("Parallel recovery on locked directory fails with LOCKED error code")
    void testLockContention(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DatabaseOptions options = DatabaseOptions.defaults();
        RecoveryManager manager = new RecoveryManager(storageIO, tempDir, options);

        RecoveryResult first = manager.recover();
        try {
            assertThatThrownBy(manager::recover)
                    .isInstanceOf(StorageException.class)
                    .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.LOCKED));
        } finally {
            first.activeWalWriter().close();
            first.manifestStore().close();
            first.lockHandle().close();
        }
    }

    @Test
    @DisplayName("Replays unsealed WAL records into MemTable and preserves highest sequence")
    void testReplayUnsealedWal(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DatabaseOptions options = DatabaseOptions.defaults();

        // 1. Initial run: write some records to WAL
        RecoveryManager manager1 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res1 = manager1.recover();
        long wal1Id = res1.activeWalId();

        WalWriter writer = res1.activeWalWriter();
        writer.append(10L, RecordKind.PUT, bytes("k1"), bytes("v1"));
        writer.append(20L, RecordKind.PUT, bytes("k2"), bytes("v2"));
        writer.append(25L, RecordKind.DELETE, bytes("k1"), null);

        // Close session
        writer.close();
        res1.manifestStore().close();
        res1.lockHandle().close();

        // 2. Restart and recover
        RecoveryManager manager2 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res2 = manager2.recover();

        try {
            assertThat(res2.highSequence()).isEqualTo(25L);
            assertThat(res2.activeWalId()).isGreaterThan(wal1Id);

            // Verify MemTable replayed accurately
            Optional<InternalRecord> k1 = res2.activeMemTable().get(key("k1"));
            assertThat(k1).isPresent();
            assertThat(k1.get().isTombstone()).isTrue();
            assertThat(k1.get().sequence()).isEqualTo(25L);

            Optional<InternalRecord> k2 = res2.activeMemTable().get(key("k2"));
            assertThat(k2).isPresent();
            assertThat(k2.get().value()).isEqualTo(bytes("v2"));
            assertThat(k2.get().sequence()).isEqualTo(20L);

            // Previous WAL should now be sealed in manifest
            ManifestSnapshot snapshot2 = res2.manifestSnapshot();
            assertThat(snapshot2.wals()).contains(new WalMeta(wal1Id, true));
            assertThat(snapshot2.wals()).contains(new WalMeta(res2.activeWalId(), false));

        } finally {
            res2.activeWalWriter().close();
            res2.manifestStore().close();
            res2.lockHandle().close();
        }
    }

    @Test
    @DisplayName("FI-01 / R-02: Torn write at EOF of WAL is truncated during recovery")
    void testTornWalTailTruncatedOnRecovery(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DatabaseOptions options = DatabaseOptions.defaults();

        RecoveryManager manager1 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res1 = manager1.recover();
        long walId = res1.activeWalId();

        WalWriter writer = res1.activeWalWriter();
        writer.append(1L, RecordKind.PUT, bytes("validKey"), bytes("validValue"));
        long validSize = ((DefaultWalWriter) writer).writeOffset();

        writer.append(2L, RecordKind.PUT, bytes("tornKey"), bytes("tornValue"));
        writer.close();
        res1.manifestStore().close();
        res1.lockHandle().close();

        // Simulate crash: cut WAL file 4 bytes into second record
        Path walPath = tempDir.resolve(RecoveryManager.walFileName(walId));
        byte[] rawBytes = Files.readAllBytes(walPath);
        byte[] tornBytes = new byte[(int) validSize + 4];
        System.arraycopy(rawBytes, 0, tornBytes, 0, tornBytes.length);
        Files.write(walPath, tornBytes);

        // Recover: should truncate torn frame and restore validKey
        RecoveryManager manager2 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res2 = manager2.recover();

        try {
            assertThat(res2.highSequence()).isEqualTo(1L);

            Optional<InternalRecord> valid = res2.activeMemTable().get(key("validKey"));
            assertThat(valid).isPresent();
            assertThat(valid.get().value()).isEqualTo(bytes("validValue"));

            Optional<InternalRecord> torn = res2.activeMemTable().get(key("tornKey"));
            assertThat(torn).isEmpty();

            // Verify physical truncation of the old WAL
            assertThat(Files.size(walPath)).isEqualTo(validSize);

        } finally {
            res2.activeWalWriter().close();
            res2.manifestStore().close();
            res2.lockHandle().close();
        }
    }
}
