package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.BatchOperation;
import java.util.List;
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
        writer.append(1L, RecordKind.PUT, bytes("key1"), bytes("val1"));
        writer.append(2L, RecordKind.PUT, bytes("key2"), bytes("val2"));
        writer.append(3L, RecordKind.DELETE, bytes("key1"), null);
        writer.close();
        res1.manifestStore().close();
        res1.lockHandle().close();

        // 2. Second recovery: should replay WAL 1 into MemTable and seal it
        RecoveryManager manager2 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res2 = manager2.recover();

        try {
            assertThat(res2.highSequence()).isEqualTo(3L);
            assertThat(res2.activeWalId()).isGreaterThan(wal1Id);

            // Verify MemTable contents
            Optional<InternalRecord> rec1 = res2.activeMemTable().get(key("key1"));
            assertThat(rec1).isPresent();
            assertThat(rec1.get().isTombstone()).isTrue();

            Optional<InternalRecord> rec2 = res2.activeMemTable().get(key("key2"));
            assertThat(rec2).isPresent();
            assertThat(rec2.get().value()).isEqualTo(bytes("val2"));

            // Manifest should have wal1 as sealed and new active WAL
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

    @Test
    @DisplayName("Bug #4: Uncommitted orphan WAL file on disk is cleaned up and does not cause FileAlreadyExistsException on recovery")
    void testUncommittedOrphanWalCleanedUpOnRecovery(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DatabaseOptions options = DatabaseOptions.defaults();

        // 1. First run creates WAL 1 and manifest with nextFileId = 2
        RecoveryManager manager1 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res1 = manager1.recover();
        res1.activeWalWriter().close();
        res1.manifestStore().close();
        res1.lockHandle().close();

        // 2. Simulate crash during rotation: 000002.wal exists on disk, but NOT in manifest
        Path orphanWal = tempDir.resolve(RecoveryManager.walFileName(2L));
        Files.write(orphanWal, bytes("leftover partial garbage"));
        assertThat(Files.exists(orphanWal)).isTrue();

        // 3. Recovery must succeed without FileAlreadyExistsException
        RecoveryManager manager2 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res2 = manager2.recover();
        try {
            assertThat(res2.activeWalId()).isEqualTo(2L);
            assertThat(Files.exists(orphanWal)).isTrue();
            // Should be a valid empty WAL, not corrupted garbage
            assertThat(Files.size(orphanWal)).isEqualTo(DefaultWalWriter.HEADER_SIZE);
        } finally {
            res2.activeWalWriter().close();
            res2.manifestStore().close();
            res2.lockHandle().close();
        }
    }

    @Test
    @DisplayName("RecoveryManager replays WAL batch frames and advances highSequence correctly")
    void testRecoveryReplaysBatchFrames(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DatabaseOptions options = DatabaseOptions.defaults();

        RecoveryManager manager1 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res1 = manager1.recover();
        long wal1Id = res1.activeWalId();

        WalWriter writer = res1.activeWalWriter();
        writer.append(1L, RecordKind.PUT, bytes("key1"), bytes("val1"));
        writer.appendBatch(2L, List.of(
                BatchOperation.put(bytes("key2"), bytes("val2")),
                BatchOperation.delete(bytes("key1")),
                BatchOperation.put(bytes("key3"), bytes("val3"))
        ));
        writer.close();
        res1.manifestStore().close();
        res1.lockHandle().close();

        RecoveryManager manager2 = new RecoveryManager(storageIO, tempDir, options);
        RecoveryResult res2 = manager2.recover();

        try {
            assertThat(res2.highSequence()).isEqualTo(4L);
            assertThat(res2.activeWalId()).isGreaterThan(wal1Id);

            Optional<InternalRecord> r1 = res2.activeMemTable().get(key("key1"));
            assertThat(r1).isPresent();
            assertThat(r1.get().isTombstone()).isTrue();

            Optional<InternalRecord> r2 = res2.activeMemTable().get(key("key2"));
            assertThat(r2).isPresent();
            assertThat(r2.get().value()).isEqualTo(bytes("val2"));

            Optional<InternalRecord> r3 = res2.activeMemTable().get(key("key3"));
            assertThat(r3).isPresent();
            assertThat(r3.get().value()).isEqualTo(bytes("val3"));
        } finally {
            res2.activeWalWriter().close();
            res2.manifestStore().close();
            res2.lockHandle().close();
        }
    }
}
