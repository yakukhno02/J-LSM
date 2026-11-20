package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.FaultInjectingStorage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FaultInjectionMatrixTest {

    @TempDir
    Path tempDir;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 8, 10, 15})
    @DisplayName("Failure on N-th I/O call results in clean rejection and safe recovery")
    void testFailureOnNthIoCall(long nthCall) throws Exception {
        Path testDir = Files.createTempDirectory(tempDir, "nth-io-" + nthCall + "-");
        FaultInjectingStorage faultStorage = new FaultInjectingStorage();
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.SYNC,
                256L,
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        // Pre-populate with consistent baseline data
        try (KVDatabase db = KVDatabase.open(testDir, options)) {
            db.put(bytes("baseline_k"), bytes("baseline_v"));
            db.sync();
        }

        // Open with fault injection enabled on N-th I/O call
        faultStorage.failOnNthIoCall(nthCall);

        try {
            RecoveryResult recoveryResult = new RecoveryManager(faultStorage, testDir, options).recover();
            try (DefaultStorageEngine engine = new DefaultStorageEngine(faultStorage, testDir, options, recoveryResult)) {
                for (int i = 0; i < 10; i++) {
                    engine.put(bytes("key_" + i), bytes("val_" + i));
                }
            }
        } catch (StorageException e) {
            // Expected I/O or corruption failure triggered by fault injector
            assertThat(e.code()).isIn(ErrorCode.IO, ErrorCode.CORRUPTION, ErrorCode.LOCKED);
        }

        // After fault reset, recovery must cleanly recover or identify clean state
        faultStorage.reset();
        try (KVDatabase recovered = KVDatabase.open(testDir, options)) {
            // Baseline data must always be preserved
            assertThat(asString(recovered.get(bytes("baseline_k")))).isEqualTo("baseline_v");
        }
    }

    @Test
    @DisplayName("Torn write during SSTable flush recovers safely")
    void testTornWriteDuringSSTableFlush() throws Exception {
        Path testDir = Files.createTempDirectory(tempDir, "torn-write-");
        FaultInjectingStorage faultStorage = new FaultInjectingStorage();
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.SYNC,
                256L,
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        RecoveryResult rec = new RecoveryManager(faultStorage, testDir, options).recover();
        DefaultStorageEngine engine = new DefaultStorageEngine(faultStorage, testDir, options, rec);
        try {
            engine.put(bytes("key1"), bytes("value1"));
            engine.rotateMemTable();

            // Fail after writing 50 bytes (torn write)
            faultStorage.failAfterBytesWritten(50);

            assertThatThrownBy(engine::flushImmutableMemTable)
                    .isInstanceOf(StorageException.class);
        } finally {
            faultStorage.reset();
            assertThatThrownBy(engine::close)
                    .isInstanceOf(StorageException.class);
        }

        // Clean recovery
        try (KVDatabase reopened = KVDatabase.open(testDir, options)) {
            assertThat(asString(reopened.get(bytes("key1")))).isEqualTo("value1");
        }
    }

    @Test
    @DisplayName("Failure during atomicMove or syncDirectory rejects cleanly")
    void testFailureDuringMoveOrSync() throws Exception {
        Path testDir = Files.createTempDirectory(tempDir, "move-sync-");
        FaultInjectingStorage faultStorage = new FaultInjectingStorage();
        DatabaseOptions defaults = DatabaseOptions.defaults();
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.SYNC,
                256L,
                defaults.compactionTrigger(),
                defaults.maxOpenScans(),
                defaults.maxSnapshotBytes(),
                defaults.maxIndexBytes(),
                defaults.maxLiveTables()
        );

        RecoveryResult rec = new RecoveryManager(faultStorage, testDir, options).recover();
        DefaultStorageEngine engine = new DefaultStorageEngine(faultStorage, testDir, options, rec);
        try {
            engine.put(bytes("keyA"), bytes("valA"));
            engine.rotateMemTable();

            faultStorage.failOnAtomicMove(true);

            assertThatThrownBy(engine::flushImmutableMemTable)
                    .isInstanceOf(StorageException.class);
        } finally {
            faultStorage.reset();
            assertThatThrownBy(engine::close)
                    .isInstanceOf(StorageException.class);
        }

        try (KVDatabase reopened = KVDatabase.open(testDir, options)) {
            assertThat(asString(reopened.get(bytes("keyA")))).isEqualTo("valA");
        }
    }
}
