package io.jlsm.manifest;

import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.io.StorageIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultManifestCompactorTest {

    @Test
    @DisplayName("Manifest compactor rewrites incremental history into a single snapshot frame")
    void testManifestCompaction(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        Path manifestPath = tempDir.resolve("MANIFEST");

        ManifestSnapshot snapshot = ManifestSnapshot.empty();
        try (ChannelIO channel = storageIO.createNew(manifestPath);
             DefaultManifestStore store = DefaultManifestStore.open(channel, manifestPath, snapshot)) {
            // Append 10 incremental snapshots
            for (int i = 1; i <= 10; i++) {
                snapshot = new ManifestSnapshot(
                        i,
                        i + 1,
                        i * 100L,
                        List.of(new WalMeta(i, false)),
                        List.of(new TableMeta(i, 0, 100, 50, 1024))
                );
                store.commit(snapshot);
            }
        }

        long uncompactedSize = storageIO.openRead(manifestPath).size();
        assertThat(uncompactedSize).isGreaterThan(0L);

        // Compact manifest
        DefaultManifestCompactor compactor = new DefaultManifestCompactor(storageIO, tempDir);
        long compactedSize = compactor.compact(snapshot);

        // Compacted size should be smaller than 10 incremental frames
        assertThat(compactedSize).isLessThan(uncompactedSize);

        // Verify reopened manifest matches the compacted snapshot exactly
        try (ChannelIO channel = storageIO.openRead(manifestPath);
             DefaultManifestStore reopened = DefaultManifestStore.open(channel, manifestPath, ManifestSnapshot.empty())) {
            ManifestSnapshot recovered = reopened.currentSnapshot();
            assertThat(recovered.epoch()).isEqualTo(10L);
            assertThat(recovered.highSequence()).isEqualTo(1000L);
            assertThat(recovered.tables()).hasSize(1);
        }
    }

    @Test
    @DisplayName("Manifest compactor validates arguments")
    void testValidation(@TempDir Path tempDir) {
        StorageIO storageIO = new StandardStorageIO();
        assertThatThrownBy(() -> new DefaultManifestCompactor(null, tempDir))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DefaultManifestCompactor(storageIO, null))
                .isInstanceOf(NullPointerException.class);

        DefaultManifestCompactor compactor = new DefaultManifestCompactor(storageIO, tempDir);
        assertThatThrownBy(() -> compactor.compact(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Manifest compactor returns actual file size and preserves directory getter")
    void testCompactorReturnSizeAndDirectory(@TempDir Path tempDir) throws Exception {
        StorageIO storageIO = new StandardStorageIO();
        DefaultManifestCompactor compactor = new DefaultManifestCompactor(storageIO, tempDir);
        assertThat(compactor.directory()).isEqualTo(tempDir);

        Path manifestPath = tempDir.resolve("MANIFEST");
        ManifestSnapshot snapshot = new ManifestSnapshot(
                1, 2, 100L,
                List.of(new WalMeta(1, false)),
                List.of(new TableMeta(1, 0, 100, 50, 1024))
        );
        try (ChannelIO channel = storageIO.createNew(manifestPath);
             DefaultManifestStore store = DefaultManifestStore.open(channel, manifestPath, ManifestSnapshot.empty())) {
            store.commit(snapshot);
        }

        // Create an existing MANIFEST.tmp to ensure deleteIfExists is called
        Path tmpPath = tempDir.resolve("MANIFEST.tmp");
        java.nio.file.Files.writeString(tmpPath, "stale temp content");

        long compactedSize = compactor.compact(snapshot);
        long actualDiskSize = java.nio.file.Files.size(manifestPath);
        assertThat(compactedSize).isEqualTo(actualDiskSize);
        assertThat(java.nio.file.Files.exists(tmpPath)).isFalse();
    }

    @Test
    @DisplayName("Manifest compactor cleans up temporary file upon store failure")
    void testFailureCleansUpTempFile(@TempDir Path tempDir) throws Exception {
        io.jlsm.internal.io.FaultInjectingStorage storage = new io.jlsm.internal.io.FaultInjectingStorage();
        DefaultManifestCompactor compactor = new DefaultManifestCompactor(storage, tempDir);

        Path manifestPath = tempDir.resolve("MANIFEST");
        ManifestSnapshot snapshot = new ManifestSnapshot(
                1, 2, 100L,
                List.of(new WalMeta(1, false)),
                List.of(new TableMeta(1, 0, 100, 50, 1024))
        );

        // Fail atomic move
        storage.failOnAtomicMove(true);

        assertThatThrownBy(() -> compactor.compact(snapshot))
                .isInstanceOf(io.jlsm.api.StorageException.class)
                .hasMessageContaining("Failed to atomically publish compacted MANIFEST");

        // The temporary file should have been cleaned up in the catch block
        Path tmpPath = tempDir.resolve("MANIFEST.tmp");
        assertThat(java.nio.file.Files.exists(tmpPath)).isFalse();
    }
}
