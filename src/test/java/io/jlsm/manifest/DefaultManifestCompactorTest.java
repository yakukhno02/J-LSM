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
}
