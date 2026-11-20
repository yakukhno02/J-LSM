package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.Entry;
import io.jlsm.api.StorageException;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.TableMeta;
import io.jlsm.manifest.WalMeta;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates global engine invariants after operations:
 * 1. get(k) returns identical value as scan(null, null) for all keys.
 * 2. memoryBudgetManager.allocatedBytes() == activeMemTable.byteSize() + immutableMemTable.byteSize().
 * 3. .wal and .sst files on disk strictly correspond to the active MANIFEST snapshot (no leaks, no missing files).
 */
public final class EngineInvariantValidator {

    private EngineInvariantValidator() {}

    public static void validateAll(DefaultStorageEngine engine) throws StorageException {
        validateMemoryBudget(engine);
        validateDiskManifestConsistency(engine);
        validateGetMatchesScan(engine);
    }

    public static void validateMemoryBudget(DefaultStorageEngine engine) {
        long activeBytes = engine.activeMemTable().byteSize();
        long immutableBytes = engine.immutableMemTable() != null ? engine.immutableMemTable().byteSize() : 0L;
        long expectedTotal = activeBytes + immutableBytes;
        long actualAllocated = engine.memoryBudgetManager().allocatedBytes();

        assertThat(actualAllocated)
                .as("Memory budget allocatedBytes must exactly match activeMemTable + immutableMemTable")
                .isEqualTo(expectedTotal);
    }

    public static void validateDiskManifestConsistency(DefaultStorageEngine engine) {
        Path directory = engine.directory();
        ManifestSnapshot snapshot = engine.manifestStore().currentSnapshot();

        Set<String> expectedFiles = new HashSet<>();
        expectedFiles.add("MANIFEST");
        expectedFiles.add("LOCK");

        for (WalMeta wal : snapshot.wals()) {
            expectedFiles.add(RecoveryManager.walFileName(wal.walId()));
        }

        for (TableMeta table : snapshot.tables()) {
            expectedFiles.add(String.format("%06d.sst", table.tableId()));
        }

        try (Stream<Path> stream = Files.list(directory)) {
            Set<String> actualFiles = stream
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.toSet());

            // Ignore temporary compaction/flush files that might be currently in flight
            Set<String> persistentActualFiles = actualFiles.stream()
                    .filter(name -> !name.endsWith(".tmp"))
                    .collect(Collectors.toSet());

            assertThat(persistentActualFiles)
                    .as("Files on disk must strictly correspond to active MANIFEST snapshot")
                    .isEqualTo(expectedFiles);
        } catch (IOException e) {
            throw new AssertionError("Failed to list database directory: " + directory, e);
        }
    }

    public static void validateGetMatchesScan(DefaultStorageEngine engine) throws StorageException {
        try (ClosableIterator<Entry> scanIt = engine.scan(null, null)) {
            while (scanIt.hasNext()) {
                Entry entry = scanIt.next();
                byte[] getVal = engine.get(entry.key());
                assertThat(Arrays.equals(getVal, entry.value()))
                        .as("get(k) must match scan(null, null) for key %s", Arrays.toString(entry.key()))
                        .isTrue();
            }
        }
    }
}
