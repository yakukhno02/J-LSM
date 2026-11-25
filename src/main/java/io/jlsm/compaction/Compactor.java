package io.jlsm.compaction;

import io.jlsm.api.StorageException;
import io.jlsm.manifest.TableMeta;
import io.jlsm.sst.TableHandle;

import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

public interface Compactor {
    /**
     * Default target SSTable file size for split compaction (2 MiB).
     */
    long DEFAULT_TARGET_FILE_SIZE = 2L * 1024 * 1024;

    // Виконує K-Way Merge переданих таблиць в один вихідний файл.
    // Якщо всі таблиці порожні або всі записи знищені Tombstones, повертає Optional.empty().
    // retainTombstones = true використовується лише при Recovery. У normal compaction = false!
    Optional<TableMeta> build(long outputId, List<TableHandle> pinnedInputs, boolean retainTombstones)
            throws StorageException;

    /**
     * Виконує K-Way Merge переданих таблиць з автоматичним розбиттям вихідного потоку
     * на декілька SSTable файлів фіксованого цільового розміру ({@code targetFileSizeBytes}).
     * Повертає порожній список, якщо після злиття не залишилось жодного живого запису.
     */
    default List<TableMeta> buildSplit(
            LongSupplier nextFileIdSupplier,
            List<TableHandle> pinnedInputs,
            boolean retainTombstones,
            long targetFileSizeBytes
    ) throws StorageException {
        if (nextFileIdSupplier == null) {
            throw new NullPointerException("nextFileIdSupplier cannot be null");
        }
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be positive: " + targetFileSizeBytes);
        }
        if (pinnedInputs == null) {
            throw new NullPointerException("pinnedInputs cannot be null");
        }
        if (pinnedInputs.isEmpty()) {
            return List.of();
        }
        Optional<TableMeta> single = build(nextFileIdSupplier.getAsLong(), pinnedInputs, retainTombstones);
        return single.map(List::of).orElseGet(List::of);
    }
}
