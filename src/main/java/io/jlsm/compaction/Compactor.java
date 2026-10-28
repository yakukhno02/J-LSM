package io.jlsm.compaction;

import io.jlsm.api.StorageException;
import io.jlsm.manifest.TableMeta;
import io.jlsm.sst.TableHandle;
import java.util.List;
import java.util.Optional;

public interface Compactor {
    // Виконує K-Way Merge переданих таблиць.
    // Якщо всі таблиці порожні або всі записи знищені Tombstones, повертає Optional.empty().
    // retainTombstones = true використовується лише при Recovery. У normal compaction = false!
    Optional<TableMeta> build(long outputId, List<TableHandle> pinnedInputs, boolean retainTombstones)
            throws StorageException;
}
