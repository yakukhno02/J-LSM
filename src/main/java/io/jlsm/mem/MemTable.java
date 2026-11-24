package io.jlsm.mem;

import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import java.util.Optional;
import java.util.Set;

public interface MemTable {
    long insert(ByteKey key, InternalRecord record);
    Optional<InternalRecord> get(ByteKey key);

    default Optional<InternalRecord> get(ByteKey key, long maxSequence) {
        return get(key);
    }

    RecordCursor scan(ByteKey fromKey, ByteKey toKey);

    default RecordCursor scan(ByteKey fromKey, ByteKey toKey, long maxSequence) {
        return scan(fromKey, toKey);
    }

    long byteSize();
    boolean isEmpty();
    void clear();
    void attachWalId(long walId);
    Set<Long> attachedWalIds();
}
