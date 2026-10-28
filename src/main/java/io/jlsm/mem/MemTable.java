package io.jlsm.mem;

import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import java.util.Optional;

public interface MemTable {
    void insert(ByteKey key, InternalRecord record);
    Optional<InternalRecord> get(ByteKey key);
    RecordCursor scan(ByteKey fromKey, ByteKey toKey);
    long byteSize();
    boolean isEmpty();
    void clear();
}
