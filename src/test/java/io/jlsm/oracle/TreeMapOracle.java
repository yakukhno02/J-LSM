package io.jlsm.oracle;

import io.jlsm.api.Entry;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordKind;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * In-memory reference oracle backed by {@link TreeMap} (Section 6.1.1 of DESIGN.md).
 * Maintains exact key-value state sorted by {@link ByteKey} (unsigned lexicographical byte order)
 * to verify J-LSM operations, range scans, and compaction invariants.
 */
public final class TreeMapOracle {

    private final NavigableMap<ByteKey, InternalRecord> state = new TreeMap<>();
    private long nextSequence = 0L;

    /**
     * Applies a PUT operation with an auto-incremented sequence number.
     */
    public InternalRecord put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return apply(new InternalRecord(ByteKey.copyOf(key), value.clone(), RecordKind.PUT, ++nextSequence));
    }

    /**
     * Applies a DELETE operation with an auto-incremented sequence number.
     */
    public InternalRecord delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        return apply(new InternalRecord(ByteKey.copyOf(key), null, RecordKind.DELETE, ++nextSequence));
    }

    /**
     * Applies an {@link InternalRecord} if its sequence number is newer than any existing record for the same key.
     */
    public InternalRecord apply(InternalRecord record) {
        Objects.requireNonNull(record, "record cannot be null");
        if (record.sequence() > nextSequence) {
            nextSequence = record.sequence();
        }
        state.compute(record.key(), (k, existing) -> {
            if (existing == null || record.sequence() >= existing.sequence()) {
                return record;
            }
            return existing;
        });
        return record;
    }

    /**
     * Point lookup returning the live value for {@code key}, or {@code null} if absent or deleted.
     */
    public byte[] get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        InternalRecord rec = state.get(ByteKey.copyOf(key));
        if (rec == null || rec.isTombstone()) {
            return null;
        }
        return rec.value();
    }

    /**
     * Returns the raw latest {@link InternalRecord} (including tombstones) for {@code key}, or {@code null} if never written.
     */
    public InternalRecord getRaw(ByteKey key) {
        return state.get(Objects.requireNonNull(key, "key cannot be null"));
    }

    /**
     * Performs a range scan over {@code [fromKey, toKey)} returning live {@link Entry} items.
     */
    public List<Entry> scan(byte[] fromKey, byte[] toKey) {
        ByteKey from = fromKey != null ? ByteKey.copyOf(fromKey) : null;
        ByteKey to = toKey != null ? ByteKey.copyOf(toKey) : null;

        if (from != null && to != null && from.compareTo(to) > 0) {
            return List.of();
        }

        NavigableMap<ByteKey, InternalRecord> view;
        if (from == null && to == null) {
            view = state;
        } else if (from == null) {
            view = state.headMap(to, false);
        } else if (to == null) {
            view = state.tailMap(from, true);
        } else {
            view = state.subMap(from, true, to, false);
        }

        List<Entry> result = new ArrayList<>();
        for (InternalRecord rec : view.values()) {
            if (!rec.isTombstone()) {
                result.add(new Entry(rec.key().bytes(), rec.value(), false));
            }
        }
        return result;
    }

    /**
     * Returns all surviving records expected after a full compaction.
     *
     * @param retainTombstones if false, tombstones are omitted; if true, tombstones are included
     */
    public List<InternalRecord> expectedCompactedRecords(boolean retainTombstones) {
        List<InternalRecord> result = new ArrayList<>();
        for (InternalRecord rec : state.values()) {
            if (retainTombstones || !rec.isTombstone()) {
                result.add(rec);
            }
        }
        return result;
    }

    public void clear() {
        state.clear();
        nextSequence = 0L;
    }
}
