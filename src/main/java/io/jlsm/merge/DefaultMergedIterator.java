package io.jlsm.merge;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Standard K-way merge iterator implementing {@link MergedIterator}.
 * <p>
 * Merges multiple sorted {@link RecordCursor} streams using a priority heap:
 * 1. Primary order: ascending by unsigned key byte order.
 * 2. Secondary order: descending by sequence number (latest version wins).
 * 3. Deduplication: multiple versions of the same key are collapsed to the highest sequence.
 * 4. Tombstone handling: tombstones can be filtered out (for scans) or retained (for compaction).
 */
public final class DefaultMergedIterator implements MergedIterator {

    private final List<RecordCursor> inputs;
    private final PriorityQueue<CursorHead> heap;
    private final boolean retainTombstones;
    private InternalRecord currentRecord;
    private boolean closed;

    public DefaultMergedIterator(List<RecordCursor> inputs, boolean retainTombstones) throws StorageException {
        Objects.requireNonNull(inputs, "inputs cannot be null");
        this.inputs = new ArrayList<>(inputs);
        this.heap = new PriorityQueue<>();
        this.retainTombstones = retainTombstones;

        for (int i = 0; i < this.inputs.size(); i++) {
            RecordCursor cursor = this.inputs.get(i);
            if (cursor != null && cursor.advance()) {
                heap.add(new CursorHead(cursor, i));
            }
        }
    }

    @Override
    public boolean advance() throws StorageException {
        ensureOpen();

        while (!heap.isEmpty()) {
            CursorHead winnerHead = heap.poll();
            InternalRecord winner = winnerHead.current();

            // Advance the winning cursor
            advanceCursorAndReinsert(winnerHead);

            // Deduplicate: advance all other cursors pointing to the exact same key
            while (!heap.isEmpty() && heap.peek().current().key().equals(winner.key())) {
                CursorHead duplicateHead = heap.poll();
                advanceCursorAndReinsert(duplicateHead);
            }

            // If tombstone filtering is active, skip tombstones
            if (winner.isTombstone() && !retainTombstones) {
                continue;
            }

            this.currentRecord = winner;
            return true;
        }

        this.currentRecord = null;
        return false;
    }

    @Override
    public InternalRecord current() {
        if (closed) {
            throw new IllegalStateException("MergedIterator is closed");
        }
        if (currentRecord == null) {
            throw new IllegalStateException("Cursor current() called before advance() or after EOF");
        }
        return currentRecord;
    }

    @Override
    public void close() throws StorageException {
        if (closed) {
            return;
        }
        closed = true;
        currentRecord = null;
        heap.clear();

        StorageException firstException = null;
        for (RecordCursor cursor : inputs) {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (StorageException e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                } catch (Exception e) {
                    StorageException wrapped = new StorageException(ErrorCode.IO, "Error closing input cursor", e);
                    if (firstException == null) {
                        firstException = wrapped;
                    } else {
                        firstException.addSuppressed(wrapped);
                    }
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    private void advanceCursorAndReinsert(CursorHead head) throws StorageException {
        if (head.cursor.advance()) {
            heap.add(head);
        }
    }

    private void ensureOpen() throws StorageException {
        if (closed) {
            throw new StorageException(ErrorCode.CLOSED, "MergedIterator is closed");
        }
    }

    private static final class CursorHead implements Comparable<CursorHead> {
        private final RecordCursor cursor;
        private final int index;

        CursorHead(RecordCursor cursor, int index) {
            this.cursor = cursor;
            this.index = index;
        }

        InternalRecord current() {
            return cursor.current();
        }

        @Override
        public int compareTo(CursorHead other) {
            InternalRecord r1 = this.current();
            InternalRecord r2 = other.current();

            // 1. Ascending by key
            int cmp = r1.key().compareTo(r2.key());
            if (cmp != 0) {
                return cmp;
            }

            // 2. Descending by sequence (highest sequence wins)
            int seqCmp = Long.compare(r2.sequence(), r1.sequence());
            if (seqCmp != 0) {
                return seqCmp;
            }

            // 3. Deterministic tie-breaker
            return Integer.compare(this.index, other.index);
        }
    }
}
