package io.jlsm.compaction;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.manifest.TableMeta;
import io.jlsm.merge.DefaultMergedIterator;
import io.jlsm.sst.DefaultSSTableFlusher;
import io.jlsm.sst.DefaultTableHandle;
import io.jlsm.sst.SSTableFlusher;
import io.jlsm.sst.TableHandle;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Standard implementation of {@link Compactor} that performs a K-way merge over a list of pinned
 * {@link TableHandle} instances, deduplicating records by sequence number and optionally purging
 * tombstones before writing out a new consolidated SSTable.
 */
public final class DefaultCompactor implements Compactor {

    private final SSTableFlusher flusher;

    public DefaultCompactor(Path baseDir, StorageIO storageIO) {
        this(new DefaultSSTableFlusher(
                Objects.requireNonNull(baseDir, "baseDir cannot be null"),
                Objects.requireNonNull(storageIO, "storageIO cannot be null")
        ));
    }

    public DefaultCompactor(SSTableFlusher flusher) {
        this.flusher = Objects.requireNonNull(flusher, "flusher cannot be null");
    }

    @Override
    public Optional<TableMeta> build(long outputId, List<TableHandle> pinnedInputs, boolean retainTombstones)
            throws StorageException {
        if (outputId < 0) {
            throw new IllegalArgumentException("outputId cannot be negative: " + outputId);
        }
        Objects.requireNonNull(pinnedInputs, "pinnedInputs cannot be null");

        if (pinnedInputs.isEmpty()) {
            return Optional.empty();
        }

        List<RecordCursor> cursors = new ArrayList<>(pinnedInputs.size());
        try {
            for (TableHandle handle : pinnedInputs) {
                Objects.requireNonNull(handle, "pinnedInputs cannot contain null TableHandle");
                if (handle instanceof DefaultTableHandle defaultHandle) {
                    cursors.add(defaultHandle.scan(null, null));
                } else {
                    throw new StorageException(ErrorCode.UNSUPPORTED_STORAGE,
                            "Unsupported TableHandle implementation: " + handle.getClass().getName());
                }
            }
        } catch (Exception e) {
            closeQuietly(cursors);
            if (e instanceof StorageException se) {
                throw se;
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new StorageException(ErrorCode.IO, "Failed to open table cursors for compaction", e);
        }

        try (DefaultMergedIterator merged = new DefaultMergedIterator(cursors, retainTombstones)) {
            if (!merged.advance()) {
                // All input tables are empty or all records were annihilated by tombstones
                return Optional.empty();
            }

            InternalRecord firstRecord = merged.current();
            RecordCursor peekedCursor = new PrependRecordCursor(firstRecord, merged);
            TableMeta meta = flusher.build(outputId, peekedCursor);
            return Optional.of(meta);
        }
    }

    private static void closeQuietly(List<RecordCursor> cursors) {
        for (RecordCursor cursor : cursors) {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Cursor wrapper that yields a pre-fetched first record before delegating subsequent
     * advances to the underlying {@link RecordCursor}.
     */
    private static final class PrependRecordCursor implements RecordCursor {
        private final InternalRecord firstRecord;
        private final RecordCursor delegate;
        private int state = 0; // 0 = before first advance, 1 = on firstRecord, 2 = delegating, 3 = EOF/closed

        PrependRecordCursor(InternalRecord firstRecord, RecordCursor delegate) {
            this.firstRecord = firstRecord;
            this.delegate = delegate;
        }

        @Override
        public boolean advance() throws StorageException {
            if (state == 0) {
                state = 1;
                return true;
            }
            if (state == 1 || state == 2) {
                if (delegate.advance()) {
                    state = 2;
                    return true;
                }
                state = 3;
                return false;
            }
            return false;
        }

        @Override
        public InternalRecord current() {
            if (state == 1) {
                return firstRecord;
            }
            if (state == 2) {
                return delegate.current();
            }
            throw new IllegalStateException("Cursor current() called before advance() or after EOF");
        }

        @Override
        public void close() throws StorageException {
            state = 3;
            delegate.close();
        }
    }
}
