package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.Entry;
import io.jlsm.api.Snapshot;
import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RangeTombstone;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.mem.MemTable;
import io.jlsm.merge.DefaultMergedIterator;
import io.jlsm.sst.DiskStorageReader;
import io.jlsm.sst.TableHandle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Standard implementation of {@link Snapshot} providing point-in-time consistent reads
 * isolated at a fixed sequence number.
 */
public final class DefaultSnapshot implements Snapshot {

    private final DefaultStorageEngine engine;
    private final long sequenceNumber;
    private final MemTable activeMemTable;
    private final MemTable immutableMemTable;
    private final List<TableHandle> pinnedTables;
    private final DiskStorageReader diskStorageReader;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DefaultSnapshot(
            DefaultStorageEngine engine,
            long sequenceNumber,
            MemTable activeMemTable,
            MemTable immutableMemTable,
            List<TableHandle> pinnedTables,
            DiskStorageReader diskStorageReader
    ) {
        this.engine = Objects.requireNonNull(engine, "engine cannot be null");
        this.sequenceNumber = sequenceNumber;
        this.activeMemTable = Objects.requireNonNull(activeMemTable, "activeMemTable cannot be null");
        this.immutableMemTable = immutableMemTable;
        this.pinnedTables = Objects.requireNonNull(pinnedTables, "pinnedTables cannot be null");
        this.diskStorageReader = Objects.requireNonNull(diskStorageReader, "diskStorageReader cannot be null");
    }

    @Override
    public byte[] get(byte[] key) throws StorageException {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        ensureOpen();
        engine.ensureEngineOpen();

        ByteKey byteKey = ByteKey.copyOf(key);

        // 1. Check active memtable with snapshot sequence filter
        Optional<InternalRecord> rec = activeMemTable.get(byteKey, sequenceNumber);
        if (rec.isPresent()) {
            return extractValue(rec.get());
        }

        // 2. Check immutable memtable if captured
        if (immutableMemTable != null) {
            rec = immutableMemTable.get(byteKey, sequenceNumber);
            if (rec.isPresent()) {
                return extractValue(rec.get());
            }
        }

        // 3. Check pinned SSTables with snapshot sequence filter
        Optional<InternalRecord> diskRec = diskStorageReader.search(pinnedTables, byteKey, sequenceNumber);
        return diskRec.map(this::extractValue).orElse(null);
    }

    @Override
    public ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException {
        ensureOpen();
        engine.ensureEngineOpen();

        ByteKey from = fromKey != null ? ByteKey.copyOf(fromKey) : null;
        ByteKey to = toKey != null ? ByteKey.copyOf(toKey) : null;

        if (from != null && to != null && from.compareTo(to) > 0) {
            return new EmptyClosableIterator();
        }

        List<RecordCursor> cursors = new ArrayList<>();
        cursors.add(activeMemTable.scan(from, to, sequenceNumber));
        if (immutableMemTable != null) {
            cursors.add(immutableMemTable.scan(from, to, sequenceNumber));
        }

        List<RangeTombstone> rangeTombstones = new ArrayList<>();
        for (InternalRecord.RangeDelete rd : activeMemTable.rangeTombstones()) {
            if (rd.sequence() <= sequenceNumber) {
                rangeTombstones.add(rd.toRangeTombstone());
            }
        }
        if (immutableMemTable != null) {
            for (InternalRecord.RangeDelete rd : immutableMemTable.rangeTombstones()) {
                if (rd.sequence() <= sequenceNumber) {
                    rangeTombstones.add(rd.toRangeTombstone());
                }
            }
        }
        for (TableHandle handle : pinnedTables) {
            if (handle.minSequence() <= sequenceNumber) {
                for (RangeTombstone rt : handle.rangeTombstones()) {
                    if (rt.sequenceNumber() <= sequenceNumber) {
                        rangeTombstones.add(rt);
                    }
                }
            }
        }

        RecordCursor diskCursor = diskStorageReader.scanDisk(pinnedTables, from, to, sequenceNumber);
        cursors.add(diskCursor);

        DefaultMergedIterator merged = new DefaultMergedIterator(cursors, false, rangeTombstones);
        return new SnapshotScanIterator(merged);
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (TableHandle handle : pinnedTables) {
                handle.unpin();
            }
            engine.onSnapshotClosed(this);
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Snapshot is closed");
        }
    }

    private byte[] extractValue(InternalRecord record) {
        if (record.isTombstone()) {
            return null;
        }
        byte[] val = record.value();
        return val != null ? Arrays.copyOf(val, val.length) : new byte[0];
    }

    private static final class SnapshotScanIterator implements ClosableIterator<Entry> {
        private final DefaultMergedIterator merged;
        private Entry nextEntry;
        private boolean iterClosed;

        private SnapshotScanIterator(DefaultMergedIterator merged) {
            this.merged = merged;
        }

        @Override
        public synchronized boolean hasNext() throws StorageException {
            if (iterClosed) {
                return false;
            }
            if (nextEntry != null) {
                return true;
            }
            while (merged.advance()) {
                InternalRecord rec = merged.current();
                if (!rec.isTombstone()) {
                    nextEntry = new Entry(rec.key().bytes(), rec.value(), false);
                    return true;
                }
            }
            return false;
        }

        @Override
        public synchronized Entry next() throws StorageException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries in scan");
            }
            Entry entry = nextEntry;
            nextEntry = null;
            return entry;
        }

        @Override
        public synchronized void close() throws StorageException {
            if (!iterClosed) {
                iterClosed = true;
                merged.close();
            }
        }
    }

    private static final class EmptyClosableIterator implements ClosableIterator<Entry> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry next() {
            throw new NoSuchElementException("Iterator is empty");
        }

        @Override
        public void close() {}
    }
}
