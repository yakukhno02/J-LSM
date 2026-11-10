package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.Entry;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.LockHandle;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.ManifestSnapshot;
import io.jlsm.manifest.ManifestStore;
import io.jlsm.manifest.TableMeta;
import io.jlsm.manifest.WalMeta;
import io.jlsm.mem.DefaultMemTable;
import io.jlsm.mem.MemTable;
import io.jlsm.merge.DefaultMergedIterator;
import io.jlsm.sst.DefaultDiskStorageReader;
import io.jlsm.sst.DefaultSSTableFlusher;
import io.jlsm.sst.DefaultTableHandle;
import io.jlsm.sst.DiskStorageReader;
import io.jlsm.sst.SSTableFlusher;
import io.jlsm.sst.TableHandle;
import io.jlsm.wal.DefaultWalWriter;
import io.jlsm.wal.WalWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class DefaultStorageEngine implements KVDatabase {

    private final StorageIO storageIO;
    private final Path directory;
    private final DatabaseOptions options;
    private final LockHandle lockHandle;
    private final ManifestStore manifestStore;
    private final DiskStorageReader diskStorageReader;
    private final SSTableFlusher sstableFlusher;

    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition capacityAvailable = stateLock.newCondition();

    private final Set<ClosableIterator<?>> openScans = ConcurrentHashMap.newKeySet();

    private MemTable activeMemTable;
    private MemTable immutableMemTable;
    private WalWriter activeWalWriter;
    private long activeWalId;
    private long immutableWalId;
    private long highSequence;
    private long nextFileId;
    private long currentEpoch;

    private final List<TableHandle> liveTables;
    private boolean closed;

    public DefaultStorageEngine(
            StorageIO storageIO,
            Path directory,
            DatabaseOptions options,
            RecoveryResult recoveryResult
    ) throws StorageException {
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
        this.directory = Objects.requireNonNull(directory, "directory cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(recoveryResult, "recoveryResult cannot be null");

        this.lockHandle = recoveryResult.lockHandle();
        this.manifestStore = recoveryResult.manifestStore();
        this.activeMemTable = recoveryResult.activeMemTable();
        this.activeWalWriter = recoveryResult.activeWalWriter();
        this.activeWalId = recoveryResult.activeWalId();
        this.highSequence = recoveryResult.highSequence();

        ManifestSnapshot snapshot = recoveryResult.manifestSnapshot();
        this.currentEpoch = snapshot.epoch();
        this.nextFileId = snapshot.nextFileId();
        this.diskStorageReader = new DefaultDiskStorageReader();
        this.sstableFlusher = new DefaultSSTableFlusher(directory, storageIO);

        this.liveTables = new ArrayList<>();
        for (TableMeta meta : snapshot.tables()) {
            Path sstPath = directory.resolve(String.format("%06d.sst", meta.tableId()));
            this.liveTables.add(DefaultTableHandle.open(storageIO, sstPath, meta));
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws StorageException {
        validateKeyValue(key, value, false);

        stateLock.lock();
        try {
            ensureOpen();
            checkAndRotateMemTable();
            long seq = ++highSequence;
            activeWalWriter.append(seq, RecordKind.PUT, key, value);
            ByteKey byteKey = ByteKey.copyOf(key);
            activeMemTable.insert(byteKey, new InternalRecord(byteKey, value, RecordKind.PUT, seq));
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public byte[] get(byte[] key) throws StorageException {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        ByteKey byteKey = ByteKey.copyOf(key);
        MemTable active;
        MemTable imm;
        List<TableHandle> pinnedTables;

        stateLock.lock();
        try {
            ensureOpen();
            Optional<InternalRecord> rec = activeMemTable.get(byteKey);
            if (rec.isPresent()) {
                return extractValue(rec.get());
            }

            if (immutableMemTable != null) {
                rec = immutableMemTable.get(byteKey);
                if (rec.isPresent()) {
                    return extractValue(rec.get());
                }
            }

            pinnedTables = new ArrayList<>(liveTables.size());
            for (TableHandle handle : liveTables) {
                if (handle instanceof DefaultTableHandle defHandle) {
                    defHandle.pin();
                    pinnedTables.add(defHandle);
                }
            }
        } finally {
            stateLock.unlock();
        }

        try {
            Optional<InternalRecord> diskRec = diskStorageReader.search(pinnedTables, byteKey);
            return diskRec.map(this::extractValue).orElse(null);
        } finally {
            for (TableHandle handle : pinnedTables) {
                if (handle instanceof DefaultTableHandle defHandle) {
                    defHandle.unpin();
                }
            }
        }
    }

    @Override
    public void delete(byte[] key) throws StorageException {
        validateKeyValue(key, null, true);

        stateLock.lock();
        try {
            ensureOpen();
            checkAndRotateMemTable();
            long seq = ++highSequence;
            activeWalWriter.append(seq, RecordKind.DELETE, key, null);
            ByteKey byteKey = ByteKey.copyOf(key);
            activeMemTable.insert(byteKey, new InternalRecord(byteKey, null, RecordKind.DELETE, seq));
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException {
        ByteKey from = fromKey != null ? ByteKey.copyOf(fromKey) : null;
        ByteKey to = toKey != null ? ByteKey.copyOf(toKey) : null;

        if (from != null && to != null && from.compareTo(to) > 0) {
            return new EmptyClosableIterator();
        }

        List<RecordCursor> cursors = new ArrayList<>();
        List<TableHandle> pinnedTables = new ArrayList<>();

        stateLock.lock();
        try {
            ensureOpen();
            if (openScans.size() >= options.maxOpenScans()) {
                throw new StorageException(ErrorCode.LIMIT_EXCEEDED,
                        "Maximum concurrent scans limit reached: " + options.maxOpenScans());
            }

            cursors.add(activeMemTable.scan(from, to));

            if (immutableMemTable != null) {
                cursors.add(immutableMemTable.scan(from, to));
            }

            for (TableHandle handle : liveTables) {
                if (handle instanceof DefaultTableHandle defHandle) {
                    defHandle.pin();
                    pinnedTables.add(defHandle);
                }
            }
        } finally {
            stateLock.unlock();
        }

        try {
            RecordCursor diskCursor = diskStorageReader.scanDisk(pinnedTables, from, to);
            cursors.add(diskCursor);

            DefaultMergedIterator merged = new DefaultMergedIterator(cursors, false);
            ScanIterator scanIterator = new ScanIterator(merged, pinnedTables);
            openScans.add(scanIterator);
            return scanIterator;
        } catch (Exception e) {
            for (TableHandle handle : pinnedTables) {
                if (handle instanceof DefaultTableHandle defHandle) {
                    defHandle.unpin();
                }
            }
            if (e instanceof StorageException se) {
                throw se;
            }
            throw new StorageException(ErrorCode.IO, "Failed to initialize scan iterator", e);
        }
    }

    @Override
    public void sync() throws StorageException {
        stateLock.lock();
        try {
            ensureOpen();
            activeWalWriter.sync();
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void close() throws StorageException {
        stateLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            for (ClosableIterator<?> it : List.copyOf(openScans)) {
                try {
                    it.close();
                } catch (Exception ignored) {}
            }
            openScans.clear();

            StorageException firstException = null;

            try {
                activeWalWriter.close();
            } catch (StorageException e) {
                firstException = e;
            }

            for (TableHandle handle : liveTables) {
                try {
                    handle.close();
                } catch (Exception e) {
                    StorageException se = new StorageException(ErrorCode.IO, "Failed to close TableHandle", e);
                    if (firstException == null) firstException = se;
                    else firstException.addSuppressed(se);
                }
            }
            liveTables.clear();

            try {
                manifestStore.close();
            } catch (StorageException e) {
                if (firstException == null) firstException = e;
                else firstException.addSuppressed(e);
            }

            try {
                lockHandle.close();
            } catch (Exception e) {
                StorageException se = new StorageException(ErrorCode.IO, "Failed to release LOCK", e);
                if (firstException == null) firstException = se;
                else firstException.addSuppressed(se);
            }

            if (firstException != null) {
                throw firstException;
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void checkAndRotateMemTable() throws StorageException {
        if (activeMemTable.byteSize() >= options.memtableBytes()) {
            rotateMemTable();
            flushImmutableMemTable();
        }
    }

    void rotateMemTable() throws StorageException {
        while (immutableMemTable != null && !closed) {
            try {
                capacityAvailable.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageException(ErrorCode.INTERRUPTED, "Interrupted while waiting for immutable MemTable flush", e);
            }
        }
        ensureOpen();

        activeWalWriter.close();

        long newWalId = nextFileId++;
        WalWriter newWalWriter;
        try {
            Path newWalPath = directory.resolve(String.format("%06d.wal", newWalId));
            ChannelIO channel = storageIO.createNew(newWalPath);
            newWalWriter = DefaultWalWriter.create(channel, newWalId, options.durabilityMode());
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to create new active WAL during rotation", e);
        }

        List<WalMeta> updatedWals = new ArrayList<>();
        ManifestSnapshot currentSnapshot = manifestStore.currentSnapshot();
        for (WalMeta wal : currentSnapshot.wals()) {
            if (wal.walId() == activeWalId) {
                updatedWals.add(new WalMeta(wal.walId(), true));
            } else {
                updatedWals.add(wal);
            }
        }
        updatedWals.add(new WalMeta(newWalId, false));

        ManifestSnapshot nextSnapshot = new ManifestSnapshot(
                ++currentEpoch,
                nextFileId,
                highSequence,
                updatedWals,
                currentSnapshot.tables()
        );
        manifestStore.commit(nextSnapshot);
        try {
            storageIO.syncDirectory(directory);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to sync directory after WAL rotation", e);
        }

        immutableMemTable = activeMemTable;
        immutableWalId = activeWalId;

        activeMemTable = new DefaultMemTable();
        activeWalId = newWalId;
        activeWalWriter = newWalWriter;
    }

    void flushImmutableMemTable() throws StorageException {
        if (immutableMemTable == null) {
            return;
        }

        long sstFileId = nextFileId++;
        TableMeta tableMeta;
        try (RecordCursor cursor = immutableMemTable.scan(null, null)) {
            tableMeta = sstableFlusher.build(sstFileId, cursor);
        }

        Path sstPath = directory.resolve(String.format("%06d.sst", sstFileId));
        TableHandle newTableHandle = DefaultTableHandle.open(storageIO, sstPath, tableMeta);

        ManifestSnapshot currentSnapshot = manifestStore.currentSnapshot();
        List<WalMeta> updatedWals = new ArrayList<>();
        for (WalMeta wal : currentSnapshot.wals()) {
            if (wal.walId() != immutableWalId) {
                updatedWals.add(wal);
            }
        }

        List<TableMeta> updatedTables = new ArrayList<>(currentSnapshot.tables());
        updatedTables.add(tableMeta);

        ManifestSnapshot nextSnapshot = new ManifestSnapshot(
                ++currentEpoch,
                nextFileId,
                highSequence,
                updatedWals,
                updatedTables
        );

        manifestStore.commit(nextSnapshot);
        try {
            storageIO.syncDirectory(directory);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to sync directory after flush commit", e);
        }

        liveTables.add(newTableHandle);

        Path oldWalPath = directory.resolve(String.format("%06d.wal", immutableWalId));
        try {
            storageIO.deleteIfExists(oldWalPath);
        } catch (IOException e) {
            throw new StorageException(ErrorCode.IO, "Failed to delete flushed WAL file: " + oldWalPath, e);
        }

        immutableMemTable = null;
        immutableWalId = 0L;
        capacityAvailable.signalAll();
    }

    MemTable activeMemTable() {
        return activeMemTable;
    }

    MemTable immutableMemTable() {
        return immutableMemTable;
    }

    long activeWalId() {
        return activeWalId;
    }

    long immutableWalId() {
        return immutableWalId;
    }

    List<TableHandle> liveTables() {
        return List.copyOf(liveTables);
    }

    private void ensureOpen() throws StorageException {
        if (closed) {
            throw new StorageException(ErrorCode.CLOSED, "Database is closed");
        }
    }

    private byte[] extractValue(InternalRecord record) {
        if (record == null || record.isTombstone()) {
            return null;
        }
        return record.value();
    }

    private static void validateKeyValue(byte[] key, byte[] value, boolean isDelete) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        if (!isDelete && value == null) {
            throw new IllegalArgumentException("value cannot be null for put()");
        }
        int valLen = value != null ? value.length : 0;
        if ((long) key.length + valLen > DefaultWalWriter.MAX_KEY_VALUE_TOTAL_BYTES) {
            throw new IllegalArgumentException("Key and value combined length ("
                    + (key.length + valLen) + ") exceeds max limit of "
                    + DefaultWalWriter.MAX_KEY_VALUE_TOTAL_BYTES + " bytes");
        }
    }

    private final class ScanIterator implements ClosableIterator<Entry> {
        private final DefaultMergedIterator merged;
        private final List<TableHandle> pinnedTables;
        private Entry nextEntry;
        private boolean closed;

        ScanIterator(DefaultMergedIterator merged, List<TableHandle> pinnedTables) {
            this.merged = merged;
            this.pinnedTables = pinnedTables;
        }

        @Override
        public synchronized boolean hasNext() throws StorageException {
            if (closed) {
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
            if (closed) {
                return;
            }
            closed = true;
            openScans.remove(this);

            try {
                merged.close();
            } finally {
                for (TableHandle handle : pinnedTables) {
                    if (handle instanceof DefaultTableHandle defHandle) {
                        defHandle.unpin();
                    }
                }
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
