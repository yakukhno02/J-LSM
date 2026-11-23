package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.Entry;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.StorageException;
import io.jlsm.api.WriteBatch;
import io.jlsm.api.StorageMetrics;
import io.jlsm.compaction.Compactor;
import io.jlsm.compaction.DefaultCompactor;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.LockHandle;
import io.jlsm.internal.io.StorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.internal.model.BatchOperation;
import io.jlsm.manifest.DefaultManifestCompactor;
import io.jlsm.manifest.DefaultManifestStore;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class DefaultStorageEngine implements KVDatabase {

    private final StorageIO storageIO;
    private final Path directory;
    private final DatabaseOptions options;
    private final LockHandle lockHandle;
    private ManifestStore manifestStore;
    private final DefaultManifestCompactor manifestCompactor;
    private final DiskStorageReader diskStorageReader;
    private final SSTableFlusher sstableFlusher;
    private final Compactor compactor;

    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition capacityAvailable = stateLock.newCondition();

    private final Set<ClosableIterator<?>> openScans = ConcurrentHashMap.newKeySet();
    private final ExecutorService backgroundWorker;
    private final AtomicBoolean isCompacting = new AtomicBoolean(false);
    private final AtomicReference<StorageException> backgroundError = new AtomicReference<>();
    private final List<ObsoleteTable> obsoleteTables = new CopyOnWriteArrayList<>();
    private final MemoryBudgetManager memoryBudgetManager;
    private final AtomicLong putsCount = new AtomicLong(0L);
    private final AtomicLong getsCount = new AtomicLong(0L);
    private final AtomicLong deletesCount = new AtomicLong(0L);
    private final AtomicLong scansCount = new AtomicLong(0L);
    private final AtomicLong flushCount = new AtomicLong(0L);
    private final AtomicLong compactionCount = new AtomicLong(0L);

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

    private record ObsoleteTable(long tableId, Path path, TableHandle handle) {}

    public DefaultStorageEngine(
            StorageIO storageIO,
            Path directory,
            DatabaseOptions options,
            RecoveryResult recoveryResult
    ) throws StorageException {
        this(storageIO, directory, options, recoveryResult, new DefaultCompactor(directory, storageIO));
    }

    public DefaultStorageEngine(
            StorageIO storageIO,
            Path directory,
            DatabaseOptions options,
            RecoveryResult recoveryResult,
            Compactor compactor
    ) throws StorageException {
        this.storageIO = Objects.requireNonNull(storageIO, "storageIO cannot be null");
        this.directory = Objects.requireNonNull(directory, "directory cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(recoveryResult, "recoveryResult cannot be null");
        this.compactor = Objects.requireNonNull(compactor, "compactor cannot be null");

        this.lockHandle = recoveryResult.lockHandle();
        this.manifestStore = recoveryResult.manifestStore();
        this.manifestCompactor = new DefaultManifestCompactor(storageIO, directory);
        this.activeMemTable = recoveryResult.activeMemTable();
        this.activeWalWriter = recoveryResult.activeWalWriter();
        this.activeWalId = recoveryResult.activeWalId();
        this.activeMemTable.attachWalId(this.activeWalId);
        this.highSequence = recoveryResult.highSequence();
        this.memoryBudgetManager = new MemoryBudgetManager(options);
        this.memoryBudgetManager.setAllocated(this.activeMemTable.byteSize());

        ManifestSnapshot snapshot = recoveryResult.manifestSnapshot();
        this.currentEpoch = snapshot.epoch();
        this.nextFileId = snapshot.nextFileId();
        this.diskStorageReader = new DefaultDiskStorageReader();
        this.sstableFlusher = new DefaultSSTableFlusher(directory, storageIO);

        this.backgroundWorker = Executors.newSingleThreadExecutor(new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = defaultFactory.newThread(r);
                t.setName("jlsm-worker-" + directory.getFileName());
                t.setDaemon(true);
                return t;
            }
        });

        this.liveTables = new ArrayList<>();
        for (TableMeta meta : snapshot.tables()) {
            Path sstPath = directory.resolve(String.format("%06d.sst", meta.tableId()));
            this.liveTables.add(DefaultTableHandle.open(storageIO, sstPath, meta));
        }

        if (this.liveTables.size() >= options.compactionTrigger()) {
            scheduleCompaction();
        }
    }

    @Override
    public void put(byte[] key, byte[] value) throws StorageException {
        validateKeyValue(key, value, false);
        putsCount.incrementAndGet();
        memoryBudgetManager.checkBackpressure();

        stateLock.lock();
        try {
            ensureOpen();
            checkAndRotateMemTable();
            long seq = ++highSequence;
            activeWalWriter.append(seq, RecordKind.PUT, key, value);
            ByteKey byteKey = ByteKey.copyOf(key);
            long delta = activeMemTable.insert(byteKey, new InternalRecord.Put(byteKey, value, seq));
            memoryBudgetManager.update(delta);
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public byte[] get(byte[] key) throws StorageException {
        getsCount.incrementAndGet();
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        ByteKey byteKey = ByteKey.copyOf(key);
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
                handle.pin();
                pinnedTables.add(handle);
            }
        } finally {
            stateLock.unlock();
        }

        try {
            Optional<InternalRecord> diskRec = diskStorageReader.search(pinnedTables, byteKey);
            return diskRec.map(this::extractValue).orElse(null);
        } finally {
            for (TableHandle handle : pinnedTables) {
                handle.unpin();
            }
        }
    }

    @Override
    public void delete(byte[] key) throws StorageException {
        validateKeyValue(key, null, true);
        deletesCount.incrementAndGet();
        memoryBudgetManager.checkBackpressure();

        stateLock.lock();
        try {
            ensureOpen();
            checkAndRotateMemTable();
            long seq = ++highSequence;
            activeWalWriter.append(seq, RecordKind.DELETE, key, null);
            ByteKey byteKey = ByteKey.copyOf(key);
            long delta = activeMemTable.insert(byteKey, new InternalRecord.Delete(byteKey, seq));
            memoryBudgetManager.update(delta);
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void write(WriteBatch batch) throws StorageException {
        if (batch == null) {
            throw new IllegalArgumentException("batch cannot be null");
        }
        if (batch.isEmpty()) {
            throw new IllegalArgumentException("batch cannot be empty");
        }
        if (!(batch instanceof DefaultWriteBatch defaultBatch)) {
            throw new IllegalArgumentException("Unsupported WriteBatch implementation: " + batch.getClass().getName());
        }

        memoryBudgetManager.checkBackpressure();

        stateLock.lock();
        try {
            ensureOpen();
            checkAndRotateMemTable();

            List<BatchOperation> ops = defaultBatch.operations();
            int count = ops.size();
            long startSeq = highSequence + 1;

            activeWalWriter.appendBatch(startSeq, ops);
            highSequence += count;

            for (int i = 0; i < count; i++) {
                BatchOperation op = ops.get(i);
                long seq = startSeq + i;
                ByteKey byteKey = ByteKey.copyOf(op.rawKey());
                InternalRecord record = op.kind() == RecordKind.PUT
                        ? new InternalRecord.Put(byteKey, op.rawValue(), seq)
                        : new InternalRecord.Delete(byteKey, seq);
                long delta = activeMemTable.insert(byteKey, record);
                memoryBudgetManager.update(delta);
                if (op.kind() == RecordKind.PUT) {
                    putsCount.incrementAndGet();
                } else {
                    deletesCount.incrementAndGet();
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public ClosableIterator<Entry> scan(byte[] fromKey, byte[] toKey) throws StorageException {
        scansCount.incrementAndGet();
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
                handle.pin();
                pinnedTables.add(handle);
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
                handle.unpin();
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

            backgroundWorker.shutdown();
            memoryBudgetManager.close();
        } finally {
            stateLock.unlock();
        }

        try {
            if (!backgroundWorker.awaitTermination(5, TimeUnit.SECONDS)) {
                backgroundWorker.shutdownNow();
            }
        } catch (InterruptedException e) {
            backgroundWorker.shutdownNow();
            Thread.currentThread().interrupt();
        }

        stateLock.lock();
        try {
            for (ClosableIterator<?> it : List.copyOf(openScans)) {
                try {
                    it.close();
                } catch (Exception ignored) {}
            }
            openScans.clear();

            cleanObsoleteTables();

            StorageException firstException = null;
            StorageException bgError = backgroundError.get();
            if (bgError != null) {
                firstException = new StorageException(bgError.code(), "Database closed with previous failure: " + bgError.getMessage(), bgError);
            }

            try {
                activeWalWriter.close();
            } catch (StorageException e) {
                if (firstException == null) firstException = e;
                else if (firstException != e) firstException.addSuppressed(e);
            }

            for (TableHandle handle : liveTables) {
                try {
                    handle.close();
                } catch (Exception e) {
                    StorageException se = new StorageException(ErrorCode.IO, "Failed to close TableHandle", e);
                    if (firstException == null) firstException = se;
                    else if (!firstException.equals(se)) firstException.addSuppressed(se);
                }
            }
            liveTables.clear();

            try {
                manifestStore.close();
            } catch (StorageException e) {
                if (firstException == null) firstException = e;
                else if (firstException != e) firstException.addSuppressed(e);
            }

            try {
                lockHandle.close();
            } catch (Exception e) {
                StorageException se = new StorageException(ErrorCode.IO, "Failed to release LOCK", e);
                if (firstException == null) firstException = se;
                else if (!firstException.equals(se)) firstException.addSuppressed(se);
            }

            if (firstException != null) {
                throw firstException;
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void checkAndRotateMemTable() throws StorageException {
        if (immutableMemTable != null) {
            flushImmutableMemTable();
        }
        if (activeMemTable.byteSize() >= options.memtableBytes() || memoryBudgetManager.isUnderMemoryPressure()) {
            rotateMemTable();
            flushImmutableMemTable();
        }
    }

    void rotateMemTable() throws StorageException {
        stateLock.lock();
        try {
            ensureOpen();
            if (immutableMemTable != null) {
                flushImmutableMemTable();
            }

            activeWalWriter.close();

            long newWalId = nextFileId++;
            WalWriter newWalWriter;
            try {
                Path newWalPath = directory.resolve(RecoveryManager.walFileName(newWalId));
                storageIO.deleteIfExists(newWalPath);
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
            maybeCompactManifest(nextSnapshot);

            immutableMemTable = activeMemTable;
            immutableWalId = activeWalId;

            activeMemTable = new DefaultMemTable();
            activeMemTable.attachWalId(newWalId);
            activeWalId = newWalId;
            activeWalWriter = newWalWriter;
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException(ErrorCode.IO, "Failed to rotate memtable", e);
        } finally {
            stateLock.unlock();
        }
    }

    void flushImmutableMemTable() throws StorageException {
        stateLock.lock();
        try {
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
            Set<Long> walIdsToFlush = new HashSet<>(immutableMemTable.attachedWalIds());
            if (immutableWalId != 0L) {
                walIdsToFlush.add(immutableWalId);
            }

            List<WalMeta> updatedWals = new ArrayList<>();
            for (WalMeta wal : currentSnapshot.wals()) {
                if (!walIdsToFlush.contains(wal.walId())) {
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
            maybeCompactManifest(nextSnapshot);

            liveTables.add(newTableHandle);

            for (long flushedWalId : walIdsToFlush) {
                Path oldWalPath = directory.resolve(RecoveryManager.walFileName(flushedWalId));
                try {
                    storageIO.deleteIfExists(oldWalPath);
                } catch (IOException e) {
                    throw new StorageException(ErrorCode.IO, "Failed to delete flushed WAL file: " + oldWalPath, e);
                }
            }

            long flushedMem = immutableMemTable.byteSize();
            immutableMemTable = null;
            immutableWalId = 0L;
            flushCount.incrementAndGet();
            memoryBudgetManager.release(flushedMem);
            capacityAvailable.signalAll();

            if (liveTables.size() >= options.compactionTrigger()) {
                scheduleCompaction();
            }
        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException(ErrorCode.IO, "Failed to flush immutable memtable", e);
        } finally {
            stateLock.unlock();
        }
    }

    void scheduleCompaction() {
        if (closed || isCompacting.get()) {
            return;
        }
        backgroundWorker.submit(() -> {
            try {
                performCompaction(false);
            } catch (StorageException e) {
                backgroundError.compareAndSet(null, e);
            } catch (Exception e) {
                backgroundError.compareAndSet(null, new StorageException(ErrorCode.IO, "Compaction failed in background", e));
            }
        });
    }

    void performCompaction(boolean force) throws StorageException {
        if (!isCompacting.compareAndSet(false, true)) {
            return;
        }
        List<TableHandle> pinnedInputs;
        long outputId;
        stateLock.lock();
        try {
            int minTables = force ? 1 : options.compactionTrigger();
            if (closed || liveTables.size() < minTables) {
                isCompacting.set(false);
                return;
            }
            pinnedInputs = new ArrayList<>(liveTables);
            for (TableHandle handle : pinnedInputs) {
                handle.pin();
            }
            outputId = nextFileId++;
        } finally {
            stateLock.unlock();
        }

        Optional<TableMeta> compactedMeta;
        try {
            compactedMeta = compactor.build(outputId, pinnedInputs, false);
        } catch (Exception e) {
            for (TableHandle handle : pinnedInputs) {
                handle.unpin();
            }
            isCompacting.set(false);
            if (e instanceof StorageException se) {
                throw se;
            }
            throw new StorageException(ErrorCode.IO, "Compaction failed during build", e);
        }

        TableHandle newTableHandle = null;
        if (compactedMeta.isPresent()) {
            Path newSstPath = directory.resolve(String.format("%06d.sst", outputId));
            try {
                newTableHandle = DefaultTableHandle.open(storageIO, newSstPath, compactedMeta.get());
            } catch (Exception e) {
                for (TableHandle handle : pinnedInputs) {
                    handle.unpin();
                }
                try {
                    storageIO.deleteIfExists(newSstPath);
                } catch (IOException ignored) {}
                isCompacting.set(false);
                if (e instanceof StorageException se) {
                    throw se;
                }
                throw new StorageException(ErrorCode.IO, "Failed to open newly compacted table", e);
            }
        }

        Set<Long> compactedTableIds = new HashSet<>();
        for (TableHandle handle : pinnedInputs) {
            compactedTableIds.add(handle.id());
        }

        stateLock.lock();
        try {
            if (closed) {
                if (newTableHandle != null) {
                    try {
                        newTableHandle.close();
                    } catch (Exception ignored) {}
                    try {
                        Path newSstPath = directory.resolve(String.format("%06d.sst", outputId));
                        storageIO.deleteIfExists(newSstPath);
                    } catch (IOException ignored) {}
                }
                for (TableHandle handle : pinnedInputs) {
                    handle.unpin();
                }
                return;
            }

            ManifestSnapshot currentSnapshot = manifestStore.currentSnapshot();
            List<TableMeta> updatedTables = new ArrayList<>();
            for (TableMeta meta : currentSnapshot.tables()) {
                if (!compactedTableIds.contains(meta.tableId())) {
                    updatedTables.add(meta);
                }
            }
            compactedMeta.ifPresent(updatedTables::add);

            ManifestSnapshot nextSnapshot = new ManifestSnapshot(
                    ++currentEpoch,
                    nextFileId,
                    highSequence,
                    currentSnapshot.wals(),
                    updatedTables
            );

            manifestStore.commit(nextSnapshot);
            try {
                storageIO.syncDirectory(directory);
            } catch (IOException e) {
                throw new StorageException(ErrorCode.IO, "Failed to sync directory after compaction commit", e);
            }
            maybeCompactManifest(nextSnapshot);

            liveTables.removeIf(t -> compactedTableIds.contains(t.id()));
            if (newTableHandle != null) {
                liveTables.add(newTableHandle);
            }
            compactionCount.incrementAndGet();

            for (TableHandle handle : pinnedInputs) {
                Path sstPath = directory.resolve(String.format("%06d.sst", handle.id()));
                obsoleteTables.add(new ObsoleteTable(handle.id(), sstPath, handle));
                handle.close();
                handle.unpin();
            }

            cleanObsoleteTables();
            capacityAvailable.signalAll();
        } finally {
            isCompacting.set(false);
            stateLock.unlock();
        }

        stateLock.lock();
        try {
            if (!closed && liveTables.size() >= options.compactionTrigger()) {
                scheduleCompaction();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void maybeCompactManifest(ManifestSnapshot snapshot) throws StorageException {
        if (snapshot.epoch() > 0 && snapshot.epoch() % 50 == 0) {
            try {
                manifestStore.close();
                manifestCompactor.compact(snapshot);
                Path manifestPath = directory.resolve(RecoveryManager.MANIFEST_FILE_NAME);
                ChannelIO channel = storageIO.openReadWrite(manifestPath);
                manifestStore = DefaultManifestStore.open(channel, manifestPath, snapshot);
            } catch (IOException e) {
                throw new StorageException(ErrorCode.IO, "Failed to reopen manifest store after compaction", e);
            }
        }
    }

    void awaitBackgroundTasks() throws InterruptedException {
        try {
            backgroundWorker.submit(() -> {}).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e instanceof InterruptedException ie) {
                throw ie;
            }
        }
    }

    void triggerCompactionSync() throws StorageException {
        try {
            awaitBackgroundTasks();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        performCompaction(true);
    }

    private void cleanObsoleteTables() {
        obsoleteTables.removeIf(obs -> {
            boolean canDelete = obs.handle().isClosed();
            if (canDelete) {
                try {
                    storageIO.deleteIfExists(obs.path());
                    return true;
                } catch (IOException e) {
                    return false;
                }
            }
            return false;
        });
    }

    @Override
    public StorageMetrics metrics() throws StorageException {
        stateLock.lock();
        try {
            ensureOpen();
            long activeMem = activeMemTable != null ? activeMemTable.byteSize() : 0L;
            long immMem = immutableMemTable != null ? immutableMemTable.byteSize() : 0L;
            long totalMem = activeMem + immMem;

            long totalSstBytes = 0L;
            for (TableHandle handle : liveTables) {
                totalSstBytes += handle.fileSize();
            }

            long totalWalBytes = 0L;
            ManifestSnapshot snapshot = manifestStore.currentSnapshot();
            for (WalMeta wal : snapshot.wals()) {
                Path walPath = directory.resolve(RecoveryManager.walFileName(wal.walId()));
                try {
                    if (Files.exists(walPath)) {
                        totalWalBytes += Files.size(walPath);
                    }
                } catch (IOException ignored) {}
            }

            return new StorageMetrics(
                    putsCount.get(),
                    getsCount.get(),
                    deletesCount.get(),
                    scansCount.get(),
                    activeMem,
                    immMem,
                    totalMem,
                    memoryBudgetManager.maxBudgetBytes(),
                    liveTables.size(),
                    totalSstBytes,
                    totalWalBytes,
                    openScans.size(),
                    flushCount.get(),
                    compactionCount.get(),
                    memoryBudgetManager.backpressureWaitNanos(),
                    highSequence
            );
        } finally {
            stateLock.unlock();
        }
    }

    MemoryBudgetManager memoryBudgetManager() {
        return memoryBudgetManager;
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

    ManifestStore manifestStore() {
        return manifestStore;
    }

    Path directory() {
        return directory;
    }

    List<Path> obsoleteTablePaths() {
        List<Path> paths = new ArrayList<>();
        for (ObsoleteTable table : obsoleteTables) {
            paths.add(table.path());
        }
        return paths;
    }

    private void ensureOpen() throws StorageException {
        StorageException bg = backgroundError.get();
        if (bg != null) {
            throw new StorageException(ErrorCode.CORRUPTION, "Storage engine is poisoned due to previous failure", bg);
        }
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
                    handle.unpin();
                }
                cleanObsoleteTables();
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
