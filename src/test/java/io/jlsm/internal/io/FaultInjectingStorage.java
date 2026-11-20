package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decorator over {@link StorageIO} that implements {@link FaultInjector} to programmatically
 * trigger simulated I/O errors, torn writes, bit rot, and controller failures.
 */
public final class FaultInjectingStorage implements StorageIO, FaultInjector {

    private final StorageIO delegate;
    private final AtomicLong remainingBytesBeforeFail = new AtomicLong(-1L);
    private final AtomicLong ioCallCount = new AtomicLong(0L);
    private final AtomicLong failOnNthIoCall = new AtomicLong(-1L);

    private volatile boolean failOnFsync;
    private volatile boolean corruptNextWrite;
    private volatile boolean diskFull;
    private volatile boolean failOnCreateNew;
    private volatile boolean failOnAtomicMove;
    private volatile boolean failOnDeleteIfExists;
    private volatile boolean failOnSyncDirectory;

    public FaultInjectingStorage() {
        this(new StandardStorageIO());
    }

    public FaultInjectingStorage(StorageIO delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
    }

    private void checkIoFailure(String operation) throws IOException {
        long current = ioCallCount.incrementAndGet();
        long target = failOnNthIoCall.get();
        if (target > 0 && current == target) {
            throw new IOException("FaultInjection: simulated I/O failure on call #" + current + " during " + operation);
        }
    }

    @Override
    public ChannelIO openRead(Path file) throws IOException {
        checkIoFailure("openRead");
        return new FaultInjectingChannelIO(delegate.openRead(file), this);
    }

    @Override
    public ChannelIO createNew(Path file) throws IOException {
        checkIoFailure("createNew");
        if (failOnCreateNew || diskFull) {
            throw new IOException("FaultInjection: createNew failed for " + file);
        }
        return new FaultInjectingChannelIO(delegate.createNew(file), this);
    }

    @Override
    public ChannelIO openReadWrite(Path file) throws IOException {
        checkIoFailure("openReadWrite");
        return new FaultInjectingChannelIO(delegate.openReadWrite(file), this);
    }

    @Override
    public void atomicMove(Path source, Path target) throws IOException {
        checkIoFailure("atomicMove");
        if (failOnAtomicMove) {
            throw new IOException("FaultInjection: atomicMove failed from " + source + " to " + target);
        }
        delegate.atomicMove(source, target);
    }

    @Override
    public void syncDirectory(Path directory) throws IOException {
        checkIoFailure("syncDirectory");
        if (failOnSyncDirectory || failOnFsync) {
            throw new IOException("FaultInjection: syncDirectory failed for " + directory);
        }
        delegate.syncDirectory(directory);
    }

    @Override
    public void deleteIfExists(Path file) throws IOException {
        checkIoFailure("deleteIfExists");
        if (failOnDeleteIfExists) {
            throw new IOException("FaultInjection: deleteIfExists failed for " + file);
        }
        delegate.deleteIfExists(file);
    }

    @Override
    public LockHandle acquireLock(Path lockFile) throws IOException {
        checkIoFailure("acquireLock");
        return delegate.acquireLock(lockFile);
    }

    @Override
    public void failAfterBytesWritten(long byteCount) {
        if (byteCount < 0) {
            throw new IllegalArgumentException("byteCount must be non-negative: " + byteCount);
        }
        remainingBytesBeforeFail.set(byteCount);
    }

    @Override
    public void failOnFsync(boolean shouldFail) {
        this.failOnFsync = shouldFail;
    }

    @Override
    public void corruptNextWrite(boolean corruptCRC) {
        this.corruptNextWrite = corruptCRC;
    }

    @Override
    public void setDiskFull(boolean diskFull) {
        this.diskFull = diskFull;
    }

    @Override
    public void failOnCreateNew(boolean shouldFail) {
        this.failOnCreateNew = shouldFail;
    }

    @Override
    public void failOnAtomicMove(boolean shouldFail) {
        this.failOnAtomicMove = shouldFail;
    }

    @Override
    public void failOnDeleteIfExists(boolean shouldFail) {
        this.failOnDeleteIfExists = shouldFail;
    }

    @Override
    public void failOnSyncDirectory(boolean shouldFail) {
        this.failOnSyncDirectory = shouldFail;
    }

    @Override
    public void failOnNthIoCall(long callNumber) {
        this.failOnNthIoCall.set(callNumber);
    }

    @Override
    public long ioCallCount() {
        return ioCallCount.get();
    }

    @Override
    public void reset() {
        remainingBytesBeforeFail.set(-1L);
        ioCallCount.set(0L);
        failOnNthIoCall.set(-1L);
        failOnFsync = false;
        corruptNextWrite = false;
        diskFull = false;
        failOnCreateNew = false;
        failOnAtomicMove = false;
        failOnDeleteIfExists = false;
        failOnSyncDirectory = false;
    }

    boolean isDiskFull() {
        return diskFull;
    }

    boolean isFailOnFsync() {
        return failOnFsync;
    }

    boolean consumeCorruptNextWrite() {
        if (corruptNextWrite) {
            corruptNextWrite = false;
            return true;
        }
        return false;
    }

    AtomicLong getRemainingBytesBeforeFail() {
        return remainingBytesBeforeFail;
    }

    public StorageIO delegate() {
        return delegate;
    }
}
