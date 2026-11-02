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
    private volatile boolean failOnFsync;
    private volatile boolean corruptNextWrite;
    private volatile boolean diskFull;

    public FaultInjectingStorage() {
        this(new StandardStorageIO());
    }

    public FaultInjectingStorage(StorageIO delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
    }

    @Override
    public ChannelIO openRead(Path file) throws IOException {
        return new FaultInjectingChannelIO(delegate.openRead(file), this);
    }

    @Override
    public ChannelIO createNew(Path file) throws IOException {
        return new FaultInjectingChannelIO(delegate.createNew(file), this);
    }

    @Override
    public ChannelIO openReadWrite(Path file) throws IOException {
        return new FaultInjectingChannelIO(delegate.openReadWrite(file), this);
    }

    @Override
    public void atomicMove(Path source, Path target) throws IOException {
        delegate.atomicMove(source, target);
    }

    @Override
    public void syncDirectory(Path directory) throws IOException {
        delegate.syncDirectory(directory);
    }

    @Override
    public void deleteIfExists(Path file) throws IOException {
        delegate.deleteIfExists(file);
    }

    @Override
    public LockHandle acquireLock(Path lockFile) throws IOException {
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
    public void reset() {
        remainingBytesBeforeFail.set(-1L);
        failOnFsync = false;
        corruptNextWrite = false;
        diskFull = false;
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
