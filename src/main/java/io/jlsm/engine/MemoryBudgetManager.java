package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class MemoryBudgetManager {

    private static final double DEFAULT_HIGH_WATERMARK_RATIO = 0.80;
    private static final double DEFAULT_LOW_WATERMARK_RATIO = 0.50;

    private final long maxBudgetBytes;
    private final long highWatermarkBytes;
    private final long lowWatermarkBytes;

    private final AtomicLong allocatedBytes = new AtomicLong(0L);
    private final AtomicLong backpressureWaitNanos = new AtomicLong(0L);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition memoryAvailable = lock.newCondition();
    private volatile boolean closed;

    public MemoryBudgetManager(DatabaseOptions options) {
        Objects.requireNonNull(options, "options cannot be null");
        this.maxBudgetBytes = calculateEffectiveBudget(options);
        this.highWatermarkBytes = (long) (maxBudgetBytes * DEFAULT_HIGH_WATERMARK_RATIO);
        this.lowWatermarkBytes = (long) (maxBudgetBytes * DEFAULT_LOW_WATERMARK_RATIO);
    }

    public MemoryBudgetManager(long maxBudgetBytes) {
        if (maxBudgetBytes <= 0) {
            throw new IllegalArgumentException("maxBudgetBytes must be positive: " + maxBudgetBytes);
        }
        this.maxBudgetBytes = maxBudgetBytes;
        this.highWatermarkBytes = (long) (maxBudgetBytes * DEFAULT_HIGH_WATERMARK_RATIO);
        this.lowWatermarkBytes = (long) (maxBudgetBytes * DEFAULT_LOW_WATERMARK_RATIO);
    }

    static long calculateEffectiveBudget(DatabaseOptions options) {
        long configured = options.memtableBytes() * 2 + options.maxSnapshotBytes();
        long maxJvmHeap = Runtime.getRuntime().maxMemory();

        if (maxJvmHeap < 128L * 1024 * 1024) {
            long heapBound = (long) (maxJvmHeap * 0.35);
            return Math.max(512L * 1024, Math.min(configured, heapBound));
        }

        return Math.max(1024L * 1024, configured);
    }

    public void acquire(long bytes) throws StorageException {
        if (bytes <= 0 || closed) {
            return;
        }

        long current = allocatedBytes.addAndGet(bytes);

        if (current >= highWatermarkBytes) {
            long startNanos = System.nanoTime();
            lock.lock();
            try {
                while (allocatedBytes.get() > lowWatermarkBytes && !closed) {
                    try {
                        memoryAvailable.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new StorageException(ErrorCode.INTERRUPTED, "Interrupted while waiting for memory backpressure release", e);
                    }
                }
            } finally {
                long elapsed = System.nanoTime() - startNanos;
                backpressureWaitNanos.addAndGet(elapsed);
                lock.unlock();
            }
        }
    }

    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }

        long remaining = allocatedBytes.addAndGet(-bytes);
        if (remaining < 0) {
            allocatedBytes.compareAndSet(remaining, 0L);
            remaining = 0L;
        }

        if (remaining <= lowWatermarkBytes) {
            lock.lock();
            try {
                memoryAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public void setAllocated(long currentBytes) {
        long old = allocatedBytes.getAndSet(Math.max(0L, currentBytes));
        if (currentBytes <= lowWatermarkBytes && old > lowWatermarkBytes) {
            lock.lock();
            try {
                memoryAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public boolean isUnderMemoryPressure() {
        return allocatedBytes.get() >= highWatermarkBytes;
    }

    public long allocatedBytes() {
        return Math.max(0L, allocatedBytes.get());
    }

    public long maxBudgetBytes() {
        return maxBudgetBytes;
    }

    public long highWatermarkBytes() {
        return highWatermarkBytes;
    }

    public long lowWatermarkBytes() {
        return lowWatermarkBytes;
    }

    public long backpressureWaitNanos() {
        return backpressureWaitNanos.get();
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            memoryAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public double budgetUtilization() {
        if (maxBudgetBytes <= 0) {
            return 0.0;
        }
        return (double) allocatedBytes.get() / (double) maxBudgetBytes;
    }

    public boolean isBackpressured() {
        return backpressureWaitNanos.get() > 0L;
    }

    public void resetWaitNanos() {
        backpressureWaitNanos.set(0L);
    }
}
