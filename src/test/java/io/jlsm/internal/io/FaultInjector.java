package io.jlsm.internal.io;

public interface FaultInjector {
    void failAfterBytesWritten(long byteCount);
    void failOnFsync(boolean shouldFail);
    void corruptNextWrite(boolean corruptCRC);
    void setDiskFull(boolean diskFull);
    void failOnCreateNew(boolean shouldFail);
    void failOnAtomicMove(boolean shouldFail);
    void failOnDeleteIfExists(boolean shouldFail);
    void failOnSyncDirectory(boolean shouldFail);
    void failOnNthIoCall(long callNumber);
    long ioCallCount();
    void reset();
}
