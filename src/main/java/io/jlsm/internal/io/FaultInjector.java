package io.jlsm.internal.io;

public interface FaultInjector {
    void failAfterBytesWritten(long byteCount);
    void failOnFsync(boolean shouldFail);
    void corruptNextWrite(boolean corruptCRC);
    void setDiskFull(boolean diskFull);
    void reset();
}
