package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.file.Path;

public interface StorageIO {
    ChannelIO openRead(Path file) throws IOException;
    ChannelIO createNew(Path file) throws IOException;
    ChannelIO openReadWrite(Path file) throws IOException;
    void atomicMove(Path source, Path target) throws IOException;
    void syncDirectory(Path directory) throws IOException;
    void deleteIfExists(Path file) throws IOException;
    LockHandle acquireLock(Path lockFile) throws IOException;
}
