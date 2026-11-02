package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Objects;

/**
 * Standard {@link LockHandle} implementation backed by a {@link FileLock} and its {@link FileChannel}.
 */
public final class FileLockHandle implements LockHandle {

    private final FileLock lock;
    private final FileChannel channel;
    private boolean closed;

    public FileLockHandle(FileLock lock, FileChannel channel) {
        this.lock = Objects.requireNonNull(lock, "lock cannot be null");
        this.channel = Objects.requireNonNull(channel, "channel cannot be null");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (lock.isValid()) {
                lock.release();
            }
        } finally {
            channel.close();
        }
    }

    public FileLock fileLock() {
        return lock;
    }
}
