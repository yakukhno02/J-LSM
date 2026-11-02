package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Production implementation of {@link StorageIO} backed by standard Java NIO and {@link FileChannel}.
 */
public final class StandardStorageIO implements StorageIO {

    @Override
    public ChannelIO openRead(Path file) throws IOException {
        Objects.requireNonNull(file, "file cannot be null");
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        return new FileChannelIO(channel, file);
    }

    @Override
    public ChannelIO createNew(Path file) throws IOException {
        Objects.requireNonNull(file, "file cannot be null");
        ensureParentDirectoryExists(file);
        FileChannel channel = FileChannel.open(
                file,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        return new FileChannelIO(channel, file);
    }

    @Override
    public ChannelIO openReadWrite(Path file) throws IOException {
        Objects.requireNonNull(file, "file cannot be null");
        ensureParentDirectoryExists(file);
        FileChannel channel = FileChannel.open(
                file,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        return new FileChannelIO(channel, file);
    }

    @Override
    public void atomicMove(Path source, Path target) throws IOException {
        Objects.requireNonNull(source, "source cannot be null");
        Objects.requireNonNull(target, "target cannot be null");
        ensureParentDirectoryExists(target);
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void syncDirectory(Path directory) throws IOException {
        Objects.requireNonNull(directory, "directory cannot be null");
        if (!Files.isDirectory(directory)) {
            return;
        }
        // In POSIX, directory fsync ensures created or renamed entries are committed to disk.
        // Some platforms or filesystems throw IOException when opening a directory with FileChannel.
        try (FileChannel dirChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException ignored) {
            // Platform unsupported, gracefully ignored
        }
    }

    @Override
    public void deleteIfExists(Path file) throws IOException {
        Objects.requireNonNull(file, "file cannot be null");
        Files.deleteIfExists(file);
    }

    @Override
    public LockHandle acquireLock(Path lockFile) throws IOException {
        Objects.requireNonNull(lockFile, "lockFile cannot be null");
        ensureParentDirectoryExists(lockFile);
        FileChannel channel = FileChannel.open(
                lockFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        try {
            FileLock lock = channel.tryLock();
            if (lock == null) {
                channel.close();
                throw new IOException("Lock is already held by another process: " + lockFile);
            }
            return new FileLockHandle(lock, channel);
        } catch (Exception e) {
            try {
                channel.close();
            } catch (IOException closeEx) {
                e.addSuppressed(closeEx);
            }
            if (e instanceof IOException ioEx) {
                throw ioEx;
            }
            throw new IOException("Failed to acquire file lock: " + lockFile, e);
        }
    }

    private static void ensureParentDirectoryExists(Path file) throws IOException {
        Path parent = file.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }
}
