package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Standard {@link ChannelIO} implementation backed by a {@link FileChannel}.
 */
public final class FileChannelIO implements ChannelIO {

    private final FileChannel channel;
    private final Path path;
    private boolean closed;

    public FileChannelIO(FileChannel channel, Path path) {
        this.channel = Objects.requireNonNull(channel, "channel cannot be null");
        this.path = path;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        checkOpen();
        Objects.requireNonNull(dst, "dst buffer cannot be null");
        return channel.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        checkOpen();
        Objects.requireNonNull(src, "src buffer cannot be null");
        return channel.write(src, position);
    }

    @Override
    public long size() throws IOException {
        checkOpen();
        return channel.size();
    }

    @Override
    public void truncate(long size) throws IOException {
        checkOpen();
        if (size < 0) {
            throw new IllegalArgumentException("Negative size: " + size);
        }
        channel.truncate(size);
    }

    @Override
    public void force(boolean metadata) throws IOException {
        checkOpen();
        channel.force(metadata);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        channel.close();
    }

    public Path path() {
        return path;
    }

    public FileChannel fileChannel() {
        return channel;
    }

    private void checkOpen() throws IOException {
        if (closed || !channel.isOpen()) {
            throw new IOException("Channel is closed: " + (path != null ? path : "unnamed"));
        }
    }
}
