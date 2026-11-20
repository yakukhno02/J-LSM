package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link ChannelIO} decorator that intercepts write and force operations to inject faults.
 */
public final class FaultInjectingChannelIO implements ChannelIO {

    private final ChannelIO delegate;
    private final FaultInjectingStorage storage;

    public FaultInjectingChannelIO(ChannelIO delegate, FaultInjectingStorage storage) {
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return delegate.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        Objects.requireNonNull(src, "src cannot be null");

        if (storage.isDiskFull()) {
            throw new IOException("FaultInjection: disk is full");
        }

        if (storage.consumeCorruptNextWrite()) {
            if (src.hasRemaining()) {
                ByteBuffer copy = ByteBuffer.allocate(src.remaining());
                copy.put(src.duplicate());
                copy.flip();
                // Flip lowest bit of the first byte to corrupt CRC / payload
                copy.put(0, (byte) (copy.get(0) ^ 0x01));
                int written = delegate.write(copy, position);
                src.position(src.position() + written);
                return written;
            }
        }

        AtomicLong allowance = storage.getRemainingBytesBeforeFail();
        long remaining = allowance.get();
        if (remaining >= 0) {
            int toWrite = src.remaining();
            if (remaining == 0) {
                throw new IOException("FaultInjection: write failure triggered after specified byte limit");
            }
            if (toWrite <= remaining) {
                allowance.addAndGet(-toWrite);
                return delegate.write(src, position);
            } else {
                int allowed = (int) remaining;
                allowance.set(0);
                int originalLimit = src.limit();
                src.limit(src.position() + allowed);
                try {
                    delegate.write(src, position);
                } finally {
                    src.limit(originalLimit);
                }
                throw new IOException("FaultInjection: write truncated and failed after writing " + allowed + " bytes");
            }
        }

        return delegate.write(src, position);
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public void truncate(long size) throws IOException {
        delegate.truncate(size);
    }

    @Override
    public void force(boolean metadata) throws IOException {
        if (storage.isFailOnFsync()) {
            throw new IOException("FaultInjection: simulated fsync failure");
        }
        delegate.force(metadata);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    public ChannelIO delegate() {
        return delegate;
    }
}
