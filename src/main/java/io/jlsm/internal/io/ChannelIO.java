package io.jlsm.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ChannelIO extends AutoCloseable {
    int read(ByteBuffer dst, long position) throws IOException;
    int write(ByteBuffer src, long position) throws IOException;
    long size() throws IOException;
    void truncate(long size) throws IOException;
    void force(boolean metadata) throws IOException;
    @Override
    void close() throws IOException;
}
