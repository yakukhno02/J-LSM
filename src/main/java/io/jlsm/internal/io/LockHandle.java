package io.jlsm.internal.io;

import java.io.IOException;

public interface LockHandle extends AutoCloseable {
    @Override
    void close() throws IOException;
}
