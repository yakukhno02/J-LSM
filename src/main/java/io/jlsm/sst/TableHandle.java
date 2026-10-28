package io.jlsm.sst;

public interface TableHandle extends AutoCloseable {
    long id();
    long minSequence();
    long maxSequence();
    long recordCount();
    long fileSize();

    @Override
    void close();
}
