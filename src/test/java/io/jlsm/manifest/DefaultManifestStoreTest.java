package io.jlsm.manifest;

import io.jlsm.api.CorruptedLogException;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultManifestStoreTest {

    private static final Path TEST_PATH = Path.of("MANIFEST");

    private static final class InMemoryChannelIO implements ChannelIO {
        private byte[] storage;
        private int size;
        private boolean closed;
        private int forceCount;

        InMemoryChannelIO() {
            this.storage = new byte[256];
            this.size = 0;
        }

        InMemoryChannelIO(byte[] initialData) {
            this.storage = initialData.clone();
            this.size = initialData.length;
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            checkNotClosed();
            if (position >= size) {
                return -1;
            }
            int toRead = (int) Math.min(dst.remaining(), size - position);
            dst.put(storage, (int) position, toRead);
            return toRead;
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            checkNotClosed();
            int bytesToWrite = src.remaining();
            int endPos = (int) (position + bytesToWrite);
            if (endPos > storage.length) {
                storage = Arrays.copyOf(storage, Math.max(storage.length * 2, endPos));
            }
            src.get(storage, (int) position, bytesToWrite);
            if (endPos > size) {
                size = endPos;
            }
            return bytesToWrite;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void truncate(long newSize) throws IOException {
            checkNotClosed();
            if (newSize < size) {
                size = (int) newSize;
            }
        }

        @Override
        public void force(boolean metadata) throws IOException {
            checkNotClosed();
            forceCount++;
        }

        @Override
        public void close() {
            closed = true;
        }

        byte[] toByteArray() {
            return Arrays.copyOf(storage, size);
        }

        private void checkNotClosed() throws IOException {
            if (closed) {
                throw new IOException("Channel closed");
            }
        }
    }

    @Test
    @DisplayName("Open on empty channel initializes header and writes initial snapshot")
    void testOpenNewFile() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        ManifestSnapshot init = new ManifestSnapshot(1L, 2L, 100L,
                List.of(new WalMeta(1L, false)),
                List.of(new TableMeta(10L, 1L, 50L, 25L, 4096L))
        );

        try (DefaultManifestStore store = DefaultManifestStore.open(channel, TEST_PATH, init)) {
            assertThat(store.currentSnapshot()).isEqualTo(init);
            assertThat(store.writeOffset()).isGreaterThan(DefaultManifestStore.HEADER_SIZE);
            assertThat(channel.size()).isEqualTo(store.writeOffset());
            assertThat(channel.forceCount).isGreaterThanOrEqualTo(2); // header + snapshot force
        }

        // Verify header
        byte[] data = channel.toByteArray();
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        assertThat(buf.getInt()).isEqualTo(DefaultManifestStore.MANIFEST_MAGIC);
        assertThat(buf.getInt()).isEqualTo(DefaultManifestStore.FORMAT_VERSION);
        assertThat(buf.getInt()).isEqualTo(0);
        int storedHeaderCrc = buf.getInt();

        CRC32 crc32 = new CRC32();
        crc32.update(data, 0, 12);
        assertThat(storedHeaderCrc).isEqualTo((int) crc32.getValue());
    }

    @Test
    @DisplayName("Reopening existing manifest restores the latest snapshot")
    void testReopenExisting() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        ManifestSnapshot s1 = new ManifestSnapshot(1L, 2L, 10L, List.of(), List.of());
        ManifestSnapshot s2 = new ManifestSnapshot(2L, 5L, 50L,
                List.of(new WalMeta(1L, true), new WalMeta(2L, false)),
                List.of(new TableMeta(100L, 1L, 20L, 10L, 4096L))
        );

        try (DefaultManifestStore store = DefaultManifestStore.open(channel, TEST_PATH, s1)) {
            store.commit(s2);
        }

        InMemoryChannelIO reopenChannel = new InMemoryChannelIO(channel.toByteArray());
        try (DefaultManifestStore store = DefaultManifestStore.open(reopenChannel, TEST_PATH, ManifestSnapshot.empty())) {
            assertThat(store.currentSnapshot()).isEqualTo(s2);
        }
    }

    @Test
    @DisplayName("Multiple commits append sequentially and advance currentSnapshot")
    void testMultipleCommits() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        ManifestSnapshot s0 = ManifestSnapshot.empty();

        try (DefaultManifestStore store = DefaultManifestStore.open(channel, TEST_PATH, s0)) {
            for (int i = 1; i <= 5; i++) {
                ManifestSnapshot next = new ManifestSnapshot(i, i + 1, i * 100L,
                        List.of(new WalMeta(i, false)),
                        List.of()
                );
                store.commit(next);
                assertThat(store.currentSnapshot()).isEqualTo(next);
            }
        }

        InMemoryChannelIO reopenChannel = new InMemoryChannelIO(channel.toByteArray());
        try (DefaultManifestStore store = DefaultManifestStore.open(reopenChannel, TEST_PATH, s0)) {
            assertThat(store.currentSnapshot().epoch()).isEqualTo(5L);
            assertThat(store.currentSnapshot().highSequence()).isEqualTo(500L);
        }
    }

    @Test
    @DisplayName("Corrupted header triggers CorruptedLogException")
    void testCorruptedHeader() {
        InMemoryChannelIO shortChannel = new InMemoryChannelIO(new byte[10]);
        assertThatThrownBy(() -> DefaultManifestStore.open(shortChannel, TEST_PATH, ManifestSnapshot.empty()))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("smaller than MANIFEST header");

        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(0xDEADBEEF);
        buf.putInt(1);
        buf.putInt(0);
        buf.putInt(0);
        InMemoryChannelIO badMagic = new InMemoryChannelIO(buf.array());
        assertThatThrownBy(() -> DefaultManifestStore.open(badMagic, TEST_PATH, ManifestSnapshot.empty()))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("Invalid MANIFEST magic");

        buf.putInt(0, DefaultManifestStore.MANIFEST_MAGIC);
        buf.putInt(12, 0x12345678);
        InMemoryChannelIO badCrc = new InMemoryChannelIO(buf.array());
        assertThatThrownBy(() -> DefaultManifestStore.open(badCrc, TEST_PATH, ManifestSnapshot.empty()))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("header CRC mismatch");
    }

    @Test
    @DisplayName("FI-04: Torn write at EOF is cleanly truncated to the previous valid snapshot")
    void testTornWriteAtEofTruncated() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        ManifestSnapshot s1 = new ManifestSnapshot(1L, 2L, 10L, List.of(), List.of());
        ManifestSnapshot s2 = new ManifestSnapshot(2L, 3L, 20L,
                List.of(new WalMeta(1L, false)), List.of());

        long validOffset;
        try (DefaultManifestStore store = DefaultManifestStore.open(channel, TEST_PATH, s1)) {
            validOffset = store.writeOffset();
            store.commit(s2);
        }

        // Corrupt/truncate EOF halfway through second snapshot frame
        byte[] tornBytes = Arrays.copyOf(channel.toByteArray(), (int) validOffset + 7);
        InMemoryChannelIO tornChannel = new InMemoryChannelIO(tornBytes);

        try (DefaultManifestStore store = DefaultManifestStore.open(tornChannel, TEST_PATH, ManifestSnapshot.empty())) {
            assertThat(store.currentSnapshot()).isEqualTo(s1);
            assertThat(tornChannel.size()).isEqualTo(validOffset);
        }
    }

    @Test
    @DisplayName("Mid-file CRC corruption triggers CorruptedLogException")
    void testMidFileCorruptionThrows() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        ManifestSnapshot s1 = new ManifestSnapshot(1L, 2L, 10L, List.of(), List.of());
        ManifestSnapshot s2 = new ManifestSnapshot(2L, 3L, 20L, List.of(), List.of());

        try (DefaultManifestStore store = DefaultManifestStore.open(channel, TEST_PATH, s1)) {
            store.commit(s2);
        }

        byte[] corrupted = channel.toByteArray();
        // Frame 1 starts at offset 16. Corrupt 1 byte in payload
        corrupted[25] ^= 0x01;

        InMemoryChannelIO corruptedChannel = new InMemoryChannelIO(corrupted);
        assertThatThrownBy(() -> DefaultManifestStore.open(corruptedChannel, TEST_PATH, ManifestSnapshot.empty()))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    @DisplayName("Operations after close throw StorageException with CLOSED error code")
    void testClosedStore() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultManifestStore store = DefaultManifestStore.open(channel, TEST_PATH, ManifestSnapshot.empty());
        store.close();
        store.close(); // idempotent

        assertThatThrownBy(() -> store.commit(ManifestSnapshot.empty()))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));
    }
}
