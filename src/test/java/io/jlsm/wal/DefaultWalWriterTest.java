package io.jlsm.wal;

import io.jlsm.api.DurabilityMode;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultWalWriterTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class InMemoryChannelIO implements ChannelIO {
        private byte[] storage;
        private int size;
        private boolean closed;
        private int forceCount;
        private boolean throwOnWrite;
        private boolean throwOnForce;

        InMemoryChannelIO() {
            this.storage = new byte[256];
            this.size = 0;
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
            if (throwOnWrite) {
                throw new IOException("Simulated disk write error");
            }
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
            if (throwOnForce) {
                throw new IOException("Simulated fsync error");
            }
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
    @DisplayName("Matches verified binary fixture byte-for-byte")
    void testOutputMatchesFixture() throws Exception {
        byte[] expectedFixtureBytes;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/wal-minimal.bin")) {
            assertThat(in).isNotNull();
            expectedFixtureBytes = in.readAllBytes();
        }

        InMemoryChannelIO channel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.SYNC)) {
            long off1 = writer.append(1L, RecordKind.PUT, bytes("key1"), bytes("value1"));
            assertThat(off1).isEqualTo(32L);

            long off2 = writer.append(2L, RecordKind.DELETE, bytes("key2"), null);
            assertThat(off2).isEqualTo(32L + 27L + 4L); // 32 + 31 = 63
        }

        assertThat(channel.toByteArray()).isEqualTo(expectedFixtureBytes);
    }

    @Test
    @DisplayName("Header format and CRC32 verification")
    void testHeaderEncoding() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 42L, DurabilityMode.BUFFERED);

        assertThat(writer.writeOffset()).isEqualTo(32L);
        assertThat(channel.size()).isEqualTo(32L);

        byte[] data = channel.toByteArray();
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

        assertThat(buf.getInt()).isEqualTo(DefaultWalWriter.WAL_MAGIC);
        assertThat(buf.getInt()).isEqualTo(1);
        assertThat(buf.getLong()).isEqualTo(42L);
        assertThat(buf.getInt()).isEqualTo(0);
        assertThat(buf.getInt()).isEqualTo(0);
        assertThat(buf.getInt()).isEqualTo(32);

        int storedCrc = buf.getInt();
        CRC32 crc = new CRC32();
        crc.update(data, 0, 28);
        assertThat(storedCrc).isEqualTo((int) crc.getValue());
    }

    @Test
    @DisplayName("DurabilityMode.SYNC forces fsync on create, append and sync")
    void testDurabilitySync() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.SYNC);
        assertThat(channel.forceCount).isEqualTo(1); // on header

        writer.append(1L, RecordKind.PUT, bytes("k"), bytes("v"));
        assertThat(channel.forceCount).isEqualTo(2);

        writer.sync();
        assertThat(channel.forceCount).isEqualTo(3);
    }

    @Test
    @DisplayName("DurabilityMode.BUFFERED does not force until explicit sync()")
    void testDurabilityBuffered() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);
        assertThat(channel.forceCount).isZero();

        writer.append(1L, RecordKind.PUT, bytes("k"), bytes("v"));
        assertThat(channel.forceCount).isZero();

        writer.sync();
        assertThat(channel.forceCount).isEqualTo(1);
    }

    @Test
    @DisplayName("Open existing WAL continues from current channel size")
    void testOpenExisting() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 5L, DurabilityMode.BUFFERED);
        writer.append(1L, RecordKind.PUT, bytes("k1"), bytes("v1"));

        long sizeBefore = channel.size();
        try (DefaultWalWriter writer2 = DefaultWalWriter.openExisting(channel, DurabilityMode.BUFFERED)) {
            assertThat(writer2.writeOffset()).isEqualTo(sizeBefore);
            writer2.append(2L, RecordKind.PUT, bytes("k2"), bytes("v2"));
            assertThat(writer2.writeOffset()).isGreaterThan(sizeBefore);
        }
    }

    @Test
    @DisplayName("Validation prevents exceeding MAX_KEY_VALUE_TOTAL_BYTES")
    void testSizeLimitValidation() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);

        byte[] bigKey = new byte[2000];
        byte[] bigVal = new byte[2064]; // sum = 4064 > 4063

        assertThatThrownBy(() -> writer.append(1L, RecordKind.PUT, bigKey, bigVal))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum allowed limit");

        // Allowed exactly 4063
        byte[] maxVal = new byte[2063];
        long offset = writer.append(1L, RecordKind.PUT, bigKey, maxVal);
        assertThat(offset).isPositive();
    }

    @Test
    @DisplayName("Null safety and parameter validations")
    void testNullValidations() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);

        assertThatThrownBy(() -> writer.append(1L, null, bytes("k"), bytes("v")))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> writer.append(1L, RecordKind.PUT, null, bytes("v")))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> writer.append(1L, RecordKind.PUT, bytes("k"), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Operations after close throw StorageException with CLOSED error code")
    void testClosedState() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);
        writer.close();

        // Idempotent close
        writer.close();

        assertThatThrownBy(() -> writer.append(1L, RecordKind.PUT, bytes("k"), bytes("v")))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));

        assertThatThrownBy(writer::sync)
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));
    }

    @Test
    @DisplayName("I/O failures are wrapped in StorageException with IO error code")
    void testIOErrorHandling() {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        channel.throwOnWrite = true;

        assertThatThrownBy(() -> DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.IO));

        channel.throwOnWrite = false;
        assertThatThrownBy(() -> {
            DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);
            channel.throwOnForce = true;
            writer.sync();
        }).isInstanceOf(StorageException.class)
          .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.IO));
    }
}
