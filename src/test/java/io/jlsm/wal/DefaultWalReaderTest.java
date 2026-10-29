package io.jlsm.wal;

import io.jlsm.api.CorruptedLogException;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultWalReaderTest {

    private static final Path TEST_PATH = Path.of("000001.wal");

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class InMemoryChannelIO implements ChannelIO {
        private byte[] storage;
        private int size;
        private boolean closed;

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
    @DisplayName("Reads wal-minimal.bin fixture and returns expected records")
    void testReadFixture() throws Exception {
        byte[] fixtureBytes;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/wal-minimal.bin")) {
            assertThat(in).isNotNull();
            fixtureBytes = in.readAllBytes();
        }

        InMemoryChannelIO channel = new InMemoryChannelIO(fixtureBytes);
        try (DefaultWalReader reader = DefaultWalReader.open(channel, TEST_PATH)) {
            assertThat(reader.fileId()).isEqualTo(1L);
            assertThat(reader.path()).isEqualTo(TEST_PATH);

            List<InternalRecord> records = new ArrayList<>();
            try (RecordCursor cursor = reader.openCursor()) {
                while (cursor.advance()) {
                    records.add(cursor.current());
                }
            }

            assertThat(records).hasSize(2);

            InternalRecord r1 = records.get(0);
            assertThat(r1.sequence()).isEqualTo(1L);
            assertThat(r1.kind()).isEqualTo(RecordKind.PUT);
            assertThat(r1.key()).isEqualTo(ByteKey.copyOf(bytes("key1")));
            assertThat(r1.value()).isEqualTo(bytes("value1"));

            InternalRecord r2 = records.get(1);
            assertThat(r2.sequence()).isEqualTo(2L);
            assertThat(r2.kind()).isEqualTo(RecordKind.DELETE);
            assertThat(r2.key()).isEqualTo(ByteKey.copyOf(bytes("key2")));
            assertThat(r2.value()).isNull();
            assertThat(r2.isTombstone()).isTrue();
        }
    }

    @Test
    @DisplayName("Round-trip: WalWriter writes, WalReader reads identical records")
    void testRoundTrip() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 77L, DurabilityMode.BUFFERED)) {
            writer.append(100L, RecordKind.PUT, bytes("user:1"), bytes("Alice"));
            writer.append(101L, RecordKind.PUT, bytes("user:2"), bytes("Bob"));
            writer.append(102L, RecordKind.DELETE, bytes("user:1"), null);
        }

        InMemoryChannelIO readChannel = new InMemoryChannelIO(writeChannel.toByteArray());
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH)) {
            assertThat(reader.fileId()).isEqualTo(77L);

            List<InternalRecord> records = new ArrayList<>();
            try (RecordCursor cursor = reader.openCursor()) {
                while (cursor.advance()) {
                    records.add(cursor.current());
                }
            }

            assertThat(records).hasSize(3);
            assertThat(records.get(0).sequence()).isEqualTo(100L);
            assertThat(records.get(1).sequence()).isEqualTo(101L);
            assertThat(records.get(2).sequence()).isEqualTo(102L);
            assertThat(records.get(2).isTombstone()).isTrue();
        }
    }

    @Test
    @DisplayName("Corrupted header triggers CorruptedLogException")
    void testCorruptedHeader() {
        InMemoryChannelIO channel = new InMemoryChannelIO(new byte[16]);
        assertThatThrownBy(() -> DefaultWalReader.open(channel, TEST_PATH))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("smaller than WAL header");

        ByteBuffer buf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(0xDEADBEEF);
        buf.putInt(1);
        buf.putLong(1L);
        buf.putInt(0);
        buf.putInt(0);
        buf.putInt(32);
        buf.putInt(0);
        InMemoryChannelIO badMagicChannel = new InMemoryChannelIO(buf.array());
        assertThatThrownBy(() -> DefaultWalReader.open(badMagicChannel, TEST_PATH))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("Invalid WAL magic");

        buf.putInt(0, DefaultWalWriter.WAL_MAGIC);
        buf.putInt(28, 0x12345678);
        InMemoryChannelIO badCrcChannel = new InMemoryChannelIO(buf.array());
        assertThatThrownBy(() -> DefaultWalReader.open(badCrcChannel, TEST_PATH))
                .isInstanceOf(CorruptedLogException.class)
                .hasMessageContaining("Header CRC mismatch");
    }

    @Test
    @DisplayName("FI-02: Mid-file bit rot triggers CorruptedLogException during read")
    void testMidFileBitRotThrows() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("keyA"), bytes("valueA"));
            writer.append(2L, RecordKind.PUT, bytes("keyB"), bytes("valueB"));
        }

        // Frame 1 starts at 32. Key "keyA" payload bytes start at offset 32 + 21 = 53
        byte[] corruptedBytes = writeChannel.toByteArray();
        corruptedBytes[53] ^= 0x01; // flip 1 bit in key byte

        InMemoryChannelIO readChannel = new InMemoryChannelIO(corruptedBytes);
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH)) {
            try (RecordCursor cursor = reader.openCursor()) {
                assertThatThrownBy(cursor::advance)
                        .isInstanceOf(CorruptedLogException.class)
                        .hasMessageContaining("Frame CRC mismatch");
            }
        }
    }

    @Test
    @DisplayName("FI-01 / R-02: Torn write at EOF is recovered and truncated")
    void testTornWriteRecovery() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        long validOffset;
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("k1"), bytes("v1"));
            validOffset = writer.writeOffset();
            writer.append(2L, RecordKind.PUT, bytes("k2"), bytes("v2"));
        }

        // Cut file 5 bytes into frame 2
        byte[] tornBytes = Arrays.copyOf(writeChannel.toByteArray(), (int) validOffset + 5);
        InMemoryChannelIO readChannel = new InMemoryChannelIO(tornBytes);

        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH)) {
            long recoveredOffset = reader.recoverAndTruncateTail();
            assertThat(recoveredOffset).isEqualTo(validOffset);
            assertThat(readChannel.size()).isEqualTo(validOffset);

            List<InternalRecord> records = new ArrayList<>();
            try (RecordCursor cursor = reader.openCursor()) {
                while (cursor.advance()) {
                    records.add(cursor.current());
                }
            }
            assertThat(records).hasSize(1);
            assertThat(records.get(0).sequence()).isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("Cursor contract: advance, current before advance, after EOF, and close")
    void testCursorContract() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("k"), bytes("v"));
        }

        InMemoryChannelIO readChannel = new InMemoryChannelIO(writeChannel.toByteArray());
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH)) {
            RecordCursor cursor = reader.openCursor();
            assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);

            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.current()).isNotNull();

            assertThat(cursor.advance()).isFalse();
            assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);

            cursor.close();
            assertThat(cursor.advance()).isFalse();
            assertThatThrownBy(cursor::current).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    @DisplayName("Operations after WalReader close throw StorageException")
    void testClosedReader() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);

        DefaultWalReader reader = DefaultWalReader.open(channel, TEST_PATH);
        reader.close();
        reader.close(); // idempotent

        assertThatThrownBy(reader::openCursor)
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));

        assertThatThrownBy(reader::recoverAndTruncateTail)
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));
    }
}
