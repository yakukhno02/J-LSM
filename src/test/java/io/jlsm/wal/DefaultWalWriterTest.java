package io.jlsm.wal;

import io.jlsm.api.DurabilityMode;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.FaultInjectingStorage;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import org.junit.jupiter.api.io.TempDir;
import java.util.ArrayList;
import java.util.List;
import io.jlsm.internal.model.BatchOperation;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
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
        private int failAfterBytes = -1;

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
            if (failAfterBytes >= 0) {
                int toWrite = Math.min(src.remaining(), failAfterBytes);
                if (toWrite > 0) {
                    int endPos = (int) (position + toWrite);
                    if (endPos > storage.length) {
                        storage = Arrays.copyOf(storage, Math.max(storage.length * 2, endPos));
                    }
                    src.get(storage, (int) position, toWrite);
                    if (endPos > size) {
                        size = endPos;
                    }
                }
                failAfterBytes = -1;
                throw new IOException("Simulated partial write disk error");
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
    @Test
    @DisplayName("Partial write failure rolls back via truncate and allows clean subsequent appends")
    void testPartialWriteRollbackViaTruncate() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 10L, DurabilityMode.BUFFERED);

        long off1 = writer.append(1L, RecordKind.PUT, bytes("key1"), bytes("value1"));
        long validSizeBeforeFailure = channel.size();
        assertThat(validSizeBeforeFailure).isGreaterThan(32L);

        // Inject partial write failure: write 10 bytes then fail
        channel.failAfterBytes = 10;
        assertThatThrownBy(() -> writer.append(2L, RecordKind.PUT, bytes("key2"), bytes("value2_long_payload")))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.IO));

        // Invariant: channel must be truncated back to valid offset before failure
        assertThat(channel.size()).isEqualTo(validSizeBeforeFailure);

        // Subsequent append should succeed at the exact valid offset
        long off2 = writer.append(3L, RecordKind.PUT, bytes("k3"), bytes("v3"));
        assertThat(off2).isEqualTo(validSizeBeforeFailure);

        // Verify reader parses cleanly without corrupted tail
        try (DefaultWalReader reader = DefaultWalReader.open(channel, Path.of("test.wal"))) {
            var cursor = reader.openCursor();
            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.current().sequence()).isEqualTo(1L);
            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.current().sequence()).isEqualTo(3L);
            assertThat(cursor.advance()).isFalse();
        }
    }

    // ---- Grader hypothesis test (midterm feedback) ----

    @Test
    @DisplayName("Grader hypothesis: a failed short append followed by a smaller append leaves a readable WAL")
    void graderShortWriteThenSmallerAppendLeavesReadableLog(@TempDir Path tempDir) throws Exception {
        Path walPath = tempDir.resolve("000001.wal");
        FaultInjectingStorage storage = new FaultInjectingStorage();
        DefaultWalWriter writer = DefaultWalWriter.create(storage.createNew(walPath), 1L, DurabilityMode.BUFFERED);
        writer.append(1L, RecordKind.PUT, bytes("before"), bytes("1"));

        storage.failAfterBytesWritten(100); // the 1,024-byte frame is cut after 100 bytes
        assertThatThrownBy(() -> writer.append(2L, RecordKind.PUT, bytes("big"), new byte[1000]))
                .isInstanceOf(StorageException.class);
        storage.reset();
        writer.append(3L, RecordKind.PUT, bytes("small"), bytes("2"));
        writer.close();

        List<String> keys = new ArrayList<>();
        try (DefaultWalReader reader = DefaultWalReader.open(new StandardStorageIO().openReadWrite(walPath), walPath)) {
            reader.recoverAndTruncateTail();
            try (RecordCursor cursor = reader.openCursor()) {
                while (cursor.advance()) {
                    InternalRecord record = cursor.current();
                    keys.add(new String(record.key().bytes(), StandardCharsets.UTF_8));
                }
            }
        }
        assertThat(keys).containsExactly("before", "small");
    }

    @Test
    @DisplayName("appendBatch writes valid binary batch frame (0x03) matching layout and CRC")
    void testAppendBatchLayoutAndCrc() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 10L, DurabilityMode.BUFFERED);

        List<BatchOperation> ops = List.of(
                BatchOperation.put(bytes("k1"), bytes("val1")),
                BatchOperation.delete(bytes("k2")),
                BatchOperation.put(bytes("k3"), bytes("val3"))
        );

        long offset = writer.appendBatch(100L, ops);
        assertThat(offset).isEqualTo(32L);

        int expectedFrameLen = 21 + 41;
        assertThat(writer.writeOffset()).isEqualTo(32L + expectedFrameLen);
        assertThat(channel.size()).isEqualTo(32L + expectedFrameLen);

        byte[] data = channel.toByteArray();
        ByteBuffer buf = ByteBuffer.wrap(data, 32, expectedFrameLen).order(ByteOrder.BIG_ENDIAN);

        int storedCrc = buf.getInt();
        long startSeq = buf.getLong();
        byte type = buf.get();
        int opCount = buf.getInt();
        int payloadLen = buf.getInt();

        assertThat(startSeq).isEqualTo(100L);
        assertThat(type).isEqualTo(DefaultWalWriter.TYPE_BATCH);
        assertThat(opCount).isEqualTo(3);
        assertThat(payloadLen).isEqualTo(41);

        CRC32 crc32 = new CRC32();
        crc32.update(data, 36, expectedFrameLen - 4);
        assertThat(storedCrc).isEqualTo((int) crc32.getValue());
    }

    @Test
    @DisplayName("appendBatch validates inputs: null list, empty list, and closed writer")
    void testAppendBatchValidations() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.BUFFERED);

        assertThatThrownBy(() -> writer.appendBatch(1L, null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> writer.appendBatch(1L, List.of()))
                .isInstanceOf(IllegalArgumentException.class);

        writer.close();
        assertThatThrownBy(() -> writer.appendBatch(1L, List.of(BatchOperation.delete(bytes("k")))))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.CLOSED));
    }

    @Test
    @DisplayName("appendBatch with DurabilityMode.SYNC forces channel write")
    void testAppendBatchDurabilitySync() throws StorageException {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 1L, DurabilityMode.SYNC);
        assertThat(channel.forceCount).isEqualTo(1);

        writer.appendBatch(1L, List.of(BatchOperation.put(bytes("k"), bytes("v"))));
        assertThat(channel.forceCount).isEqualTo(2);
    }

    @Test
    @DisplayName("appendBatch partial write failure rolls back via truncate")
    void testAppendBatchPartialWriteRollback() throws Exception {
        InMemoryChannelIO channel = new InMemoryChannelIO();
        DefaultWalWriter writer = DefaultWalWriter.create(channel, 10L, DurabilityMode.BUFFERED);

        writer.append(1L, RecordKind.PUT, bytes("key1"), bytes("val1"));
        long validOffsetBefore = channel.size();

        channel.failAfterBytes = 15;
        List<BatchOperation> ops = List.of(
                BatchOperation.put(bytes("b1"), bytes("batchVal1")),
                BatchOperation.put(bytes("b2"), bytes("batchVal2"))
        );

        assertThatThrownBy(() -> writer.appendBatch(2L, ops))
                .isInstanceOf(StorageException.class)
                .satisfies(e -> assertThat(((StorageException) e).code()).isEqualTo(ErrorCode.IO));

        assertThat(channel.size()).isEqualTo(validOffsetBefore);
        assertThat(writer.writeOffset()).isEqualTo(validOffsetBefore);

        long nextOff = writer.append(2L, RecordKind.PUT, bytes("keyOk"), bytes("valOk"));
        assertThat(nextOff).isEqualTo(validOffsetBefore);
    }
}
