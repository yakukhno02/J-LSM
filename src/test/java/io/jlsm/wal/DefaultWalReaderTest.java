package io.jlsm.wal;

import io.jlsm.api.CorruptedLogException;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.model.BatchOperation;
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

            InternalRecord r1 = records.getFirst();
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
    @DisplayName("Round-trip: WalWriter writes, WalReader reads identical records including full object and empty byte[] value")
    void testRoundTrip() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 77L, DurabilityMode.BUFFERED)) {
            writer.append(100L, RecordKind.PUT, bytes("user:1"), bytes("Alice"));
            writer.append(101L, RecordKind.PUT, bytes("user:2"), bytes("Bob"));
            writer.append(102L, RecordKind.DELETE, bytes("user:1"), null);
            writer.append(103L, RecordKind.PUT, bytes("user:empty"), new byte[0]);
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

            assertThat(records).hasSize(4);

            InternalRecord r1 = records.getFirst();
            assertThat(r1.sequence()).isEqualTo(100L);
            assertThat(r1.kind()).isEqualTo(RecordKind.PUT);
            assertThat(r1.key()).isEqualTo(ByteKey.copyOf(bytes("user:1")));
            assertThat(r1.value()).isEqualTo(bytes("Alice"));
            assertThat(r1.isTombstone()).isFalse();

            InternalRecord r2 = records.get(1);
            assertThat(r2.sequence()).isEqualTo(101L);
            assertThat(r2.kind()).isEqualTo(RecordKind.PUT);
            assertThat(r2.key()).isEqualTo(ByteKey.copyOf(bytes("user:2")));
            assertThat(r2.value()).isEqualTo(bytes("Bob"));
            assertThat(r2.isTombstone()).isFalse();

            InternalRecord r3 = records.get(2);
            assertThat(r3.sequence()).isEqualTo(102L);
            assertThat(r3.kind()).isEqualTo(RecordKind.DELETE);
            assertThat(r3.key()).isEqualTo(ByteKey.copyOf(bytes("user:1")));
            assertThat(r3.value()).isNull();
            assertThat(r3.isTombstone()).isTrue();

            InternalRecord r4 = records.get(3);
            assertThat(r4.sequence()).isEqualTo(103L);
            assertThat(r4.kind()).isEqualTo(RecordKind.PUT);
            assertThat(r4.key()).isEqualTo(ByteKey.copyOf(bytes("user:empty")));
            assertThat(r4.value()).isNotNull().isEmpty();
            assertThat(r4.isTombstone()).isFalse();
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
            assertThat(records.getFirst().sequence()).isEqualTo(1L);
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

    // ---- Grader hypothesis test (midterm feedback) ----

    @Test
    @DisplayName("Grader hypothesis: an empty PUT value survives the WAL round trip as an empty value, not as null")
    void graderEmptyValueSurvivesRoundTrip() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("k"), new byte[0]);
        }

        InMemoryChannelIO readChannel = new InMemoryChannelIO(writeChannel.toByteArray());
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH);
             RecordCursor cursor = reader.openCursor()) {
            assertThat(cursor.advance()).isTrue();
            assertThat(cursor.current())
                    .isEqualTo(InternalRecord.of(ByteKey.copyOf(bytes("k")), new byte[0], RecordKind.PUT, 1L));
        }
    }

    @Test
    @DisplayName("Reads batch frame and yields individual records with sequential sequence numbers")
    void testReadBatchFrameYieldsRecords() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 10L, DurabilityMode.BUFFERED)) {
            List<BatchOperation> ops = List.of(
                    BatchOperation.put(bytes("k1"), bytes("v1")),
                    BatchOperation.delete(bytes("k2")),
                    BatchOperation.put(bytes("k3"), new byte[0])
            );
            writer.appendBatch(500L, ops);
        }

        InMemoryChannelIO readChannel = new InMemoryChannelIO(writeChannel.toByteArray());
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH);
             RecordCursor cursor = reader.openCursor()) {

            assertThat(cursor.advance()).isTrue();
            InternalRecord r1 = cursor.current();
            assertThat(r1.sequence()).isEqualTo(500L);
            assertThat(r1.kind()).isEqualTo(RecordKind.PUT);
            assertThat(r1.key()).isEqualTo(ByteKey.copyOf(bytes("k1")));
            assertThat(r1.value()).isEqualTo(bytes("v1"));

            assertThat(cursor.advance()).isTrue();
            InternalRecord r2 = cursor.current();
            assertThat(r2.sequence()).isEqualTo(501L);
            assertThat(r2.kind()).isEqualTo(RecordKind.DELETE);
            assertThat(r2.key()).isEqualTo(ByteKey.copyOf(bytes("k2")));
            assertThat(r2.isTombstone()).isTrue();

            assertThat(cursor.advance()).isTrue();
            InternalRecord r3 = cursor.current();
            assertThat(r3.sequence()).isEqualTo(502L);
            assertThat(r3.kind()).isEqualTo(RecordKind.PUT);
            assertThat(r3.key()).isEqualTo(ByteKey.copyOf(bytes("k3")));
            assertThat(r3.value()).isNotNull().isEmpty();

            assertThat(cursor.advance()).isFalse();
        }
    }

    @Test
    @DisplayName("Seamlessly interleaves single records and batch records in cursor iteration")
    void testMixedSingleAndBatchRecords() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("s1"), bytes("val1"));
            writer.appendBatch(2L, List.of(
                    BatchOperation.put(bytes("b1"), bytes("bval1")),
                    BatchOperation.delete(bytes("b2"))
            ));
            writer.append(4L, RecordKind.DELETE, bytes("s2"), null);
            writer.appendBatch(5L, List.of(
                    BatchOperation.put(bytes("b3"), bytes("bval3"))
            ));
        }

        InMemoryChannelIO readChannel = new InMemoryChannelIO(writeChannel.toByteArray());
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH);
             RecordCursor cursor = reader.openCursor()) {

            List<Long> seqs = new ArrayList<>();
            List<String> keys = new ArrayList<>();
            while (cursor.advance()) {
                seqs.add(cursor.current().sequence());
                keys.add(new String(cursor.current().key().bytes(), StandardCharsets.UTF_8));
            }

            assertThat(seqs).containsExactly(1L, 2L, 3L, 4L, 5L);
            assertThat(keys).containsExactly("s1", "b1", "b2", "s2", "b3");
        }
    }

    @Test
    @DisplayName("FI-01 / R-03: Torn batch at EOF during header write is truncated without recovering batch records")
    void testTornBatchHeaderAtEOFIsTruncated() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        long validOffsetBefore;
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("committedKey"), bytes("committedVal"));
            validOffsetBefore = writer.writeOffset();
            writer.appendBatch(2L, List.of(
                    BatchOperation.put(bytes("torn1"), bytes("val1")),
                    BatchOperation.put(bytes("torn2"), bytes("val2"))
            ));
        }

        byte[] tornBytes = Arrays.copyOf(writeChannel.toByteArray(), (int) validOffsetBefore + 10);
        InMemoryChannelIO readChannel = new InMemoryChannelIO(tornBytes);

        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH)) {
            long recovered = reader.recoverAndTruncateTail();
            assertThat(recovered).isEqualTo(validOffsetBefore);
            assertThat(readChannel.size()).isEqualTo(validOffsetBefore);

            try (RecordCursor cursor = reader.openCursor()) {
                assertThat(cursor.advance()).isTrue();
                assertThat(cursor.current().sequence()).isEqualTo(1L);
                assertThat(cursor.advance()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("FI-01 / R-03: Torn batch at EOF during payload write is truncated without recovering batch records")
    void testTornBatchPayloadAtEOFIsTruncated() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        long validOffsetBefore;
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.append(1L, RecordKind.PUT, bytes("committedKey"), bytes("committedVal"));
            validOffsetBefore = writer.writeOffset();
            writer.appendBatch(2L, List.of(
                    BatchOperation.put(bytes("torn1"), bytes("val1")),
                    BatchOperation.put(bytes("torn2"), bytes("val2"))
            ));
        }

        byte[] tornBytes = Arrays.copyOf(writeChannel.toByteArray(), (int) validOffsetBefore + 28);
        InMemoryChannelIO readChannel = new InMemoryChannelIO(tornBytes);

        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH)) {
            long recovered = reader.recoverAndTruncateTail();
            assertThat(recovered).isEqualTo(validOffsetBefore);
            assertThat(readChannel.size()).isEqualTo(validOffsetBefore);

            try (RecordCursor cursor = reader.openCursor()) {
                assertThat(cursor.advance()).isTrue();
                assertThat(cursor.current().sequence()).isEqualTo(1L);
                assertThat(cursor.advance()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Corrupted batch frame CRC triggers CorruptedLogException during cursor advance")
    void testCorruptedBatchCrcThrows() throws Exception {
        InMemoryChannelIO writeChannel = new InMemoryChannelIO();
        try (DefaultWalWriter writer = DefaultWalWriter.create(writeChannel, 1L, DurabilityMode.BUFFERED)) {
            writer.appendBatch(1L, List.of(
                    BatchOperation.put(bytes("k1"), bytes("v1")),
                    BatchOperation.put(bytes("k2"), bytes("v2"))
            ));
        }

        byte[] corrupted = writeChannel.toByteArray();
        corrupted[32 + 25] ^= 0x01; // flip bit in batch payload

        InMemoryChannelIO readChannel = new InMemoryChannelIO(corrupted);
        try (DefaultWalReader reader = DefaultWalReader.open(readChannel, TEST_PATH);
             RecordCursor cursor = reader.openCursor()) {
            assertThatThrownBy(cursor::advance)
                    .isInstanceOf(CorruptedLogException.class)
                    .hasMessageContaining("Frame CRC mismatch");
        }
    }
}
