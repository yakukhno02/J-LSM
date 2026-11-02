package io.jlsm.internal.io;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FaultInjectingStorageTest {

    @TempDir
    Path tempDir;

    private final FaultInjectingStorage storage = new FaultInjectingStorage();

    @Test
    @DisplayName("setDiskFull causes subsequent write operations to fail with IOException")
    void testDiskFullInjection() throws IOException {
        Path file = tempDir.resolve("disk-full.bin");
        try (ChannelIO channel = storage.createNew(file)) {
            channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3}), 0L);
            assertThat(channel.size()).isEqualTo(3L);

            storage.setDiskFull(true);

            assertThatThrownBy(() -> channel.write(ByteBuffer.wrap(new byte[]{4, 5}), 3L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("disk is full");

            // Reset restores normal write behavior
            storage.reset();
            channel.write(ByteBuffer.wrap(new byte[]{4, 5}), 3L);
            assertThat(channel.size()).isEqualTo(5L);
        }
    }

    @Test
    @DisplayName("failOnFsync causes channel.force() to fail")
    void testFailOnFsync() throws IOException {
        Path file = tempDir.resolve("fsync-fail.bin");
        try (ChannelIO channel = storage.createNew(file)) {
            channel.write(ByteBuffer.wrap(new byte[]{1}), 0L);
            channel.force(false); // succeeds

            storage.failOnFsync(true);
            assertThatThrownBy(() -> channel.force(false))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("fsync failure");

            storage.reset();
            channel.force(true); // succeeds
        }
    }

    @Test
    @DisplayName("corruptNextWrite flips bit in the written data on next write only")
    void testCorruptNextWrite() throws IOException {
        Path file = tempDir.resolve("corrupt-write.bin");
        byte[] original = new byte[]{(byte) 0b00000000, 10, 20};

        try (ChannelIO channel = storage.createNew(file)) {
            storage.corruptNextWrite(true);

            channel.write(ByteBuffer.wrap(original), 0L);

            // Next write without corruptNextWrite should be clean
            channel.write(ByteBuffer.wrap(new byte[]{30}), 3L);
        }

        byte[] onDisk = Files.readAllBytes(file);
        assertThat(onDisk).hasSize(4);
        // Bit 0 was flipped in byte 0: 0b00000000 ^ 1 = 0b00000001
        assertThat(onDisk[0]).isEqualTo((byte) 0b00000001);
        assertThat(onDisk[1]).isEqualTo((byte) 10);
        assertThat(onDisk[2]).isEqualTo((byte) 20);
        assertThat(onDisk[3]).isEqualTo((byte) 30);
    }

    @Test
    @DisplayName("failAfterBytesWritten simulates torn writes and stops future writes")
    void testFailAfterBytesWritten() throws IOException {
        Path file = tempDir.resolve("torn-write.bin");
        try (ChannelIO channel = storage.createNew(file)) {
            // Allow 5 bytes total
            storage.failAfterBytesWritten(5L);

            // Write 3 bytes -> succeeds (2 bytes allowance left)
            channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3}), 0L);
            assertThat(channel.size()).isEqualTo(3L);

            // Attempting to write 4 bytes: 2 bytes allowed, then throws IOException
            ByteBuffer nextBuf = ByteBuffer.wrap(new byte[]{4, 5, 6, 7});
            assertThatThrownBy(() -> channel.write(nextBuf, 3L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("write truncated and failed");

            // File should have received exactly 5 bytes (1, 2, 3, 4, 5)
            assertThat(channel.size()).isEqualTo(5L);

            // Further writes immediately fail
            assertThatThrownBy(() -> channel.write(ByteBuffer.wrap(new byte[]{8}), 5L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("write failure triggered after specified byte limit");

            // Resetting removes the constraint
            storage.reset();
            channel.write(ByteBuffer.wrap(new byte[]{8, 9}), 5L);
            assertThat(channel.size()).isEqualTo(7L);
        }
    }

    @Test
    @DisplayName("failAfterBytesWritten validates non-negative argument")
    void testFailAfterBytesWrittenValidation() {
        assertThatThrownBy(() -> storage.failAfterBytesWritten(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Delegates lifecycle and file system operations properly")
    void testDelegation() throws IOException {
        Path file = tempDir.resolve("delegation.bin");
        try (ChannelIO channel = storage.openReadWrite(file)) {
            channel.write(ByteBuffer.wrap("data".getBytes(StandardCharsets.UTF_8)), 0L);
            assertThat(channel.size()).isEqualTo(4L);
            channel.truncate(2L);
            assertThat(channel.size()).isEqualTo(2L);
        }

        try (ChannelIO readChannel = storage.openRead(file)) {
            ByteBuffer buf = ByteBuffer.allocate(2);
            readChannel.read(buf, 0L);
            assertThat(new String(buf.array(), StandardCharsets.UTF_8)).isEqualTo("da");
        }

        Path target = tempDir.resolve("target.bin");
        storage.atomicMove(file, target);
        assertThat(Files.exists(file)).isFalse();
        assertThat(Files.exists(target)).isTrue();

        storage.syncDirectory(tempDir);
        storage.deleteIfExists(target);
        assertThat(Files.exists(target)).isFalse();

        Path lockFile = tempDir.resolve("LOCK");
        try (LockHandle handle = storage.acquireLock(lockFile)) {
            assertThat(handle).isNotNull();
        }
    }
}
