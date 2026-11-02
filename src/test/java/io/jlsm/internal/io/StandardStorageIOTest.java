package io.jlsm.internal.io;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StandardStorageIOTest {

    @TempDir
    Path tempDir;

    private final StandardStorageIO storage = new StandardStorageIO();

    @Test
    @DisplayName("createNew creates file and allows writing and reading")
    void testCreateNewAndRead() throws IOException {
        Path file = tempDir.resolve("test-create.bin");
        byte[] payload = "Hello LSM Storage!".getBytes(StandardCharsets.UTF_8);

        try (ChannelIO channel = storage.createNew(file)) {
            assertThat(channel.size()).isZero();
            ByteBuffer buf = ByteBuffer.wrap(payload);
            int written = channel.write(buf, 0L);
            assertThat(written).isEqualTo(payload.length);
            assertThat(channel.size()).isEqualTo(payload.length);
            channel.force(true);
        }

        // Verify read back with openRead
        try (ChannelIO readChannel = storage.openRead(file)) {
            assertThat(readChannel.size()).isEqualTo(payload.length);
            ByteBuffer readBuf = ByteBuffer.allocate(payload.length);
            int read = readChannel.read(readBuf, 0L);
            assertThat(read).isEqualTo(payload.length);
            assertThat(readBuf.array()).isEqualTo(payload);
        }
    }

    @Test
    @DisplayName("createNew fails if file already exists")
    void testCreateNewAlreadyExists() throws IOException {
        Path file = tempDir.resolve("existing.bin");
        Files.write(file, new byte[]{1, 2, 3});

        assertThatThrownBy(() -> storage.createNew(file))
                .isInstanceOf(FileAlreadyExistsException.class);
    }

    @Test
    @DisplayName("openRead fails if file does not exist")
    void testOpenReadNotExists() {
        Path file = tempDir.resolve("not-found.bin");
        assertThatThrownBy(() -> storage.openRead(file))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    @DisplayName("openReadWrite opens existing file or creates new")
    void testOpenReadWrite() throws IOException {
        Path file = tempDir.resolve("read-write.bin");

        // 1. Creates new
        try (ChannelIO channel = storage.openReadWrite(file)) {
            channel.write(ByteBuffer.wrap(new byte[]{10, 20}), 0L);
        }

        // 2. Opens existing without truncating
        try (ChannelIO channel = storage.openReadWrite(file)) {
            assertThat(channel.size()).isEqualTo(2L);
            channel.write(ByteBuffer.wrap(new byte[]{30, 40}), 2L);
            assertThat(channel.size()).isEqualTo(4L);
        }

        byte[] all = Files.readAllBytes(file);
        assertThat(all).containsExactly(10, 20, 30, 40);
    }

    @Test
    @DisplayName("truncate truncates file size")
    void testTruncate() throws IOException {
        Path file = tempDir.resolve("truncate.bin");
        try (ChannelIO channel = storage.createNew(file)) {
            channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}), 0L);
            assertThat(channel.size()).isEqualTo(5L);

            channel.truncate(3L);
            assertThat(channel.size()).isEqualTo(3L);

            assertThatThrownBy(() -> channel.truncate(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisplayName("atomicMove moves source to target atomically")
    void testAtomicMove() throws IOException {
        Path source = tempDir.resolve("source.tmp");
        Path target = tempDir.resolve("target.sst");
        byte[] data = "table content".getBytes(StandardCharsets.UTF_8);

        Files.write(source, data);
        assertThat(Files.exists(source)).isTrue();
        assertThat(Files.exists(target)).isFalse();

        storage.atomicMove(source, target);

        assertThat(Files.exists(source)).isFalse();
        assertThat(Files.exists(target)).isTrue();
        assertThat(Files.readAllBytes(target)).isEqualTo(data);
    }

    @Test
    @DisplayName("deleteIfExists deletes file and returns cleanly if absent")
    void testDeleteIfExists() throws IOException {
        Path file = tempDir.resolve("to-delete.bin");
        Files.write(file, new byte[]{1});

        storage.deleteIfExists(file);
        assertThat(Files.exists(file)).isFalse();

        // Idempotent
        storage.deleteIfExists(file);
    }

    @Test
    @DisplayName("syncDirectory flushes directory metadata")
    void testSyncDirectory() throws IOException {
        Path subDir = tempDir.resolve("sub-dir");
        Files.createDirectories(subDir);
        storage.syncDirectory(subDir);

        // Non-directory path should safely return
        storage.syncDirectory(tempDir.resolve("absent"));
    }

    @Test
    @DisplayName("acquireLock acquires exclusive file lock and prevents double locking")
    void testFileLocking() throws IOException {
        Path lockFile = tempDir.resolve("LOCK");

        LockHandle lock1 = storage.acquireLock(lockFile);
        assertThat(lock1).isNotNull();

        // Second lock attempt from the same or another thread fails
        assertThatThrownBy(() -> storage.acquireLock(lockFile))
                .isInstanceOf(IOException.class);

        // Releasing lock allows subsequent acquisition
        lock1.close();
        // Idempotent close
        lock1.close();

        try (LockHandle lock2 = storage.acquireLock(lockFile)) {
            assertThat(lock2).isNotNull();
        }
    }

    @Test
    @DisplayName("Operations on closed ChannelIO throw IOException")
    void testClosedChannel() throws IOException {
        Path file = tempDir.resolve("closed.bin");
        ChannelIO channel = storage.createNew(file);
        channel.close();
        channel.close(); // idempotent

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(1), 0L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");

        assertThatThrownBy(() -> channel.write(ByteBuffer.wrap(new byte[]{1}), 0L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");

        assertThatThrownBy(channel::size)
                .isInstanceOf(IOException.class);

        assertThatThrownBy(() -> channel.truncate(0L))
                .isInstanceOf(IOException.class);

        assertThatThrownBy(() -> channel.force(false))
                .isInstanceOf(IOException.class);
    }
}
