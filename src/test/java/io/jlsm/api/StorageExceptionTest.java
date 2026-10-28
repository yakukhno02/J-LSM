package io.jlsm.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageExceptionTest {

    @Test
    @DisplayName("StorageException preserves error code and cause")
    void testStorageException() {
        IOException cause = new IOException("disk full");
        StorageException ex = new StorageException(ErrorCode.IO, "I/O failure", cause);

        assertThat(ex.code()).isEqualTo(ErrorCode.IO);
        assertThat(ex.getMessage()).isEqualTo("I/O failure");
        assertThat(ex.getCause()).isSameAs(cause);

        StorageException simple = new StorageException(ErrorCode.CORRUPTION, "corrupt");
        assertThat(simple.code()).isEqualTo(ErrorCode.CORRUPTION);
    }

    @Test
    @DisplayName("CorruptedLogException preserves path, offset and code")
    void testCorruptedLogException() {
        Path p = Paths.get("data", "000001.wal");
        CorruptedLogException ex = new CorruptedLogException(p, 128L, "CRC mismatch");

        assertThat(ex.code()).isEqualTo(ErrorCode.CORRUPTION);
        assertThat(ex.path()).isEqualTo(p);
        assertThat(ex.offset()).isEqualTo(128L);
        assertThat(ex.getMessage()).contains("000001.wal", "128", "CRC mismatch");
    }

    @Test
    @DisplayName("CorruptedTableException preserves path, offset and code")
    void testCorruptedTableException() {
        Path p = Paths.get("data", "000002.sst");
        CorruptedTableException ex = new CorruptedTableException(p, 4092L, "Invalid magic");

        assertThat(ex.code()).isEqualTo(ErrorCode.CORRUPTION);
        assertThat(ex.path()).isEqualTo(p);
        assertThat(ex.offset()).isEqualTo(4092L);
        assertThat(ex.getMessage()).contains("000002.sst", "4092", "Invalid magic");
    }

    @Test
    @DisplayName("ErrorCode values are complete")
    void testErrorCodes() {
        assertThat(ErrorCode.values()).contains(
                ErrorCode.IO,
                ErrorCode.CORRUPTION,
                ErrorCode.UNSUPPORTED_FORMAT,
                ErrorCode.UNSUPPORTED_STORAGE,
                ErrorCode.LOCKED,
                ErrorCode.CLOSED,
                ErrorCode.FAILED,
                ErrorCode.INTERRUPTED,
                ErrorCode.LIMIT_EXCEEDED,
                ErrorCode.RESOURCE_LIMIT
        );
    }

    @Test
    @DisplayName("KVDatabase.open delegates to DatabaseFactory")
    void testDatabaseFactoryOpen() {
        assertThatThrownBy(() -> KVDatabase.open(Paths.get("test-db"), DatabaseOptions.defaults()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Milestone M3");
    }
}
