package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SSTableHeaderTest {

    @Test
    @DisplayName("Encode and decode roundtrip succeeds")
    void testEncodeDecodeRoundtrip() throws Exception {
        byte[] bytes = SSTableHeader.encode(42L);
        assertThat(bytes).hasSize(SSTableHeader.HEADER_SIZE);

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        SSTableHeader header = SSTableHeader.decode(buf);

        assertThat(header.fileId()).isEqualTo(42L);
        assertThat(header.blockSize()).isEqualTo(4096);
        assertThat(header.formatVersion()).isEqualTo(1);
    }

    @Test
    @DisplayName("Encode matches fixture header byte values exactly")
    void testFixtureHeaderCRC() throws Exception {
        // From sstable-minimal.bin: fileId = 1, expected header CRC = 0x765CD30C
        byte[] bytes = SSTableHeader.encode(1L);
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        assertThat(bb.getInt(0)).isEqualTo(SSTableHeader.SST_MAGIC);
        assertThat(bb.getInt(4)).isEqualTo(1); // Version
        assertThat(bb.getLong(8)).isEqualTo(1L); // File ID
        assertThat(bb.getInt(16)).isEqualTo(4096); // Block size
        assertThat(bb.getInt(20)).isEqualTo(0); // Reserved
        assertThat(bb.getInt(24)).isEqualTo(32); // Header size
        assertThat(bb.getInt(28)).isEqualTo(0x765CD30C); // Header CRC32

        SSTableHeader decoded = SSTableHeader.decode(bb);
        assertThat(decoded.fileId()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Decode fails on CRC corruption")
    void testDecodeCorruptedCRC() {
        byte[] bytes = SSTableHeader.encode(1L);
        bytes[10] ^= 0x01; // Corrupt fileId byte

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        assertThatThrownBy(() -> SSTableHeader.decode(buf))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("CRC32 mismatch");
                });
    }

    @Test
    @DisplayName("Decode fails on invalid magic")
    void testDecodeInvalidMagic() {
        byte[] bytes = SSTableHeader.encode(1L);
        // Overwrite magic with garbage and recalculate CRC
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(0, 0x12345678);
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, 28);
        bb.putInt(28, (int) crc.getValue());

        assertThatThrownBy(() -> SSTableHeader.decode(ByteBuffer.wrap(bytes)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Invalid SSTable header magic");
                });
    }

    @Test
    @DisplayName("Decode fails on unsupported format version")
    void testDecodeUnsupportedVersion() {
        byte[] bytes = SSTableHeader.encode(1L);
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(4, 999); // Unsupported version
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, 28);
        bb.putInt(28, (int) crc.getValue());

        assertThatThrownBy(() -> SSTableHeader.decode(ByteBuffer.wrap(bytes)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.UNSUPPORTED_FORMAT);
                    assertThat(e.getMessage()).contains("Unsupported SSTable header version");
                });
    }

    @Test
    @DisplayName("Decode fails on truncated header buffer")
    void testDecodeTruncatedBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(16);
        assertThatThrownBy(() -> SSTableHeader.decode(buf))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Insufficient bytes");
                });
    }
}
