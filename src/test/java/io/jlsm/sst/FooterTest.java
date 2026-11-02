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

class FooterTest {

    @Test
    @DisplayName("Encode and decode roundtrip succeeds")
    void testEncodeDecodeRoundtrip() throws Exception {
        Footer original = new Footer(4128L, 28L, 2L, 1, 1);
        byte[] encoded = original.encode();

        assertThat(encoded).hasSize(Footer.FOOTER_SIZE);

        Footer decoded = Footer.decode(ByteBuffer.wrap(encoded));
        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.indexOffset()).isEqualTo(4128L);
        assertThat(decoded.indexLength()).isEqualTo(28L);
        assertThat(decoded.totalRecordCount()).isEqualTo(2L);
        assertThat(decoded.blockCount()).isEqualTo(1);
        assertThat(decoded.formatVersion()).isEqualTo(1);
    }

    @Test
    @DisplayName("Footer CRC32 matches fixture value from sstable-minimal.bin")
    void testFixtureFooterCRC() throws Exception {
        // sstable-minimal: indexOffset=4128, indexLength=28, totalRecordCount=2, blockCount=1, version=1
        // expected footerCRC32 = 0xEEC0FA42
        Footer footer = new Footer(4128L, 28L, 2L, 1, 1);
        byte[] bytes = footer.encode();
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        assertThat(bb.getLong(0)).isEqualTo(4128L);
        assertThat(bb.getLong(8)).isEqualTo(28L);
        assertThat(bb.getLong(16)).isEqualTo(2L);
        assertThat(bb.getInt(24)).isEqualTo(1);
        assertThat(bb.getInt(28)).isEqualTo(1);
        assertThat(bb.getInt(32)).isEqualTo(Footer.FOOTER_MAGIC);
        assertThat(bb.getInt(36)).isEqualTo(0xEEC0FA42);

        Footer decoded = Footer.decode(bb);
        assertThat(decoded).isEqualTo(footer);
    }

    @Test
    @DisplayName("Decode fails when CRC32 does not match")
    void testCorruptedCRC() {
        Footer footer = new Footer(100L, 50L, 10L, 2, 1);
        byte[] bytes = footer.encode();
        bytes[0] ^= 0x01; // Corrupt indexOffset byte

        assertThatThrownBy(() -> Footer.decode(ByteBuffer.wrap(bytes)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Footer CRC32 mismatch");
                });
    }

    @Test
    @DisplayName("Decode fails when magic number is wrong")
    void testInvalidMagic() {
        byte[] bytes = new Footer(100L, 50L, 10L, 2, 1).encode();
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(32, 0x99999999); // bad magic
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, 36);
        bb.putInt(36, (int) crc.getValue());

        assertThatThrownBy(() -> Footer.decode(ByteBuffer.wrap(bytes)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Invalid SSTable footer magic");
                });
    }

    @Test
    @DisplayName("Decode fails when format version is unsupported")
    void testUnsupportedFormatVersion() {
        byte[] bytes = new Footer(100L, 50L, 10L, 2, 1).encode();
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(28, 42); // bad version
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, 36);
        bb.putInt(36, (int) crc.getValue());

        assertThatThrownBy(() -> Footer.decode(ByteBuffer.wrap(bytes)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.UNSUPPORTED_FORMAT);
                    assertThat(e.getMessage()).contains("Unsupported SSTable format version");
                });
    }

    @Test
    @DisplayName("Decode fails on truncated buffer")
    void testTruncatedBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(30);
        assertThatThrownBy(() -> Footer.decode(buf))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Insufficient bytes");
                });
    }

    @Test
    @DisplayName("Constructor rejects negative arguments")
    void testConstructorValidation() {
        assertThatThrownBy(() -> new Footer(-1L, 10L, 10L, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Footer(10L, -1L, 10L, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Footer(10L, 10L, -1L, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Footer(10L, 10L, 10L, -1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
