package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBlockDecoderTest {

    @Test
    @DisplayName("Decode matches records encoded by DataBlockEncoder")
    void testEncodeDecodeRoundtrip() throws Exception {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(100L, RecordKind.PUT, "alpha".getBytes(StandardCharsets.UTF_8), "val-alpha".getBytes(StandardCharsets.UTF_8));
        encoder.append(101L, RecordKind.DELETE, "beta".getBytes(StandardCharsets.UTF_8), null);
        encoder.append(102L, RecordKind.PUT, "gamma".getBytes(StandardCharsets.UTF_8), "val-gamma".getBytes(StandardCharsets.UTF_8));

        FinishedBlock finished = encoder.finishBlock();
        List<InternalRecord> records = DataBlockDecoder.decode(finished.blockBytes());

        assertThat(records).hasSize(3);

        InternalRecord r0 = records.get(0);
        assertThat(r0.key()).isEqualTo(ByteKey.copyOf("alpha".getBytes(StandardCharsets.UTF_8)));
        assertThat(r0.value()).isEqualTo("val-alpha".getBytes(StandardCharsets.UTF_8));
        assertThat(r0.kind()).isEqualTo(RecordKind.PUT);
        assertThat(r0.sequence()).isEqualTo(100L);

        InternalRecord r1 = records.get(1);
        assertThat(r1.key()).isEqualTo(ByteKey.copyOf("beta".getBytes(StandardCharsets.UTF_8)));
        assertThat(r1.value()).isNull();
        assertThat(r1.kind()).isEqualTo(RecordKind.DELETE);
        assertThat(r1.sequence()).isEqualTo(101L);

        InternalRecord r2 = records.get(2);
        assertThat(r2.key()).isEqualTo(ByteKey.copyOf("gamma".getBytes(StandardCharsets.UTF_8)));
        assertThat(r2.value()).isEqualTo("val-gamma".getBytes(StandardCharsets.UTF_8));
        assertThat(r2.kind()).isEqualTo(RecordKind.PUT);
        assertThat(r2.sequence()).isEqualTo(102L);
    }

    @Test
    @DisplayName("Decodes sstable-minimal.bin fixture data block correctly")
    void testDecodeFixtureDataBlock() throws Exception {
        byte[] fixtureBytes;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            fixtureBytes = in.readAllBytes();
        }

        // Data block is at offset 32 with size 4096
        byte[] blockBytes = Arrays.copyOfRange(fixtureBytes, 32, 32 + 4096);
        List<InternalRecord> records = DataBlockDecoder.decode(blockBytes);

        assertThat(records).hasSize(2);

        InternalRecord r1 = records.get(0);
        assertThat(r1.sequence()).isEqualTo(1L);
        assertThat(r1.key()).isEqualTo(ByteKey.copyOf("key1".getBytes(StandardCharsets.UTF_8)));
        assertThat(r1.value()).isEqualTo("value1".getBytes(StandardCharsets.UTF_8));
        assertThat(r1.kind()).isEqualTo(RecordKind.PUT);

        InternalRecord r2 = records.get(1);
        assertThat(r2.sequence()).isEqualTo(2L);
        assertThat(r2.key()).isEqualTo(ByteKey.copyOf("key2".getBytes(StandardCharsets.UTF_8)));
        assertThat(r2.value()).isNull();
        assertThat(r2.kind()).isEqualTo(RecordKind.DELETE);
    }

    @Test
    @DisplayName("searchInBlock finds existing key and returns empty for absent key")
    void testSearchInBlock() throws Exception {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(1L, RecordKind.PUT, "k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8));
        encoder.append(2L, RecordKind.PUT, "k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8));
        FinishedBlock finished = encoder.finishBlock();

        Optional<InternalRecord> found = DataBlockDecoder.searchInBlock(
                ByteBuffer.wrap(finished.blockBytes()),
                ByteKey.copyOf("k2".getBytes(StandardCharsets.UTF_8))
        );
        assertThat(found).isPresent();
        assertThat(found.get().sequence()).isEqualTo(2L);

        Optional<InternalRecord> notFound = DataBlockDecoder.searchInBlock(
                ByteBuffer.wrap(finished.blockBytes()),
                ByteKey.copyOf("k999".getBytes(StandardCharsets.UTF_8))
        );
        assertThat(notFound).isEmpty();
    }

    @Test
    @DisplayName("Fails with CORRUPTION on CRC mismatch")
    void testDecodeCorruptedCRC() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(1L, RecordKind.PUT, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
        byte[] block = encoder.finishBlock().blockBytes();

        // Invert bit in data payload
        block[10] ^= 0x01;

        assertThatThrownBy(() -> DataBlockDecoder.decode(block))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("CRC32 mismatch");
                });
    }

    @Test
    @DisplayName("Fails with CORRUPTION on invalid usedBytes header")
    void testDecodeInvalidUsedBytes() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(1L, RecordKind.PUT, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
        byte[] block = encoder.finishBlock().blockBytes();

        // Corrupt usedBytes to 5000 (> 4092) and recalculate CRC
        ByteBuffer bb = ByteBuffer.wrap(block);
        bb.putInt(4, 5000);
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(block, 0, 4092);
        bb.putInt(4092, (int) crc.getValue());

        assertThatThrownBy(() -> DataBlockDecoder.decode(block))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Invalid usedBytes");
                });
    }

    @Test
    @DisplayName("Fails with CORRUPTION on unknown record flags")
    void testDecodeUnknownFlags() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(1L, RecordKind.PUT, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
        byte[] block = encoder.finishBlock().blockBytes();

        // Record flags is at offset 8 + 8 + 4 + 4 = 24
        block[24] = 0x05; // unknown flag
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(block, 0, 4092);
        ByteBuffer.wrap(block).putInt(4092, (int) crc.getValue());

        assertThatThrownBy(() -> DataBlockDecoder.decode(block))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Unknown record flags");
                });
    }

    @Test
    @DisplayName("Fails with CORRUPTION on truncated block buffer")
    void testDecodeTruncatedBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(100);
        assertThatThrownBy(() -> DataBlockDecoder.decode(buf))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Insufficient bytes");
                });
    }
}
