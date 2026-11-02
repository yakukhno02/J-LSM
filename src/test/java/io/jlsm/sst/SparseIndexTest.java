package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SparseIndexTest {

    @Test
    @DisplayName("Encode and decode roundtrip succeeds for multiple blocks")
    void testEncodeDecodeRoundtrip() throws Exception {
        BlockMeta b0 = new BlockMeta(32L, 4096, "apple".getBytes(StandardCharsets.UTF_8));
        BlockMeta b1 = new BlockMeta(4128L, 4096, "banana".getBytes(StandardCharsets.UTF_8));
        BlockMeta b2 = new BlockMeta(8224L, 4096, "cherry".getBytes(StandardCharsets.UTF_8));

        SparseIndex original = new SparseIndex(List.of(b0, b1, b2));
        byte[] bytes = original.encode();

        SparseIndex decoded = SparseIndex.decode(ByteBuffer.wrap(bytes));
        assertThat(decoded.blocks()).hasSize(3);
        assertThat(decoded.blocks().get(0)).isEqualTo(b0);
        assertThat(decoded.blocks().get(1)).isEqualTo(b1);
        assertThat(decoded.blocks().get(2)).isEqualTo(b2);
    }

    @Test
    @DisplayName("Encode matches fixture sparse index from sstable-minimal.bin")
    void testFixtureSparseIndexCRC() throws Exception {
        // sstable-minimal: 1 block, offset 32L, size 4096, firstKey "key1"
        // expected index CRC = 0x50539E2F
        BlockMeta b0 = new BlockMeta(32L, 4096, "key1".getBytes(StandardCharsets.UTF_8));
        byte[] bytes = SparseIndex.encode(List.of(b0));

        assertThat(bytes).hasSize(28); // 4 + 8 + 4 + 4 + 4 + 4 = 28

        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        assertThat(bb.getInt(0)).isEqualTo(1); // blockCount
        assertThat(bb.getLong(4)).isEqualTo(32L); // blockOffset
        assertThat(bb.getInt(12)).isEqualTo(4096); // blockSize
        assertThat(bb.getInt(16)).isEqualTo(4); // firstKeyLen
        byte[] keyBytes = new byte[4];
        bb.position(20);
        bb.get(keyBytes);
        assertThat(keyBytes).isEqualTo("key1".getBytes(StandardCharsets.UTF_8));
        assertThat(bb.getInt(24)).isEqualTo(0x50539E2F); // indexCRC32

        SparseIndex decoded = SparseIndex.decode(ByteBuffer.wrap(bytes));
        assertThat(decoded.blocks()).containsExactly(b0);
    }

    @Test
    @DisplayName("Encode and decode empty sparse index")
    void testEmptySparseIndex() throws Exception {
        SparseIndex empty = new SparseIndex(List.of());
        byte[] bytes = empty.encode();
        assertThat(bytes).hasSize(8); // 4B blockCount + 4B CRC32

        SparseIndex decoded = SparseIndex.decode(ByteBuffer.wrap(bytes));
        assertThat(decoded.blocks()).isEmpty();
    }

    @Test
    @DisplayName("Decode fails on corrupted CRC")
    void testCorruptedCRC() {
        BlockMeta b0 = new BlockMeta(32L, 4096, "key1".getBytes(StandardCharsets.UTF_8));
        byte[] bytes = SparseIndex.encode(List.of(b0));
        bytes[5] ^= 0x01; // Corrupt block offset

        assertThatThrownBy(() -> SparseIndex.decode(ByteBuffer.wrap(bytes)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Sparse index CRC32 mismatch");
                });
    }

    @Test
    @DisplayName("Decode fails on truncated payload")
    void testTruncatedPayload() {
        ByteBuffer buf = ByteBuffer.allocate(6); // less than 8 bytes
        assertThatThrownBy(() -> SparseIndex.decode(buf))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Insufficient bytes");
                });
    }

    @Test
    @DisplayName("BlockMeta ensures defensive copy of firstKey")
    void testBlockMetaDefensiveCopy() {
        byte[] key = new byte[]{1, 2, 3};
        BlockMeta meta = new BlockMeta(0L, 4096, key);

        key[0] = 99;
        assertThat(meta.firstKey()[0]).isEqualTo((byte) 1);

        byte[] returned = meta.firstKey();
        returned[0] = 77;
        assertThat(meta.firstKey()[0]).isEqualTo((byte) 1);
    }
}
