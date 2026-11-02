package io.jlsm.sst;

import io.jlsm.internal.model.RecordKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBlockEncoderTest {

    @Test
    @DisplayName("Matches fixture data block from sstable-minimal.bin byte-for-byte")
    void testFixtureDataBlockByteParity() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(1L, RecordKind.PUT, "key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8));
        encoder.append(2L, RecordKind.DELETE, "key2".getBytes(StandardCharsets.UTF_8), null);

        assertThat(encoder.hasRecords()).isTrue();
        assertThat(encoder.recordCount()).isEqualTo(2);
        assertThat(encoder.usedBytes()).isEqualTo(56);

        FinishedBlock finished = encoder.finishBlock();
        assertThat(finished.recordCount()).isEqualTo(2);
        assertThat(finished.firstKey()).isEqualTo("key1".getBytes(StandardCharsets.UTF_8));

        byte[] block = finished.blockBytes();
        assertThat(block).hasSize(4096);

        ByteBuffer bb = ByteBuffer.wrap(block).order(ByteOrder.BIG_ENDIAN);
        assertThat(bb.getInt(0)).isEqualTo(2); // recordCount
        assertThat(bb.getInt(4)).isEqualTo(56); // usedBytes

        // Record 1 at offset 8:
        assertThat(bb.getLong(8)).isEqualTo(1L); // seq
        assertThat(bb.getInt(16)).isEqualTo(4); // keyLen
        assertThat(bb.getInt(20)).isEqualTo(6); // valLen
        assertThat(bb.get(24)).isEqualTo((byte) 0x00); // flags PUT
        byte[] key1 = new byte[4];
        bb.position(25);
        bb.get(key1);
        assertThat(key1).isEqualTo("key1".getBytes(StandardCharsets.UTF_8));
        byte[] val1 = new byte[6];
        bb.get(val1);
        assertThat(val1).isEqualTo("value1".getBytes(StandardCharsets.UTF_8));

        // Record 2 at offset 35:
        assertThat(bb.getLong(35)).isEqualTo(2L); // seq
        assertThat(bb.getInt(43)).isEqualTo(4); // keyLen
        assertThat(bb.getInt(47)).isEqualTo(0); // valLen
        assertThat(bb.get(51)).isEqualTo((byte) 0x01); // flags DELETE
        byte[] key2 = new byte[4];
        bb.position(52);
        bb.get(key2);
        assertThat(key2).isEqualTo("key2".getBytes(StandardCharsets.UTF_8));

        // Check padding between 56 and 4092
        for (int i = 56; i < 4092; i++) {
            assertThat(block[i]).withFailMessage("Padding byte at %d is not 0", i).isEqualTo((byte) 0);
        }

        // Expected CRC from fixture: 0x539D931A
        int crc = bb.getInt(4092);
        assertThat(crc).isEqualTo(0x539D931A);

        CRC32 verified = new CRC32();
        verified.update(block, 0, 4092);
        assertThat((int) verified.getValue()).isEqualTo(0x539D931A);
    }

    @Test
    @DisplayName("Block payload boundary: canFit behaves accurately at 4092 bytes")
    void testBoundaryFitting() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        // Record 1: 17B overhead + 100B key + 2000B value = 2117B.
        // usedBytes becomes 8 + 2117 = 2125B.
        byte[] key1 = new byte[100];
        byte[] val1 = new byte[2000];
        encoder.append(10L, RecordKind.PUT, key1, val1);
        assertThat(encoder.usedBytes()).isEqualTo(2125);

        // We have 4092 - 2125 = 1967B remaining.
        // Record 2 with 17B overhead + 100B key + 1850B value = 1967B fits exactly!
        assertThat(encoder.canFit(100, 1850)).isTrue();

        byte[] key2 = new byte[100];
        byte[] val2 = new byte[1850];
        encoder.append(11L, RecordKind.PUT, key2, val2);
        assertThat(encoder.usedBytes()).isEqualTo(4092);

        // Now block is completely full, even 0-byte key/val cannot fit
        assertThat(encoder.canFit(0, 0)).isFalse();

        assertThatThrownBy(() -> encoder.append(12L, RecordKind.DELETE, new byte[0], null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not fit");

        FinishedBlock finished = encoder.finishBlock();
        assertThat(finished.blockBytes()).hasSize(4096);
    }

    @Test
    @DisplayName("Rejects record exceeding maximum key-value limit of 4063 bytes")
    void testOversizedRecordRejection() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        byte[] key = new byte[2000];
        byte[] val = new byte[2064]; // 2000 + 2064 = 4064 > 4063

        assertThatThrownBy(() -> encoder.append(1L, RecordKind.PUT, key, val))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum allowed limit");
    }

    @Test
    @DisplayName("finishBlock on empty block throws IllegalStateException")
    void testFinishEmptyBlockThrows() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        assertThatThrownBy(encoder::finishBlock)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot finish an empty data block");
    }

    @Test
    @DisplayName("Reset restores clean initial state")
    void testReset() {
        DataBlockEncoder encoder = new DataBlockEncoder();
        encoder.append(1L, RecordKind.PUT, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8));
        assertThat(encoder.hasRecords()).isTrue();

        encoder.reset();
        assertThat(encoder.hasRecords()).isFalse();
        assertThat(encoder.recordCount()).isEqualTo(0);
        assertThat(encoder.usedBytes()).isEqualTo(8);
        assertThat(encoder.firstKey()).isNull();
    }
}
