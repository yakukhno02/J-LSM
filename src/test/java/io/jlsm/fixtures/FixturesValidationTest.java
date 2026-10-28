package io.jlsm.fixtures;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

class FixturesValidationTest {

    private static final int EXPECTED_WAL_HEADER_CRC32 = 0xFBB79D6F;
    private static final int EXPECTED_WAL_FRAME1_CRC32 = 0x5B799445;
    private static final int EXPECTED_WAL_FRAME2_CRC32 = 0x09CB0F97;

    private static final int EXPECTED_SST_HEADER_CRC32 = 0x765CD30C;
    private static final int EXPECTED_SST_BLOCK0_CRC32 = 0x539D931A;
    private static final int EXPECTED_SST_INDEX_CRC32  = 0x50539E2F;
    private static final int EXPECTED_SST_FOOTER_CRC32 = 0xEEC0FA42;

    @Test
    @DisplayName("Verify wal-minimal.bin layout, fields, and manually verified CRC32 values")
    void testWalMinimalFixture() throws IOException {
        byte[] walBytes;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/wal-minimal.bin")) {
            assertThat(in).isNotNull();
            walBytes = in.readAllBytes();
        }

        assertThat(walBytes).hasSize(88);
        ByteBuffer buf = ByteBuffer.wrap(walBytes).order(ByteOrder.BIG_ENDIAN);

        // 1. Header (32 bytes)
        int magic = buf.getInt();
        assertThat(magic).isEqualTo(0x4A57414C); // "JWAL"

        int version = buf.getInt();
        assertThat(version).isEqualTo(1);

        long fileId = buf.getLong();
        assertThat(fileId).isEqualTo(1L);

        int blockSize = buf.getInt();
        assertThat(blockSize).isEqualTo(0);

        int reserved = buf.getInt();
        assertThat(reserved).isEqualTo(0);

        int headerSize = buf.getInt();
        assertThat(headerSize).isEqualTo(32);

        int storedHeaderCrc = buf.getInt();
        CRC32 headerCrc = new CRC32();
        headerCrc.update(walBytes, 0, 28);
        int calculatedHeaderCrc = (int) headerCrc.getValue();

        assertThat(storedHeaderCrc).isEqualTo(EXPECTED_WAL_HEADER_CRC32);
        assertThat(calculatedHeaderCrc).isEqualTo(EXPECTED_WAL_HEADER_CRC32);

        // 2. Frame 1 (PUT)
        int storedFrame1Crc = buf.getInt();
        int frame1PayloadStart = buf.position();

        long seq1 = buf.getLong();
        assertThat(seq1).isEqualTo(1L);

        byte type1 = buf.get();
        assertThat(type1).isEqualTo((byte) 0x01); // PUT

        int keyLen1 = buf.getInt();
        assertThat(keyLen1).isEqualTo(4);

        int valLen1 = buf.getInt();
        assertThat(valLen1).isEqualTo(6);

        byte[] key1 = new byte[keyLen1];
        buf.get(key1);
        assertThat(new String(key1, StandardCharsets.UTF_8)).isEqualTo("key1");

        byte[] val1 = new byte[valLen1];
        buf.get(val1);
        assertThat(new String(val1, StandardCharsets.UTF_8)).isEqualTo("value1");

        int frame1PayloadEnd = buf.position();
        int frame1PayloadLen = frame1PayloadEnd - frame1PayloadStart;
        assertThat(frame1PayloadLen).isEqualTo(27);

        CRC32 frame1Crc = new CRC32();
        frame1Crc.update(walBytes, frame1PayloadStart, frame1PayloadLen);
        int calculatedFrame1Crc = (int) frame1Crc.getValue();

        assertThat(storedFrame1Crc).isEqualTo(EXPECTED_WAL_FRAME1_CRC32);
        assertThat(calculatedFrame1Crc).isEqualTo(EXPECTED_WAL_FRAME1_CRC32);

        // 3. Frame 2 (DELETE)
        int storedFrame2Crc = buf.getInt();
        int frame2PayloadStart = buf.position();

        long seq2 = buf.getLong();
        assertThat(seq2).isEqualTo(2L);

        byte type2 = buf.get();
        assertThat(type2).isEqualTo((byte) 0x02); // DELETE

        int keyLen2 = buf.getInt();
        assertThat(keyLen2).isEqualTo(4);

        int valLen2 = buf.getInt();
        assertThat(valLen2).isEqualTo(0);

        byte[] key2 = new byte[keyLen2];
        buf.get(key2);
        assertThat(new String(key2, StandardCharsets.UTF_8)).isEqualTo("key2");

        int frame2PayloadEnd = buf.position();
        int frame2PayloadLen = frame2PayloadEnd - frame2PayloadStart;
        assertThat(frame2PayloadLen).isEqualTo(21);

        CRC32 frame2Crc = new CRC32();
        frame2Crc.update(walBytes, frame2PayloadStart, frame2PayloadLen);
        int calculatedFrame2Crc = (int) frame2Crc.getValue();

        assertThat(storedFrame2Crc).isEqualTo(EXPECTED_WAL_FRAME2_CRC32);
        assertThat(calculatedFrame2Crc).isEqualTo(EXPECTED_WAL_FRAME2_CRC32);
        assertThat(buf.hasRemaining()).isFalse();
    }

    @Test
    @DisplayName("Verify sstable-minimal.bin layout, fields, and manually verified CRC32 values")
    void testSSTableMinimalFixture() throws IOException {
        byte[] sstBytes;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            sstBytes = in.readAllBytes();
        }

        assertThat(sstBytes).hasSize(4196);
        ByteBuffer buf = ByteBuffer.wrap(sstBytes).order(ByteOrder.BIG_ENDIAN);

        // 1. Common Header (32 bytes)
        int magic = buf.getInt();
        assertThat(magic).isEqualTo(0x4A535354); // "JSST"

        int version = buf.getInt();
        assertThat(version).isEqualTo(1);

        long fileId = buf.getLong();
        assertThat(fileId).isEqualTo(1L);

        int blockSize = buf.getInt();
        assertThat(blockSize).isEqualTo(4096);

        int reserved = buf.getInt();
        assertThat(reserved).isEqualTo(0);

        int headerSize = buf.getInt();
        assertThat(headerSize).isEqualTo(32);

        int storedHeaderCrc = buf.getInt();
        CRC32 headerCrc = new CRC32();
        headerCrc.update(sstBytes, 0, 28);
        int calculatedHeaderCrc = (int) headerCrc.getValue();

        assertThat(storedHeaderCrc).isEqualTo(EXPECTED_SST_HEADER_CRC32);
        assertThat(calculatedHeaderCrc).isEqualTo(EXPECTED_SST_HEADER_CRC32);

        // 2. Data Block 0 (4096 bytes)
        int blockStart = buf.position();
        assertThat(blockStart).isEqualTo(32);

        int recordCount = buf.getInt();
        assertThat(recordCount).isEqualTo(2);

        int usedBytes = buf.getInt();
        assertThat(usedBytes).isEqualTo(56);

        // Record 1 (PUT)
        long r1Seq = buf.getLong();
        assertThat(r1Seq).isEqualTo(1L);

        int r1KeyLen = buf.getInt();
        assertThat(r1KeyLen).isEqualTo(4);

        int r1ValLen = buf.getInt();
        assertThat(r1ValLen).isEqualTo(6);

        byte r1Flags = buf.get();
        assertThat(r1Flags).isEqualTo((byte) 0x00); // PUT

        byte[] r1Key = new byte[r1KeyLen];
        buf.get(r1Key);
        assertThat(new String(r1Key, StandardCharsets.UTF_8)).isEqualTo("key1");

        byte[] r1Val = new byte[r1ValLen];
        buf.get(r1Val);
        assertThat(new String(r1Val, StandardCharsets.UTF_8)).isEqualTo("value1");

        // Record 2 (DELETE)
        long r2Seq = buf.getLong();
        assertThat(r2Seq).isEqualTo(2L);

        int r2KeyLen = buf.getInt();
        assertThat(r2KeyLen).isEqualTo(4);

        int r2ValLen = buf.getInt();
        assertThat(r2ValLen).isEqualTo(0);

        byte r2Flags = buf.get();
        assertThat(r2Flags).isEqualTo((byte) 0x01); // DELETE

        byte[] r2Key = new byte[r2KeyLen];
        buf.get(r2Key);
        assertThat(new String(r2Key, StandardCharsets.UTF_8)).isEqualTo("key2");

        // Verify zero padding
        int paddingStart = buf.position();
        assertThat(paddingStart).isEqualTo(32 + usedBytes); // 32 + 56 = 88
        for (int i = paddingStart; i < 32 + 4092; i++) {
            assertThat(sstBytes[i]).isEqualTo((byte) 0x00);
        }

        buf.position(32 + 4092);
        int storedBlockCrc = buf.getInt();

        CRC32 blockCrc = new CRC32();
        blockCrc.update(sstBytes, blockStart, 4092);
        int calculatedBlockCrc = (int) blockCrc.getValue();

        assertThat(storedBlockCrc).isEqualTo(EXPECTED_SST_BLOCK0_CRC32);
        assertThat(calculatedBlockCrc).isEqualTo(EXPECTED_SST_BLOCK0_CRC32);

        // 3. Sparse Index Block (28 bytes)
        int indexStart = buf.position();
        assertThat(indexStart).isEqualTo(32 + 4096); // 4128

        int indexBlockCount = buf.getInt();
        assertThat(indexBlockCount).isEqualTo(1);

        long indexedBlockOffset = buf.getLong();
        assertThat(indexedBlockOffset).isEqualTo(32L);

        int indexedBlockSize = buf.getInt();
        assertThat(indexedBlockSize).isEqualTo(4096);

        int firstKeyLen = buf.getInt();
        assertThat(firstKeyLen).isEqualTo(4);

        byte[] firstKey = new byte[firstKeyLen];
        buf.get(firstKey);
        assertThat(new String(firstKey, StandardCharsets.UTF_8)).isEqualTo("key1");

        int storedIndexCrc = buf.getInt();
        CRC32 indexCrc = new CRC32();
        indexCrc.update(sstBytes, indexStart, 24);
        int calculatedIndexCrc = (int) indexCrc.getValue();

        assertThat(storedIndexCrc).isEqualTo(EXPECTED_SST_INDEX_CRC32);
        assertThat(calculatedIndexCrc).isEqualTo(EXPECTED_SST_INDEX_CRC32);

        // 4. Footer (40 bytes)
        int footerStart = buf.position();
        assertThat(footerStart).isEqualTo(4156);

        long footerIndexOffset = buf.getLong();
        assertThat(footerIndexOffset).isEqualTo(4128L);

        long footerIndexLength = buf.getLong();
        assertThat(footerIndexLength).isEqualTo(28L);

        long footerTotalRecords = buf.getLong();
        assertThat(footerTotalRecords).isEqualTo(2L);

        int footerBlockCount = buf.getInt();
        assertThat(footerBlockCount).isEqualTo(1);

        int footerFormatVersion = buf.getInt();
        assertThat(footerFormatVersion).isEqualTo(1);

        int footerMagic = buf.getInt();
        assertThat(footerMagic).isEqualTo(0x4A4C534D); // "JLSM"

        int storedFooterCrc = buf.getInt();
        CRC32 footerCrc = new CRC32();
        footerCrc.update(sstBytes, footerStart, 36);
        int calculatedFooterCrc = (int) footerCrc.getValue();

        assertThat(storedFooterCrc).isEqualTo(EXPECTED_SST_FOOTER_CRC32);
        assertThat(calculatedFooterCrc).isEqualTo(EXPECTED_SST_FOOTER_CRC32);
        assertThat(buf.hasRemaining()).isFalse();
    }
}
