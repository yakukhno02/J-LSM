package io.jlsm.fixtures;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

public final class FixtureGenerator {

    public static final int WAL_MAGIC = 0x4A57414C; // "JWAL"
    public static final int SST_MAGIC = 0x4A535354; // "JSST"
    public static final int FOOTER_MAGIC = 0x4A4C534D; // "JLSM"
    public static final int FORMAT_VERSION = 1;

    public static byte[] generateMinimalWal(long fileId) {
        byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] val1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);

        // Header: 32 bytes
        ByteBuffer headerBuf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
        headerBuf.putInt(WAL_MAGIC);
        headerBuf.putInt(FORMAT_VERSION);
        headerBuf.putLong(fileId);
        headerBuf.putInt(0); // BlockSize = 0 for WAL
        headerBuf.putInt(0); // Reserved = 0
        headerBuf.putInt(32); // HeaderSize = 32

        CRC32 headerCrc = new CRC32();
        headerCrc.update(headerBuf.array(), 0, 28);
        int headerCrcValue = (int) headerCrc.getValue();
        headerBuf.putInt(headerCrcValue);

        // Frame 1: PUT (seq 1, "key1", "value1")
        // Payload: Sequence (8B) | Type (1B) | KeyLen (4B) | ValLen (4B) | Key (4B) | Val (6B) = 27B
        ByteBuffer frame1Payload = ByteBuffer.allocate(8 + 1 + 4 + 4 + key1.length + val1.length).order(ByteOrder.BIG_ENDIAN);
        frame1Payload.putLong(1L);
        frame1Payload.put((byte) 0x01); // PUT
        frame1Payload.putInt(key1.length);
        frame1Payload.putInt(val1.length);
        frame1Payload.put(key1);
        frame1Payload.put(val1);

        CRC32 frame1Crc = new CRC32();
        frame1Crc.update(frame1Payload.array(), 0, frame1Payload.capacity());
        int frame1CrcValue = (int) frame1Crc.getValue();

        // Frame 2: DELETE (seq 2, "key2", val = null/empty)
        // Payload: Sequence (8B) | Type (1B) | KeyLen (4B) | ValLen (4B) | Key (4B) = 21B
        ByteBuffer frame2Payload = ByteBuffer.allocate(8 + 1 + 4 + 4 + key2.length).order(ByteOrder.BIG_ENDIAN);
        frame2Payload.putLong(2L);
        frame2Payload.put((byte) 0x02); // DELETE
        frame2Payload.putInt(key2.length);
        frame2Payload.putInt(0); // ValLen = 0
        frame2Payload.put(key2);

        CRC32 frame2Crc = new CRC32();
        frame2Crc.update(frame2Payload.array(), 0, frame2Payload.capacity());
        int frame2CrcValue = (int) frame2Crc.getValue();

        // Assemble total WAL
        int totalSize = 32 + (4 + frame1Payload.capacity()) + (4 + frame2Payload.capacity());
        ByteBuffer totalBuf = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);
        totalBuf.put(headerBuf.array());

        totalBuf.putInt(frame1CrcValue);
        totalBuf.put(frame1Payload.array());

        totalBuf.putInt(frame2CrcValue);
        totalBuf.put(frame2Payload.array());

        return totalBuf.array();
    }

    public static byte[] generateMinimalSSTable(long fileId) {
        byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] val1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);

        // 1. Common Header: 32 bytes
        ByteBuffer headerBuf = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
        headerBuf.putInt(SST_MAGIC);
        headerBuf.putInt(FORMAT_VERSION);
        headerBuf.putLong(fileId);
        headerBuf.putInt(4096); // BlockSize = 4096 for SST
        headerBuf.putInt(0); // Reserved = 0
        headerBuf.putInt(32); // HeaderSize = 32

        CRC32 headerCrc = new CRC32();
        headerCrc.update(headerBuf.array(), 0, 28);
        int headerCrcValue = (int) headerCrc.getValue();
        headerBuf.putInt(headerCrcValue);

        // 2. Data Block 0: exactly 4096 bytes
        ByteBuffer blockBuf = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN);
        blockBuf.putInt(2); // recordCount = 2
        // usedBytes = 8 + (8 + 4 + 4 + 1 + 4 + 6) + (8 + 4 + 4 + 1 + 4 + 0) = 8 + 27 + 21 = 56
        int usedBytes = 8 + (8 + 4 + 4 + 1 + key1.length + val1.length) + (8 + 4 + 4 + 1 + key2.length);
        blockBuf.putInt(usedBytes);

        // Record 1: PUT (seq 1)
        blockBuf.putLong(1L);
        blockBuf.putInt(key1.length);
        blockBuf.putInt(val1.length);
        blockBuf.put((byte) 0x00); // Flags: 0x00 = PUT
        blockBuf.put(key1);
        blockBuf.put(val1);

        // Record 2: DELETE (seq 2)
        blockBuf.putLong(2L);
        blockBuf.putInt(key2.length);
        blockBuf.putInt(0); // ValLen = 0
        blockBuf.put((byte) 0x01); // Flags: 0x01 = DELETE
        blockBuf.put(key2);

        // Zero padding up to byte 4092 (already 0 in freshly allocated ByteBuffer)
        blockBuf.position(4092);

        CRC32 blockCrc = new CRC32();
        blockCrc.update(blockBuf.array(), 0, 4092);
        int blockCrcValue = (int) blockCrc.getValue();
        blockBuf.putInt(blockCrcValue);

        // 3. Sparse Index Block
        // blockCount (4B) + 1 * [blockOffset (8B) | blockSize (4B) | firstKeyLen (4B) | firstKeyBytes (4B)] = 24 bytes
        ByteBuffer indexBuf = ByteBuffer.allocate(4 + 8 + 4 + 4 + key1.length + 4).order(ByteOrder.BIG_ENDIAN);
        indexBuf.putInt(1); // blockCount = 1
        indexBuf.putLong(32L); // blockOffset = 32 (starts right after header)
        indexBuf.putInt(4096); // blockSize = 4096
        indexBuf.putInt(key1.length);
        indexBuf.put(key1);

        CRC32 indexCrc = new CRC32();
        indexCrc.update(indexBuf.array(), 0, 24);
        int indexCrcValue = (int) indexCrc.getValue();
        indexBuf.putInt(indexCrcValue); // 24 + 4 = 28 bytes total

        // 4. Footer: exactly 40 bytes
        // indexOffset (8B) | indexLength (8B) | totalRecordCount (8B) | blockCount (4B) | formatVersion (4B) | magic (4B) | footerCRC32 (4B)
        ByteBuffer footerBuf = ByteBuffer.allocate(40).order(ByteOrder.BIG_ENDIAN);
        long indexOffset = 32L + 4096L; // 4128L
        long indexLength = indexBuf.capacity(); // 28L
        long totalRecordCount = 2L;
        int blockCount = 1;

        footerBuf.putLong(indexOffset);
        footerBuf.putLong(indexLength);
        footerBuf.putLong(totalRecordCount);
        footerBuf.putInt(blockCount);
        footerBuf.putInt(FORMAT_VERSION);
        footerBuf.putInt(FOOTER_MAGIC);

        CRC32 footerCrc = new CRC32();
        footerCrc.update(footerBuf.array(), 0, 36);
        int footerCrcValue = (int) footerCrc.getValue();
        footerBuf.putInt(footerCrcValue);

        // Assemble total SSTable: 32 + 4096 + 28 + 40 = 4196 bytes
        ByteBuffer sstBuf = ByteBuffer.allocate(32 + 4096 + indexBuf.capacity() + 40).order(ByteOrder.BIG_ENDIAN);
        sstBuf.put(headerBuf.array());
        sstBuf.put(blockBuf.array());
        sstBuf.put(indexBuf.array());
        sstBuf.put(footerBuf.array());

        return sstBuf.array();
    }

    public static void writeFixturesTo(Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        Files.write(targetDir.resolve("wal-minimal.bin"), generateMinimalWal(1L));
        Files.write(targetDir.resolve("sstable-minimal.bin"), generateMinimalSSTable(1L));
    }
}
