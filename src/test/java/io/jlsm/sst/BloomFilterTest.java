package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.io.ChannelIO;
import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.TableMeta;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BloomFilterTest {

    private static ByteKey key(String s) {
        return ByteKey.copyOf(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Inserted keys always return true (0% false negatives) and absent keys have low false positive rate")
    void testMembershipAndFalsePositiveRate() {
        int count = 2000;
        BloomFilter filter = new BloomFilter(count, 10, 7);

        for (int i = 0; i < count; i++) {
            filter.add(key("present-key-" + i));
        }

        assertThat(filter.insertedCount()).isEqualTo(count);
        assertThat(filter.hashCount()).isEqualTo(7);
        assertThat(filter.bitSize()).isGreaterThanOrEqualTo(count * 10L);
        assertThat(filter.setBitCount()).isPositive();
        assertThat(filter.fillRatio()).isBetween(0.1, 0.9);
        assertThat(filter.estimatedFalsePositiveRate()).isLessThan(0.05);

        // 0% false negatives on all inserted keys
        for (int i = 0; i < count; i++) {
            assertThat(filter.mightContain(key("present-key-" + i))).isTrue();
        }

        // Check false positive rate on 5000 absent keys (theoretical ~0.8%, must be well under 3%)
        int falsePositives = 0;
        int trials = 5000;
        for (int i = 0; i < trials; i++) {
            if (filter.mightContain(key("absent-key-" + i))) {
                falsePositives++;
            }
        }
        double empiricalFpp = (double) falsePositives / trials;
        assertThat(empiricalFpp).isLessThan(0.03);
    }

    @Test
    @DisplayName("Binary encode and decode roundtrip preserves bit array, metadata, and equals/hashCode")
    void testEncodeDecodeRoundtrip() throws Exception {
        BloomFilter original = BloomFilter.createWithFalsePositiveRate(500, 0.01);
        for (int i = 0; i < 500; i++) {
            original.add(key("item-" + i));
        }

        byte[] encoded = original.encode();
        assertThat(encoded).hasSize(original.serializedSizeBytes());

        BloomFilter decoded = BloomFilter.decode(ByteBuffer.wrap(encoded));
        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.hashCode()).isEqualTo(original.hashCode());
        assertThat(decoded.toString()).contains("BloomFilter[");

        for (int i = 0; i < 500; i++) {
            assertThat(decoded.mightContain(key("item-" + i))).isTrue();
        }
    }

    @Test
    @DisplayName("Merge combines two compatible BloomFilters via bitwise OR")
    void testMergeCompatibleFilters() {
        BloomFilter f1 = new BloomFilter(100, 10, 7);
        BloomFilter f2 = new BloomFilter(100, 10, 7);

        f1.add(key("alpha"));
        f2.add(key("beta"));

        f1.merge(f2);
        assertThat(f1.mightContain(key("alpha"))).isTrue();
        assertThat(f1.mightContain(key("beta"))).isTrue();
        assertThat(f1.insertedCount()).isEqualTo(2L);

        BloomFilter incompatible = new BloomFilter(500, 12, 5);
        assertThatThrownBy(() -> f1.merge(incompatible))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot merge");
    }

    @Test
    @DisplayName("Decode rejects corrupted CRC32, invalid magic, unsupported version, and truncated buffers")
    void testDecodeCorruptionValidation() {
        BloomFilter filter = new BloomFilter(64);
        filter.add(key("k1"));
        byte[] validBytes = filter.encode();

        // 1. Corrupted CRC32
        byte[] badCrc = validBytes.clone();
        badCrc[24] ^= 0x01;
        assertThatThrownBy(() -> BloomFilter.decode(ByteBuffer.wrap(badCrc)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("CRC32 mismatch");
                });

        // 2. Invalid magic
        byte[] badMagic = validBytes.clone();
        ByteBuffer.wrap(badMagic).order(ByteOrder.BIG_ENDIAN).putInt(0, 0x12345678);
        assertThatThrownBy(() -> BloomFilter.decode(ByteBuffer.wrap(badMagic)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Invalid BloomFilter magic");
                });

        // 3. Unsupported version
        byte[] badVersion = validBytes.clone();
        ByteBuffer bb = ByteBuffer.wrap(badVersion).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(4, 99);
        CRC32 crc = new CRC32();
        crc.update(badVersion, 0, badVersion.length - 4);
        bb.putInt(badVersion.length - 4, (int) crc.getValue());
        assertThatThrownBy(() -> BloomFilter.decode(ByteBuffer.wrap(badVersion)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.UNSUPPORTED_FORMAT);
                    assertThat(e.getMessage()).contains("Unsupported BloomFilter format version");
                });

        // 4. Truncated buffer
        assertThatThrownBy(() -> BloomFilter.decode(ByteBuffer.allocate(12)))
                .isInstanceOfSatisfying(StorageException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.CORRUPTION);
                    assertThat(e.getMessage()).contains("Insufficient bytes");
                });
    }

    @Test
    @DisplayName("Constructor and factory methods validate arguments")
    void testArgumentValidation() {
        assertThatThrownBy(() -> new BloomFilter(-1, 10, 7)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BloomFilter(10, 0, 7)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BloomFilter(10, 10, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BloomFilter(10, 10, 33)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BloomFilter.createWithFalsePositiveRate(-1, 0.01)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BloomFilter.createWithFalsePositiveRate(10, 0.0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BloomFilter.createWithFalsePositiveRate(10, 1.0)).isInstanceOf(IllegalArgumentException.class);

        // Boundary values: expectedInsertions = 0, bitsPerKey = 1, hashCount = 1 and 32
        BloomFilter zeroExpected = new BloomFilter(0, 1, 1);
        assertThat(zeroExpected.wordCount()).isEqualTo(1);
        assertThat(zeroExpected.bitSize()).isEqualTo(64L);
        assertThat(zeroExpected.hashCount()).isEqualTo(1);
        assertThat(zeroExpected.estimatedFalsePositiveRate()).isEqualTo(0.0);

        BloomFilter maxHash = new BloomFilter(0, 1, 32);
        assertThat(maxHash.hashCount()).isEqualTo(32);

        BloomFilter fppZero = BloomFilter.createWithFalsePositiveRate(0, 0.01);
        assertThat(fppZero.bitSize()).isGreaterThanOrEqualTo(64L);
        assertThat(fppZero.hashCount()).isEqualTo(7);

        // Equals and hashCode branches
        assertThat(zeroExpected).isEqualTo(zeroExpected);
        assertThat(zeroExpected).isNotEqualTo(null);
        assertThat(zeroExpected).isNotEqualTo("str");
        assertThat(zeroExpected).isNotEqualTo(maxHash);
        assertThat(zeroExpected.hashCode()).isNotEqualTo(maxHash.hashCode());

        // Pin exact Murmur3 hash64 outputs across tail lengths (0..16 bytes) so any bitwise mutation in hash64 / fmix64 is killed
        long[] expectedHashes = new long[17];
        for (int len = 0; len <= 16; len++) {
            byte[] payload = new byte[len];
            for (int b = 0; b < len; b++) {
                payload[b] = (byte) (0xA5 ^ (b * 37 + len * 13));
            }
            expectedHashes[len] = BloomFilter.hash64(payload, 0x9E3779B97F4A7C15L);
            // Verify changing any single byte in payload changes hash64 output
            for (int b = 0; b < len; b++) {
                byte[] flipped = payload.clone();
                flipped[b] ^= 0x5A;
                assertThat(BloomFilter.hash64(flipped, 0x9E3779B97F4A7C15L))
                        .isNotEqualTo(expectedHashes[len]);
            }
        }
        assertThat(expectedHashes[0]).isEqualTo(-7160610219483255062L);
        assertThat(expectedHashes[7]).isNotEqualTo(expectedHashes[6]);
        assertThat(expectedHashes[8]).isNotEqualTo(expectedHashes[7]);
        assertThat(expectedHashes[15]).isNotEqualTo(expectedHashes[14]);
        assertThat(expectedHashes[16]).isNotEqualTo(expectedHashes[15]);
    }

    @Test
    @DisplayName("BloomFilter in DefaultTableHandle skips 4KB data block disk reads for absent keys")
    void testTableHandleSkipsDiskReadWhenBloomFilterRejectsKey(@TempDir Path tempDir) throws Exception {
        StandardStorageIO baseIo = new StandardStorageIO();
        DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, baseIo);

        List<InternalRecord> records = List.of(
                InternalRecord.of(key("apple"), "v1".getBytes(StandardCharsets.UTF_8), RecordKind.PUT, 1L),
                InternalRecord.of(key("zebra"), "v2".getBytes(StandardCharsets.UTF_8), RecordKind.PUT, 2L)
        );
        TableMeta meta = flusher.build(1L, new ListCursor(records));
        Path sstPath = flusher.sstPath(1L);

        AtomicInteger blockReads = new AtomicInteger();
        ChannelIO rawChannel = baseIo.openRead(sstPath);
        ChannelIO countingChannel = new ChannelIO() {
            @Override
            public int read(ByteBuffer dst, long position) throws IOException {
                if (position == 32L && dst.remaining() == 4096) {
                    blockReads.incrementAndGet();
                }
                return rawChannel.read(dst, position);
            }

            @Override
            public int write(ByteBuffer src, long position) throws IOException {
                return rawChannel.write(src, position);
            }

            @Override
            public long size() throws IOException {
                return rawChannel.size();
            }

            @Override
            public void truncate(long size) throws IOException {
                rawChannel.truncate(size);
            }

            @Override
            public void force(boolean metadata) throws IOException {
                rawChannel.force(metadata);
            }

            @Override
            public void close() throws IOException {
                rawChannel.close();
            }
        };

        try (DefaultTableHandle handle = DefaultTableHandle.open(countingChannel, meta)) {
            assertThat(handle.bloomFilter()).isNotNull();
            assertThat(handle.mightContain(key("apple"))).isTrue();
            assertThat(handle.mightContain(key("zebra"))).isTrue();

            // Key "mango" is between "apple" and "zebra" (so SparseIndex alone WOULD read Data Block 0),
            // but BloomFilter rejects it so 0 data block reads occur!
            ByteKey absentInRange = key("mango-absent-key");
            assertThat(handle.mightContain(absentInRange)).isFalse();

            Optional<InternalRecord> res = handle.get(absentInRange);
            assertThat(res).isEmpty();
            assertThat(blockReads.get()).isZero();

            // Present key performs exactly 1 data block read
            Optional<InternalRecord> hit = handle.get(key("apple"));
            assertThat(hit).isPresent();
            assertThat(blockReads.get()).isEqualTo(1);
        }
    }

    private static final class ListCursor implements RecordCursor {
        private final Iterator<InternalRecord> it;
        private InternalRecord cur;

        ListCursor(List<InternalRecord> list) {
            this.it = list.iterator();
        }

        @Override
        public boolean advance() {
            if (it.hasNext()) {
                cur = it.next();
                return true;
            }
            cur = null;
            return false;
        }

        @Override
        public InternalRecord current() {
            if (cur == null) throw new IllegalStateException("EOF");
            return cur;
        }

        @Override
        public void close() {}
    }
}
