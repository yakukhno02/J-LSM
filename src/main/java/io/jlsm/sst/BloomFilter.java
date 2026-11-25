package io.jlsm.sst;

import io.jlsm.api.ErrorCode;
import io.jlsm.api.StorageException;
import io.jlsm.internal.model.ByteKey;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Space-efficient probabilistic Bloom Filter for SSTable point-lookup pruning (Milestone M8 Scope Extension).
 * <p>
 * Uses Kirsch-Mitzenmacher double hashing over two independent 64-bit MurmurHash3-style mixers:
 * <ul>
 *     <li>Default bits per key: {@code m / n = 10}</li>
 *     <li>Default hash probes: {@code k = 7}</li>
 *     <li>Guarantees 0% false negatives: if {@link #mightContain(ByteKey)} returns {@code false},
 *     the key is definitely not present in the SSTable and disk block I/O can be skipped completely.</li>
 * </ul>
 * <p>
 * Binary format of the Bloom Filter Block inside an SSTable:
 * <pre>
 * +-------------------+-------------------+-------------------+-------------------+
 * | Magic JBLM (4B)   | Version = 1 (4B)  | HashCount k (4B)  | WordCount w (4B)  |
 * +-------------------+-------------------+-------------------+-------------------+
 * | InsertedCount (8B)                    | Bit Words (w * 8B, Big-Endian)        |
 * +---------------------------------------+---------------------------------------+
 * | ...                                   | BloomCRC32 (4B over preceding bytes)  |
 * +---------------------------------------+---------------------------------------+
 * </pre>
 */
public final class BloomFilter {

    public static final int BLOOM_MAGIC = 0x4A424C4D; // ASCII: "JBLM"
    public static final int FORMAT_VERSION = 1;
    public static final int DEFAULT_BITS_PER_KEY = 10;
    public static final int DEFAULT_HASH_COUNT = 7;
    public static final int MAX_HASH_COUNT = 32;
    public static final int HEADER_BYTES = 24; // magic(4) + version(4) + hashCount(4) + wordCount(4) + insertedCount(8)
    public static final int TRAILER_CRC_BYTES = 4;

    private static final long SEED_1 = 0x9747B28C452821E6L;
    private static final long SEED_2 = 0xC6A4A7935BD1E995L;
    private static final long C1 = 0x87C37B91114253D5L;
    private static final long C2 = 0x4CF5AD432745937FL;

    private final long[] words;
    private final int hashCount;
    private final long bitSize;
    private long insertedCount;

    /**
     * Creates a BloomFilter sized for the expected number of keys using default parameters (m/n = 10, k = 7).
     *
     * @param expectedInsertions expected number of keys
     */
    public BloomFilter(long expectedInsertions) {
        this(expectedInsertions, DEFAULT_BITS_PER_KEY, DEFAULT_HASH_COUNT);
    }

    /**
     * Creates a BloomFilter with custom bits-per-key and hash function count.
     *
     * @param expectedInsertions expected number of keys (at least 0)
     * @param bitsPerKey         bits allocated per expected key (at least 1)
     * @param hashCount          number of hash probes per key (between 1 and 32)
     */
    public BloomFilter(long expectedInsertions, int bitsPerKey, int hashCount) {
        if (expectedInsertions < 0) {
            throw new IllegalArgumentException("expectedInsertions cannot be negative: " + expectedInsertions);
        }
        if (bitsPerKey <= 0) {
            throw new IllegalArgumentException("bitsPerKey must be positive: " + bitsPerKey);
        }
        if (hashCount <= 0 || hashCount > MAX_HASH_COUNT) {
            throw new IllegalArgumentException("hashCount must be between 1 and " + MAX_HASH_COUNT + ": " + hashCount);
        }

        long rawBits = Math.max(64L, expectedInsertions * (long) bitsPerKey);
        long wordCountLong = (rawBits + 63L) / 64L;
        if (wordCountLong > Integer.MAX_VALUE - 8) {
            throw new IllegalArgumentException("Requested BloomFilter size is too large: " + rawBits + " bits");
        }

        int wordCount = (int) Math.max(1L, wordCountLong);
        this.words = new long[wordCount];
        this.hashCount = hashCount;
        this.bitSize = (long) wordCount * 64L;
        this.insertedCount = 0L;
    }

    private BloomFilter(long[] words, int hashCount, long insertedCount) {
        this.words = words;
        this.hashCount = hashCount;
        this.bitSize = (long) words.length * 64L;
        this.insertedCount = insertedCount;
    }

    /**
     * Creates an optimal BloomFilter targeting a specific false-positive probability {@code fpp}.
     *
     * @param expectedInsertions expected number of keys
     * @param fpp                target false positive probability in (0.0, 1.0)
     * @return configured {@link BloomFilter}
     */
    public static BloomFilter createWithFalsePositiveRate(long expectedInsertions, double fpp) {
        if (expectedInsertions < 0) {
            throw new IllegalArgumentException("expectedInsertions cannot be negative: " + expectedInsertions);
        }
        if (Double.isNaN(fpp) || fpp <= 0.0 || fpp >= 1.0) {
            throw new IllegalArgumentException("fpp must be strictly between 0.0 and 1.0: " + fpp);
        }
        long n = Math.max(1L, expectedInsertions);
        double ln2 = Math.log(2.0);
        long optimalBits = (long) Math.ceil((-n * Math.log(fpp)) / (ln2 * ln2));
        int bitsPerKey = (int) Math.max(1L, (optimalBits + n - 1L) / n);
        int optimalK = (int) Math.max(1L, Math.min(MAX_HASH_COUNT, Math.round(bitsPerKey * ln2)));
        return new BloomFilter(expectedInsertions, bitsPerKey, optimalK);
    }

    /**
     * Inserts a {@link ByteKey} into the filter.
     */
    public void add(ByteKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        add(key.bytes());
    }

    /**
     * Inserts a raw byte array key into the filter.
     */
    public void add(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes cannot be null");
        long h1 = hash64(keyBytes, SEED_1);
        long h2 = hash64(keyBytes, SEED_2);
        if (h2 == 0L) {
            h2 = 0x9E3779B97F4A7C15L;
        }

        for (int i = 0; i < hashCount; i++) {
            long combined = h1 + (i * h2) + ((long) i * i);
            long bitIndex = Long.remainderUnsigned(combined, bitSize);
            int wordIndex = (int) (bitIndex >>> 6);
            long bitMask = 1L << (bitIndex & 63L);
            words[wordIndex] |= bitMask;
        }
        insertedCount++;
    }

    /**
     * Tests whether the given {@link ByteKey} might be present in the SSTable.
     *
     * @param key key to check
     * @return {@code false} if the key is definitely NOT in the set; {@code true} if it might be present
     */
    public boolean mightContain(ByteKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        return mightContain(key.bytes());
    }

    /**
     * Tests whether the given raw byte array key might be present in the SSTable.
     *
     * @param keyBytes key bytes to check
     * @return {@code false} if the key is definitely NOT in the set; {@code true} if it might be present
     */
    public boolean mightContain(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes cannot be null");
        long h1 = hash64(keyBytes, SEED_1);
        long h2 = hash64(keyBytes, SEED_2);
        if (h2 == 0L) {
            h2 = 0x9E3779B97F4A7C15L;
        }

        for (int i = 0; i < hashCount; i++) {
            long combined = h1 + (i * h2) + ((long) i * i);
            long bitIndex = Long.remainderUnsigned(combined, bitSize);
            int wordIndex = (int) (bitIndex >>> 6);
            long bitMask = 1L << (bitIndex & 63L);
            if ((words[wordIndex] & bitMask) == 0L) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merges another compatible BloomFilter into this filter via bitwise OR.
     */
    public void merge(BloomFilter other) {
        Objects.requireNonNull(other, "other cannot be null");
        if (this.words.length != other.words.length || this.hashCount != other.hashCount) {
            throw new IllegalArgumentException("Cannot merge BloomFilters with different dimensions or hashCount");
        }
        for (int i = 0; i < words.length; i++) {
            this.words[i] |= other.words[i];
        }
        this.insertedCount += other.insertedCount;
    }

    public long bitSize() {
        return bitSize;
    }

    public int wordCount() {
        return words.length;
    }

    public int hashCount() {
        return hashCount;
    }

    public long insertedCount() {
        return insertedCount;
    }

    /**
     * Counts the total number of set bits (1s) in the bit array.
     */
    public long setBitCount() {
        long count = 0L;
        for (long word : words) {
            count += Long.bitCount(word);
        }
        return count;
    }

    /**
     * Returns the fraction of bits currently set to 1 in [0.0, 1.0].
     */
    public double fillRatio() {
        return (double) setBitCount() / (double) bitSize;
    }

    /**
     * Estimates the current false-positive probability based on the bit fill ratio: {@code (fillRatio)^k}.
     */
    public double estimatedFalsePositiveRate() {
        return Math.pow(fillRatio(), hashCount);
    }

    /**
     * Returns the serialized size in bytes of this BloomFilter block.
     */
    public int serializedSizeBytes() {
        return HEADER_BYTES + (words.length * Long.BYTES) + TRAILER_CRC_BYTES;
    }

    /**
     * Encodes this BloomFilter into a byte array with header, bit array, and trailing IEEE 802.3 CRC32.
     */
    public byte[] encode() {
        int payloadSize = HEADER_BYTES + (words.length * Long.BYTES);
        ByteBuffer buffer = ByteBuffer.allocate(payloadSize + TRAILER_CRC_BYTES).order(ByteOrder.BIG_ENDIAN);

        buffer.putInt(BLOOM_MAGIC);
        buffer.putInt(FORMAT_VERSION);
        buffer.putInt(hashCount);
        buffer.putInt(words.length);
        buffer.putLong(insertedCount);

        for (long word : words) {
            buffer.putLong(word);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, payloadSize);
        buffer.putInt((int) crc32.getValue());

        return buffer.array();
    }

    /**
     * Decodes and validates a {@link BloomFilter} from a {@link ByteBuffer}.
     *
     * @param buffer buffer positioned at the start of the Bloom Filter Block
     * @return decoded {@link BloomFilter}
     * @throws StorageException if magic, version, bounds, or CRC32 validation fails
     */
    public static BloomFilter decode(ByteBuffer buffer) throws StorageException {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        buffer.order(ByteOrder.BIG_ENDIAN);

        int startPos = buffer.position();
        if (buffer.remaining() < HEADER_BYTES + Long.BYTES + TRAILER_CRC_BYTES) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Insufficient bytes for BloomFilter block: " + buffer.remaining());
        }

        int magic = buffer.getInt();
        if (magic != BLOOM_MAGIC) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("Invalid BloomFilter magic: 0x%08X (expected 0x%08X)", magic, BLOOM_MAGIC));
        }

        int version = buffer.getInt();
        if (version != FORMAT_VERSION) {
            throw new StorageException(ErrorCode.UNSUPPORTED_FORMAT,
                    "Unsupported BloomFilter format version: " + version);
        }

        int hashCount = buffer.getInt();
        if (hashCount <= 0 || hashCount > MAX_HASH_COUNT) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Invalid hashCount in BloomFilter block: " + hashCount);
        }

        int wordCount = buffer.getInt();
        if (wordCount <= 0 || wordCount > (64 * 1024 * 1024)) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Invalid wordCount in BloomFilter block: " + wordCount);
        }

        long insertedCount = buffer.getLong();
        if (insertedCount < 0) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Negative insertedCount in BloomFilter block: " + insertedCount);
        }

        long requiredWordsBytes = (long) wordCount * Long.BYTES;
        if (buffer.remaining() < requiredWordsBytes + TRAILER_CRC_BYTES) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    "Truncated BloomFilter bit array: needed " + (requiredWordsBytes + TRAILER_CRC_BYTES)
                            + " bytes, remaining " + buffer.remaining());
        }

        long[] words = new long[wordCount];
        for (int i = 0; i < wordCount; i++) {
            words[i] = buffer.getLong();
        }

        int payloadEndPos = buffer.position();
        int expectedCrc = buffer.getInt();
        int payloadLen = payloadEndPos - startPos;

        byte[] payloadBytes = new byte[payloadLen];
        int endPos = buffer.position();
        buffer.position(startPos);
        buffer.get(payloadBytes);
        buffer.position(endPos);

        CRC32 crc32 = new CRC32();
        crc32.update(payloadBytes);
        int actualCrc = (int) crc32.getValue();

        if (actualCrc != expectedCrc) {
            throw new StorageException(ErrorCode.CORRUPTION,
                    String.format("BloomFilter CRC32 mismatch: expected 0x%08X, actual 0x%08X", expectedCrc, actualCrc));
        }

        return new BloomFilter(words, hashCount, insertedCount);
    }

    /**
     * Computes a 64-bit MurmurHash3-inspired hash over {@code data} with the given {@code seed}.
     */
    static long hash64(byte[] data, long seed) {
        int len = data.length;
        long h = seed ^ (len * C1);

        int nblocks = len >>> 3;
        for (int i = 0; i < nblocks; i++) {
            int offset = i << 3;
            long k = ((long) data[offset] & 0xFFL)
                    | (((long) data[offset + 1] & 0xFFL) << 8)
                    | (((long) data[offset + 2] & 0xFFL) << 16)
                    | (((long) data[offset + 3] & 0xFFL) << 24)
                    | (((long) data[offset + 4] & 0xFFL) << 32)
                    | (((long) data[offset + 5] & 0xFFL) << 40)
                    | (((long) data[offset + 6] & 0xFFL) << 48)
                    | (((long) data[offset + 7] & 0xFFL) << 56);

            k *= C1;
            k = Long.rotateLeft(k, 31);
            k *= C2;

            h ^= k;
            h = Long.rotateLeft(h, 27);
            h = h * 5L + 0x52DCE729L;
        }

        long tail = 0L;
        int tailStart = nblocks << 3;
        switch (len & 7) {
            case 7:
                tail ^= ((long) data[tailStart + 6] & 0xFFL) << 48;
                // fallthrough
            case 6:
                tail ^= ((long) data[tailStart + 5] & 0xFFL) << 40;
                // fallthrough
            case 5:
                tail ^= ((long) data[tailStart + 4] & 0xFFL) << 32;
                // fallthrough
            case 4:
                tail ^= ((long) data[tailStart + 3] & 0xFFL) << 24;
                // fallthrough
            case 3:
                tail ^= ((long) data[tailStart + 2] & 0xFFL) << 16;
                // fallthrough
            case 2:
                tail ^= ((long) data[tailStart + 1] & 0xFFL) << 8;
                // fallthrough
            case 1:
                tail ^= ((long) data[tailStart] & 0xFFL);
                tail *= C1;
                tail = Long.rotateLeft(tail, 31);
                tail *= C2;
                h ^= tail;
                break;
            default:
                break;
        }

        h ^= len;
        return fmix64(h);
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xFF51AFD7ED558CCDL;
        k ^= k >>> 33;
        k *= 0xC4CEB9FE1A85EC53L;
        k ^= k >>> 33;
        return k;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BloomFilter other)) return false;
        return hashCount == other.hashCount
                && insertedCount == other.insertedCount
                && Arrays.equals(words, other.words);
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(hashCount);
        result = 31 * result + Long.hashCode(insertedCount);
        result = 31 * result + Arrays.hashCode(words);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "BloomFilter[bits=%d, k=%d, inserted=%d, fillRatio=%.3f, estFpp=%.5f]",
                bitSize, hashCount, insertedCount, fillRatio(), estimatedFalsePositiveRate());
    }
}
