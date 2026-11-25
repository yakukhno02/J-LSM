package io.jlsm.property;

import io.jlsm.internal.io.StandardStorageIO;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.internal.model.InternalRecord;
import io.jlsm.internal.model.RecordCursor;
import io.jlsm.internal.model.RecordKind;
import io.jlsm.manifest.TableMeta;
import io.jlsm.sst.BloomFilter;
import io.jlsm.sst.DefaultSSTableFlusher;
import io.jlsm.sst.DefaultTableHandle;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-Based Tests for {@link BloomFilter} (Property P-07 from Section 6.2 of DESIGN.md):
 * Verifies that for any arbitrary set of binary keys inserted into a BloomFilter or flushed into
 * an SSTable, the BloomFilter exhibits 0% false negatives before and after binary serialization.
 */
public class BloomFilterPropertyTest {

    @Provide
    public Arbitrary<List<ByteKey>> uniqueByteKeys() {
        Arbitrary<byte[]> rawKeyArb = Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(128);
        return rawKeyArb.list().ofMinSize(1).ofMaxSize(200).map(rawList -> {
            TreeSet<ByteKey> sortedSet = new TreeSet<>();
            for (byte[] bytes : rawList) {
                sortedSet.add(ByteKey.copyOf(bytes));
            }
            return new ArrayList<>(sortedSet);
        });
    }

    /**
     * P-07 (Bloom Filter No False Negatives — In-Memory & Serialized):
     * Every key added to a BloomFilter must always return {@code true} from {@link BloomFilter#mightContain(ByteKey)},
     * both in-memory and after binary encode/decode roundtrip.
     */
    @Property(tries = 50)
    public void p07_bloomFilterNeverProducesFalseNegatives(@ForAll("uniqueByteKeys") List<ByteKey> keys) throws Exception {
        BloomFilter filter = new BloomFilter(keys.size(), BloomFilter.DEFAULT_BITS_PER_KEY, BloomFilter.DEFAULT_HASH_COUNT);

        for (ByteKey key : keys) {
            filter.add(key);
        }

        byte[] encoded = filter.encode();
        BloomFilter decoded = BloomFilter.decode(ByteBuffer.wrap(encoded));

        for (ByteKey key : keys) {
            assertThat(filter.mightContain(key))
                    .as("In-memory BloomFilter must return true for inserted key %s", key)
                    .isTrue();
            assertThat(decoded.mightContain(key))
                    .as("Decoded BloomFilter must return true for inserted key %s", key)
                    .isTrue();
        }
    }

    /**
     * P-07b (SSTable Embedded Bloom Filter No False Negatives):
     * Every key written to an SSTable via {@link DefaultSSTableFlusher} must pass {@link DefaultTableHandle#mightContain(ByteKey)}
     * and be retrievable via {@link DefaultTableHandle#get(ByteKey)}.
     */
    @Property(tries = 35)
    public void p07_sstableEmbeddedBloomFilterContainsAllFlushedKeys(@ForAll("uniqueByteKeys") List<ByteKey> keys) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-p07-");
        try {
            StandardStorageIO storageIO = new StandardStorageIO();
            DefaultSSTableFlusher flusher = new DefaultSSTableFlusher(tempDir, storageIO);

            List<InternalRecord> records = new ArrayList<>(keys.size());
            long seq = 1L;
            for (ByteKey k : keys) {
                records.add(InternalRecord.of(k, k.bytes(), (seq % 4 == 0) ? RecordKind.DELETE : RecordKind.PUT, seq++));
            }

            TableMeta meta = flusher.build(1L, new ListCursor(records));
            try (DefaultTableHandle handle = DefaultTableHandle.open(storageIO, flusher.sstPath(1L), meta)) {
                assertThat(handle.bloomFilter()).isNotNull();
                assertThat(handle.bloomFilter().insertedCount()).isEqualTo(records.size());

                for (InternalRecord rec : records) {
                    assertThat(handle.mightContain(rec.key())).isTrue();
                    assertThat(handle.get(rec.key())).contains(rec);
                }
            }
        } finally {
            deleteDirectoryQuietly(tempDir);
        }
    }

    private static void deleteDirectoryQuietly(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
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
