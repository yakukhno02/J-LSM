package io.jlsm.property;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.WriteBatch;
import io.jlsm.internal.model.ByteKey;
import io.jlsm.oracle.TreeMapOracle;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WriteBatchPropertyTest {

    public record BatchItem(boolean isDelete, byte[] key, byte[] value) {}

    @Provide
    public Arbitrary<BatchItem> batchItems() {
        Arbitrary<byte[]> keys = Arbitraries.strings()
                .alpha()
                .ofMinLength(1)
                .ofMaxLength(16)
                .map(s -> s.getBytes(StandardCharsets.UTF_8));

        Arbitrary<byte[]> values = Arbitraries.strings()
                .alpha()
                .ofMinLength(0)
                .ofMaxLength(32)
                .map(s -> s.getBytes(StandardCharsets.UTF_8));

        Arbitrary<Boolean> isDelete = Arbitraries.of(true, false);

        return Combinators.combine(isDelete, keys, values).as(BatchItem::new);
    }

    @Provide
    public Arbitrary<List<List<BatchItem>>> batchWorkloads() {
        return batchItems()
                .list()
                .ofMinSize(1)
                .ofMaxSize(15)
                .list()
                .ofMinSize(1)
                .ofMaxSize(8);
    }

    @Property(tries = 25)
    void p06BatchExecutionEquivalenceWithOracle(@ForAll("batchWorkloads") List<List<BatchItem>> workload) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-batch-prop-");
        DatabaseOptions options = DatabaseOptions.defaults().withDurabilityMode(DurabilityMode.BUFFERED);
        TreeMapOracle oracle = new TreeMapOracle();

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            List<byte[]> allKeys = new ArrayList<>();

            for (List<BatchItem> batchData : workload) {
                try (WriteBatch batch = WriteBatch.create()) {
                    for (BatchItem item : batchData) {
                        allKeys.add(item.key());
                        if (item.isDelete()) {
                            batch.delete(item.key());
                            oracle.delete(item.key());
                        } else {
                            batch.put(item.key(), item.value());
                            oracle.put(item.key(), item.value());
                        }
                    }
                    db.write(batch);
                }
            }

            for (byte[] key : allKeys) {
                byte[] expected = oracle.get(key);
                byte[] actual = db.get(key);
                assertThat(actual).isEqualTo(expected);
            }

            List<Entry> oracleEntries = oracle.scan(null, null);
            List<Entry> actualEntries = new ArrayList<>();
            try (ClosableIterator<Entry> it = db.scan(null, null)) {
                while (it.hasNext()) {
                    actualEntries.add(it.next());
                }
            }

            assertThat(actualEntries.size()).isEqualTo(oracleEntries.size());
            for (int i = 0; i < oracleEntries.size(); i++) {
                assertThat(actualEntries.get(i).key()).isEqualTo(oracleEntries.get(i).key());
                assertThat(actualEntries.get(i).value()).isEqualTo(oracleEntries.get(i).value());
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Property(tries = 25)
    void p06BatchInternalOrderPreservation(@ForAll("batchItems") BatchItem item1, @ForAll("batchItems") BatchItem item2) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-batch-order-");
        DatabaseOptions options = DatabaseOptions.defaults();

        byte[] sharedKey = "shared_key".getBytes(StandardCharsets.UTF_8);

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            try (WriteBatch batch = WriteBatch.create()) {
                if (item1.isDelete()) {
                    batch.delete(sharedKey);
                } else {
                    batch.put(sharedKey, item1.value());
                }

                if (item2.isDelete()) {
                    batch.delete(sharedKey);
                } else {
                    batch.put(sharedKey, item2.value());
                }

                db.write(batch);
            }

            byte[] expected = item2.isDelete() ? null : item2.value();
            assertThat(db.get(sharedKey)).isEqualTo(expected);
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Property(tries = 20)
    void p06BatchAllOrNothingOnValidationFailure(@ForAll("batchItems") BatchItem validItem) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-batch-fail-");
        DatabaseOptions options = DatabaseOptions.defaults();

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            byte[] baselineKey = "baseline_k".getBytes(StandardCharsets.UTF_8);
            byte[] baselineVal = "baseline_v".getBytes(StandardCharsets.UTF_8);
            db.put(baselineKey, baselineVal);

            try (WriteBatch batch = WriteBatch.create()) {
                batch.put(validItem.key(), validItem.value());
                assertThatThrownBy(() -> batch.put(new byte[0], bytes("val")))
                        .isInstanceOf(IllegalArgumentException.class);
            }

            assertThat(db.get(baselineKey)).isEqualTo(baselineVal);
            if (!Arrays.equals(validItem.key(), baselineKey)) {
                assertThat(db.get(validItem.key())).isNull();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void deleteRecursively(Path path) {
        try {
            if (Files.exists(path)) {
                try (var stream = Files.walk(path)) {
                    stream.sorted((a, b) -> b.compareTo(a))
                            .forEach(p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException ignored) {}
                            });
                }
            }
        } catch (Exception ignored) {}
    }
}
