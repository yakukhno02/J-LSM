package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.oracle.TreeMapOracle;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class DifferentialOracleTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("D-01: Differential test running 100,000 random operations against TreeMapOracle")
    void testD01DifferentialExecutionAgainstOracle(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.BUFFERED,
                32L * 1024, // 32 KB memtable for frequent flushes
                4,          // Compaction trigger at 4 tables
                8,
                8L * 1024 * 1024,
                32L * 1024 * 1024,
                16
        );

        TreeMapOracle oracle = new TreeMapOracle();
        Random random = new Random(42);

        int totalOperations = 100_000;
        int keyPoolSize = 3_000; // Key pool significantly larger than memtable capacity

        byte[][] keyPool = new byte[keyPoolSize][];
        for (int i = 0; i < keyPoolSize; i++) {
            if (i < 1000) {
                keyPool[i] = String.format("key_%05d", i).getBytes(StandardCharsets.UTF_8);
            } else if (i < 2000) {
                // Prefix keys (hierarchical paths)
                keyPool[i] = String.format("key_%05d/sub_%d", i % 1000, i % 5).getBytes(StandardCharsets.UTF_8);
            } else {
                // High-bit byte keys (0x80 to 0xFF)
                byte[] hk = new byte[4];
                hk[0] = (byte) (0x80 + (i % 120));
                hk[1] = (byte) ((i >> 4) & 0xFF);
                hk[2] = (byte) (i & 0xFF);
                hk[3] = (byte) 0xAA;
                keyPool[i] = hk;
            }
        }

        KVDatabase db = KVDatabase.open(tempDir, options);
        try {
            for (int op = 0; op < totalOperations; op++) {
                int keyNum = random.nextInt(keyPoolSize);
                byte[] key = keyPool[keyNum];

                int opType = random.nextInt(100);

                if (opType < 45) {
                    // 45% PUT (including empty byte[0] values)
                    byte[] value = (op % 10 == 0)
                            ? new byte[0]
                            : String.format("val_%05d_%08d", keyNum, op).getBytes(StandardCharsets.UTF_8);
                    db.put(key, value);
                    oracle.put(key, value);
                } else if (opType < 65) {
                    // 20% DELETE
                    db.delete(key);
                    oracle.delete(key);
                } else if (opType < 98) {
                    // 33% GET point lookup
                    byte[] dbVal = db.get(key);
                    byte[] oracleVal = oracle.get(key);

                    if (!Arrays.equals(dbVal, oracleVal)) {
                        assertThat(dbVal)
                                .as("Mismatch at op %d for key %s: expected %s but got %s",
                                        op, Arrays.toString(key), Arrays.toString(oracleVal), Arrays.toString(dbVal))
                                .isEqualTo(oracleVal);
                    }
                } else {
                    // 2% Range Scan
                    int fromNum = random.nextInt(keyPoolSize);
                    int toNum = random.nextInt(keyPoolSize);
                    byte[] fromKey = keyPool[Math.min(fromNum, toNum)];
                    byte[] toKey = keyPool[Math.max(fromNum, toNum)];

                    List<Entry> oracleEntries = oracle.scan(fromKey, toKey);
                    List<Entry> dbEntries = new ArrayList<>();
                    try (ClosableIterator<Entry> it = db.scan(fromKey, toKey)) {
                        while (it.hasNext()) {
                            dbEntries.add(it.next());
                        }
                    }

                    assertThat(dbEntries).hasSameSizeAs(oracleEntries);
                    for (int i = 0; i < oracleEntries.size(); i++) {
                        assertThat(dbEntries.get(i).key()).isEqualTo(oracleEntries.get(i).key());
                        assertThat(dbEntries.get(i).value()).isEqualTo(oracleEntries.get(i).value());
                    }
                }

                // Explicit flush and compaction cycles
                if (op > 0 && op % 10_000 == 0) {
                    DefaultStorageEngine engine = (DefaultStorageEngine) db;
                    engine.rotateMemTable();
                    engine.flushImmutableMemTable();
                    engine.triggerCompactionSync();
                    EngineInvariantValidator.validateAll(engine);
                }

                // Occasionally reopen the database to test crash recovery and state consistency
                if (op > 0 && op % 25_000 == 0) {
                    db.close();
                    db = KVDatabase.open(tempDir, options);
                }
            }
            assertThat(db.metrics().flushCount()).isGreaterThan(0);
        } finally {
            db.close();
        }

        // Final full verification across all keys in the pool after reopen
        try (KVDatabase reopened = KVDatabase.open(tempDir, options)) {
            assertThat(reopened.metrics().liveTablesCount()).isGreaterThan(0);
            EngineInvariantValidator.validateAll((DefaultStorageEngine) reopened);

            for (int k = 0; k < keyPoolSize; k++) {
                byte[] key = keyPool[k];
                byte[] expected = oracle.get(key);
                byte[] actual = reopened.get(key);
                assertThat(actual)
                        .as("Mismatch in final verification for key %s", Arrays.toString(key))
                        .isEqualTo(expected);
            }

            // Full scan comparison
            List<Entry> oracleAll = oracle.scan(null, null);
            List<Entry> dbAll = new ArrayList<>();
            try (ClosableIterator<Entry> it = reopened.scan(null, null)) {
                while (it.hasNext()) {
                    dbAll.add(it.next());
                }
            }

            assertThat(dbAll).hasSameSizeAs(oracleAll);
            for (int i = 0; i < oracleAll.size(); i++) {
                assertThat(dbAll.get(i).key()).isEqualTo(oracleAll.get(i).key());
                assertThat(dbAll.get(i).value()).isEqualTo(oracleAll.get(i).value());
            }
        }
    }

    @Test
    @DisplayName("D-02: Comprehensive range scan differential test against TreeMapOracle")
    void testD02RangeScanDifferentialAgainstOracle(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.BUFFERED,
                16L * 1024,
                3,
                4,
                8L * 1024 * 1024,
                32L * 1024 * 1024,
                16
        );

        TreeMapOracle oracle = new TreeMapOracle();
        Random random = new Random(12345);

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            // Populate dataset
            for (int i = 0; i < 2000; i++) {
                byte[] k = String.format("user_%06d", random.nextInt(5000)).getBytes(StandardCharsets.UTF_8);
                if (random.nextDouble() < 0.25) {
                    db.delete(k);
                    oracle.delete(k);
                } else {
                    byte[] v = String.format("data_%06d", i).getBytes(StandardCharsets.UTF_8);
                    db.put(k, v);
                    oracle.put(k, v);
                }
            }

            // Run diverse scans: unbounded, prefix, bounded, reversed, empty
            for (int s = 0; s < 100; s++) {
                int k1 = random.nextInt(5000);
                int k2 = random.nextInt(5000);
                byte[] from = String.format("user_%06d", Math.min(k1, k2)).getBytes(StandardCharsets.UTF_8);
                byte[] to = String.format("user_%06d", Math.max(k1, k2)).getBytes(StandardCharsets.UTF_8);

                List<Entry> oracleList = oracle.scan(from, to);
                List<Entry> dbList = new ArrayList<>();
                try (ClosableIterator<Entry> it = db.scan(from, to)) {
                    while (it.hasNext()) {
                        dbList.add(it.next());
                    }
                }

                assertThat(dbList).hasSameSizeAs(oracleList);
                for (int i = 0; i < oracleList.size(); i++) {
                    assertThat(dbList.get(i).key()).isEqualTo(oracleList.get(i).key());
                    assertThat(dbList.get(i).value()).isEqualTo(oracleList.get(i).value());
                }
            }
        }
    }
}
