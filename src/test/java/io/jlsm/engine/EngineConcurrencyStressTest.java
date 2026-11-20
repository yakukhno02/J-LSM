package io.jlsm.engine;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class EngineConcurrencyStressTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Stress: concurrent put, get, scan, and background compaction with start barrier")
    void testConcurrentReadWriteCompactionStress(@TempDir Path tempDir) throws Exception {
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.BUFFERED,
                4096L, // small memtable to trigger frequent flushes
                2,     // compaction trigger of 2 tables
                100,
                32L * 1024 * 1024,
                8L * 1024 * 1024,
                16
        );

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            DefaultStorageEngine engine = (DefaultStorageEngine) db;
            int numWriters = 2;
            int numReaders = 2;
            int numScanners = 1;
            int numCompactors = 1;
            int totalThreads = numWriters + numReaders + numScanners + numCompactors;

            CyclicBarrier barrier = new CyclicBarrier(totalThreads);
            AtomicBoolean stopFlag = new AtomicBoolean(false);
            ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
            List<Future<Void>> futures = new ArrayList<>();

            int keyPoolSize = 50;

            // 1. Writer threads
            for (int w = 0; w < numWriters; w++) {
                final int writerId = w;
                futures.add(executor.submit(() -> {
                    barrier.await();
                    int iteration = 0;
                    while (!stopFlag.get()) {
                        int keyNum = (iteration + writerId) % keyPoolSize;
                        byte[] key = bytes(String.format("key_%04d", keyNum));
                        if (iteration % 7 == 0) {
                            db.delete(key);
                        } else {
                            byte[] val = bytes(String.format("val_w%d_iter_%06d", writerId, iteration));
                            db.put(key, val);
                        }
                        iteration++;
                        if ((iteration & 63) == 0) {
                            Thread.yield();
                        }
                    }
                    return null;
                }));
            }

            // 2. Reader threads
            for (int r = 0; r < numReaders; r++) {
                futures.add(executor.submit(() -> {
                    barrier.await();
                    int iteration = 0;
                    while (!stopFlag.get()) {
                        int keyNum = iteration % keyPoolSize;
                        byte[] key = bytes(String.format("key_%04d", keyNum));
                        byte[] val = db.get(key);
                        if (val != null) {
                            String s = asString(val);
                            assertThat(s).startsWith("val_w");
                        }
                        iteration++;
                    }
                    return null;
                }));
            }

            // 3. Scanner thread
            for (int s = 0; s < numScanners; s++) {
                futures.add(executor.submit(() -> {
                    barrier.await();
                    while (!stopFlag.get()) {
                        try (ClosableIterator<Entry> it = db.scan(null, null)) {
                            byte[] prevKey = null;
                            while (it.hasNext()) {
                                Entry entry = it.next();
                                assertThat(entry.key()).isNotNull();
                                assertThat(entry.value()).isNotNull();
                                if (prevKey != null) {
                                    assertThat(Arrays.compareUnsigned(prevKey, entry.key())).isLessThan(0);
                                }
                                prevKey = entry.key();
                            }
                        }
                        Thread.sleep(10);
                    }
                    return null;
                }));
            }

            // 4. Compactor / Flusher thread
            futures.add(executor.submit(() -> {
                barrier.await();
                while (!stopFlag.get()) {
                    try {
                        engine.rotateMemTable();
                        engine.flushImmutableMemTable();
                    } catch (Exception ignored) {
                    }
                    try {
                        engine.triggerCompactionSync();
                    } catch (Exception ignored) {
                    }
                    Thread.sleep(20);
                }
                return null;
            }));

            // Let threads run concurrently for 3 seconds
            Thread.sleep(3000);
            stopFlag.set(true);

            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

            for (Future<Void> f : futures) {
                f.get(); // Propagate any assertions or unexpected exceptions
            }

            // Global invariants check after concurrent stress
            EngineInvariantValidator.validateAll(engine);
        }
    }
}
