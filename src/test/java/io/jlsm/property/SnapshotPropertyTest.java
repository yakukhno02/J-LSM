package io.jlsm.property;

import io.jlsm.api.ClosableIterator;
import io.jlsm.api.DatabaseOptions;
import io.jlsm.api.DurabilityMode;
import io.jlsm.api.Entry;
import io.jlsm.api.KVDatabase;
import io.jlsm.api.Snapshot;
import io.jlsm.oracle.TreeMapOracle;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotPropertyTest {

    public enum OpType { PUT, DELETE, SNAPSHOT, CLOSE_OLD_SNAPSHOT }

    public record Step(OpType type, byte[] key, byte[] value) {}

    @Provide
    public Arbitrary<Step> steps() {
        Arbitrary<byte[]> keys = Arbitraries.strings()
                .alpha()
                .ofMinLength(1)
                .ofMaxLength(8)
                .map(s -> s.getBytes(StandardCharsets.UTF_8));

        Arbitrary<byte[]> values = Arbitraries.strings()
                .alpha()
                .ofMinLength(1)
                .ofMaxLength(16)
                .map(s -> s.getBytes(StandardCharsets.UTF_8));

        Arbitrary<OpType> opTypes = Arbitraries.of(
                OpType.PUT, OpType.PUT, OpType.PUT,
                OpType.DELETE,
                OpType.SNAPSHOT,
                OpType.CLOSE_OLD_SNAPSHOT
        );

        return Combinators.combine(opTypes, keys, values).as(Step::new);
    }

    @Provide
    public Arbitrary<List<Step>> stepWorkloads() {
        return steps().list().ofMinSize(10).ofMaxSize(60);
    }

    private record OpenSnapshotRecord(Snapshot snapshot, TreeMapOracle expectedOracle) {}

    @Property(tries = 25)
    void p08SnapshotIsolationWithOracle(@ForAll("stepWorkloads") List<Step> workload) throws Exception {
        Path tempDir = Files.createTempDirectory("jlsm-snap-prop-");
        // Use a small memtable to ensure flushes and compactions happen during the workload
        DatabaseOptions options = new DatabaseOptions(
                DurabilityMode.BUFFERED,
                1024L,
                2,
                4,
                8L * 1024 * 1024,
                32L * 1024 * 1024,
                16
        );

        TreeMapOracle liveOracle = new TreeMapOracle();
        List<OpenSnapshotRecord> openSnapshots = new ArrayList<>();
        List<byte[]> keysSeen = new ArrayList<>();

        try (KVDatabase db = KVDatabase.open(tempDir, options)) {
            for (Step step : workload) {
                keysSeen.add(step.key());

                switch (step.type()) {
                    case PUT -> {
                        db.put(step.key(), step.value());
                        liveOracle.put(step.key(), step.value());
                    }
                    case DELETE -> {
                        db.delete(step.key());
                        liveOracle.delete(step.key());
                    }
                    case SNAPSHOT -> {
                        Snapshot snap = db.snapshot();
                        openSnapshots.add(new OpenSnapshotRecord(snap, liveOracle.snapshot()));
                    }
                    case CLOSE_OLD_SNAPSHOT -> {
                        if (!openSnapshots.isEmpty()) {
                            OpenSnapshotRecord old = openSnapshots.removeFirst();
                            old.snapshot().close();
                        }
                    }
                }

                // Verify all currently open snapshots still observe their exact point-in-time oracle
                for (OpenSnapshotRecord snapRec : openSnapshots) {
                    // Check point lookups
                    for (byte[] k : keysSeen) {
                        byte[] expected = snapRec.expectedOracle().get(k);
                        byte[] actual = snapRec.snapshot().get(k);
                        assertThat(actual).isEqualTo(expected);
                    }

                    // Check range scan
                    List<Entry> expectedScan = snapRec.expectedOracle().scan(null, null);
                    List<Entry> actualScan = new ArrayList<>();
                    try (ClosableIterator<Entry> it = snapRec.snapshot().scan(null, null)) {
                        while (it.hasNext()) {
                            actualScan.add(it.next());
                        }
                    }
                    assertThat(actualScan).hasSameSizeAs(expectedScan);
                    for (int i = 0; i < expectedScan.size(); i++) {
                        assertThat(actualScan.get(i).key()).isEqualTo(expectedScan.get(i).key());
                        assertThat(actualScan.get(i).value()).isEqualTo(expectedScan.get(i).value());
                    }
                }
            }

            // Clean up remaining open snapshots
            for (OpenSnapshotRecord snapRec : openSnapshots) {
                snapRec.snapshot().close();
            }
        }
    }
}
