package io.jlsm.fixtures;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FixtureGeneratorTest {

    @Test
    void shouldGenerateFixtures() throws IOException {
        Path fixturesDir = Paths.get("src/test/resources/fixtures");
        FixtureGenerator.writeFixturesTo(fixturesDir);

        assertTrue(fixturesDir.resolve("wal-minimal.bin").toFile().exists());
        assertTrue(fixturesDir.resolve("sstable-minimal.bin").toFile().exists());
    }
}
