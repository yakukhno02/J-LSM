package io.jlsm.fixtures;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class FixtureGeneratorTest {

    @Test
    @DisplayName("Golden fixture files match the independent generator output without rewriting")
    void testGoldenFixturesMatchGenerator() throws IOException {
        byte[] expectedWal = FixtureGenerator.generateMinimalWal(1L);
        try (InputStream in = getClass().getResourceAsStream("/fixtures/wal-minimal.bin")) {
            assertThat(in).isNotNull();
            assertThat(in.readAllBytes()).isEqualTo(expectedWal);
        }

        byte[] expectedSst = FixtureGenerator.generateMinimalSSTable(1L);
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sstable-minimal.bin")) {
            assertThat(in).isNotNull();
            assertThat(in.readAllBytes()).isEqualTo(expectedSst);
        }

        byte[] expectedManifest = FixtureGenerator.generateMinimalManifest();
        try (InputStream in = getClass().getResourceAsStream("/fixtures/manifest-minimal.bin")) {
            assertThat(in).isNotNull();
            assertThat(in.readAllBytes()).isEqualTo(expectedManifest);
        }
    }
}
