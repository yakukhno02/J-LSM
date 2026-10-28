package io.jlsm.manifest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TableMetaTest {

    @Test
    @DisplayName("TableMeta preserves metadata fields")
    void testTableMetaFields() {
        TableMeta meta = new TableMeta(10L, 100L, 200L, 50L, 4096L);

        assertThat(meta.tableId()).isEqualTo(10L);
        assertThat(meta.minSeq()).isEqualTo(100L);
        assertThat(meta.maxSeq()).isEqualTo(200L);
        assertThat(meta.recordCount()).isEqualTo(50L);
        assertThat(meta.fileSize()).isEqualTo(4096L);
    }
}
