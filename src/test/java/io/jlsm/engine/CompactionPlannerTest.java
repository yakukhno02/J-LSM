package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import io.jlsm.sst.TableHandle;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompactionPlannerTest {

    @Test
    @DisplayName("Evaluate returns correct plan for empty or small table lists")
    void testSmallTableLists() {
        DatabaseOptions options = DatabaseOptions.defaults();

        CompactionPlanner.CompactionPlan plan0 = CompactionPlanner.evaluate(List.of(), options);
        assertThat(plan0.shouldCompact()).isFalse();
        assertThat(plan0.urgency()).isEqualTo(CompactionPlanner.Urgency.NONE);

        assertThatThrownBy(() -> CompactionPlanner.evaluate(null, options))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> CompactionPlanner.evaluate(List.of(), null))
                .isInstanceOf(NullPointerException.class);
    }
}
