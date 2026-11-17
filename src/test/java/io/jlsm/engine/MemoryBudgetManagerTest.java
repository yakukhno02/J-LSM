package io.jlsm.engine;

import io.jlsm.api.DatabaseOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemoryBudgetManagerTest {

    @Test
    @DisplayName("Acquire and release memory within budget thresholds")
    void testBasicAcquireAndRelease() throws Exception {
        MemoryBudgetManager manager = new MemoryBudgetManager(1000L);
        assertThat(manager.maxBudgetBytes()).isEqualTo(1000L);
        assertThat(manager.highWatermarkBytes()).isEqualTo(800L);
        assertThat(manager.lowWatermarkBytes()).isEqualTo(500L);

        manager.acquire(300L);
        assertThat(manager.allocatedBytes()).isEqualTo(300L);
        assertThat(manager.isUnderMemoryPressure()).isFalse();

        manager.acquire(300L);
        assertThat(manager.allocatedBytes()).isEqualTo(600L);
        assertThat(manager.isUnderMemoryPressure()).isFalse();

        manager.release(400L);
        assertThat(manager.allocatedBytes()).isEqualTo(200L);

        manager.release(500L); // releases more than allocated
        assertThat(manager.allocatedBytes()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Backpressure stalls writers when crossing high watermark until memory is released")
    void testBackpressureStallAndRelease() throws Exception {
        MemoryBudgetManager manager = new MemoryBudgetManager(1000L);
        manager.acquire(700L);

        CountDownLatch threadStarted = new CountDownLatch(1);
        CountDownLatch threadCompleted = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            try {
                threadStarted.countDown();
                // 700 + 200 = 900 >= 800 (high watermark) -> stalls!
                manager.acquire(200L);
                completed.set(true);
                threadCompleted.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        writer.start();

        assertThat(threadStarted.await(2, TimeUnit.SECONDS)).isTrue();
        // Give thread a moment to block
        Thread.sleep(100);
        assertThat(completed.get()).isFalse();

        // Release to drop below low watermark (500L): 900 - 500 = 400 <= 500
        manager.release(500L);

        assertThat(threadCompleted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(completed.get()).isTrue();
        assertThat(manager.backpressureWaitNanos()).isGreaterThan(0L);

        writer.join();
    }

    @Test
    @DisplayName("Effective budget calculates safe bounds under constrained heap")
    void testEffectiveBudgetCalculation() {
        DatabaseOptions defaults = DatabaseOptions.defaults();
        long budget = MemoryBudgetManager.calculateEffectiveBudget(defaults);
        assertThat(budget).isGreaterThan(0L);

        // Invalid budget throws
        assertThatThrownBy(() -> new MemoryBudgetManager(0L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
