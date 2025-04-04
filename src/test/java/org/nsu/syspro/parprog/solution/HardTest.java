package org.nsu.syspro.parprog.solution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.nsu.syspro.parprog.helpers.TestEnvironment;
import org.nsu.syspro.parprog.helpers.TestLevels;
import org.nsu.syspro.parprog.helpers.TestMethod;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HardTest extends TestLevels {
    @Override
    public TestEnvironment testEnvironment() {
        return new TestEnvironment(
                Duration.ofNanos(0),
                Duration.ofNanos(0),
                Duration.ofNanos(0),
                Duration.ofMillis(10),
                Duration.ofMillis(100)
        );
    }

    @EnabledIf("hardEnabled")
    @Test
    @Timeout(3)
    void thread_bound_compilation() throws InterruptedException {
        // `Thread-bound-compilation`: at any moment of time, number of concurrently executed `compile_l1`/`compile_l2` is limited by
        // `UserThread.compilationThreadBound` which is guaranteed to be `>= 2` and provided as `UserThread` constructor parameter

        final var env = testEnvironment();
        for (int i = 0; i < 10; i++) {
            env.startSeparateUserThread(() -> {
                final TestMethod method = TestMethod.of();
                for (int j = 0; j < 100_001; j++) {
                    env.checkedExec(method);
                }
            });
        }

        env.terminate(2);
    }
}
