package org.nsu.syspro.parprog.solution;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.nsu.syspro.parprog.helpers.TestEnvironment;
import org.nsu.syspro.parprog.helpers.TestLevels;
import org.nsu.syspro.parprog.helpers.TestMethod;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MediumTest extends TestLevels {

    @Override
    public TestEnvironment testEnvironment() {
        return new TestEnvironment(
                Duration.ofNanos(0),
                Duration.ofNanos(0),
                Duration.ofNanos(0),
                Duration.ofNanos(0),
                Duration.ofNanos(0)
        );
    }

    @EnabledIf("mediumEnabled")
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 100})
    @Timeout(2)
    void eventual_per_thread_progress_2(int N) throws InterruptedException {
        // `Eventual-per-thread-progress-2`: if `UserThread.executeMethod(id)` was invoked `100_000` times by `Thread A` as `CompiledMethod(id)`,
        // at least one invocation inside this thread was using code produced by level 2 JIT.

        final var env = testEnvironment();
        final var method = TestMethod.of();

        env.startSeparateUserThread(() -> {
            for (int i = 0; i < 100_010; i++) {
                env.checkedExec(method);
            }
        }).join();

        assertTrue(env.counter(TestEnvironment.EventType.L2_EXECUTED) > 0);

        env.terminate(1);
    }
}
