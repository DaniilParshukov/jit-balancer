package org.nsu.syspro.parprog.solution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.helpers.TestEnvironment;
import org.nsu.syspro.parprog.helpers.TestMethod;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class EasyUltraFastTest extends EasyBase {
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
    @Test
    @Timeout(2)
    void eventual_per_thread_progress_1() throws InterruptedException {
        // `Eventual-per-thread-progress-1`: if `UserThread.executeMethod(id)` was invoked `10_000` times by `Thread A`, at least one invocation
        // inside this thread was using code produced level 1 or level 2 JIT.

        final var env = testEnvironment();
        final var method = TestMethod.of();

        env.startSeparateUserThread(() -> {
            for (int i = 0; i < 10_010; i++) {
                env.checkedExec(method);
            }
        }).join();

        assertTrue((env.counter(TestEnvironment.EventType.L1_EXECUTED) + env.counter(TestEnvironment.EventType.L2_EXECUTED)) > 0);
        env.terminate(1);
    }

    @Test
    @EnabledIf("easyEnabled")
    @Timeout(2)
    void weak_global_caching() throws InterruptedException {
        // `Weak-global-caching`: denote `t1` the moment of time when `Thread A` finished `UserThread.executeMethod(id)`
        // produced by level `x` JIT. 80% of all invocations (in all threads) of this method that start after `t1` are using same or higher level of optimization.

        final var env = testEnvironment();
        final var method = TestMethod.of();

        env.startSeparateUserThread(() -> {
            for (int j = 0; j < 10_001; j++) {
                env.checkedExec(method);
            }
        }).join();

        // by now, method must have been compiled and successfully executed (because of `Eventual-per-thread-progress-1`)

        for (int i = 0; i < 100; i++) {
            env.startSeparateUserThread(
                    () -> {
                        final long INVOCATIONS = 100;
                        final long compiled_pre = env.counter(TestEnvironment.EventType.L1_EXECUTED) + env.counter(TestEnvironment.EventType.L2_EXECUTED);
                        for (int j = 0; j < INVOCATIONS; j++) {
                            // at least 80% of executions must be non-interpreted
                            env.checkedExec(method);
                        }
                        final long compiled_post = env.counter(TestEnvironment.EventType.L1_EXECUTED) + env.counter(TestEnvironment.EventType.L2_EXECUTED);

                        final long delta = compiled_post - compiled_pre;
                        final long expect = (INVOCATIONS * 80) / 100;
                        if (delta < expect) {
                            fail("Thread " + UserThread.current().id + " performed " + delta + " executions out of " + INVOCATIONS + " in non-interpreted mode, expected at least " + expect);
                        }
                    }
            );
        }

        env.terminate(1);
    }

    @EnabledIf("easyEnabled")
    @Test
    @Timeout(2)
    void recursion_correctness_1() throws InterruptedException {
        // `Recursion-correctness`: `ExecutionEngine.*(id1)` cause invocation of `UserThread.executeMethod(id2)`, everything eventually finishes

        final var env = testEnvironment();

        final TestMethod id2 = TestMethod.of();
        final TestMethod id1 = TestMethod.of(() -> env.checkedExec(id2));

        env.startSeparateUserThread(
                () -> {
                    final int INVOKES = 10_001;
                    for (int i = 0; i < INVOKES; i++) {
                        env.checkedExec(id1);
                    }
                    assertEquals(2 * INVOKES, env.counter(TestEnvironment.EventType.FINISHED_CHECKED_EXECUTIONS));

                    // id1 and id2 were executed at least 10_000, they should have been compiled because of `weak-global-caching`
                    // all id2 invocations should go to compiled code because of `per-thread-monotonicity`

                    final int ID2_INVOCATIONS = 1_000;

                    final long compiled_pre = env.counter(TestEnvironment.EventType.L1_EXECUTED) + env.counter(TestEnvironment.EventType.L2_EXECUTED);
                    for (int i = 0; i < ID2_INVOCATIONS; i++) {
                        env.checkedExec(id2);
                    }
                    final long compiled_post = env.counter(TestEnvironment.EventType.L1_EXECUTED) + env.counter(TestEnvironment.EventType.L2_EXECUTED);

                    assertEquals(ID2_INVOCATIONS, compiled_post - compiled_pre);
                }
        ).join();

        env.terminate(1);
    }
}
