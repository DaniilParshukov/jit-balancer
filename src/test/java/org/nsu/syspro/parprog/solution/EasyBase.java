package org.nsu.syspro.parprog.solution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.helpers.TestEnvironment;
import org.nsu.syspro.parprog.helpers.TestLevels;
import org.nsu.syspro.parprog.helpers.TestMethod;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class EasyBase extends TestLevels {

    @EnabledIf("easyEnabled")
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 100})
    @Timeout(2)
    void multithread_1_correctness(int N) throws InterruptedException {
        // `Multithread-1-correctness`: several threads request `UserThread.executeMethod(id)` and eventually all requests are finished

        final var env = testEnvironment();
        final var method = TestMethod.of();

        for (int i = 0; i < N; i++) {
            env.doUntilShutdownInSeparateUserThread(() -> env.checkedExec(method));
        }

        sleepSeconds(1);
        env.terminate(1);
    }

    @EnabledIf("easyEnabled")
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 100})
    @Timeout(2)
    void multithread_many_correctness(int N) throws InterruptedException {
        // Multithread-many-correctness`: several threads request `UserThread.executeMethod(id1,id2...idN)` and eventually all requests are finished

        final var env = testEnvironment();

        final ArrayList<MethodID> methods = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            methods.add(TestMethod.of());
        }

        for (int i = 0; i < N; i++) {
            env.doUntilShutdownInSeparateUserThread(() -> {
                final int idx = ThreadLocalRandom.current().nextInt(0, methods.size());
                env.checkedExec(methods.get(idx));
            });
        }

        sleepSeconds(1);
        env.terminate(1);
    }

    @EnabledIf("easyEnabled")
    @Test
    @Timeout(2)
    void recursion_correctness_2() throws InterruptedException {
        // `Recursion-correctness`: `ExecutionEngine.*(id1)` cause invocation of `UserThread.executeMethod(id2)`, everything eventually finishes

        final var env = testEnvironment();

        final int RECURSION_DEPTH = 100;
        TestMethod previous = TestMethod.of();
        for (int i = 0; i < RECURSION_DEPTH; i++) {
            final var capture = previous;
            previous = TestMethod.of(() -> env.checkedExec(capture));
        }

        final var upperLevelMethod = previous;
        env.startSeparateUserThread(
                () -> {
                    final int INVOKES = 10;
                    for (int i = 0; i < INVOKES; i++) {
                        env.checkedExec(upperLevelMethod);
                    }
                    assertEquals((RECURSION_DEPTH + 1) * INVOKES, env.counter(TestEnvironment.EventType.FINISHED_CHECKED_EXECUTIONS));
                }
        ).join();

        env.terminate(1);
    }
}
