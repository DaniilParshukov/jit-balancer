package org.nsu.syspro.parprog.helpers;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;
import org.nsu.syspro.parprog.solution.EasyFastTest;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class TestEnvironment {

    public enum EventType {
        STARTED_CHECKED_EXECUTIONS,
        FINISHED_CHECKED_EXECUTIONS,

        STARTED_TASKS,
        FINISHED_TASKS,

        INTERPRETED,
        L1_EXECUTED,
        L2_EXECUTED,

        L1_COMPILATION_START,
        L1_COMPILATION_END,

        L2_COMPILATION_START,
        L2_COMPILATION_END,

        EXECUTED_LOWER_OPT_LEVEL_THAN_GLOBALLY_AVAILABLE,
    }

    private final AtomicLong[] counters;

    private final TestExecutionEngine engine;
    private final TestCompilationEngine compiler;
    private final TestExecutor taskExecutor;
    private final ScheduledExecutorService utilityPool;

    private final long idOnStart = UserThread.firstUnusedThreadNum();

    public TestEnvironment(Duration interpret, Duration l1Exec, Duration l2Exec, Duration l1comp, Duration l2comp) {
        engine = new TestExecutionEngine(interpret, l1Exec, l2Exec);
        compiler = new TestCompilationEngine(l1comp, l2comp);
        taskExecutor = new TestExecutor();
        utilityPool = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        counters = new AtomicLong[EventType.values().length];
        for (int i = 0; i < counters.length; i++) {
            counters[i] = new AtomicLong(0);
        }
    }

    private void inc(EventType type) {
        counters[type.ordinal()].addAndGet(1);
    }

    public long counter(EventType type) {
        return counters[type.ordinal()].get();
    }

    private static final ThreadLocal<Map<Long, Integer>> perThreadJITLevels = ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Map<Long, Integer>> perThreadInvokes = ThreadLocal.withInitial(HashMap::new);
    private final ConcurrentHashMap<Long, Integer> globalJITLevels = new ConcurrentHashMap<>();

    public void checkedExec(MethodID id) {
        final UserThread thread = UserThread.current();
        final long methodId = id.id();

        final int globalLevelBefore = globalJITLevels.compute(methodId, (x, y) -> Objects.requireNonNullElse(y, 0));

        inc(EventType.STARTED_CHECKED_EXECUTIONS);
        final ExecutionResult result;
        try {
            result = thread.executeMethod(id);
        } finally {
            inc(EventType.FINISHED_CHECKED_EXECUTIONS);
        }

        if (result == null) {
            fail("Execution of MethodId(" + methodId + ") returned unexpected null");
        }

        assertInstanceOf(Finished.class, result);
        final var r = (Finished) result;
        // `Same-method-correctness`: `UserThread.executeMethod(id)` executes specified method
        if (r.id.id() != methodId) {
            fail("Same-method-correctness: asked to execute MethodId(" + methodId + "), really executed MethodId(" + r.id.id() + ")");
        }

        // `Same-thread-correctness`: `UserThread.executeMethod(id)` executes specified method in the same thread
        if (r.executor != thread) {
            fail("Same-thread-correctness: asked to execute in " + thread + ", really executed by " + r.executor);
        }

        final var levels = perThreadJITLevels.get();
        final var invokes = perThreadInvokes.get();

        final long localMaxLevel = levels.getOrDefault(methodId, 0);
        final int currentLevel =
                (result instanceof Finished.L2Executed)
                        ? 2
                        : (result instanceof Finished.L1Executed
                        ? 1
                        : 0);

        globalJITLevels.computeIfPresent(methodId, (x, y) -> Math.max(y, currentLevel));

        // `Per-thread-monotonicity`: if `Thread A` executed `CompiledMethod(id)` which was produced by JIT level `x` then all
        // executions of the same method in the same thread will execute same or higher level of optimization.
        if (currentLevel > localMaxLevel) {
            levels.put(methodId, currentLevel);
        } else if (currentLevel < localMaxLevel) {
            fail("Per-thread-monotonicity: MethodID(" + methodId + "): currentLevel = " + currentLevel + ", localMaxLevel = " + localMaxLevel);
        }

        if (currentLevel < globalLevelBefore) {
            inc(EventType.EXECUTED_LOWER_OPT_LEVEL_THAN_GLOBALLY_AVAILABLE);
        }

        final Integer cnt = invokes.getOrDefault(methodId, 0);
        invokes.put(methodId, cnt + 1);

        // `Eventual-per-thread-progress-1`: if `UserThread.executeMethod(id)` was invoked `10_000` times by `Thread A`, at least one invocation
        // inside this thread was using code produced level 1 or level 2 JIT.
        if (cnt >= 10_000) {
            if (localMaxLevel == 0) {
                fail("Eventual-per-thread-progress-1: thread " + thread + " interpreted MethodId(" + methodId + ") " + cnt + " times without compiling it");
            }
        }

        // `Eventual-per-thread-progress-2`: if `UserThread.executeMethod(id)` was invoked `100_000` times by `Thread A` as `CompiledMethod(id)`,
        // at least one invocation inside this thread was using code produced by level 2 JIT.
        if (TestLevels.mediumEnabled() && (cnt >= 100_000)) {
            if (localMaxLevel < 2) {
                fail("Eventual-per-thread-progress-2: thread " + thread + " interpreted/l1 executed MethodId(" + methodId + ") " + cnt + " times without compiling it by L2 JIT");
            }
        }
    }

    public void doUntilShutdownInSeparateUserThread(Runnable iteration) {
        utilityPool.submit(() ->
                taskExecutor.execute(
                        () -> {
                            while (!utilityPool.isShutdown()) {
                                iteration.run();
                            }
                        }
                )
        );
    }

    public UserThread startSeparateUserThread(Runnable runnable) throws InterruptedException {
        try {
            return utilityPool.submit(() -> taskExecutor.execute(runnable)).get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean awaitTerminationSeconds(int s) throws InterruptedException {
        final boolean success = utilityPool.awaitTermination(s, TimeUnit.SECONDS);
        if (!success) {
            return false;
        }
        return taskExecutor.awaitTerminationSeconds(s);
    }

    public void terminate(int seconds) throws InterruptedException {
        utilityPool.shutdown();

        final boolean terminated = awaitTerminationSeconds(seconds);
        // no deadlock/hang
        assertTrue(terminated);

        // all executions started by harness successfully finished
        assertEquals(counter(EventType.STARTED_CHECKED_EXECUTIONS), counter(EventType.FINISHED_CHECKED_EXECUTIONS));

        // all tasks started by test harness were wrapped in checkedExec
        assertEquals(counter(EventType.STARTED_CHECKED_EXECUTIONS), counter(EventType.STARTED_TASKS));

        // there were no additional executions triggered by Solution code
        // `No-side-effect-correctness`: `UserThread.executeMethod(id)` does not triggered execution of other unexpected methods
        final long finished = counter(EventType.FINISHED_TASKS);
        assertEquals(counter(EventType.FINISHED_CHECKED_EXECUTIONS), finished);

        final long missedOpportunities = counter(EventType.EXECUTED_LOWER_OPT_LEVEL_THAN_GLOBALLY_AVAILABLE);

        // `Weak-global-caching`: only 20% of finishedTasks allowed to be missed opportunities
        final long fraction = (finished * 20) / 100;
        if (missedOpportunities > fraction) {
            System.out.println(report());
            fail("Weak-global-caching: total executed methods = " + finished + ", methods executed with lower optimization level than globally available = " + missedOpportunities);
        }

        // print some statistics for better understanding of system behaviour
        System.out.println(report());
    }

    private String u(String x, EventType tpe) {
        return x + " = " + counter(tpe) + "\n";
    }

    private String report() {
        final long idOnReport = UserThread.firstUnusedThreadNum();
        final long delta = idOnReport - idOnStart;
        return "UserThreads.estimate = " + delta + "\n" +
                u("startedTasks", EventType.STARTED_TASKS) +
                u("finishedTasks", EventType.FINISHED_TASKS) +
                u("interpretedTasks", EventType.INTERPRETED) +
                u("l1Tasks", EventType.L1_EXECUTED) +
                u("l2Tasks", EventType.L2_EXECUTED) +
                u("l1Compilations_start", EventType.L1_COMPILATION_START) +
                u("l1Compilations_end", EventType.L1_COMPILATION_END) +
                u("l2Compilations_start", EventType.L2_COMPILATION_START) +
                u("l2Compilations_end", EventType.L2_COMPILATION_END);
    }

    private class TestExecutionEngine implements ExecutionEngine {
        private final Duration interpret;
        private final Duration l1;
        private final Duration l2;

        TestExecutionEngine(Duration interpret, Duration l1, Duration l2) {
            this.interpret = interpret;
            this.l1 = l1;
            this.l2 = l2;
        }

        @Override
        public ExecutionResult interpret(MethodID id) {
            assertInstanceOf(TestMethod.class, id);
            return interpretImpl((TestMethod) id);
        }

        public ExecutionResult interpretImpl(TestMethod id) {
            inc(EventType.STARTED_TASKS);
            try {
                id.invokePayload();
                TestLevels.sleepNanos(interpret.toNanos());
            } finally {
                inc(EventType.INTERPRETED);
                inc(EventType.FINISHED_TASKS);
            }
            return new Finished.Interpreted(id, UserThread.current());
        }

        @Override
        public ExecutionResult execute(CompiledMethod method) {
            assertInstanceOf(Compiled.class, method);
            return executeImpl((Compiled) method);
        }

        public ExecutionResult executeImpl(Compiled method) {
            inc(EventType.STARTED_TASKS);
            if (method instanceof Compiled.L1) {
                try {
                    method.id().invokePayload();
                    TestLevels.sleepNanos(l1.toNanos());
                } finally {
                    inc(EventType.L1_EXECUTED);
                    inc(EventType.FINISHED_TASKS);
                }
                return ((Compiled.L1) method).exec();
            }
            if (method instanceof Compiled.L2) {
                try {
                    method.id().invokePayload();
                    TestLevels.sleepNanos(l2.toNanos());
                } finally {
                    inc(EventType.L2_EXECUTED);
                    inc(EventType.FINISHED_TASKS);
                }
                return ((Compiled.L2) method).exec();
            }
            throw new IllegalStateException(method.toString());
        }
    }

    private class TestCompilationEngine implements CompilationEngine {
        private final Duration l1;
        private final Duration l2;

        private final AtomicLong concurrentCompilations = new AtomicLong(0);

        private final Map<Long, Integer> l1Compilations = new ConcurrentHashMap<>();
        private final Map<Long, Integer> l2Compilations = new ConcurrentHashMap<>();

        private TestCompilationEngine(Duration l1, Duration l2) {
            this.l1 = l1;
            this.l2 = l2;
        }

        private void startCompilation(int level, TestMethod method) {
            assert 1 <= level && level <= 2;
            final long active = concurrentCompilations.addAndGet(1);
            testConcurrentCompilations(active, method);

            final var map = (level == 2)
                    ? l2Compilations
                    : l1Compilations;

            // `CPU-bound-compilation`: any method `id` was `compile_l1(id)` no more than twice, any method `id` was `compile_l2(id)` no more than once
            final int totalCompilations = map.compute(method.id(), (k, v) -> Objects.requireNonNullElse(v, 0) + 1);
            if (TestLevels.hardEnabled()) {
                if (level == 1 && totalCompilations > 2) {
                    fail(Thread.currentThread() + " tried to compile MethodId(" + method.id() + ") with level 1 JIT at " + totalCompilations + "-th time");
                }

                if (level == 2 && totalCompilations > 1) {
                    fail(Thread.currentThread() + " tried to compile MethodId(" + method.id() + ") with level 2 JIT at " + totalCompilations + "-th time");
                }
            }
        }

        private void endCompilation(int level, TestMethod method) {
            concurrentCompilations.addAndGet(-1);
        }

        private void testConcurrentCompilations(long active, TestMethod method) {
            if (TestLevels.hardEnabled() && active > TestLevels.compilationThreadBound()) {
                // `Thread-bound-compilation`: at any moment of time, number of concurrently executed `compile_l1`/`compile_l2` is limited by
                // `N` which is guaranteed to be `>= 2`
                fail("Compilation of MethodID(" + method.id() + ") was " + active + "-th, but limit is only " + TestLevels.compilationThreadBound());
            }
        }

        @Override
        public CompiledMethod compile_l1(MethodID method) {
            assertInstanceOf(TestMethod.class, method);
            return compile_l1_impl((TestMethod) method);
        }

        public CompiledMethod compile_l1_impl(TestMethod method) {
            startCompilation(1, method);
            inc(EventType.L1_COMPILATION_START);
            try {
                TestLevels.sleepNanos(l1.toNanos());
            } finally {
                endCompilation(1, method);
                inc(EventType.L1_COMPILATION_END);
            }
            return new Compiled.L1(method);
        }

        @Override
        public CompiledMethod compile_l2(MethodID method) {
            assertInstanceOf(TestMethod.class, method);
            return compile_l2_impl((TestMethod) method);
        }

        public CompiledMethod compile_l2_impl(TestMethod method) {
            if (TestLevels.mediumEnabled()) {
                //  `Heavy-compilation-offloading`: there is no `UserThread` that ever execute `compile_l2`
                if (Thread.currentThread() instanceof UserThread) {
                    final UserThread t = (UserThread) Thread.currentThread();
                    fail("UserThread (" + t.id + ") tried to compile MethodId(" + method.id() + ") by L2 JIT");
                }
            }

            startCompilation(2, method);
            inc(EventType.L2_COMPILATION_START);
            try {
                TestLevels.sleepNanos(l2.toNanos());
            } finally {
                endCompilation(2, method);
                inc(EventType.L2_COMPILATION_END);
            }
            return new Compiled.L2(method);
        }
    }

    private final class TestExecutor {
        private final Set<UserThread> running = new HashSet<>();

        public UserThread execute(Runnable command) {
            final UserThread thread = EasyFastTest.createUserThread(engine, compiler, () -> {
                command.run();

                synchronized (running) {
                    final boolean wasRegistered = running.remove(UserThread.current());
                    assertTrue(wasRegistered);

                    if (running.isEmpty()) {
                        running.notify();
                    }
                }
            });

            synchronized (running) {
                assert !utilityPool.isShutdown();
                running.add(thread);
            }

            thread.start();
            return thread;
        }

        public boolean awaitTerminationSeconds(int seconds) throws InterruptedException {
            assert utilityPool.isTerminated();
            final long start = System.currentTimeMillis();
            final long deadline = start + Duration.ofSeconds(seconds).toMillis();
            synchronized (running) {
                while (!running.isEmpty()) {
                    final long now = System.currentTimeMillis();
                    if (now >= deadline) {
                        return running.isEmpty();
                    }
                    final long delay = deadline - now;
                    assert delay > 0;
                    running.wait(delay);
                }

                return true;
            }
        }
    }
}
