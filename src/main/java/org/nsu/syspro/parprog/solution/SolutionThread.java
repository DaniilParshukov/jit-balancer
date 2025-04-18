package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SolutionThread extends UserThread {
    private static final int L1_COMPILATION_MIN = 100;
    private static final int L2_COMPILATION_MIN = 1000;
    private static final int L1_COMPILATION_MAX = 10_000;
    private static final int L2_COMPILATION_MAX = 100_000;

    private static final ConcurrentHashMap<MethodID, CompiledMethod> globalCacheL1 = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MethodID, CompiledMethod> globalCacheL2 = new ConcurrentHashMap<>();
    private final Map<MethodID, Integer> counters = new HashMap<>();

    private static ExecutorService compilationPool;

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);

        synchronized (SolutionThread.class) {
            if (compilationPool == null) {
                compilationPool = Executors.newFixedThreadPool(compilationThreadBound);
            }
        }
    }

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        int count = counters.merge(id, 1, Integer::sum);
        CompiledMethod currentL1 = globalCacheL1.get(id);
        CompiledMethod currentL2 = globalCacheL2.get(id);
        OptimizationLevel currentLevel = (currentL2 != null) ? OptimizationLevel.L2 : ((currentL1 != null) ? OptimizationLevel.L1 : OptimizationLevel.INTERPRETED);

        if (count >= L2_COMPILATION_MIN && OptimizationLevel.L2.isBetterThan(currentLevel)) {
            if (count >= L2_COMPILATION_MAX || (count >= L1_COMPILATION_MAX && OptimizationLevel.L1.isBetterThan(currentLevel))) {
                compilation(id, OptimizationLevel.L2);
            } else {
                scheduleCompilation(id, OptimizationLevel.L2);
            }
        } else if (count >= L1_COMPILATION_MIN && OptimizationLevel.L1.isBetterThan(currentLevel)) {
            if (count >= L1_COMPILATION_MAX) {
                compilation(id, OptimizationLevel.L1);
            } else {
                scheduleCompilation(id, OptimizationLevel.L1);
            }
        }

        currentL1 = globalCacheL1.get(id);
        currentL2 = globalCacheL2.get(id);
        
        if (currentL2 != null) {
            return exec.execute(currentL2);
        }
        if (currentL1 != null) {
            return exec.execute(currentL1);
        }
        return exec.interpret(id);
    }

    private void scheduleCompilation(MethodID id, OptimizationLevel level) {
        compilationPool.submit(() -> {
            if ((globalCacheL1.get(id) == null && level == OptimizationLevel.L1) || (level == OptimizationLevel.L2 && globalCacheL2.get(id) == null)) {
                if (level == OptimizationLevel.L2) {
                    globalCacheL2.put(id, compiler.compile_l2(id));
                } else {
                    globalCacheL1.put(id, compiler.compile_l1(id));
                }
            }
        });
    }

    private void compilation(MethodID id, OptimizationLevel level) {
        Future<CompiledMethod> future = compilationPool.submit(() -> {
            return (level == OptimizationLevel.L1) ? compiler.compile_l1(id) : compiler.compile_l2(id);
        });
        try {
            //future.get() waits if necessary for the computation to complete, and then retrieves its result.
            CompiledMethod compiled = future.get();
            if (level == OptimizationLevel.L2) {
                globalCacheL2.put(id, compiled);
            } else {
                globalCacheL1.put(id, compiled);
            }
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
        }
    }

    public enum OptimizationLevel {
        INTERPRETED(0),
        L1(1),
        L2(2);
        
        private final int level;
        
        OptimizationLevel(int level) {
            this.level = level;
        }
        
        public boolean isBetterThan(OptimizationLevel other) {
            return this.level > other.level;
        }
    }
}