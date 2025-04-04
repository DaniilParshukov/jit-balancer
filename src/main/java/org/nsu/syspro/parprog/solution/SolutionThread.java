package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SolutionThread extends UserThread {
    private static final int L1_COMPILATION_THRESHOLD = 10_000;
    private static final int L2_COMPILATION_THRESHOLD = 100_000;

    private static final ConcurrentHashMap<MethodID, CompiledMethodWithLevel> globalCache = new ConcurrentHashMap<>();
    private final Map<MethodID, Integer> counters = new HashMap<>();

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
    }

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        int count = counters.merge(id, 1, Integer::sum);
        CompiledMethodWithLevel current = globalCache.get(id);
        OptimizationLevel currentLevel = (current != null) ? current.optimizationLevel : OptimizationLevel.INTERPRETED;

        if (count >= L2_COMPILATION_THRESHOLD && OptimizationLevel.L2.isBetterThan(currentLevel)) {
            current = new CompiledMethodWithLevel(compiler.compile_l2(id), OptimizationLevel.L2);
            globalCache.put(id, current);
        } else if (count >= L1_COMPILATION_THRESHOLD && OptimizationLevel.L1.isBetterThan(currentLevel)) {
            current = new CompiledMethodWithLevel(compiler.compile_l1(id), OptimizationLevel.L1);
            globalCache.put(id, current);
        }

        if (current != null) {
            return exec.execute(current.compiledMethod);
        }
        return exec.interpret(id);
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

    private static final class CompiledMethodWithLevel {
        private final CompiledMethod compiledMethod;
        private final OptimizationLevel optimizationLevel;

        public CompiledMethodWithLevel(CompiledMethod compiledMethod, OptimizationLevel optimizationLevel) {
            this.compiledMethod = compiledMethod;
            this.optimizationLevel = optimizationLevel;
        }
    }
}