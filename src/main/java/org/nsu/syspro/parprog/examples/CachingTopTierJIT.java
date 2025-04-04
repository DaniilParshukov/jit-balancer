package org.nsu.syspro.parprog.examples;

import org.nsu.syspro.parprog.external.*;
import org.nsu.syspro.parprog.UserThread;

/**
 * Naive caching approach: compile method by best JIT level, save it, execute it. Discard compiled code if there are many cache misses.
 */
public final class CachingTopTierJIT extends UserThread {

    public CachingTopTierJIT(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
    }

    private CompiledMethod cache; // thread-private data (private field of UserThread), accessed from `executeMethod` only, no need to use synchronization
                                         // you could also use standard ThreadLocal<CompiledMethod> class, effect will be the same

    private int misses = 0;

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        if (cache == null) {
            misses = 0;
            cache = compiler.compile_l2(id);
            return exec.execute(cache);
        }

        if (cache.id() == id) {
            return exec.execute(cache);
        }

        misses++;
        if (misses <= 10) {
            return exec.interpret(id);
        }

        misses = 0;
        cache = compiler.compile_l2(id);
        return exec.execute(cache);
    }
}
