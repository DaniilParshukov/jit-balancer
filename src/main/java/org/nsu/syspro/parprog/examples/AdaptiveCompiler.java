package org.nsu.syspro.parprog.examples;

import org.nsu.syspro.parprog.external.*;
import org.nsu.syspro.parprog.UserThread;

import java.util.HashMap;
import java.util.Map;

/**
 * Solution that tries to detect "hot" methods and compile them by l1 JIT.
 */
public final class AdaptiveCompiler extends UserThread {

    public AdaptiveCompiler(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
    }

    private final Map<Long, Long> hotness = new HashMap<>();

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long methodID = id.id();
        final long hotLevel = hotness.getOrDefault(methodID, 0L);
        hotness.put(methodID, hotLevel + 1);

        if (hotLevel > 9_000) {
            final CompiledMethod code = compiler.compile_l1(id);
            return exec.execute(code);
        }

        return exec.interpret(id);
    }
}
