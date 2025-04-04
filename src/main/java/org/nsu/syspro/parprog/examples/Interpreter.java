package org.nsu.syspro.parprog.examples;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.ExecutionEngine;
import org.nsu.syspro.parprog.external.ExecutionResult;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.UserThread;

/**
 * Straightforward solution that always interprets given method.
 */
public final class Interpreter extends UserThread {

    public Interpreter(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
    }

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        return exec.interpret(id);
    }
}
