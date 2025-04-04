package org.nsu.syspro.parprog.external;

/**
 * Marker interface represents {@link MethodID} that was compiled to machine code by {@link CompilationEngine} and therefore
 * could be executed faster by {@link ExecutionEngine}.
 */
public interface CompiledMethod {
    MethodID id();
    public int compilation = 0;
}
