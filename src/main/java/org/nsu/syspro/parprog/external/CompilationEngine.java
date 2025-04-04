package org.nsu.syspro.parprog.external;

/**
 * Abstraction of compilation engine of some Virtual Machine featuring Just-In-Time (JIT) compilation.
 * This interface support multi-level (multi-tier) compilation:
 * <ul>
 *  <li> "Fast" compilation which finishes faster, resulting {@link CompiledMethod} is better than
 *  {@link ExecutionEngine#interpret(MethodID) interpretation} but major performance optimization are not applied.
 *
 *  <li> "Heavyweight" compilation that may take significant resources (CPU, RAM, GC pressure) but resulting {@link CompiledMethod}
 *  guarantees top performance.
 * </ul>
 * <br>
 * Both compilation methods are thread-safe. You could do compilation in "user-level" thread (the one that should execute method) or
 * create any number of auxiliary "compilation threads", if it fits your design. Do not forget to properly synchronize data if you use
 * cross-thread approach.
 * <br>
 * You could start as many compilations of the same method as you need, no internal caching will happen. You could treat `compile`
 * methods as idempotent and side effect free.
 */
public interface CompilationEngine {
    CompiledMethod compile_l1(MethodID method);

    CompiledMethod compile_l2(MethodID method);
}
