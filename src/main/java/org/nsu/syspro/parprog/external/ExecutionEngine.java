package org.nsu.syspro.parprog.external;

/**
 * Abstraction of run-time execution engine of some Virtual Machine.
 * <ul>
 *  <li> Allows to {@link #interpret(MethodID)} arbitrary method immediately. Interpretation is thread-safe: different
 *  {@link MethodID} could be interpreted simultaneously, several identical {@link MethodID} could be interpreted in
 *  different threads.
 *  <li> Allows to {@link #execute(CompiledMethod)} already {@link CompiledMethod}. It is also thread-safe.
 * </ul>
 * <br>
 * Hint 1: in real life, execution of some methods may lead to execution of additional methods. Be prepared.
 * <br>
 * Hint 2: in real life, threads may "own" unique resources. It would be incorrect to replace "execution of methodA in threadA"
 * by "execution of methodA in thread B", even if all synchronization stuff (data transferring, visibility) is done properly.
 */
public interface ExecutionEngine {
    ExecutionResult interpret(MethodID id);

    ExecutionResult execute(CompiledMethod method);
}
