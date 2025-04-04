package org.nsu.syspro.parprog;

import org.nsu.syspro.parprog.examples.AdaptiveCompiler;
import org.nsu.syspro.parprog.examples.CachingTopTierJIT;
import org.nsu.syspro.parprog.examples.Interpreter;
import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.ExecutionEngine;
import org.nsu.syspro.parprog.external.ExecutionResult;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.solution.SolutionThread;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents some managed thread started by Virtual Machine.
 * <br>
 * Any {@link UserThread} will be created and then will invoke {@link #executeMethod(MethodID)} with arbitrary frequency
 * using unpredictable number of {@link MethodID methods}.
 * <br>
 * Intended usage: inherit from {@link UserThread}, add any additional fields for your convenience, implement {@link #executeMethod(MethodID)}.
 * <br>
 * Look at {@link Interpreter}, {@link AdaptiveCompiler}, {@link CachingTopTierJIT} for reference.
 * <br>
 * Write your solution in {@link SolutionThread}.
 */
public abstract class UserThread extends Thread {
    private static final AtomicLong idProvider = new AtomicLong(0);
    private static final ThreadLocal<UserThread> currentUserThread = new ThreadLocal<>();

    public final long id;
    public final ExecutionEngine exec;
    public final CompilationEngine compiler;
    public final int compilationThreadBound; // should be used for `Thread-bound-compilation` constraint

    public UserThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(r);
        this.compilationThreadBound = compilationThreadBound;
        this.exec = exec;
        this.compiler = compiler;
        this.id = idProvider.getAndAdd(1);
    }

    @Override
    public final void run() {
        assert currentUserThread.get() == null;
        currentUserThread.set(this);
        super.run();
    }

    public abstract ExecutionResult executeMethod(MethodID id);

    public static UserThread current() {
        final UserThread result = currentUserThread.get();
        assert result != null;
        return result;
    }

    public static long firstUnusedThreadNum() {
        return idProvider.get();
    }

    @Override
    public String toString() {
        return String.format("UserThread(%d)", id);
    }
}
