package org.nsu.syspro.parprog.helpers;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.ExecutionResult;
import org.nsu.syspro.parprog.external.MethodID;

public abstract class Finished implements ExecutionResult {
    public final MethodID id;
    public final UserThread executor;

    private Finished(MethodID id, UserThread executor) {
        this.id = id;
        this.executor = executor;
    }

    static final class Interpreted extends Finished {
        Interpreted(MethodID id, UserThread executor) {
            super(id, executor);
        }
    }

    static final class L1Executed extends Finished {
        L1Executed(MethodID id, UserThread executor) {
            super(id, executor);
        }
    }

    static final class L2Executed extends Finished {
        L2Executed(MethodID id, UserThread executor) {
            super(id, executor);
        }
    }
}
