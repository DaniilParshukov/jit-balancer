package org.nsu.syspro.parprog.helpers;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;

abstract class Compiled implements CompiledMethod {
    private final TestMethod id;

    private Compiled(TestMethod id) {
        this.id = id;
    }

    @Override
    public TestMethod id() {
        return id;
    }

    public abstract Finished exec();

    static final class L1 extends Compiled {
        L1(TestMethod id) {
            super(id);
        }

        @Override
        public Finished exec() {
            return new Finished.L1Executed(id(), UserThread.current());
        }
    }

    static final class L2 extends Compiled {
        L2(TestMethod id) {
            super(id);
        }

        @Override
        public Finished exec() {
            return new Finished.L2Executed(id(), UserThread.current());
        }
    }
}
