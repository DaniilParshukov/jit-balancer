package org.nsu.syspro.parprog.helpers;

import org.nsu.syspro.parprog.external.MethodID;

import java.util.concurrent.atomic.AtomicLong;

public final class TestMethod implements MethodID {

    private static final AtomicLong idProvider = new AtomicLong(0);

    private final long id;
    private final Runnable payload;

    private TestMethod(long id, Runnable payload) {
        this.id = id;
        this.payload = payload;
    }

    public void invokePayload() {
        if (payload != null) {
            payload.run();
        }
    }

    @Override
    public long id() {
        return id;
    }

    public static TestMethod of() {
        return of(null);
    }

    public static TestMethod of(Runnable r) {
        return new TestMethod(idProvider.getAndAdd(1), r);
    }
}
