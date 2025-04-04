package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.helpers.TestEnvironment;

import java.time.Duration;

public class EasyFastTest extends EasyBase {
    @Override
    public TestEnvironment testEnvironment() {
        return new TestEnvironment(
                Duration.ofNanos(5),
                Duration.ofNanos(3),
                Duration.ofNanos(1),
                Duration.ofNanos(50),
                Duration.ofNanos(1_000)
        );
    }
}



