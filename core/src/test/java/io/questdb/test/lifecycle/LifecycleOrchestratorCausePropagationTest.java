package io.questdb.test.lifecycle;

import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import io.questdb.test.lifecycle.fakes.ThrowingComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Verifies that a failing component's throwable survives into the
 * LifecycleStartupException thrown by LifecycleOrchestrator.run().
 */
public class LifecycleOrchestratorCausePropagationTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testFirstFailureCauseIsChained() {
        // Register a component that throws a tagged exception. After the fix,
        // the thrown LifecycleStartupException must carry that exception as its
        // cause so operators and boot tests can see the real failure, not just
        // the generic wrapper message.
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        final String marker = "boot probe failure marker";
        ThrowingComponent failing = new ThrowingComponent(
                "failing-component",
                () -> new RuntimeException(marker)
        );
        orch.register(failing);
        try {
            orch.run();
            Assert.fail("expected LifecycleStartupException");
        } catch (LifecycleStartupException e) {
            // The cause must carry the original exception with the marker.
            Throwable cause = e.getCause();
            Assert.assertNotNull("cause must not be null after the fix", cause);
            Assert.assertTrue(
                    "cause message must contain the marker; got: " + cause.getMessage(),
                    cause.getMessage() != null && cause.getMessage().contains(marker)
            );
        } finally {
            orch.close();
        }
    }

    @Test
    public void testFirstFailureNotOverwrittenByLater() {
        // A passing component followed by two failing ones: only the FIRST
        // failing component's cause must be chained.
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        final String firstMarker = "first-boot-probe-marker";
        final String secondMarker = "second-boot-probe-marker";
        ProbeComponent passing = new ProbeComponent("passing-component");
        ThrowingComponent firstFail = new ThrowingComponent(
                "first-fail",
                () -> new RuntimeException(firstMarker)
        );
        ThrowingComponent secondFail = new ThrowingComponent(
                "second-fail",
                () -> new RuntimeException(secondMarker)
        );
        orch.register(passing);
        orch.register(firstFail);
        orch.register(secondFail);
        try {
            orch.run();
            Assert.fail("expected LifecycleStartupException");
        } catch (LifecycleStartupException e) {
            Throwable cause = e.getCause();
            Assert.assertNotNull("cause must not be null", cause);
            // The FIRST failure's marker must be present.
            Assert.assertTrue(
                    "first failure's cause must be chained; got: " + cause.getMessage(),
                    cause.getMessage() != null && cause.getMessage().contains(firstMarker)
            );
            // The second marker must NOT appear as the cause.
            Assert.assertFalse(
                    "second failure must not overwrite the first",
                    cause.getMessage() != null && cause.getMessage().contains(secondMarker)
            );
        } finally {
            orch.close();
        }
    }
}
