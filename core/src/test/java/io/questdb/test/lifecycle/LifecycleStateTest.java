package io.questdb.test.lifecycle;

import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.State;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

public class LifecycleStateTest {

    // W3 fix: 30s timeout matches LifecycleOrchestratorTest; failed assertions surface fast.
    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(30, TimeUnit.SECONDS).build();

    @Test
    public void testFailedIsTerminal() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.publish(State.STARTING);
        ctx.publish(State.FAILED, "boom");
        ctx.publish(State.READY);   // should be rejected -- FAILED is terminal
        Assert.assertEquals(State.FAILED, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testInitToStartingValid() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        orch.contextFor("a").publish(State.STARTING);
        Assert.assertEquals(State.STARTING, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testStoppedOnlyFromStopping() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.publish(State.STARTING);
        ctx.publish(State.READY);
        ctx.publish(State.STOPPED);   // direct READY -> STOPPED rejected
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        ctx.publish(State.STOPPING);
        ctx.publish(State.STOPPED);
        Assert.assertEquals(State.STOPPED, orch.stateOf("a"));
        orch.close();
    }

    private static LifecycleOrchestrator newOrchestrator() {
        return new LifecycleOrchestrator(null, null, null);
    }
}
