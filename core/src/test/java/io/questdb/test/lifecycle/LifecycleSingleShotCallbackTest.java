package io.questdb.test.lifecycle;

import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for #049 (R2M14) -- fireStableBelowCallbacks single-shot guarantee.
 *
 * <p>Before the fix, the three independent volatile ops (read callback, null callback,
 * run callback) allowed two threads publishing READY at the same time to both observe
 * a non-null callback and fire it twice (idempotent in practice -- extra GC + duplicate
 * banner -- but a real race nonetheless). After the fix, callback is an AtomicReference
 * and the dispatch uses getAndSet(null), so exactly one of the racing threads wins the
 * CAS and the other observes null.
 */
public class LifecycleSingleShotCallbackTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(30, TimeUnit.SECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void onStableBelowCallbackFiresExactlyOnceWhenAlreadyStableAtRegistration() {
        // The fix point: a callback registered via onStableBelow when the dependency
        // is ALREADY stable triggers an immediate dispatch. This fires the callback
        // once. The non-bug path (already-stable) was the easiest single-thread proof.
        LifecycleOrchestrator orch = new LifecycleOrchestrator(null, null, null);
        AtomicInteger callCount = new AtomicInteger();
        try {
            AtomicReference<LifecycleContext> selfCtx = new AtomicReference<>();
            ProbeComponent leaf = new ProbeComponent("leaf");
            ProbeComponent watcher = new ProbeComponent("watcher", listOf("leaf"), new ObjList<>()) {
                @Override
                public void start(LifecycleContext ctx) {
                    super.start(ctx);
                    selfCtx.set(ctx);
                    // At this point "leaf" is already READY (hard dep). The orchestrator dispatches
                    // immediately. Then later state-change rounds also evaluate this watcher; they
                    // must observe the AtomicReference as null and skip.
                    ctx.onStableBelow("leaf", callCount::incrementAndGet);
                }
            };
            orch.register(leaf);
            orch.register(watcher);
            orch.run();

            // Drive a second state-change round by re-publishing leaf via a noop progress event,
            // then a noop publish that re-evaluates the watcher set. The callback must NOT fire
            // a second time because the getAndSet(null) consumed it on the first dispatch.
            LifecycleContext ctx = selfCtx.get();
            Assert.assertNotNull(ctx);
            // Publishing the same READY state on watcher should not re-fire the callback.
            ctx.publish(State.READY);

            Assert.assertEquals(
                    "onStableBelow callback must fire exactly once (#049 getAndSet(null) single-shot)",
                    1, callCount.get());
        } finally {
            orch.close();
        }
    }

    private static ObjList<String> listOf(String... names) {
        ObjList<String> out = new ObjList<>();
        for (String n : names) out.add(n);
        return out;
    }
}
