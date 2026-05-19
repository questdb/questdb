package io.questdb.test.lifecycle;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleSnapshot;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.State;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.fakes.ProbeComponent;

/**
 * In-process test harness for per-envelope lifecycle tests. Wraps a
 * {@link LifecycleOrchestrator} and exposes convenience helpers so
 * individual envelope tests do not need to construct orchestrators manually.
 * <p>
 * Usage:
 * <pre>{@code
 * try (LifecycleTestHarness h = new LifecycleTestHarness(Role.PRIMARY)) {
 *     h.registerFakeReady("factory-provider");
 *     h.registerFakeReady("engine");
 *     h.registerFakeReady("worker-pool-manager", "engine");
 *     h.register(myEnvelope);
 *     h.start();
 *     h.assertState("my-envelope", State.READY);
 * }
 * }</pre>
 */
public final class LifecycleTestHarness implements AutoCloseable {

    private static final Log LOG = LogFactory.getLog(LifecycleTestHarness.class);

    private final LifecycleOrchestrator orchestrator;
    private boolean started;

    public LifecycleTestHarness(Role initialRole) {
        this.orchestrator = new LifecycleOrchestrator(initialRole, LOG, null, null);
    }

    public void assertState(String name, State expected) {
        State actual = orchestrator.stateOf(name);
        if (actual != expected) {
            throw new AssertionError("expected " + expected + " for " + name + " but got " + actual);
        }
    }

    public void awaitState(String name, State expected, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (orchestrator.stateOf(name) == expected) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting " + name + "=" + expected);
            }
        }
        throw new AssertionError(
                "timed out awaiting " + name + "=" + expected + " (got " + orchestrator.stateOf(name) + ")"
        );
    }

    @Override
    public void close() {
        orchestrator.close();
    }

    /**
     * Returns a fresh {@link io.questdb.lifecycle.LifecycleContext} bound to {@code name}.
     * Used by stop()/start() round-trip tests that re-invoke start(ctx) on a single component
     * without re-registering it with the orchestrator. The underlying
     * {@link LifecycleOrchestrator#contextFor(String)} hook is itself test-only.
     */
    public io.questdb.lifecycle.LifecycleContext contextFor(String name) {
        return orchestrator.contextFor(name);
    }

    public <T extends Component> T getEnvelope(String name, Class<T> type) {
        Component c = orchestrator.getComponent(name);
        return type.cast(c);
    }

    public LifecycleTestHarness register(Component... components) {
        if (started) {
            throw new IllegalStateException("cannot register after start()");
        }
        for (Component c : components) {
            orchestrator.register(c);
        }
        return this;
    }

    public LifecycleTestHarness registerFakeReady(String name, String... hardDeps) {
        if (started) {
            throw new IllegalStateException("cannot register after start()");
        }
        ObjList<String> deps = new ObjList<>();
        for (String d : hardDeps) {
            deps.add(d);
        }
        orchestrator.register(new ProbeComponent(name, deps, new ObjList<>()));
        return this;
    }

    public LifecycleSnapshot snapshot() {
        return orchestrator.snapshot();
    }

    public void start() {
        started = true;
        orchestrator.run();
    }

    public State stateOf(String name) {
        return orchestrator.stateOf(name);
    }

    public void switchRole(Role newRole) {
        orchestrator.switchRole(newRole);
    }
}
