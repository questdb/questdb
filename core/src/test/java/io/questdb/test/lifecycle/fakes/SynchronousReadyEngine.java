package io.questdb.test.lifecycle.fakes;

import io.questdb.lifecycle.Component;
import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.State;
import io.questdb.std.ObjList;

/**
 * Test fake engine that publishes {@link State#READY} synchronously inside its own
 * {@link #start(LifecycleContext)} body, mirroring production
 * {@code ServerMain.EngineEnvelope.start()} (lines 758-769) which calls
 * {@code engine.completeInit() / engine.load()} and then publishes READY before
 * returning. This synchronous publish is exactly the trigger for CR-01: when the
 * orchestrator dispatches the READY state-change to dependent envelopes, those
 * envelopes have not yet captured their LifecycleContext, so their
 * {@link Component#onDependencyState} callback misses the transition.
 * <p>
 * The {@code D1} replay block on each protocol envelope's start() tail compensates
 * by re-checking {@code ctx.state("engine") == READY} after publishing DEGRADED.
 * This fake is the engine-side stand-in for the D2 regression tests that prove the
 * replay block reaches READY (without the bug-masking
 * {@code ProbeComponent.holdInDegraded()} path used by earlier tests).
 */
public final class SynchronousReadyEngine implements Component {

    private final ObjList<String> empty = new ObjList<>();
    private final ObjList<String> hardDeps;

    public SynchronousReadyEngine() {
        this(new ObjList<>());
    }

    public SynchronousReadyEngine(ObjList<String> hardDeps) {
        this.hardDeps = hardDeps;
    }

    @Override
    public ObjList<String> hardRequiredDependencies() {
        return hardDeps;
    }

    @Override
    public String name() {
        return "engine";
    }

    @Override
    public ObjList<String> softDependencies() {
        return empty;
    }

    @Override
    public void start(LifecycleContext ctx) {
        // Mirrors production EngineEnvelope.start (ServerMain.java:758-769): publish
        // STARTING then READY synchronously. The orchestrator dispatches the READY
        // transition immediately, but dependent envelopes have not yet set their ctxRef --
        // this is the CR-01 race the D1 replay block fixes.
        ctx.publish(State.STARTING);
        ctx.publish(State.READY);
    }

    @Override
    public void stop() {
    }
}
