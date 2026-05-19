package io.questdb.lifecycle;

import io.questdb.std.ObjList;

/**
 * Lifecycle component (LIFE-01). Implementations participate in the
 * component DAG owned by {@link LifecycleOrchestrator}.
 * <p>
 * Per D-07 (CONTEXT.md), {@link #start(LifecycleContext)} MAY be called
 * after {@link #stop()} on the same instance -- implementations MUST be
 * tolerant of that and re-acquire any cleared state. The default
 * {@link #switchRole(LifecycleContext, Role)} relies on this contract.
 */
public interface Component {

    /**
     * Names of components that MUST reach {@link State#READY} or
     * {@link State#DEGRADED} before this component's {@link #start} runs.
     * A FAILED hard-required dep auto-cascades this component to FAILED
     * (per CONTEXT.md "Claude's Discretion" failure-propagation rules).
     * Empty list means no hard deps.
     */
    ObjList<String> hardRequiredDependencies();

    /**
     * Stable identifier; ASCII; unique within an orchestrator. Used in
     * the OBS-01 transition log line and in the snapshot.
     */
    String name();

    /**
     * Notification of a dependency's state transition. Async; runs on the
     * orchestrator's executor; per-dependent ordering preserved. Default no-op.
     */
    default void onDependencyState(String depName, State previous, State current) {
    }

    /**
     * Names of components whose state changes are observed via
     * {@link #onDependencyState} but NOT auto-cascaded. Empty in Phase 2.
     */
    ObjList<String> softDependencies();

    /**
     * Invoked after all hard-required deps reach READY/DEGRADED. Convention:
     * returning normally implies {@code ctx.publish(State.READY)} (or
     * {@code DEGRADED} if explicitly published); throwing implies
     * {@code ctx.publish(State.FAILED, reason=throwable.toString())}.
     * <p>
     * Implementations MUST tolerate being called after {@link #stop()} on the
     * same instance (D-07 contract).
     */
    void start(LifecycleContext ctx);

    /**
     * Reverse-topological shutdown / FAILED cleanup. Idempotent. Safe to call
     * on a component that never reached READY (D-07).
     */
    void stop();

    /**
     * Default LIFE-01 contract: stop the component then start it on the same
     * instance. Phase 2 wires the dispatch path; production envelopes inherit
     * this default but it is NEVER invoked from production code (per D-05/D-06).
     * Phase 5/6 override this per envelope where needed.
     */
    default void switchRole(LifecycleContext ctx, Role newRole) {
        stop();
        start(ctx);
    }
}
