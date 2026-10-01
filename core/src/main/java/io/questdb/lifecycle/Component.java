package io.questdb.lifecycle;

import io.questdb.std.ObjList;

/**
 * Lifecycle component. Implementations participate in the
 * component DAG owned by {@link LifecycleOrchestrator}.
 * <p>
 * {@link #start(LifecycleContext)} MAY be called after {@link #stop()} on the
 * same instance -- implementations MUST be tolerant of that and re-acquire
 * any cleared state.
 */
public interface Component {

    /**
     * Signals that a deadline-aware {@link #stop(long)} attempt retained resources and must be retried.
     * The orchestrator catches only this signal and keeps the component in {@link State#STOPPING}; all
     * other exceptions keep their existing failure semantics.
     */
    final class ShutdownIncompleteException extends RuntimeException {
        public ShutdownIncompleteException() {
            super(null, null, false, false);
        }
    }

    /**
     * Names of components that MUST reach {@link State#READY} or
     * {@link State#DEGRADED} before this component's {@link #start} runs.
     * A FAILED hard-required dep auto-cascades this component to FAILED.
     * Empty list means no hard deps.
     */
    ObjList<String> hardRequiredDependencies();

    /**
     * Stable identifier; ASCII; unique within an orchestrator. Used in
     * the transition log line and in the snapshot.
     */
    String name();

    /**
     * Notification of a dependency's state transition. The orchestrator dispatches
     * this synchronously, on the thread that drives the dependency's transition --
     * it does NOT hand off to a separate executor. Per-dependent ordering is
     * preserved (a dependent observes its dependencies' transitions in order).
     * Implementations must therefore return promptly and must not block the
     * dispatching thread. Default no-op.
     */
    default void onDependencyState(String depName, State previous, State current) {
    }

    /**
     * Requests current or subsequently published cancellable lifecycle work to stop promptly.
     * Implementations must make this method non-blocking, idempotent, and safe to call concurrently
     * with {@link #start(LifecycleContext)} or a role switch. The request must remain visible to work
     * that publishes its cancellable resource after this method returns. This method only signals
     * cancellation; {@link #stop()} or {@link #stop(long)} retains responsibility for waiting and
     * releasing resources.
     */
    default void requestStop() {
    }

    /**
     * Names of components whose state changes are observed via
     * {@link #onDependencyState} but NOT auto-cascaded.
     */
    ObjList<String> softDependencies();

    /**
     * Invoked after all hard-required deps reach READY/DEGRADED. Convention:
     * returning normally implies {@code ctx.publish(State.READY)} (or
     * {@code DEGRADED} if explicitly published); throwing implies
     * {@code ctx.publish(State.FAILED, reason=throwable.toString())}.
     * If {@link #requestStop()} causes an in-flight start to abort, the implementation must throw
     * {@link java.util.concurrent.CancellationException} directly. The orchestrator treats no other
     * exception as cancellation, even when shutdown races the failure.
     * <p>
     * Implementations MUST tolerate being called after {@link #stop()} on the
     * same instance.
     */
    void start(LifecycleContext ctx);

    /**
     * Reverse-topological shutdown / FAILED cleanup. This method must wait for owned work to stop
     * and release every owned resource before it returns. It must be idempotent and safe to call on
     * a component that never reached READY.
     */
    void stop();

    /**
     * Attempts to stop this component using one absolute {@link System#nanoTime()} deadline.
     * Implementations must retain resources still in use when the deadline expires so a later
     * attempt can retry safely. A normal return means shutdown completed; an implementation that
     * retains resources must throw {@link ShutdownIncompleteException}. Implementations that do not
     * wait may use this default.
     */
    default void stop(long deadlineNanos) {
        stop();
    }
}
