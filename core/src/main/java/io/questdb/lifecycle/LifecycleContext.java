package io.questdb.lifecycle;

import io.questdb.WorkerPoolManager;
import org.jetbrains.annotations.Nullable;

/**
 * Per-component context surfaced to {@link Component#start(LifecycleContext)}.
 * <p>
 * NOT given to {@link Component#stop()} -- stop's responsibility is local
 * resource teardown.
 */
public interface LifecycleContext {

    /**
     * Register a callback that fires when every hard-required dependent of
     * {@code componentName} has reached a stable state ({READY, DEGRADED, FAILED}).
     * Used by the worker-pool-manager envelope's two-stage start:
     * stage 1 publishes DEGRADED, then registers a callback that on stage-2
     * trigger calls {@code workerPoolManager.start(log)} and publishes READY.
     * Returns a watch ID for unregistration via {@link #unwatchStable(long)}.
     */
    long onStableBelow(String componentName, Runnable callback);

    /**
     * Submit a typed progress event. Async; eventually overwrites
     * {@link LifecycleSnapshot.ComponentSnapshot#latestProgress()}.
     */
    void progress(ProgressEvent event);

    /**
     * Publish a state transition. Validates against the state transition
     * table; emits the single log line; runs failure cascade
     * if {@code next == FAILED}.
     */
    void publish(State next);

    /**
     * Same as {@link #publish(State)} but carries a human-readable reason that
     * is included in the log line ONLY when {@code next == FAILED}.
     */
    void publish(State next, CharSequence reason);

    /** Current state of any registered component. Volatile read. */
    State state(String componentName);

    /**
     * Tokio runtime handle. Lazy via EntCairoEngine.getOrInitTokioRuntime().
     * Returns {@code null} if no enterprise component has registered one.
     * Cast to {@code com.questdb.tokio.TokioRuntime} at the enterprise call site.
     */
    @Nullable
    Object tokioRuntime();

    /** Cancel a stable-below watch returned by {@link #onStableBelow}. */
    void unwatchStable(long watchId);

    /**
     * Returns the shared {@link WorkerPoolManager}, or {@code null} until the
     * worker-pool-manager envelope has reached DEGRADED. Components that need
     * the manager MUST hard-dep on {@code worker-pool-manager}.
     */
    @Nullable
    WorkerPoolManager workerPoolManager();
}
