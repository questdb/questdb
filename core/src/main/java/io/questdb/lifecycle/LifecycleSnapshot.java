package io.questdb.lifecycle;

import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Eventually-consistent snapshot of all registered components.
 * Assembled by reading each component's volatile state field independently;
 * no global lock. Operators may very rarely observe inconsistent pairs
 * during transitions; acceptable trade-off for the polling cadence at
 * which {@code GET /lifecycle} is consumed.
 */
public record LifecycleSnapshot(
        long capturedAtMicros,
        ObjList<ComponentSnapshot> components
) {
    /**
     * Per-component point-in-time view. {@code latestProgress} is populated
     * by components that emit progress via {@link LifecycleContext#progress}.
     */
    public record ComponentSnapshot(
            String name,
            State state,
            long lastTransitionMicros,
            @Nullable ProgressEvent latestProgress,
            ObjList<String> hardRequiredDependencies,
            ObjList<String> softDependencies
    ) {
    }
}
