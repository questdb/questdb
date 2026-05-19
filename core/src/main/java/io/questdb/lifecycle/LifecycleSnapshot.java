package io.questdb.lifecycle;

import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Eventually-consistent snapshot of all registered components (LIFE-04).
 * Assembled by reading each component's volatile state field independently;
 * no global lock. Operators may very rarely observe inconsistent pairs
 * during transitions; acceptable trade-off for the polling cadence at
 * which {@code GET /lifecycle} is consumed (Phase 3 wiring).
 * <p>
 * WR-03: {@code currentRole} is the role the orchestrator has actually committed to.
 * When {@code switchInFlight=true}, {@code currentRole} reflects the OLD (pre-switch)
 * role; the new role becomes visible only after the topo-order cascade completes
 * successfully. Coordinators should key role-flip detection on
 * {@code switchInFlight=false && currentRole=desired}.
 */
public record LifecycleSnapshot(
        long capturedAtMicros,
        Role currentRole,
        boolean switchInFlight,
        ObjList<ComponentSnapshot> components
) {
    /**
     * Per-component point-in-time view. {@code latestProgress} is {@code null}
     * in Phase 2; Phase 3 populates it via {@link LifecycleContext#progress}.
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
