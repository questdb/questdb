package io.questdb.lifecycle;

/**
 * Component lifecycle states.
 * <p>
 * Constants are listed in lifecycle order (INIT through FAILED) rather than
 * alphabetical because the readability of the lifecycle progression outweighs
 * the alphabetical-member convention; enum constants are not "members" in the
 * convention's sense (see WorkerPoolManager.Requester for in-tree precedent).
 * <p>
 * Valid transitions are enforced inside {@link LifecycleContext#publish(State)}.
 * FAILED is reachable from any non-terminal state. STOPPED is reachable only
 * from STOPPING. SWITCHING is reserved for reconfiguration cycles driven by
 * subclasses.
 */
public enum State {
    INIT,
    STARTING,
    DEGRADED,
    READY,
    SWITCHING,
    STOPPING,
    STOPPED,
    FAILED;

    public boolean isStable() {
        return this == READY || this == DEGRADED || this == FAILED;
    }

    public boolean isTerminal() {
        return this == STOPPED || this == FAILED;
    }
}
