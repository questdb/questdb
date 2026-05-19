package io.questdb.lifecycle;

/**
 * Thrown by {@link LifecycleOrchestrator#submitSwitch(Role)} when a switch is already in flight.
 * Mapped to HTTP 409 Conflict by SwitchProcessor (Phase 6 D6-04/D6-05). Unchecked.
 * Carries current and target roles for the 409 response body.
 */
public final class SwitchInFlightException extends RuntimeException {
    private final Role currentRole;
    private final Role targetRole;

    public SwitchInFlightException(Role currentRole, Role targetRole) {
        super("switch in flight: current=" + currentRole + " target=" + targetRole);
        this.currentRole = currentRole;
        this.targetRole = targetRole;
    }

    public Role currentRole() {
        return currentRole;
    }

    public Role targetRole() {
        return targetRole;
    }
}
