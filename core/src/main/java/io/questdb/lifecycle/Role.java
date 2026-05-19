package io.questdb.lifecycle;

/**
 * Server role surfaced to components via {@link LifecycleContext#role()}.
 * Initial value is read from {@code replication.role} config at orchestrator
 * construction time. Mutated only by {@link LifecycleOrchestrator#switchRole(Role)}.
 */
public enum Role {
    PRIMARY,
    REPLICA
}
