package io.questdb.lifecycle;

/**
 * Marker for component-emitted progress events surfaced via
 * {@link LifecycleSnapshot.ComponentSnapshot#latestProgress()}.
 * <p>
 * The {@code permits} clause includes {@code RestoreProgress} for
 * backup-restore progress events, plus a placeholder no-op record.
 * <p>
 * Test code may create custom progress events by implementing the
 * {@link TestOnly} non-sealed escape hatch, which satisfies the sealed
 * hierarchy without requiring test source roots to be visible during the
 * main compilation phase.
 */
public sealed interface ProgressEvent
        permits ProgressEvent.NoOpProgress, ProgressEvent.TestOnly, RestoreProgress {

    /**
     * Placeholder permitted record so the sealed file compiles. No production
     * component emits this.
     */
    record NoOpProgress() implements ProgressEvent {
    }

    /**
     * Non-sealed escape hatch for test-only progress event implementations.
     * Test code implements this interface (not {@code ProgressEvent} directly)
     * to avoid requiring test source roots on the main compilation classpath.
     * Production progress types (such as {@code RestoreProgress}) are added
     * directly to the {@code permits} clause instead.
     */
    non-sealed interface TestOnly extends ProgressEvent {
    }
}
