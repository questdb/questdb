package io.questdb.lifecycle;

/**
 * Marker for component-emitted progress events surfaced via
 * {@link LifecycleSnapshot.ComponentSnapshot#latestProgress()}.
 * <p>
 * Phase 2 ships the wiring with a placeholder permitted record. Phase 3
 * extends the {@code permits} clause to include {@code RestoreProgress}.
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
     * component emits this in Phase 2.
     */
    record NoOpProgress() implements ProgressEvent {
    }

    /**
     * Non-sealed escape hatch for test-only progress event implementations.
     * Test code implements this interface (not {@code ProgressEvent} directly)
     * to avoid requiring test source roots on the main compilation classpath.
     * Phase 3 does NOT use this; it adds a top-level {@code RestoreProgress}
     * record to the {@code permits} clause instead.
     */
    non-sealed interface TestOnly extends ProgressEvent {
    }
}
