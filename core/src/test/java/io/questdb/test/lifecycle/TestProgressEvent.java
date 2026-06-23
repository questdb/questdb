package io.questdb.test.lifecycle;

import io.questdb.lifecycle.ProgressEvent;

/**
 * Test-only ProgressEvent implementation for sealed-interface and
 * exhaustive-switch verification (Plan 02 of phase 02-lifecycle-foundation).
 * Implements {@link ProgressEvent.TestOnly} (the non-sealed escape hatch)
 * rather than {@code ProgressEvent} directly, so this record can live under
 * {@code io.questdb.test.lifecycle} without JPMS split-package conflicts.
 */
public record TestProgressEvent(String tag) implements ProgressEvent.TestOnly {
}
