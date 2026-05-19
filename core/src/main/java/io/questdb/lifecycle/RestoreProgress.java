package io.questdb.lifecycle;

/**
 * Restore progress event emitted by the enterprise BackupRestoreEnvelope at table-iteration
 * boundaries during a PITR / backup restore. One event per restored table.
 *
 * <p>Schema contract: in the current wire schema, {@code bytesDone} and
 * {@code bytesTotal} are RESERVED placeholders. The production emitter always passes 0L
 * for both because the Rust-side restore loop does not track byte counts yet. Consumers
 * of the /lifecycle JSON MUST treat {@code bytesDone:0, bytesTotal:0} as "byte tracking
 * not implemented" rather than as "zero bytes restored". A subsequent revision will wire
 * the Rust accountancy and start emitting meaningful values without a schema change.
 */
public record RestoreProgress(
        int tablesDone,
        int tablesTotal,
        long bytesDone,
        long bytesTotal
) implements ProgressEvent {
}
