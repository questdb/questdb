/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.lv;

/**
 * Logical lifecycle state of a live view.
 * <p>
 * Derived state, not a persisted field. Registry visibility (locked / committed /
 * marked-dropped), {@code _lv.s.invalid}, {@code _lv.s.seedState} and the
 * checkpoint format block together determine the state.
 * <p>
 * Three of those four signals are durable. The fourth, the format block, is
 * re-derived from the checkpoint superblock on every start
 * ({@link LiveViewCheckpointRecoveryPhase#BLOCKED}), and it reports as
 * {@link #INVALID} because that is what it is to an operator: refresh has
 * stopped, the rows the view already has stay queryable, and the way back is a
 * re-CREATE. Reporting it under a status of its own would hide it from the
 * queries operators already run to find stopped views.
 * {@code live_views().checkpoint_recovery_phase} is what tells the two apart.
 */
public enum LiveViewLifecycleState {
    /**
     * Registry entry is locked but not yet committed. View is not visible to readers.
     */
    CREATING,
    /**
     * Registry committed, refresh worker running, queryable.
     */
    ACTIVE,
    /**
     * Registry committed, {@code _lv.s.seedState=SEEDING}; the seed
     * sweep is in progress. The view is queryable (rows materialise incrementally
     * through the sweep) but incremental drain is parked until the sweep
     * completes and flips to ACTIVE.
     */
    SEEDING,
    /**
     * Registry committed, {@code _lv.s.invalid=true}; refresh stopped, last persisted state remains queryable.
     */
    INVALID,
    /**
     * Registry entry marked-dropped; draining, not visible to new readers.
     */
    DROPPING,
    /**
     * Restart load saw an on-disk format version this build cannot read.
     * The LV's row data is intact but its definition / state cannot be
     * deserialised. Set externally by the catalogue load path when it fails
     * to deserialise {@code _lv} / {@code _lv.s}; not reachable from
     * {@link #derive}, since the caller of {@code derive} holds a live
     * {@link LiveViewInstance} (the deserialisation already succeeded).
     */
    VERSION_UNSUPPORTED,
    /**
     * Restart load found the on-disk {@code _lv} / {@code _lv.s} torn or
     * corrupt (a non-version read error), and could not reconstruct the
     * durable watermarks from the LV's own WAL sequencer log either, so the
     * view cannot resume refreshing. Distinct from {@link #VERSION_UNSUPPORTED}
     * (a too-new format an upgrade would fix) so operators can tell corruption
     * apart. The row data may still be queryable; the load path surfaces the
     * view here as a droppable stub instead of stranding it invisibly. Set
     * externally by the catalogue load path; not reachable from
     * {@link #derive}.
     */
    STATE_UNREADABLE;

    /**
     * Derives the lifecycle state of a registered {@link LiveViewInstance}
     * from its durable signals.
     * <p>
     * Only callable for instances that have completed CREATE and not yet been
     * fully torn down, so {@link #CREATING} is unreachable here. {@code !registryVisible}
     * therefore means "the instance has been marked dropped" and resolves to
     * {@link #DROPPING}.
     *
     * @param registryVisible        {@code true} iff the live view has a committed
     *                               registry entry not marked for drop
     * @param invalid                {@code _lv.s.invalid}
     * @param checkpointFormatBlocked the view's checkpoint timeline declares a format
     *                               version this build does not implement. Reports as
     *                               {@link #INVALID}: refresh is stopped either way, and
     *                               an operator looking for stopped views must find it
     * @param seeding                {@code _lv.s.seedState == SEEDING}
     */
    public static LiveViewLifecycleState derive(
            boolean registryVisible,
            boolean invalid,
            boolean checkpointFormatBlocked,
            boolean seeding
    ) {
        if (!registryVisible) {
            return DROPPING;
        }
        if (invalid || checkpointFormatBlocked) {
            return INVALID;
        }
        return seeding ? SEEDING : ACTIVE;
    }

    /**
     * Lower-case label suitable for {@code live_views().view_status}.
     */
    public String catalogueName() {
        return switch (this) {
            case CREATING -> "creating";
            case ACTIVE -> "active";
            case SEEDING -> "seeding";
            case INVALID -> "invalid";
            case DROPPING -> "dropping";
            case VERSION_UNSUPPORTED -> "version_unsupported";
            case STATE_UNREADABLE -> "state_unreadable";
        };
    }
}
