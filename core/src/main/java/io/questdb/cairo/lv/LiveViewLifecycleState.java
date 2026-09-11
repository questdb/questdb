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
 * Derived state, not a persisted field. The combination of registry visibility
 * (locked / committed / marked-dropped), {@code _lv.s.invalid},
 * {@code _lv.s.seedState} and the sequencer's suspension flag for the view's own
 * WAL table uniquely determines the state.
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
     * Registry committed, not invalid, but the sequencer has suspended the view's
     * own WAL table: an inline apply failed, or an operator ran
     * {@code ALTER LIVE VIEW ... SUSPEND WAL}. Output the refresh worker commits into
     * the view's WAL stays off disk until an apply lands, and the first one that does
     * clears the suspension. The refresh worker's own inline applies carry no suspension
     * gate, so they retry the table and a transient fault heals without an operator;
     * {@code ALTER LIVE VIEW ... RESUME WAL} is what moves a view left with nothing else
     * to drive it. Those retries do not honour an operator's {@code SUSPEND WAL} either,
     * so the view's next flush ends that suspension too. A SEEDING view whose table is
     * suspended reports this state as well. {@code wal_tables()} carries the error tag and
     * message behind the suspension.
     */
    SUSPENDED,
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
     * @param registryVisible {@code true} iff the live view has a committed
     *                        registry entry not marked for drop
     * @param invalid         {@code _lv.s.invalid}
     * @param seeding         {@code _lv.s.seedState == SEEDING}
     * @param walSuspended    {@code true} iff the sequencer reports the view's own
     *                        WAL table suspended
     */
    public static LiveViewLifecycleState derive(
            boolean registryVisible,
            boolean invalid,
            boolean seeding,
            boolean walSuspended
    ) {
        if (!registryVisible) {
            return DROPPING;
        }
        if (invalid) {
            return INVALID;
        }
        // Suspension outranks the seed signal: a suspended table keeps the view's
        // output off disk whether the sweep or incremental refresh produced it.
        if (walSuspended) {
            return SUSPENDED;
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
            case SUSPENDED -> "suspended";
            case INVALID -> "invalid";
            case DROPPING -> "dropping";
            case VERSION_UNSUPPORTED -> "version_unsupported";
            case STATE_UNREADABLE -> "state_unreadable";
        };
    }
}
