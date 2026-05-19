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
 * (locked / committed / marked-dropped), {@code _lv.s.invalid}, and
 * {@code _lv.s.backfillState} uniquely determines the state.
 */
public enum LiveViewLifecycleState {
    /** Registry entry is locked but not yet committed. View is not visible to readers. */
    CREATING,
    /** Registry committed, refresh worker running, queryable. */
    ACTIVE,
    /**
     * Registry committed, {@code _lv.s.backfillState=BACKFILLING}; the backfill
     * sweep is in progress. The view is queryable (rows materialise incrementally
     * through the sweep) but incremental drain is parked until the sweep
     * completes and flips to ACTIVE.
     */
    BACKFILLING,
    /** Registry committed, {@code _lv.s.invalid=true}; refresh stopped, last persisted state remains queryable. */
    INVALID,
    /** Registry entry marked-dropped; draining, not visible to new readers. */
    DROPPING,
    /**
     * Restart load saw an on-disk format version this build cannot read.
     * The LV's row data is intact but its definition / state cannot be
     * deserialised. Set externally by the catalogue load path when it fails
     * to deserialise {@code _lv} / {@code _lv.s}; not reachable from
     * {@link #derive}, since the caller of {@code derive} holds a live
     * {@link LiveViewInstance} (the deserialisation already succeeded).
     */
    VERSION_UNSUPPORTED;

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
     * @param backfilling     {@code _lv.s.backfillState == BACKFILLING}
     */
    public static LiveViewLifecycleState derive(
            boolean registryVisible,
            boolean invalid,
            boolean backfilling
    ) {
        if (!registryVisible) {
            return DROPPING;
        }
        if (invalid) {
            return INVALID;
        }
        return backfilling ? BACKFILLING : ACTIVE;
    }

    /** Lower-case label suitable for {@code live_views().view_status}. */
    public String catalogueName() {
        return switch (this) {
            case CREATING -> "creating";
            case ACTIVE -> "active";
            case BACKFILLING -> "backfilling";
            case INVALID -> "invalid";
            case DROPPING -> "dropping";
            case VERSION_UNSUPPORTED -> "version_unsupported";
        };
    }
}
