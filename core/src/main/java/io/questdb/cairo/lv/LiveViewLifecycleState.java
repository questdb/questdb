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
 * Per RFC 123: this is derived state, not a persisted field. The combination of
 * registry visibility (locked / committed / marked-dropped) and {@code _lv.s.invalid}
 * uniquely determines the state. Phase 1 omits {@code BACKFILLING} because
 * {@code BACKFILL} is rejected at CREATE.
 */
public enum LiveViewLifecycleState {
    /** Registry entry is locked but not yet committed. View is not visible to readers. */
    CREATING,
    /** Registry committed, refresh worker running, queryable. */
    ACTIVE,
    /** Registry committed, {@code _lv.s.invalid=true}; refresh stopped, last persisted state remains queryable. */
    INVALID,
    /** Registry entry marked-dropped; draining, not visible to new readers. */
    DROPPING,
    /**
     * Restart load saw an on-disk format version this build cannot read.
     * The LV's row data is intact but its definition / state cannot be
     * deserialised; the refresh worker skips it and the catalogue surfaces
     * the view so operators can DROP + CREATE to recover.
     */
    VERSION_UNSUPPORTED;

    /**
     * Derives the lifecycle state from durable signals. Mirrors the RFC's "single
     * source of truth" rule.
     *
     * @param registryVisible {@code true} iff the registry has a committed entry
     *                        for this view (not locked, not marked-dropped)
     * @param markedDropped   {@code true} iff the registry entry is marked for drop
     * @param locked          {@code true} iff the registry entry is locked (CREATE
     *                        in flight)
     * @param invalid         {@code _lv.s.invalid}
     */
    public static LiveViewLifecycleState derive(
            boolean registryVisible,
            boolean markedDropped,
            boolean locked,
            boolean invalid
    ) {
        if (markedDropped) {
            return DROPPING;
        }
        if (locked && !registryVisible) {
            return CREATING;
        }
        if (registryVisible) {
            return invalid ? INVALID : ACTIVE;
        }
        // Defensive default: registry has no entry and no other signal — treat as
        // CREATING so callers don't spuriously hand out queries.
        return CREATING;
    }

    /** Lower-case label suitable for {@code live_views().view_status}. */
    public String catalogueName() {
        return switch (this) {
            case CREATING -> "creating";
            case ACTIVE -> "active";
            case INVALID -> "invalid";
            case DROPPING -> "dropping";
            case VERSION_UNSUPPORTED -> "version_unsupported";
        };
    }
}
