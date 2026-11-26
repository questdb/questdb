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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.QuietCloseable;

/**
 * Where probes read the payload columns of the build rows they match. The build stores each
 * row's id in its {@link HashJoinRowHeap}; a probe positions its own {@link Reader} at the id of
 * a match and reads the columns where they live, as the light hash join reads its slave record
 * through {@code recordAt()}. Payload column {@code i} is the i-th column of the build's payload
 * metadata, and a reader answers for exactly those columns, symbol tables included.
 * <p>
 * A source may also copy the payload columns of every build row once the build is frozen, in
 * build order, so that a probe whose matches read each build row many times reads one compact
 * row per match instead of one page per column; see {@code HashJoinBuildFrames}. The build's
 * layout does not change: its rows keep their ids either way.
 * <p>
 * The source belongs to the build's owner and lives across executions; the owner binds it to an
 * execution's build input before the build freezes, and keeps that input open until the build
 * closes. A reader belongs to one probe, so to one execution slot: it keeps per-slot positioning
 * state and symbol tables, and must not be shared across workers.
 * <p>
 * A reader identifies the row it is positioned at by a locator, which any reader of the same
 * source can use to get back to that row's payload. This lets a thread that walks a build's
 * matches pass them to another thread as locators; that thread then positions its own reader at
 * each one without touching the build's row heap. See {@link Reader#locator()}.
 */
public interface HashJoinPayloadSource {

    /**
     * A reader for one probe. The probe owns it: it closes it with every execution it ends.
     */
    Reader newReader();

    /**
     * The payload columns of one build row at a time. A closed reader holds nothing of the
     * execution that used it; {@link #reopen()} binds it to the next one.
     */
    interface Reader extends Record, SymbolTableSource, QuietCloseable {

        /**
         * Returns the locator of the build row the reader is positioned at. A locator is a
         * non-negative value that {@link #positionLocator(long)} turns back into the same row's
         * payload, on this reader or any other reader of the same source, until the execution ends.
         * <p>
         * A reader over a copy of the payload columns uses the row's ordinal in the copy. A reader
         * over the original columns packs the row's frame index and its row within the frame into as
         * few bits as the execution's frames need, so creating and following a locator costs a
         * shift. A source switches its readers to a copy only before any probe of the execution
         * runs, so all locators the probes obtain use the same encoding; a locator obtained before
         * the switch is not valid after it.
         * <p>
         * The value is defined once {@link #position} or {@link #positionLocator} has positioned the
         * reader in the current execution.
         */
        long locator();

        /**
         * Positions the reader at a build row. Probes call it once per match. {@code rowIdAddress}
         * is where the build stores the row's id, and {@code ordinal} counts the rows the build
         * appended before it. A reader that reads the row's columns where they live loads the id;
         * a reader over a copy of those columns in build order reads the ordinal-th copied row and
         * leaves the id unread.
         */
        void position(long rowIdAddress, long ordinal);

        /**
         * Positions the reader at the build row identified by a locator that {@link #locator()}
         * returned, on any reader of this source, in the current execution. The effect is the same
         * as the {@link #position} call that produced the locator: the getters then read that row's
         * payload.
         */
        void positionLocator(long locator);

        /**
         * Binds the reader to the source's current execution. This takes symbol tables from the
         * build input, so call it on the owner.
         */
        void reopen();
    }
}
