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
 * A reader names the row it is positioned at by a locator, which any reader of the same source
 * follows back to that row's payload: a thread that walks a build's matches hands them to another
 * thread as locators, and that thread positions a reader of its own at each, without the build's
 * row heap; see {@link Reader#locator()}.
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
         * The locator of the build row that the reader is positioned at: a non-negative value that
         * {@link #positionLocator(long)} takes back to the same row's payload, on this reader or on
         * any other reader of the same source, until the execution ends. A reader over a copy of the
         * payload columns names the row by its ordinal, the copied row it reads, and a reader that
         * reads the columns where they live by the build row's id, so a locator costs nothing to take
         * and nothing to follow. A source switches its readers to a copy only before any probe of the
         * execution runs, so every locator that the probes take names its row the same way; a
         * locator taken before the switch does not survive it. Defined once {@link #position} or
         * {@link #positionLocator} positioned the reader in the execution.
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
         * Positions the reader at the build row that a {@link #locator()} of a reader of this source
         * named in the current execution, as the call to {@link #position} that the locator came
         * from did: the getters then read that row's payload.
         */
        void positionLocator(long locator);

        /**
         * Binds the reader to the source's current execution. This takes symbol tables from the
         * build input, so call it on the owner.
         */
        void reopen();
    }
}
