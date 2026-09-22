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
 * The source belongs to the build's owner and lives across executions; the owner binds it to an
 * execution's build input before the build freezes, and keeps that input open until the build
 * closes. A reader belongs to one probe, so to one execution slot: it keeps per-slot positioning
 * state and symbol tables, and must not be shared across workers.
 */
public interface HashJoinPayloadSource {

    /** A reader for one probe. The probe owns it: it closes it with every execution it ends. */
    Reader newReader();

    /**
     * The payload columns of one build row at a time. A closed reader holds nothing of the
     * execution that used it; {@link #reopen()} binds it to the next one.
     */
    interface Reader extends Record, SymbolTableSource, QuietCloseable {

        /** Positions the reader at the build row with this id. Probes call it once per match. */
        void position(long rowId);

        /**
         * Binds the reader to the source's current execution. This takes symbol tables from the
         * build input, so call it on the owner.
         */
        void reopen();
    }
}
