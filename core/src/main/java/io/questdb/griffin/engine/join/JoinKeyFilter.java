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

/**
 * The hash table of an INNER hash join, which a CROSS or nested loop LEFT join that the hash join
 * reads as its master consults to skip the master rows that the hash join drops anyway. SqlCodeGenerator
 * connects the two only when every key column of the hash join reads the master side of the join that
 * consults it, so the slave row that a skipped master row is joined with does not matter.
 */
public interface JoinKeyFilter {

    /**
     * Returns false when no row of the hash join's table has the key of the record, whose master side
     * is positioned on the master row in question. Returns true while the table is not built yet.
     *
     * @param record the record of the join that consults the filter; only its master columns are read
     * @return false when the hash join cannot match any row with the record's key
     */
    boolean hasMatch(Record record);
}
