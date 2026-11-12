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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;

/**
 * The LIMIT count the parallel filter of a join's master applies, which the negative LIMIT over the join controls
 * through {@link MasterSuffixRows}: the LIMIT's own count when the filter can hold that many rows, else NULL, all master
 * rows.
 */
public final class MasterSuffixLimitFunction extends LongFunction implements UnaryFunction {
    private final Function count;
    private final MasterSuffixRows rows;

    public MasterSuffixLimitFunction(Function count, MasterSuffixRows rows) {
        this.count = count;
        this.rows = rows;
    }

    @Override
    public Function getArg() {
        return count;
    }

    @Override
    public long getLong(Record rec) {
        final long value = count.getLong(rec);
        return rows.isWithinFilter(value) ? value : Numbers.LONG_NULL;
    }

    MasterSuffixRows getRows() {
        return rows;
    }
}
