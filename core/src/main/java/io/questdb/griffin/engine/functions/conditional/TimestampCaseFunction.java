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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

@SuppressWarnings("resource")
class TimestampCaseFunction extends TimestampFunction implements CaseFunction {
    private final ObjList<Function> args;
    private final CaseFunctionPicker picker;
    // the branches the picker can return uncast (a DATE, or a timestamp of the other unit: CASE has
    // no cast for them) and the unit each one answers in, resolved once here instead of per row
    private final ObjList<Function> uncastBranches = new ObjList<>();
    private final IntList uncastBranchTypes = new IntList();

    public TimestampCaseFunction(CaseFunctionPicker picker, ObjList<Function> args, int columnType) {
        super(columnType);
        this.picker = picker;
        this.args = args;
        for (int i = 0, n = args.size(); i < n; i++) {
            final Function arg = args.getQuick(i);
            final int timestampType = ColumnType.getTimestampType(arg.getType());
            if (timestampType != ColumnType.UNDEFINED && timestampType != columnType) {
                uncastBranches.add(arg);
                uncastBranchTypes.add(timestampType);
            }
        }
    }

    @Override
    public ObjList<Function> args() {
        return args;
    }

    @Override
    public long getTimestamp(Record rec) {
        final Function f = picker.pick(rec);
        final long value = f.getTimestamp(rec);
        // a cast branch already answers in this function's unit; only an uncast original converts
        for (int i = 0, n = uncastBranches.size(); i < n; i++) {
            if (uncastBranches.getQuick(i) == f) {
                return timestampDriver.from(value, uncastBranchTypes.getQuick(i));
            }
        }
        return value;
    }
}
