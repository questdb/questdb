/*******************************************************************************
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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.UInt16Function;
import io.questdb.std.ObjList;

class UInt16CoalesceFunction extends UInt16Function {
    private final ObjList<Function> args;

    public UInt16CoalesceFunction(ObjList<Function> args) {
        this.args = new ObjList<>(args);
    }

    @Override
    public short getShort(Record rec) {
        for (int i = 0, n = args.size(); i < n; i++) {
            Function arg = args.getQuick(i);
            if (!arg.isNull(rec)) {
                return arg.getShort(rec);
            }
        }
        return 0;
    }

    @Override
    public boolean isNull(Record rec) {
        for (int i = 0, n = args.size(); i < n; i++) {
            if (!args.getQuick(i).isNull(rec)) {
                return false;
            }
        }
        return true;
    }
}
