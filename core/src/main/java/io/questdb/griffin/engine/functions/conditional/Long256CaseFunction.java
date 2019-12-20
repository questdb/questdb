/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

class Long256CaseFunction extends Long256Function {
    private final ObjList<Function> args;
    private final int argsLen;
    private final Function elseBranch;

    public Long256CaseFunction(int position, ObjList<Function> args, Function elseBranch) {
        super(position);
        this.args = args;
        this.argsLen = args.size();
        this.elseBranch = elseBranch;
    }

    @Override
    public Long256 getLong256A(Record rec) {
        for (int i = 0; i < argsLen; i += 2) {
            if (args.getQuick(i).getBool(rec)) {
                return args.getQuick(i + 1).getLong256A(rec);
            }
        }
        return elseBranch == null ? NullRecordFactory.LONG_256_NULL : elseBranch.getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        for (int i = 0; i < argsLen; i += 2) {
            if (args.getQuick(i).getBool(rec)) {
                return args.getQuick(i + 1).getLong256B(rec);
            }
        }
        return elseBranch == null ? NullRecordFactory.LONG_256_NULL : elseBranch.getLong256B(rec);
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        for (int i = 0; i < argsLen; i += 2) {
            if (args.getQuick(i).getBool(rec)) {
                args.getQuick(i + 1).getLong256(rec, sink);
                return;
            }
        }
        if (elseBranch != null) {
            elseBranch.getLong256(rec, sink);
        }
    }
}
