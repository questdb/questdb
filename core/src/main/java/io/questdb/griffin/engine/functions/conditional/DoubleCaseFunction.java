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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.ObjList;

class DoubleCaseFunction extends DoubleFunction {
    private final ObjList<Function> args;
    private final int argsLen;
    private final Function elseBranch;

    public DoubleCaseFunction(int position, ObjList<Function> args, Function elseBranch) {
        super(position);
        this.args = args;
        this.argsLen = args.size();
        this.elseBranch = elseBranch;
    }

    @Override
    public double getDouble(Record rec) {
        for (int i = 0; i < argsLen; i += 2) {
            if (args.getQuick(i).getBool(rec)) {
                return args.getQuick(i + 1).getDouble(rec);
            }
        }
        return elseBranch == null ? Double.NaN : elseBranch.getDouble(rec);
    }
}
