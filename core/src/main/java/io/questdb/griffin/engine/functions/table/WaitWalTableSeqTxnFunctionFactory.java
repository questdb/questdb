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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class WaitWalTableSeqTxnFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        // Uppercase 'L' (non-constant) so overload resolution admits runtime constants (e.g. bind
        // variables); the newInstance() guard below then enforces constant-or-runtime-constant.
        // A lowercase 'l' would make the parser reject anything but a compile-time constant, leaving
        // the guard unreachable. The table name stays lowercase 's' because newInstance() reads it
        // at compile time, so it genuinely must be a compile-time constant.
        return "wait_wal_table(sL)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        if (args.getQuick(0).isNullConstant()) {
            throw SqlException.$(argPositions.getQuick(0), "tableName cannot be NULL");
        }
        final CharSequence tableName = args.getQuick(0).getStrA(null);
        final Function seqTxnArg = args.getQuick(1);
        if (!seqTxnArg.isConstant() && !seqTxnArg.isRuntimeConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "seq_txn argument must be a constant or runtime constant");
        }
        return new WaitWalFunction(tableName, seqTxnArg);
    }
}
