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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import io.questdb.std.ObjList;

public class RandomCursorFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "random_cursor(lV)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {

        int argLen = args.size();
        if (argLen % 2 == 0) {
            throw SqlException.position(position).put("invalid number of arguments. Expected rnd_table(count, 'column', rnd_function(), ...)");
        }

        if (argLen < 3) {
            throw SqlException.$(position, "not enough arguments");
        }

        final long recordCount = args.getQuick(0).getLong(null);
        if (recordCount < 0) {
            throw SqlException.$(args.getQuick(0).getPosition(), "invalid record count");

        }
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final ObjList<Function> functions = new ObjList<>();

        for (int i = 1, n = args.size(); i < n; i += 2) {

            // validate column name expression
            // ideally we need column name just a string, but it can also be a function
            // as long as it returns constant value
            //
            // edge condition here is NULL, which is a constant we do not allow
            Function columnName = args.getQuick(i);
            String columnNameStr;
            if (columnName.isConstant()) {
                switch (columnName.getType()) {
                    case ColumnType.STRING:
                        CharSequence cs = columnName.getStr(null);
                        if (cs == null) {
                            throw SqlException.position(columnName.getPosition()).put("column name must not be NULL");
                        }
                        columnNameStr = cs.toString();
                        break;
                    case ColumnType.CHAR:
                        columnNameStr = new String(new char[]{columnName.getChar(null)});
                        break;
                    default:
                        throw SqlException.position(columnName.getPosition()).put("STRING constant expected");
                }
            } else {
                throw SqlException.position(columnName.getPosition()).put("STRING constant expected");
            }

            // random function is the second argument in pair
            // functions implementing RandomFunction interface can be seeded
            // with Rnd instance so that they don't return the same value
            Function rndFunc = args.getQuick(i + 1);
            metadata.add(new TableColumnMetadata(columnNameStr, rndFunc.getType()));
            functions.add(rndFunc);
        }

        final VirtualRecord record = new VirtualRecord(functions);
        return new CursorFunction(position,
                new GenericRecordCursorFactory(metadata, new RandomRecordCursor(recordCount, record), false)
        );
    }
}
