/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ColumnType;
import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.VirtualRecord;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.CursorFunction;
import com.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import com.questdb.std.ObjList;

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
