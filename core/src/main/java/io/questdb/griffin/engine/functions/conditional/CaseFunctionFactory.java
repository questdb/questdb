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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.ObjList;

public class CaseFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "case(V)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        int n = args.size();
        int returnType = -1;
        final ObjList<Function> vars = new ObjList<>(n);

        Function elseBranch;
        if (n % 2 == 1) {
            elseBranch = args.getQuick(n - 1);
            n--;
        } else {
            elseBranch = null;
        }

        // compute return type in this loop
        for (int i = 0; i < n; i += 2) {
            Function bool = args.getQuick(i);
            Function value = args.getQuick(i + 1);

            if (bool.getType() != ColumnType.BOOLEAN) {
                throw SqlException.position(bool.getPosition()).put("BOOLEAN expected, found ").put(ColumnType.nameOf(bool.getType()));
            }

            returnType = CaseCommon.getCommonType(returnType, value.getType(), value.getPosition());

            vars.add(bool);
            vars.add(value);
        }

        if (elseBranch != null) {
            returnType = CaseCommon.getCommonType(returnType, elseBranch.getType(), elseBranch.getPosition());
        }

        // next calculate cast functions
        for (int i = 1; i < n; i += 2) {
            args.setQuick(i, CaseCommon.getCastFunction(
                    args.getQuick(i),
                    returnType,
                    configuration
            ));
        }

        if (elseBranch != null) {
            elseBranch = CaseCommon.getCastFunction(elseBranch, returnType, configuration);
        }

        final int argsLen = vars.size();
        final Function elseB = elseBranch != null ? elseBranch : Constants.getNullConstant(returnType);

        final CaseFunctionPicker picker = record -> {
            for (int i = 0; i < argsLen; i += 2) {
                if (vars.getQuick(i).getBool(record)) {
                    return args.getQuick(i + 1);
                }
            }
            return elseB;
        };

        return CaseCommon.getCaseFunction(position, returnType, picker);
    }
}
