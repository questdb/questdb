/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class GlobStrFunctionFactory implements FunctionFactory {
    StringSink sink = new StringSink();

    public static void convertGlobPatternToRegex(CharSequence globPattern, StringSink sink) {
        int bracketStackDepth = 0;
        sink.put('^'); // start anchor
        for (int i = 0, n = globPattern.length(); i < n; i++) {
            char c = globPattern.charAt(i);
            switch (c) {
                case '.':
                case '^':
                case '$':
                case '+':
                case '{':
                case '}':
                case '(':
                case ')':
                case '|':
                    sink.put('\\');
                    sink.put(c);
                    break;
                case '*':
                    sink.put(".*");
                    break;
                case '?':
                    sink.put('.');
                    break;
                case '[':
                    bracketStackDepth++;
                    sink.put('[');
                    break;
                case ']':
                    bracketStackDepth--;
                    sink.put(']');
                    break;
                case '!':
                    if (bracketStackDepth > 0) {
                        sink.put('^');
                    } else {
                        sink.put('!');
                    }
                    break;
                default:
                    sink.put(c);
            }
        }
        sink.put('$'); // end anchor
        assert bracketStackDepth == 0;
    }

    @Override
    public String getSignature() {
        return "glob(Ss)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function arg = args.getQuick(1);
        assert arg.isConstant();
        sink.clear();
        convertGlobPatternToRegex(arg.getStrA(null), sink);
        StrConstant regex = StrConstant.newInstance(sink);
        final ObjList<Function> newArgList = args.copy();
        newArgList.set(1, regex);
        return new MatchStrFunctionFactory().newInstance(position, newArgList, argPositions, configuration, sqlExecutionContext);
    }
}
