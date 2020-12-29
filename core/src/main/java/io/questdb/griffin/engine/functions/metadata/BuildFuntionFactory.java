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

package io.questdb.griffin.engine.functions.metadata;

import io.questdb.BuildInformation;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class BuildFuntionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "build()";
    }

    @Override
    public Function newInstance(final ObjList<Function> args,
                                final int position,
                                final CairoConfiguration configuration,
                                final SqlExecutionContext sqlExecutionContext) throws SqlException {

        return Memoizer.getInstance(configuration).function;
    }

    private static class Memoizer {
        private static Memoizer instance;

        private final StrFunction function;

        private Memoizer(final CharSequence message) {
            this.function = new StrConstant(0, message);
        }

        public static Memoizer getInstance(final CairoConfiguration configuration) {
            if (instance == null) {
                synchronized (Memoizer.class) {
                    if (instance == null) {
                        instance = new Memoizer(message(configuration));
                    }
                }
            }
            return instance;
        }

        private static CharSequence message(final CairoConfiguration configuration) {
            final CharSink sink = new StringSink();
            final BuildInformation buildInformation = configuration.getBuildInformation();

            sink.put("Build Information:")
                .put(Misc.EOL)
                .put("QuestDB server ")
                .put(buildInformation.getQuestDbVersion())
                .put(Misc.EOL)
                .put("JDK ")
                .put(buildInformation.getJdkVersion())
                .put(Misc.EOL)
                .put("Commit Hash ")
                .put(buildInformation.getCommitHash());

            return sink.toString();
        }
    }
}
