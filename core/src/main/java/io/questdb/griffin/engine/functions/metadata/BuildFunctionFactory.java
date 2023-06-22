/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class BuildFunctionFactory implements FunctionFactory {
    private StrFunction instance;

    @Override
    public String getSignature() {
        return "build()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            final int position,
            final ObjList<Function> args,
            IntList argPositions,
            final CairoConfiguration configuration,
            final SqlExecutionContext sqlExecutionContext
    ) {
        if (instance == null) {
            instance = createInstance(configuration);
        }
        return instance;
    }

    private StrFunction createInstance(final CairoConfiguration configuration) {
        final BuildInformation buildInformation = configuration.getBuildInformation();

        final CharSequence information = "Build Information: " +
                buildInformation.getSwName() + " " +
                buildInformation.getSwVersion() +
                ", JDK " +
                buildInformation.getJdkVersion() +
                ", Commit Hash " +
                buildInformation.getCommitHash();

        return new StrConstant(information);
    }
}
