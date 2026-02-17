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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import org.jetbrains.annotations.NotNull;

public class CovarPopGroupByFunction extends CovarSampleGroupByFunctionFactory.CovarSampleGroupByFunction {

    protected CovarPopGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
        super(arg0, arg1);
    }

    @Override
    public double getDouble(Record rec) {
        long count = rec.getLong(valueIndex + 3);
        if (count > 0) {
            double sumXY = rec.getDouble(valueIndex + 2);
            return sumXY / count;
        }
        return Double.NaN;
    }

    @Override
    public String getName() {
        return "covar_pop";
    }
}
