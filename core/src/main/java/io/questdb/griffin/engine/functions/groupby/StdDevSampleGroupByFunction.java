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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import org.jetbrains.annotations.NotNull;

public class StdDevSampleGroupByFunction extends AbstractStdDevGroupByFunction {

    public StdDevSampleGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public double getDouble(Record rec) {
        long count = rec.getLong(valueIndex + 2);
        if (count - 1 > 0) {
            double sum = rec.getDouble(valueIndex + 1);
            double variance = sum / (count - 1);
            return Math.sqrt(variance);
        }
        return Double.NaN;
    }

    @Override
    public String getName() {
        return "stddev_samp";
    }
}
