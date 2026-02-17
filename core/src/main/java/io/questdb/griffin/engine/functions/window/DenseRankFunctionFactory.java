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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class DenseRankFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "dense_rank";
    private static final String SIGNATURE = NAME + "()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (windowContext.getNullsDescPos() > 0) {
            throw SqlException.$(windowContext.getNullsDescPos(), "RESPECT/IGNORE NULLS is not supported for current window function");
        }

        // Sacrifice a little runtime performance to reuse the 'rank' function code.
        if (windowContext.isOrdered()) {
            // Rank() over (partition by xxx order by xxx)
            if (windowContext.getPartitionByRecord() != null) {
                return new RankFunctionFactory.RankOverPartitionFunction(
                        windowContext.getPartitionByKeyTypes(),
                        windowContext.getPartitionByRecord(),
                        windowContext.getPartitionBySink(),
                        configuration,
                        true,
                        NAME);
            } else {
                // Rank() over (order by xxx)
                return new RankFunctionFactory.RankFunction(configuration, true, NAME);
            }
        } else {
            // Rank() over ([partition by xxx | ]), without ORDER BY, all rows are peers.
            return new RankFunctionFactory.RankNoOrderFunction(windowContext.getPartitionByRecord(), NAME);
        }
    }
}
