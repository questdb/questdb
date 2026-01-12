/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Example QuestDB Plugin - Weighted Average GROUP BY Function
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 ******************************************************************************/

package io.questdb.plugin.example;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Example GROUP BY aggregate function that computes a weighted average.
 *
 * Usage in SQL (after loading the plugin):
 *   SELECT category, "questdb-plugin-example-1.0.0".example_weighted_avg(value, weight)
 *   FROM data
 *   GROUP BY category
 *
 * Formula: sum(value * weight) / sum(weight)
 *
 * This demonstrates how to create a GROUP BY (aggregate) function that:
 * - Takes multiple arguments
 * - Maintains state across rows (sum of products and sum of weights)
 * - Computes a final result from the accumulated state
 */
public class ExampleWeightedAvgDoubleGroupByFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        // DD = two double arguments
        return "example_weighted_avg(DD)";
    }

    @Override
    public boolean isGroupBy() {
        // Mark as GROUP BY function - this is critical!
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        // args[0] = value column
        // args[1] = weight column
        return new ExampleWeightedAvgDoubleGroupByFunction(
                args.getQuick(0),
                args.getQuick(1)
        );
    }
}
