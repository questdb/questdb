/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Example QuestDB Plugin - Scalar Function
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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Example scalar function that squares a double value.
 *
 * Usage in SQL (after loading the plugin):
 *   SELECT "questdb-plugin-example-1.0.0".example_square(4.0)
 *   -- Returns: 16.0
 *
 * This demonstrates the simplest type of plugin function - a row-by-row
 * transformation that takes one value and returns one value.
 */
public class ExampleSquareDoubleFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        // D = double type (uppercase means non-constant expression allowed)
        return "example_square(D)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new ExampleSquareFunction(args.getQuick(0));
    }

    /**
     * The actual function implementation.
     * Extends DoubleFunction since we return a double value.
     * Implements UnaryFunction since we take exactly one argument.
     */
    private static class ExampleSquareFunction extends DoubleFunction implements UnaryFunction {
        private final Function arg;

        public ExampleSquareFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public double getDouble(Record rec) {
            double value = arg.getDouble(rec);
            // Handle null (NaN in QuestDB)
            if (Double.isNaN(value)) {
                return Double.NaN;
            }
            return value * value;
        }

        @Override
        public String getName() {
            return "example_square";
        }
    }
}
