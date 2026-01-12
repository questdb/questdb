/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Example QuestDB Plugin - String Function
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
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Example string function that reverses a string.
 *
 * Usage in SQL (after loading the plugin):
 *   SELECT "questdb-plugin-example-1.0.0".example_reverse('hello')
 *   -- Returns: 'olleh'
 *
 * This demonstrates a string transformation function with proper memory handling
 * using StringSink buffers (zero-GC pattern).
 */
public class ExampleReverseFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        // S = string type (uppercase means non-constant expression allowed)
        return "example_reverse(S)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new ExampleReverseFunction(args.getQuick(0));
    }

    /**
     * The actual function implementation.
     * Extends StrFunction since we return a string value.
     * Implements UnaryFunction since we take exactly one argument.
     *
     * Important: String functions should use StringSink buffers for zero-GC operation.
     * We provide two sinks (A and B) to support alternating buffer access patterns.
     */
    private static class ExampleReverseFunction extends StrFunction implements UnaryFunction {
        private final Function arg;
        // Two sinks for alternating buffer access (zero-GC pattern)
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ExampleReverseFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return reverse(arg.getStrA(rec), sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return reverse(arg.getStrB(rec), sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            return arg.getStrLen(rec);
        }

        @Override
        public String getName() {
            return "example_reverse";
        }

        @Override
        public boolean isThreadSafe() {
            // Not thread-safe because we use mutable sinks
            return false;
        }

        private static CharSequence reverse(CharSequence str, StringSink sink) {
            if (str == null) {
                return null;
            }
            sink.clear();
            for (int i = str.length() - 1; i >= 0; i--) {
                sink.put(str.charAt(i));
            }
            return sink;
        }
    }
}
