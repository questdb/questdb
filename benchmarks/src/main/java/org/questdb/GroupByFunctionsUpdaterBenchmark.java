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

package org.questdb;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.ObjList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark to compare bytecode-generated GroupByFunctionsUpdater vs SimpleGroupByFunctionUpdater.
 * This helps determine the optimal threshold for switching between the two implementations.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class GroupByFunctionsUpdaterBenchmark {
    @Param({"8", "16", "32", "64", "128"})
    public int functionCount;

    private GroupByFunctionsUpdater bytecodeUpdater;
    private SimpleMapValue mapValue;
    private Record record;
    private GroupByFunctionsUpdater simpleUpdater;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupByFunctionsUpdaterBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        // Create functions for bytecode-generated updater
        ObjList<GroupByFunction> functionsForBytecode = new ObjList<>();
        for (int i = 0; i < functionCount; i++) {
            functionsForBytecode.add(new TestGroupByFunction());
        }

        // Create functions for simple updater (separate instances)
        ObjList<GroupByFunction> functionsForSimple = new ObjList<>();
        for (int i = 0; i < functionCount; i++) {
            functionsForSimple.add(new TestGroupByFunction());
        }

        // Create bytecode-generated updater
        BytecodeAssembler asm = new BytecodeAssembler();
        Class<? extends GroupByFunctionsUpdater> bytecodeClass = GroupByFunctionsUpdaterFactory.generateInstanceClass(asm, functionCount);
        bytecodeUpdater = GroupByFunctionsUpdaterFactory.getInstance(bytecodeClass, functionsForBytecode);

        // Create loop-based updater
        simpleUpdater = new GroupByFunctionsUpdaterFactory.SimpleGroupByFunctionUpdater();
        simpleUpdater.setFunctions(functionsForSimple);

        // Create map value and record for benchmarks
        mapValue = new SimpleMapValue(2);
        mapValue.putLong(0, 0);
        mapValue.putLong(1, 0);
        record = new TestRecord();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        mapValue.close();
    }

    @Benchmark
    public long testBytecodeUpdateNew() {
        bytecodeUpdater.updateNew(mapValue, record, 42L);
        return mapValue.getLong(0);
    }

    @Benchmark
    public long testSimpleUpdateNew() {
        simpleUpdater.updateNew(mapValue, record, 42L);
        return mapValue.getLong(0);
    }

    private static class TestGroupByFunction extends LongFunction implements GroupByFunction, UnaryFunction {

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            mapValue.addLong(0, 1);
            mapValue.putLong(1, rowId);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public Function getArg() {
            return null;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public int getValueIndex() {
            return 0;
        }

        @Override
        public void initValueIndex(int valueIndex) {
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
        }

        @Override
        public void setNull(MapValue mapValue) {
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }
    }

    private static class TestRecord implements Record {
    }
}
