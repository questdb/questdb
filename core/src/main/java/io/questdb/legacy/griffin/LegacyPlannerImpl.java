/*+*****************************************************************************
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


package io.questdb.legacy.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.LegacyPlanner;
import io.questdb.legacy.cairo.pool.SqlCompilerPool;
import io.questdb.legacy.griffin.engine.functions.groupby.GroupingFunctionFactory;
import io.questdb.legacy.griffin.engine.functions.groupby.GroupingIdFunctionFactory;
import io.questdb.std.Misc;

import java.util.ArrayList;

/**
 * The engine's {@link LegacyPlanner}: a pool of master's compilers, and the functions they bind.
 */
public class LegacyPlannerImpl implements LegacyPlanner {
    private final SqlCompilerPool compilerPool;
    private final FunctionFactoryCache functionFactoryCache;

    public LegacyPlannerImpl(CairoEngine engine) {
        this(engine, SqlCompilerFactoryImpl.INSTANCE);
    }

    public LegacyPlannerImpl(CairoEngine engine, SqlCompilerFactory compilerFactory) {
        this.functionFactoryCache = newFunctionFactoryCache(engine);
        this.compilerPool = new SqlCompilerPool(engine, compilerFactory);
    }

    /**
     * The functions the legacy compilers bind: the engine's, with master's GROUPING() and GROUPING_ID(), whose
     * contract with the planner changed.
     */
    public static FunctionFactoryCache getFunctionFactoryCache(CairoEngine engine) {
        return ((LegacyPlannerImpl) engine.getLegacyPlanner()).functionFactoryCache;
    }

    /**
     * Leases a compiler of the engine's legacy planner, for a legacy class that compiles a statement of its own where
     * master leased one from {@link CairoEngine#getSqlCompiler()}.
     */
    public static SqlCompiler getSqlCompiler(CairoEngine engine) {
        return ((LegacyPlannerImpl) engine.getLegacyPlanner()).getSqlCompiler();
    }

    @Override
    public void close() {
        Misc.free(compilerPool);
    }

    @Override
    public SqlCompilerPool.C getSqlCompiler() {
        return compilerPool.get();
    }

    private static FunctionFactory legacyFactoryOf(FunctionFactory factory) {
        if (factory instanceof io.questdb.griffin.engine.functions.groupby.GroupingFunctionFactory) {
            return new GroupingFunctionFactory();
        }
        if (factory instanceof io.questdb.griffin.engine.functions.groupby.GroupingIdFunctionFactory) {
            return new GroupingIdFunctionFactory();
        }
        return factory;
    }

    private static FunctionFactoryCache newFunctionFactoryCache(CairoEngine engine) {
        final ArrayList<FunctionFactory> factories = new ArrayList<>();
        for (FunctionFactory factory : engine.getFunctionFactories()) {
            factories.add(legacyFactoryOf(factory));
        }
        return new FunctionFactoryCache(engine.getConfiguration(), factories);
    }
}
