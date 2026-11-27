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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.BatchCallback;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.LegacyPlanner;
import io.questdb.griffin.SqlCompilerFactory;
import io.questdb.griffin.SqlCompilerFactoryImpl;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.legacy.griffin.LegacyPlannerImpl;
import io.questdb.test.TestCairoEngineFactory;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The engine's compilers, with legacy compilers that count the statements, batches and view queries they compile, so
 * that a test tells which planner served a statement.
 */
public final class CountingSqlCompilerFactory implements SqlCompilerFactory {
    private final AtomicInteger legacyCompiles = new AtomicInteger();

    /**
     * Engines that compile with this factory.
     */
    public TestCairoEngineFactory engineFactory() {
        return configuration -> new CairoEngine(configuration) {
            @Override
            public SqlCompilerFactory getSqlCompilerFactory() {
                return CountingSqlCompilerFactory.this;
            }
        };
    }

    @Override
    public SqlCompilerImpl getInstance(CairoEngine engine) {
        return SqlCompilerFactoryImpl.INSTANCE.getInstance(engine);
    }

    /**
     * The count of statements, batches and view queries the legacy planner compiled.
     */
    public int getLegacyCompiles() {
        return legacyCompiles.get();
    }

    @Override
    public LegacyPlanner newLegacyPlanner(CairoEngine engine) {
        return new LegacyPlannerImpl(engine, legacyEngine -> new io.questdb.legacy.griffin.SqlCompilerImpl(legacyEngine) {
            @Override
            public @NotNull CompiledQuery compile(@NotNull CharSequence sqlText, @NotNull SqlExecutionContext executionContext) throws SqlException {
                legacyCompiles.incrementAndGet();
                return super.compile(sqlText, executionContext);
            }

            @Override
            public void compileBatch(
                    @NotNull CharSequence batchText,
                    @NotNull SqlExecutionContext executionContext,
                    BatchCallback batchCallback,
                    @NotNull io.questdb.griffin.SqlCompiler owner
            ) throws Exception {
                legacyCompiles.incrementAndGet();
                super.compileBatch(batchText, executionContext, batchCallback, owner);
            }

            @Override
            public RecordCursorFactory generateViewFactory(CharSequence viewSql, SqlExecutionContext executionContext) throws SqlException {
                legacyCompiles.incrementAndGet();
                return super.generateViewFactory(viewSql, executionContext);
            }
        });
    }
}
