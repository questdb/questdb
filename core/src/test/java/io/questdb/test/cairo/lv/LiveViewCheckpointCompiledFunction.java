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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.ObjList;
import org.junit.Assert;

/**
 * What the per-family checkpoint tests read off the sole window function of
 * {@code SELECT ts, sym, <fn> OVER (<frame>) AS v FROM fx}, captured before the factory
 * closes.
 */
final class LiveViewCheckpointCompiledFunction {
    final String className;
    final LiveViewCheckpointDependency dependency;
    final int fixedStateLength;
    final boolean hasFrameLocalCheckpointState;
    final int ringScalarWords;
    final int ringValueKind;
    final boolean supportsCheckpointRingState;
    final boolean supportsCheckpointState;

    private LiveViewCheckpointCompiledFunction(WindowFunction function) {
        this.className = function.getClass().getSimpleName();
        this.dependency = function.checkpointDependency();
        this.fixedStateLength = function.checkpointStateFixedLength();
        this.hasFrameLocalCheckpointState = function.hasFrameLocalCheckpointState();
        this.ringScalarWords = function.checkpointRingScalarWords();
        this.ringValueKind = function.checkpointRingValueKind();
        this.supportsCheckpointRingState = function.supportsCheckpointRingState();
        this.supportsCheckpointState = function.supportsCheckpointState();
    }

    // Compiles `fn` over `frame` against the fx table, inside a live view compile or outside
    // one, and resets the context's live view flag whatever the outcome.
    static LiveViewCheckpointCompiledFunction compile(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String fn,
            String frame,
            boolean isLiveViewCompile
    ) throws SqlException {
        final String sql = "SELECT ts, sym, " + fn + " OVER (" + frame + ") AS v FROM fx";
        sqlExecutionContext.setLiveViewCompile(isLiveViewCompile);
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = CairoEngine.select(compiler, sql, sqlExecutionContext)
        ) {
            return new LiveViewCheckpointCompiledFunction(soleWindowFunction(factory, sql));
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    static WindowFunction soleWindowFunction(RecordCursorFactory factory, String sql) {
        RecordCursorFactory root = factory;
        while (root instanceof QueryProgress) {
            root = root.getBaseFactory();
        }
        Assert.assertTrue(sql, root instanceof WindowRecordCursorFactory);
        final ObjList<WindowFunction> functions = ((WindowRecordCursorFactory) root).getWindowFunctions();
        Assert.assertEquals(sql + ": expected exactly one window function", 1, functions.size());
        return functions.getQuick(0);
    }
}
