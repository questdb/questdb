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


package io.questdb.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A compiler of the {@link LegacyPlanner}, leased by the {@link SqlCompiler} a statement was given to. The compiled
 * query stays valid while the lease lasts.
 */
public interface LegacySqlCompiler extends QuietCloseable {

    @NotNull
    CompiledQuery compile(@NotNull CharSequence sqlText, @NotNull SqlExecutionContext executionContext) throws SqlException;

    /**
     * Compiles a batch as {@link SqlCompiler#compileBatch} does, handing the callback the compiler that leased this
     * one, the compiler the caller gave the batch to.
     */
    void compileBatch(
            @NotNull CharSequence batchText,
            @NotNull SqlExecutionContext executionContext,
            @Nullable BatchCallback batchCallback,
            @NotNull SqlCompiler owner
    ) throws Exception;

    /**
     * Compiles the SELECT of a view into its factory, as {@link SqlCompiler#generateViewFactory} does.
     */
    RecordCursorFactory generateViewFactory(CharSequence viewSql, SqlExecutionContext executionContext) throws SqlException;
}
