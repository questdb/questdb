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

package io.questdb.test.griffin.engine.functions.table.parquet;

import io.questdb.cairo.sql.RemoteParquetFsProvider;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.str.DirectUtf8StringList;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only provider registered via {@code META-INF/services/io.questdb.cairo.sql.RemoteParquetFsProvider}.
 * Claims the {@code stub://} scheme. Per-test fields let the test set up canned
 * responses and assert how many times each entry point was called.
 * <p>
 * The provider is always loaded (its META-INF entry is on the test classpath) -
 * tests that don't use {@code stub://} are unaffected because {@link #canHandle}
 * returns false for any other scheme. Tests that DO use it set
 * {@link #localSingleResponse} or {@link #localGlobResponse} per scenario.
 */
public class StubRemoteParquetFsProvider implements RemoteParquetFsProvider {

    public static final AtomicInteger canHandleCalls = new AtomicInteger();
    public static final AtomicInteger expandGlobCalls = new AtomicInteger();
    public static final AtomicInteger resolveLocalCalls = new AtomicInteger();
    public static volatile ResolvedGlob localGlobResponse;
    public static volatile Utf8String localSingleResponse;
    public static volatile SqlException throwOnResolve;

    public static void reset() {
        canHandleCalls.set(0);
        resolveLocalCalls.set(0);
        expandGlobCalls.set(0);
        localSingleResponse = null;
        localGlobResponse = null;
        throwOnResolve = null;
    }

    @Override
    public boolean canHandle(@NotNull CharSequence uri) {
        canHandleCalls.incrementAndGet();
        return Chars.startsWith(uri, "stub://");
    }

    @Override
    @NotNull
    public ResolvedGlob expandAndResolveGlob(@NotNull CharSequence globUri, int argPos, @NotNull SqlExecutionContext ctx) throws SqlException {
        expandGlobCalls.incrementAndGet();
        if (throwOnResolve != null) {
            throw throwOnResolve;
        }
        if (localGlobResponse == null) {
            throw SqlException.$(argPos, "stub provider has no localGlobResponse set; test forgot to prime it");
        }
        return localGlobResponse;
    }

    @Override
    @NotNull
    public Utf8String resolveLocal(@NotNull CharSequence uri, int argPos, @NotNull SqlExecutionContext ctx) throws SqlException {
        resolveLocalCalls.incrementAndGet();
        if (throwOnResolve != null) {
            throw throwOnResolve;
        }
        if (localSingleResponse == null) {
            throw SqlException.$(argPos, "stub provider has no localSingleResponse set; test forgot to prime it");
        }
        return localSingleResponse;
    }
}
