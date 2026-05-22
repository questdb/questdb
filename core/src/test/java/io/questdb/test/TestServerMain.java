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

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class TestServerMain extends ServerMain {
    private final StringSink sink = new StringSink();
    private SqlExecutionContext sqlExecutionContext;

    public TestServerMain(String... args) {
        super(args);
    }

    public TestServerMain(final Bootstrap bootstrap) {
        super(bootstrap);
    }

    public static TestServerMain createWithManualWalRun(String... args) {
        return new TestServerMain(args) {
            @Override
            protected void setupWalApplyJob(
                    WorkerPool workerPool,
                    CairoEngine engine,
                    int sharedQueryWorkerCount
            ) {
            }
        };
    }

    public void assertSql(String sql, String expected) {
        try {
            ensureContext();
            TestUtils.assertSql(getEngine(), sqlExecutionContext, sql, sink, expected);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Drains pending WAL work synchronously before delegating to {@link ServerMain#close()}.
     * <p>
     * Without this drain, the engine's worker pool can still be processing a dropped-table
     * cleanup (e.g., {@code ApplyWal2TableJob} blocked acquiring the reader-pool lock)
     * when {@code super.close()} returns. The closing of those writers releases cached
     * file descriptors moments later -- right inside the next test's
     * {@code assertMemoryLeak()} window -- and the next test fails {@code LeakCheck}
     * with cached/OS fd counts that drop below the baseline it captured at start.
     * Running the drain here makes any pending drop notifications get processed on the
     * current thread, so all cached fds tied to dropped tables are released before close
     * completes.
     */
    @Override
    public void close() {
        if (!hasBeenClosed()) {
            try {
                TestUtils.drainWalQueue(getEngine());
            } catch (Throwable ignore) {
                // Best-effort drain: any failure here must not mask the original
                // test outcome. super.close() still runs below.
            }
        }
        super.close();
        // Safety net: even after super.close() returns, wait for the process-global
        // FD cache to stabilise. If anything (a halted-but-not-yet-joined worker,
        // a deferred pool entry teardown) is still closing cached files, give it a
        // brief window to finish so the next test's LeakCheck baseline is clean.
        TestUtils.awaitFdCountStable();
    }

    public void compile(String sql) {
        try {
            if (sqlExecutionContext == null) {
                getEngine().execute(sql);
            } else {
                getEngine().execute(sql, sqlExecutionContext);
            }
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    public void execute(String sql) {
        try {
            ensureContext();
            getEngine().execute(sql, sqlExecutionContext);
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    public void reset() {
        // Drop all tables
        CairoEngine engine = this.getEngine();
        engine.releaseInactive();
        engine.clear();
        engine.closeNameRegistry();
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot());
            ff.mkdir(p.$(), engine.getConfiguration().getMkDirMode());
        }
        engine.getTableIdGenerator().open();
        engine.resetNameRegistryMemory();
        resetQueryCache();
        engine.setUp();
    }

    private void ensureContext() {
        if (sqlExecutionContext == null) {
            sqlExecutionContext = new SqlExecutionContextImpl(getEngine(), 1).with(
                    getEngine().getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null,
                    -1,
                    null
            );
        }
    }
}
