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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Pins the per-statement scope of the WAL apply context's target name. The context is reused for
 * every statement a WAL apply worker compiles, so the name statement N declared as its target must
 * not still be treated as the target while statement N+1 is being compiled: with a stale name the
 * context would hijack another table's resolution, and {@code ApplyWal2TableJob} - which asks
 * whether a missing table is the statement's target to decide between retrying and suspending -
 * would misclassify a failure. {@code WalApplySqlExecutionContext} and its constructor are
 * package-private in a package this module cannot share (test sources are the separate JPMS module
 * {@code io.questdb.test}, so a same-package test is a split package and will not compile), hence
 * the reflective handles; every assertion below goes through the public
 * {@link SqlExecutionContext} surface.
 */
public class WalApplySqlExecutionContextTest extends AbstractCairoTest {

    @Test
    public void testRemapForNextStatementClearsPreviousTargetName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE u (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken t = engine.verifyTableName("t");
            final TableToken u = engine.verifyTableName("u");

            final Class<?> contextClass = Class.forName("io.questdb.cairo.wal.WalApplySqlExecutionContext");
            final Constructor<?> constructor = contextClass.getDeclaredConstructor(CairoEngine.class, int.class);
            constructor.setAccessible(true);
            final Method remapTo = contextClass.getMethod("remapTableNameResolutionTo", TableToken.class);
            remapTo.setAccessible(true);

            final SqlExecutionContext context = (SqlExecutionContext) constructor.newInstance(engine, 1);
            try {
                // Statement 1 targets u, and declares so: u resolves to the writer's token.
                remapTo.invoke(context, u);
                context.setStatementTargetTableName("u");
                Assert.assertEquals(u, context.getTableToken("u"));

                // Statement 2 targets t and has not declared its target yet. Nothing is the target
                // until it does, so u must resolve normally rather than to t's token.
                remapTo.invoke(context, t);
                Assert.assertEquals(u, context.getTableToken("u"));
            } finally {
                Misc.free(context);
            }
        });
    }
}
