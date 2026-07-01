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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that {@link io.questdb.griffin.engine.functions.bool.InLongFunctionFactory}
 * releases its native hash sets when a set allocation fails half-way through
 * building the IN-list function.
 * <p>
 * A narrow-integer key needs a second set for the elements the key wraps against,
 * so both the constant-list path and the runtime-constant constructor allocate two
 * {@code DirectLongHashSet}s. If the second allocation trips the RSS memory limit
 * after the first has already succeeded, the first set must still be freed. The
 * query fuzzer's malloc fault injection surfaces exactly this kind of leak.
 */
public class InLongFunctionFactoryOomTest extends AbstractCairoTest {

    @Test
    public void testConstListCleansUpWhenSetAllocationRunsOutOfMemory() throws Exception {
        // A narrow-int (INT) key with several constant elements builds an INT-width
        // set and a long-width set; a failure on the second must free the first.
        assertNoLeakOnCompileOom("SELECT * FROM x WHERE i32 IN (10, 20, 30, 40, 50)");
    }

    @Test
    public void testRuntimeConstCleansUpWhenSetAllocationRunsOutOfMemory() throws Exception {
        // A narrow-int (INT) key with a runtime-constant (bind variable) element
        // routes to InLongRuntimeConstFunction, whose constructor allocates both
        // sets; a failure on the second must free the first.
        bindVariableService.clear();
        bindVariableService.setLong(0, 100);
        assertNoLeakOnCompileOom("SELECT * FROM x WHERE i32 IN (5, $1)");
    }

    private void assertNoLeakOnCompileOom(String query) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT x::int i32 FROM long_sequence(10))");

            // Warm the compiler/reader pools so the swept failure lands on the
            // IN-list set allocations, not on first-touch pool init.
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertNotNull(factory);
            }

            boolean sawOom = false;
            // Sweep the native-memory ceiling across the compile allocation points.
            // Some ceiling lets the first set allocate and trips the second; the
            // pre-fix code then leaked the first set.
            for (int slack = 0; slack <= 32 * 1024; slack += 8) {
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertNotNull(factory);
                } catch (Throwable e) {
                    Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), isOom(e));
                    sawOom = true;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }
            Assert.assertTrue("sweep never tripped the RSS limit; widen the range", sawOom);

            // Recovery: with the ceiling removed the same query compiles cleanly.
            Unsafe.setRssMemLimit(0);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertNotNull(factory);
            }
        });
    }

    private static boolean isOom(Throwable e) {
        if (e instanceof CairoException && ((CairoException) e).isOutOfMemory()) {
            return true;
        }
        // FunctionParser flattens the OOM CairoException into a SqlException message.
        final String msg = e.getMessage();
        return msg != null && msg.contains("RSS memory limit exceeded");
    }
}
