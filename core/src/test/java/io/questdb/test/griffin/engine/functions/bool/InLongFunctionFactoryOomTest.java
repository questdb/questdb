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
 * releases its native hash set when an allocation fails half-way through building the
 * IN-list function.
 * <p>
 * Both the constant-list path and the runtime-constant constructor allocate a
 * {@code DirectLongHashSet} and then fill it from the IN elements. If any malloc along the
 * way - the set's initial allocation or a rehash while adding elements - trips the RSS memory
 * limit, the partially built set must still be freed. The query fuzzer's malloc fault
 * injection surfaces exactly this kind of leak.
 */
public class InLongFunctionFactoryOomTest extends AbstractCairoTest {

    @Test
    public void testConstListCleansUpWhenSetAllocationRunsOutOfMemory() throws Exception {
        // The all-constant path (3+ elements) allocates one DirectLongHashSet and fills it; a
        // swept malloc failure at any point must free it.
        assertNoLeakOnCompileOom("SELECT * FROM x WHERE i32 * 3 IN (10, 20, 30, 40, 5_000_000_000)");
    }

    @Test
    public void testRuntimeConstCleansUpWhenSetAllocationRunsOutOfMemory() throws Exception {
        // A runtime-constant (bind variable) element routes to InLongRuntimeConstFunction, whose
        // constructor allocates the set and whose init() fills it; a swept malloc failure must
        // free the set.
        bindVariableService.clear();
        bindVariableService.setLong(0, 100);
        assertNoLeakOnCompileOom("SELECT * FROM x WHERE i32 * 3 IN (5, $1)");
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
            // Sweep the native-memory ceiling across the compile allocation points. Some ceiling
            // lets earlier allocations through and trips the IN-list set's own malloc or a rehash
            // while it fills; the set must be freed on that failure. Keep the step fine: the window
            // that trips the set allocation is one set wide (DirectLongHashSet's MIN_CAPACITY of 16
            // slots = 128 bytes here), and the ceiling drifts a little per iteration, so a coarse
            // step walks straight over it. A step of 8 lands in that window ~11 times per sweep; a
            // step of 64 lands in it zero times and the sweep stops catching the leak it exists to
            // catch. The sweep is cheap regardless - a warm re-compile is well under a millisecond.
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
