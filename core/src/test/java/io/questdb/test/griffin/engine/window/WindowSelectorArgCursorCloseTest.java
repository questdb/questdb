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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The subsample selectors uniform() and cadence() store their argument functions in fields of
 * their own and pass {@code super(null)}, so {@link io.questdb.griffin.engine.functions.window.BaseWindowFunction}
 * has no {@code arg} to notify. Each therefore has to forward {@code cursorClosed()} to the
 * arguments it owns, exactly as the sibling selector BucketSelectWindowFunction does.
 * <p>
 * Without that forwarding the omission is invisible to an ordinary correctness test: the cached
 * window factory calls {@code cursorClosed()}, the notification stops at the selector, and the
 * argument keeps its cursor-scoped native state. Results stay correct because {@code init()}
 * re-inflates the buffer on the next execution, so only native memory reveals the defect - a
 * {@code json_extract} argument holds its ~2 MiB UTF-8 output sink for the whole life of a cached
 * factory instead of releasing it per cursor.
 * <p>
 * The measurement is a differential one. A bare {@code length(json_extract(...))} projection with
 * no window at all is the control: it reaches {@code cursorClosed()} through the ordinary
 * VirtualFunctionRecordCursor path and so deflates correctly. It retains ~160 bytes of unrelated
 * residue, against ~2 MiB for an unforwarded selector argument - roughly four orders of magnitude
 * of separation, which is what makes the 2 KiB threshold below safe rather than arbitrary.
 */
public class WindowSelectorArgCursorCloseTest extends AbstractCairoTest {

    // A runtime (non-constant) argument that allocates native state per cursor. It has to be a bind
    // variable: a constant would be folded at compile time and never allocate a per-cursor sink.
    private static final String JSON_ARG = "length(json_extract($1, '$.n'))";
    // Well above the ~160 bytes of unrelated residue the control case leaves behind, and far below
    // the ~2 MiB a retained json_extract sink costs.
    private static final long RETAINED_LIMIT = 2048;

    @Test
    public void testCadenceOwnedSeedIsNotifiedOnCursorClose() throws Exception {
        // Second argument position. Stride is a constant > 1 so that init() actually reads the seed.
        assertArgDeflatesOnCursorClose("cadence(4, " + JSON_ARG + ")", true);
    }

    @Test
    public void testCadenceOwnedStrideIsNotifiedOnCursorClose() throws Exception {
        assertArgDeflatesOnCursorClose("cadence(" + JSON_ARG + ")", true);
    }

    @Test
    public void testNonWindowProjectionDeflatesOnCursorClose() throws Exception {
        // Control. This path never touches the selectors, so it must pass both before and after the
        // fix. If it ever fails, the measurement itself is broken and the three assertions above
        // prove nothing.
        assertArgDeflatesOnCursorClose(JSON_ARG, false);
    }

    @Test
    public void testUniformOwnedTargetIsNotifiedOnCursorClose() throws Exception {
        assertArgDeflatesOnCursorClose("uniform(" + JSON_ARG + ")", true);
    }

    private static long utf8SinkBytes() {
        return Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DIRECT_UTF8_SINK);
    }

    private void assertArgDeflatesOnCursorClose(String projection, boolean window) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::timestamp ts from long_sequence(20)) timestamp(ts)");

            // Both cached window factories forward cursorClosed(), so both have to be covered.
            for (boolean light : new boolean[]{true, false}) {
                setProperty(PropertyKey.CAIRO_SQL_WINDOW_CACHED_LIGHT_ENABLED, Boolean.toString(light));
                bindVariableService.setVarchar(0, new Utf8String("{\"n\":\"12345\"}"));

                final String sql = "select ts, " + projection + (window ? " over(order by ts)" : "") + " result from t";
                final long before = utf8SinkBytes();

                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
                ) {
                    // Re-execute the same factory repeatedly, which is what a cached factory does.
                    // The bound value alternates length so a stale buffer would be observable.
                    for (int run = 0; run < 6; run++) {
                        final int len = run % 2 == 0 ? 5 : 7;
                        bindVariableService.setVarchar(0, new Utf8String(len == 5 ? "{\"n\":\"12345\"}" : "{\"n\":\"1234567\"}"));

                        int rows = 0;
                        int kept = 0;
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            while (cursor.hasNext()) {
                                rows++;
                                if (window) {
                                    if (cursor.getRecord().getBool(1)) {
                                        kept++;
                                    }
                                } else {
                                    // The control also pins down that the argument still evaluates
                                    // correctly after a deflate/re-inflate cycle.
                                    Assert.assertEquals(len, cursor.getRecord().getInt(1));
                                }
                            }
                        }

                        Assert.assertEquals(20, rows);
                        if (projection.startsWith("uniform")) {
                            // Behaviour guard: forwarding the notification must not perturb selection.
                            Assert.assertEquals(len, kept);
                        }

                        final long retained = utf8SinkBytes() - before;
                        Assert.assertTrue(
                                "cursor close must deflate the argument's JSON output buffer (light=" + light
                                        + ", run=" + run + ", sql=" + sql + "); retained bytes=" + retained,
                                retained < RETAINED_LIMIT
                        );
                    }
                }

                Assert.assertEquals(
                        "factory close must release all owned UTF-8 native state (light=" + light + ")",
                        before,
                        utf8SinkBytes()
                );
            }
        });
    }
}
