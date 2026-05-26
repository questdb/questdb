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
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;

import java.util.Arrays;

/**
 * Shared assertion helper used by per-shape, edge-case, and fuzz equivalence tests for the
 * streaming-LEAD path. Each call toggles the streaming-lead session flag, runs the query under
 * both flag states, and asserts the resulting row multisets are identical (sorted, because
 * streaming emits partition-major while cached emits scan-order).
 */
final class StreamingLeadEquivalence {

    private StreamingLeadEquivalence() {
    }

    /**
     * Render {@code sql} under the cached path (flag off) and the streaming path (flag on); assert
     * the header line matches verbatim and the data rows match as a sorted multiset. Caller is
     * responsible for setting up the table state before invoking.
     */
    static void assertEquivalent(CairoEngine engine, SqlExecutionContext ctx, String sql, String contextMsg) throws Exception {
        AbstractCairoTest.staticOverrides.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "false");
        StringSink cachedSink = new StringSink();
        engine.print(sql, cachedSink, ctx);

        AbstractCairoTest.staticOverrides.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STREAMING_LEAD_ENABLED, "true");
        StringSink streamingSink = new StringSink();
        engine.print(sql, streamingSink, ctx);

        String[] cachedLines = splitLines(cachedSink.toString());
        String[] streamingLines = splitLines(streamingSink.toString());

        Assert.assertEquals(
                "row count differs between cached and streaming. " + contextMsg + " sql=" + sql,
                cachedLines.length,
                streamingLines.length
        );
        Assert.assertEquals(
                "header differs. " + contextMsg + " sql=" + sql,
                cachedLines[0],
                streamingLines[0]
        );
        String[] cachedData = Arrays.copyOfRange(cachedLines, 1, cachedLines.length);
        String[] streamingData = Arrays.copyOfRange(streamingLines, 1, streamingLines.length);
        Arrays.sort(cachedData);
        Arrays.sort(streamingData);
        Assert.assertEquals(
                "data rows differ between cached and streaming. " + contextMsg + " sql=" + sql,
                String.join("\n", cachedData),
                String.join("\n", streamingData)
        );
    }

    private static String[] splitLines(String s) {
        if (s.endsWith("\n")) {
            s = s.substring(0, s.length() - 1);
        }
        return s.split("\n", -1);
    }
}
