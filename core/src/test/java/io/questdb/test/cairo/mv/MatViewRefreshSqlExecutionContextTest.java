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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.griffin.WhereClauseParser;
import io.questdb.griffin.model.IntrinsicModel;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers {@link MatViewRefreshSqlExecutionContext#overrideWhereIntrinsics}: a mat view refresh
 * replaces the base table's WHERE timestamp intrinsics with a runtime BETWEEN whose endpoints
 * link to bind variables 1 (lo, inclusive) and 2 (hi, exclusive, stored as hi-1). The override
 * must adopt both endpoints into the interval model - which takes ownership and closes them -
 * forward the refresh range set by setRange(), intersect the scan with that range even when a
 * NOT BETWEEN left the model's BETWEEN polarity negated, and remain a strict no-op for any table
 * other than the base table. Also covers which views' refresh reads a bare name as a call to a
 * zero-argument function.
 */
public class MatViewRefreshSqlExecutionContextTest extends AbstractCairoTest {

    @Test
    public void testBareNoArgFunctionCallsFollowPassthroughFlag() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp) timestamp(ts) partition by day");
            final MatViewRefreshSqlExecutionContext refreshCtx = new MatViewRefreshSqlExecutionContext(engine, 0);
            try (TableReader baseReader = getReader("base")) {
                refreshCtx.of(baseReader, baseReader.getTableToken(), true);
                Assert.assertFalse(
                        "a passthrough view's refresh reads a bare name as a column",
                        refreshCtx.allowBareNoArgFunctionCalls()
                );

                refreshCtx.of(baseReader, baseReader.getTableToken(), false);
                Assert.assertTrue(
                        "an aggregating view's refresh reads a bare name that matches no column as a call",
                        refreshCtx.allowBareNoArgFunctionCalls()
                );

                refreshCtx.of(baseReader, baseReader.getTableToken(), true);
                refreshCtx.clearReader();
                Assert.assertTrue(
                        "clearReader() drops the passthrough flag of the previous view",
                        refreshCtx.allowBareNoArgFunctionCalls()
                );
            }
        });
    }

    @Test
    public void testOverrideWhereIntrinsicsForwardsBetweenEndpoints() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp) timestamp(ts) partition by day");
            execute("create table other (ts timestamp) timestamp(ts) partition by day");
            final MatViewRefreshSqlExecutionContext refreshCtx = new MatViewRefreshSqlExecutionContext(engine, 0);
            try (
                    TableReader baseReader = getReader("base");
                    TableReader otherReader = getReader("other")
            ) {
                refreshCtx.of(baseReader);
                final WhereClauseParser parser = new WhereClauseParser();
                final IntrinsicModel intrinsicModel = parser.getEmpty(ColumnType.TIMESTAMP, PartitionBy.DAY, engine.getConfiguration());

                // the override must not install intrinsics for foreign tables
                refreshCtx.overrideWhereIntrinsics(otherReader.getTableToken(), intrinsicModel, ColumnType.TIMESTAMP);
                Assert.assertFalse(
                        "foreign tables must keep their intrinsics untouched",
                        intrinsicModel.hasIntervalFilters()
                );

                // the base table receives both BETWEEN endpoints as dynamic bind-variable links
                refreshCtx.overrideWhereIntrinsics(baseReader.getTableToken(), intrinsicModel, ColumnType.TIMESTAMP);
                Assert.assertTrue(
                        "the base table must receive the BETWEEN endpoints",
                        intrinsicModel.hasIntervalFilters()
                );

                refreshCtx.setRange(1_000L, 3_001L, ColumnType.TIMESTAMP);
                try (RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel()) {
                    final LongList out = intervalModel.calculateIntervals(refreshCtx);
                    Assert.assertEquals(2, out.size());
                    Assert.assertEquals("interval lo must read bind variable 1", 1_000L, out.getQuick(0));
                    Assert.assertEquals("interval hi must read bind variable 2 (exclusive hi - 1)", 3_000L, out.getQuick(1));
                }
            }
        });
    }

    @Test
    public void testOverrideWhereIntrinsicsResetsNegatedBetween() throws Exception {
        // A NOT BETWEEN on the designated timestamp leaves the model's BETWEEN polarity negated:
        // the view query's own NOT BETWEEN does it in the same compile, and a pooled model keeps
        // it from an earlier compile. The refresh range still narrows the scan to [lo, hi - 1]
        // rather than everything outside it.
        assertMemoryLeak(() -> {
            execute("create table base (ts timestamp) timestamp(ts) partition by day");
            final MatViewRefreshSqlExecutionContext refreshCtx = new MatViewRefreshSqlExecutionContext(engine, 0);
            try (TableReader baseReader = getReader("base")) {
                refreshCtx.of(baseReader);
                final WhereClauseParser parser = new WhereClauseParser();
                final IntrinsicModel intrinsicModel = parser.getEmpty(ColumnType.TIMESTAMP, PartitionBy.DAY, engine.getConfiguration());
                intrinsicModel.setBetweenNegated(true);

                refreshCtx.overrideWhereIntrinsics(baseReader.getTableToken(), intrinsicModel, ColumnType.TIMESTAMP);
                refreshCtx.setRange(1_000L, 3_001L, ColumnType.TIMESTAMP);
                try (RuntimeIntrinsicIntervalModel intervalModel = intrinsicModel.buildIntervalModel()) {
                    Assert.assertEquals("[1000,3000]", intervalModel.calculateIntervals(refreshCtx).toString());
                }
            }
        });
    }
}
