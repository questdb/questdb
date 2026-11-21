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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.clauses.GroupByClause;
import io.questdb.test.griffin.fuzz.types.BooleanType;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pins the computed columns {@link FuzzSource} writes into the projection of a sub-query or CTE, so QueryFuzzTest keeps
 * drawing queries whose outer clause leaves a computed column unread, such as a bare count over the sub-query, and
 * queries that read one. Column pruning removes the unread ones; a generator that only renamed columns would leave
 * that path untested while the run stays green.
 * <p>
 * The generator is random, so each assertion runs over a corpus; the seed is fixed so a failure is reproducible.
 */
public class FuzzSourceShapeCoverageTest {
    private static final Pattern COMPUTED_COLUMN = Pattern.compile("\\((\\w+) IS NULL\\) AS (r\\d+)");
    private static final int CORPUS_SIZE = 2_000;

    @Test
    public void testComputedColumnIsTypedBoolean() {
        // the outer clauses draw predicates, keys and aggregates by the column's type, which must be the result's
        final Rnd rnd = new Rnd(0x5eed, 0xc01d);
        final FuzzSource base = base();
        int computedCount = 0;
        int renamedCount = 0;
        for (int i = 0; i < CORPUS_SIZE; i++) {
            final FuzzSource source = i % 2 == 0 ? FuzzSource.subquery(rnd, base) : FuzzSource.cte(rnd, base, i);
            final String sql = source.getPrefixSql() + source.getFromSql();
            final FuzzTable table = source.getTable();
            for (int k = 0, n = table.getColumnCount(); k < n; k++) {
                final FuzzColumn column = table.getColumn(k);
                final Matcher matcher = Pattern.compile("\\((\\w+) IS NULL\\) AS " + column.getName() + "\\b").matcher(sql);
                if (matcher.find()) {
                    Assert.assertSame(sql, BooleanType.INSTANCE, column.getType());
                    Assert.assertNotEquals(sql, "ts", matcher.group(1));
                    Assert.assertNotEquals(sql, column.getName(), table.getTsColumnName());
                    computedCount++;
                } else if (column.getName().startsWith("r")) {
                    renamedCount++;
                }
            }
        }
        // one kept non-timestamp column in COMPUTED_COLUMN_ODDS is computed: the shares hold far from either bound
        Assert.assertTrue("computed " + computedCount, computedCount > 100);
        Assert.assertTrue("computed " + computedCount + ", renamed " + renamedCount, renamedCount > computedCount);
    }

    @Test
    public void testCountOverComputedColumnIsDrawn() {
        final Rnd rnd = new Rnd(0x5eed, 0xc0c0);
        final FuzzSource base = base();
        final Pattern countOverComputed = Pattern.compile("count\\(\\)[^\\n]* FROM \\(SELECT [^\\n]*\\(\\w+ IS NULL\\) AS r\\d+");
        boolean hasCountOverComputed = false;
        for (int i = 0; i < CORPUS_SIZE && !hasCountOverComputed; i++) {
            final String sql = GroupByClause.generate(rnd, FuzzSource.subquery(rnd, base), null, false).sql();
            hasCountOverComputed = countOverComputed.matcher(sql).find() && COMPUTED_COLUMN.matcher(sql).find();
        }
        Assert.assertTrue("the generator produced no count() over a sub-query computing a column in " + CORPUS_SIZE + " queries", hasCountOverComputed);
    }

    private static FuzzSource base() {
        final ObjList<FuzzColumn> columns = new ObjList<>();
        columns.add(new FuzzColumn("sym", SymbolType.INSTANCE));
        columns.add(new FuzzColumn("c_int", IntType.INSTANCE));
        columns.add(new FuzzColumn("c_vc", VarcharType.INSTANCE));
        columns.add(new FuzzColumn("c_dbl", DoubleType.INSTANCE));
        columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
        return FuzzSource.direct(new FuzzTable("t", columns, "ts"));
    }
}
