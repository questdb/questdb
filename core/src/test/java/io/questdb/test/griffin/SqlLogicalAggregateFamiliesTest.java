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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;
import org.junit.Test;

public class SqlLogicalAggregateFamiliesTest extends AbstractCairoTest {
    @Test
    public void testFirstLastKeepInputOrderAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertFamily("SELECT first(v) f,last(v) l FROM (SELECT * FROM lp_agg_family ORDER BY id DESC)",
                    "f\tl\n40\t10\n", """
                    GroupBy vectorized: false
                      values: [first(v),last(v)]
                        Encode sort light
                          keys: [id desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT first(v) f,last(v) l FROM (SELECT * FROM lp_agg_family ORDER BY id DESC LIMIT 2)",
                    "f\tl\n40\t30\n", """
                    GroupBy vectorized: false
                      values: [first(v),last(v)]
                        Async Top K lo: 2 workers: 1
                          filter: null
                          keys: [id desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT k,first(v) f,last(v) l FROM (SELECT * FROM lp_agg_family ORDER BY id DESC) ORDER BY k",
                    "k\tf\tl\n1\tnull\t10\n2\t40\t30\n", null);
            assertFamily("SELECT first(s) f,last(s) l FROM (SELECT * FROM lp_agg_family ORDER BY id DESC)",
                    "f\tl\nz\tb\n", null);
            assertFamily("SELECT first(v)+last(v) total,min(v) lo FROM (SELECT * FROM lp_agg_family ORDER BY id DESC)",
                    "total\tlo\n50\t10\n", null);
        });
    }

    @Test
    public void testFirstLastNullsEmptyInputsAndComputedArguments() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertFamily("SELECT k,first(v) f,last(v) l FROM lp_agg_family ORDER BY k",
                    "k\tf\tl\n1\t10\tnull\n2\t30\t40\n", """
                    Encode sort light
                      keys: [k]
                        Async Group By workers: 1
                          keys: [k]
                          values: [first(v),last(v)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT first(v) f,last(v) l FROM lp_agg_family WHERE false",
                    "f\tl\nnull\tnull\n", """
                    GroupBy vectorized: false
                      values: [first(v),last(v)]
                        Empty table
                    """);
            assertFamily("SELECT k,first(v) f,last(v) l FROM lp_agg_family WHERE false ORDER BY k",
                    "k\tf\tl\n", """
                    Encode sort light
                      keys: [k]
                        GroupBy vectorized: false
                          keys: [k]
                          values: [first(v),last(v)]
                            Empty table
                    """);
            assertFamily("SELECT first(v+1) f,last(v+1) l FROM lp_agg_family",
                    "f\tl\n11\t41\n", """
                    Async Group By workers: 1
                      vectorized: false
                      values: [first(v+1),last(v+1)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT k,first(s) f,last(vs) l FROM lp_agg_family ORDER BY k", """
                    k	f	l
                    1	b\t
                    2	a	z
                    """, """
                    Encode sort light
                      keys: [k]
                        Async Group By workers: 1
                          keys: [k]
                          values: [first(s),last(vs)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT first(ts) f,last(ts) l,first(tn) fn,last(tn) ln FROM lp_agg_family", """
                    f	l	fn	ln
                    2020-01-01T00:00:00.000000Z	2020-01-04T00:00:00.000000Z	2020-01-01T00:00:00.000000001Z	2020-01-04T00:00:00.000000004Z
                    """, """
                    Async Group By workers: 1
                      vectorized: true
                      values: [first(ts),last(ts),first(tn),last(tn)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT first(d) f,last(d) l FROM lp_agg_family", """
                    f	l
                    2020-01-01T00:00:00.000Z	2020-01-04T00:00:00.000Z
                    """, """
                    Async Group By workers: 1
                      vectorized: true
                      values: [first(d),last(d)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_agg_family
                    """);
        });
    }

    @Test
    public void testOrderedAggregateFactoriesSurviveCompilerReuseAndRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 2);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT first(v) f,last(v) l FROM "
                            + "(SELECT id,v FROM lp_agg_family WHERE id>=$1 ORDER BY id DESC)", sqlExecutionContext)
                            .getRecordCursorFactory();
                    assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                            .sizeMayVary().returns("f\tl\n40\tnull\n");
                    try (RecordCursorFactory ignored = compiler.compile("SELECT min(s),max(tn) FROM lp_agg_family",
                            sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    compiler.clear();
                }
                bindVariableService.setInt(0, 3);
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("f\tl\n40\t30\n");
                bindVariableService.setInt(0, 5);
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess()
                        .sizeMayVary().returns("f\tl\nnull\tnull\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testSymbolAggregatesRetainDictionaryCapabilities() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertFamily("SELECT k,first(sym) f,last(sym) l FROM lp_agg_family ORDER BY k",
                    "k\tf\tl\n1\tb\t\n2\ta\tz\n", """
                    Encode sort light
                      keys: [k]
                        Async Group By workers: 1
                          keys: [k]
                          values: [first(sym),last(sym)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT upper(f) f,l IN('z') hit FROM (SELECT first(sym) f,last(sym) l FROM lp_agg_family)",
                    "f\thit\nB\ttrue\n", """
                    VirtualRecord
                      functions: [to_uppercase(f),l in [z]]
                        Async Group By workers: 1
                          vectorized: false
                          values: [first(sym),last(sym)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT k,first(sym) f,last(sym) l FROM (SELECT k,sym FROM lp_agg_family "
                            + "UNION ALL SELECT k,sym FROM lp_agg_family) ORDER BY k",
                    "k\tf\tl\n1\tb\t\n2\ta\tz\n", """
                    Encode sort light
                      keys: [k]
                        GroupBy vectorized: false
                          keys: [k]
                          values: [first(sym),last(sym)]
                            UnionSymbolCast
                              functions: [k,sym::symbol]
                                Union All
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_agg_family
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT first(sym) f,last(sym) l FROM lp_agg_family WHERE false",
                    "f\tl\n\t\n", """
                    GroupBy vectorized: false
                      values: [first(sym),last(sym)]
                        Empty table
                    """);
            assertStaticSymbols("SELECT k,first(sym) f,last(sym) l FROM lp_agg_family ORDER BY k", 1, true);
            assertStaticSymbols("SELECT k,first(sym) f,last(sym) l FROM (SELECT k,sym FROM lp_agg_family "
                    + "UNION ALL SELECT k,sym FROM lp_agg_family) ORDER BY k", 1, false);
            assertStaticSymbols("SELECT first(sym) f,last(sym) l FROM lp_agg_family WHERE false", 0, true);
        });
    }

    @Test
    public void testTemporalAndTextExtrema() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String columns = "min(d) dmin,max(d) dmax,min(ts) tmin,max(ts) tmax,min(tn) nmin,max(tn) nmax,"
                    + "min(s) smin,max(s) smax,min(vs) vmin,max(vs) vmax,min(sym) symin,max(sym) symax";
            assertFamily("SELECT " + columns + " FROM lp_agg_family", """
                    dmin	dmax	tmin	tmax	nmin	nmax	smin	smax	vmin	vmax	symin	symax
                    2020-01-01T00:00:00.000Z	2020-01-04T00:00:00.000Z	2020-01-01T00:00:00.000000Z	2020-01-04T00:00:00.000000Z	2020-01-01T00:00:00.000000001Z	2020-01-04T00:00:00.000000004Z	a	z	a	z	a	z
                    """, """
                    Async Group By workers: 1
                      vectorized: true
                      values: [min(d),max(d),min_designated(ts),max_designated(ts),min(tn),max(tn),min(s),max(s),min(vs),max(vs),min(sym),max(sym)]
                      filter: null
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT k," + columns + " FROM lp_agg_family ORDER BY k", """
                    k	dmin	dmax	tmin	tmax	nmin	nmax	smin	smax	vmin	vmax	symin	symax
                    1	2020-01-01T00:00:00.000Z	2020-01-01T00:00:00.000Z	2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z	2020-01-01T00:00:00.000000001Z	2020-01-01T00:00:00.000000001Z	b	b	b	b	b	b
                    2	2020-01-03T00:00:00.000Z	2020-01-04T00:00:00.000Z	2020-01-03T00:00:00.000000Z	2020-01-04T00:00:00.000000Z	2020-01-03T00:00:00.000000003Z	2020-01-04T00:00:00.000000004Z	a	z	a	z	a	z
                    """, """
                    Encode sort light
                      keys: [k]
                        Async Group By workers: 1
                          keys: [k]
                          values: [min(d),max(d),min(ts),max(ts),min(tn),max(tn),min(s),max(s),min(vs),max(vs),min(sym),max(sym)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT " + columns + " FROM lp_agg_family WHERE false", """
                    dmin	dmax	tmin	tmax	nmin	nmax	smin	smax	vmin	vmax	symin	symax
                    										\t
                    """, """
                    GroupBy vectorized: false
                      values: [min(d),max(d),min(ts),max(ts),min(tn),max(tn),min(s),max(s),min(vs),max(vs),min(sym),max(sym)]
                        Empty table
                    """);
            assertFamily("SELECT " + columns + " FROM lp_agg_family WHERE id=2", """
                    dmin	dmax	tmin	tmax	nmin	nmax	smin	smax	vmin	vmax	symin	symax
                    		2020-01-02T00:00:00.000000Z	2020-01-02T00:00:00.000000Z							\t
                    """, """
                    Async JIT Group By workers: 1
                      vectorized: false
                      values: [min(d),max(d),min_designated(ts),max_designated(ts),min(tn),max(tn),min(s),max(s),min(vs),max(vs),min(sym),max(sym)]
                      filter: id=2
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT k,min(s) lo,max(s) hi FROM lp_agg_family ORDER BY k",
                    "k\tlo\thi\n1\tb\tb\n2\ta\tz\n", """
                    Encode sort light
                      keys: [k]
                        Async Group By workers: 1
                          keys: [k]
                          values: [min(s),max(s)]
                          filter: null
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT min(ts),max(ts),min(tn),max(tn) FROM "
                    + "(SELECT tn,ts FROM lp_agg_family ORDER BY id DESC LIMIT 3)", """
                    min	max	min1	max1
                    2020-01-02T00:00:00.000000Z	2020-01-04T00:00:00.000000Z	2020-01-03T00:00:00.000000003Z	2020-01-04T00:00:00.000000004Z
                    """, """
                    GroupBy vectorized: false
                      values: [min(ts),max(ts),min(tn),max(tn)]
                        SelectedRecord
                            Async Top K lo: 3 workers: 1
                              filter: null
                              keys: [id desc]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: lp_agg_family
                    """);
        });
    }

    @Test
    public void testWorkerFiltersAndComputedKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertFamily("SELECT k+1 key,first(v) f,last(v) l,min(s) lo,max(vs) hi FROM lp_agg_family "
                    + "WHERE lower(CASE WHEN id IN (1,3,4) THEN 'KEEP' ELSE 'DROP' END)='keep' ORDER BY key",
                    "key\tf\tl\tlo\thi\n2\t10\t10\tb\tb\n3\t30\t40\ta\tz\n", """
                    Encode sort light
                      keys: [key]
                        Async Group By workers: 1
                          keys: [key]
                          keyFunctions: [k+1]
                          values: [first(v),last(v),min(s),max(vs)]
                          filter: to_lowercase(case([id in [1,3,4],'KEEP','DROP']))='keep'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: lp_agg_family
                    """);
            assertFamily("SELECT first(id) f,last(id) l,min(s) lo,max(vs) hi FROM lp_agg_family "
                    + "WHERE lower(CASE WHEN id IN (1,3,4) THEN 'KEEP' ELSE 'DROP' END)='keep'",
                    "f\tl\tlo\thi\n1\t4\ta\tz\n", """
                    Async Group By workers: 1
                      vectorized: false
                      values: [first(id),last(id),min(s),max(vs)]
                      filter: to_lowercase(case([id in [1,3,4],'KEEP','DROP']))='keep'
                        PageFrame
                            Row forward scan
                            Frame forward scan on: lp_agg_family
                    """);
        });
    }

    private void assertFamily(String sql, String expected, String plan) throws Exception {
        final QueryAssertion assertion = assertQuery(sql).noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary();
        if (plan != null) {
            assertion.withPlan(plan);
        }
        assertion.returns(expected);
    }

    private void assertStaticSymbols(String sql, int firstSymbolColumn, boolean expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(sql, expected, factory.getMetadata().isSymbolTableStatic(firstSymbolColumn));
            Assert.assertEquals(sql, expected, factory.getMetadata().isSymbolTableStatic(firstSymbolColumn + 1));
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_agg_family(id INT,k INT,v INT,s STRING,vs VARCHAR,sym SYMBOL,d DATE,ts TIMESTAMP,tn TIMESTAMP_NS) TIMESTAMP(ts)");
        execute("""
                INSERT INTO lp_agg_family VALUES
                (1,1,10,'b','b','b','2020-01-01','2020-01-01','2020-01-01T00:00:00.000000001Z'),
                (2,1,null,null,null,null,null,'2020-01-02',null),
                (3,2,30,'a','a','a','2020-01-03','2020-01-03','2020-01-03T00:00:00.000000003Z'),
                (4,2,40,'z','z','z','2020-01-04','2020-01-04','2020-01-04T00:00:00.000000004Z')
                """);
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        return sink.getSink().toString();
    }
}
