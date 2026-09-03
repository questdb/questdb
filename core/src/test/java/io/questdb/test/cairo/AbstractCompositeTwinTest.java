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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;

/**
 * Shared harness for composite-vs-plain TWIN tests: a composite table {@code c} and a plain table
 * {@code p} fed identical rows, with every assertion stated as "composite matches plain" rather than as
 * a hand-computed number.
 * <p>
 * The comparison helpers live here rather than in each test because getting the BACKWARD one right is
 * genuinely difficult, and it was got wrong four separate times while this feature was being tested. On
 * a composite table a query that LOOKS like it reads backwards very often does not:
 * <ul>
 *     <li>{@code ORDER BY ts DESC, exch DESC, ...} — a MULTI-KEY sort makes the optimiser sort over a
 *     FORWARD scan, so the backward cursor is never entered;</li>
 *     <li>{@code SELECT * FROM (... ORDER BY ts DESC) ORDER BY ts, ...} — an outer sort lets the
 *     optimiser DROP the inner one, with the same result.</li>
 * </ul>
 * Both produce a test that passes against a build with the defect present. {@link #assertTwinEqual}
 * therefore uses a single sort key, projects only {@code ts} (rows tied on timestamp are identical in
 * that projection, so no outer sort is needed to make the comparison deterministic), and ASSERTS THE
 * QUERY PLAN — if a query stops using the backward cursor the test fails instead of quietly testing
 * nothing.
 * <p>
 * Subclasses supply the composite table's shape; {@code p} is always the plain twin with the same
 * columns and no dimensions.
 */
public abstract class AbstractCompositeTwinTest extends AbstractCairoTest {

    /**
     * The default twin pair: {@code (ts, exch, px)} with {@code exch} as the only dimension, PLAIN
     * layout. Subclasses needing a different shape call
     * {@link #createTwins(String, String)} instead.
     */
    protected void createTwins() throws SqlException {
        createTwins("ts TIMESTAMP, exch SYMBOL, px DOUBLE", "PARTITION BY DAY, exch LAYOUT PLAIN");
    }

    /**
     * @param columns         column list shared by both tables, e.g. {@code "ts TIMESTAMP, exch SYMBOL, px DOUBLE"}
     * @param compositeClause the composite table's partitioning clause, e.g.
     *                        {@code "PARTITION BY DAY, exch LAYOUT PLAIN"}. The plain twin always gets
     *                        {@code PARTITION BY DAY}.
     */
    protected void createTwins(String columns, String compositeClause) throws SqlException {
        execute("CREATE TABLE c (" + columns + ") TIMESTAMP(ts) " + compositeClause + " WAL");
        execute("CREATE TABLE p (" + columns + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    protected void insertIntoBoth(String values) throws SqlException {
        execute("INSERT INTO c VALUES " + values);
        execute("INSERT INTO p VALUES " + values);
    }

    /**
     * Asserts the twins agree for {@code where}, three ways: the full forward row set, {@code count()},
     * and the BACKWARD timestamp sequence. All three are needed —
     * <ul>
     *     <li>the row scan and {@code count()} take different code paths, so fixing one alone yields
     *     correct rows and a wrong count;</li>
     *     <li>the backward cursor is a separate implementation of the same walk, and it shipped broken
     *     for a while precisely because the tests only ever read forward. It returned a CORRECT count
     *     alongside an EMPTY row set, so neither of the first two checks could see it.</li>
     * </ul>
     *
     * @param where full predicate including the leading {@code " WHERE "}, or {@code ""} for none
     */
    protected void assertTwinEqual(String where) throws SqlException {
        assertTwinEqual(where, " ORDER BY ts, exch, px");
    }

    /**
     * As {@link #assertTwinEqual(String)}, with an explicit forward ordering for tables whose columns
     * differ from the default shape.
     */
    protected void assertTwinEqual(String where, String forwardOrder) throws SqlException {
        assertSqlCursors("SELECT * FROM p" + where + forwardOrder, "SELECT * FROM c" + where + forwardOrder);
        assertSqlCursors("SELECT count() FROM p" + where, "SELECT count() FROM c" + where);
        assertForwardTsOrderTwinEqual(where);
        assertBackwardTwinEqual(where);
    }

    /**
     * The FORWARD timestamp sequence under a single-column {@code ORDER BY ts}, and the reason it is a
     * separate check from the full row comparison above.
     *
     * <p><b>This exists because its absence hid a wrong-answer bug.</b> The forward comparison orders by
     * {@code ts, exch, px}. A multi-column sort is never elided, so it re-sorts whatever the scan emits
     * and therefore CANNOT observe the order the scan produced. Single-column {@code ORDER BY ts} is
     * elided against a scan the optimiser believes is already timestamp-ordered — so it, and only it,
     * compares the raw scan order. Measured 2026-08-18: with a split fragment present, a composite table
     * returned {@code 01:00, 21:00, 22:00, 10:00, 20:00} from {@code ORDER BY ts} while the full forward
     * comparison passed, because the multi-column sort silently repaired the stream.
     *
     * <p>Only the timestamp column is projected, exactly as the backward half does: rows that tie on
     * {@code ts} have no defined order among themselves, so comparing whole rows here would fail on
     * ties rather than on ordering. This is the forward twin of {@link #assertBackwardTwinEqual}, which
     * already had it — the asymmetry was the gap.
     */
    protected void assertForwardTsOrderTwinEqual(String where) throws SqlException {
        final String asc = " ORDER BY ts";
        assertSqlCursors("SELECT ts FROM p" + where + asc, "SELECT ts FROM c" + where + asc);
    }

    /**
     * The backward half on its own, for tests that need it without the forward comparisons.
     */
    protected void assertBackwardTwinEqual(String where) throws SqlException {
        final String desc = " ORDER BY ts DESC";
        final StringSink plan = new StringSink();
        printSql("EXPLAIN SELECT ts FROM c" + where + desc, plan);
        // A filtered query must reach the interval cursor; an unfiltered one legitimately has no
        // interval and uses a plain backward frame scan. Asserting the wrong one of those would be
        // asserting something false, so the expectation follows the query.
        TestUtils.assertContains(plan, where.isEmpty() ? "Frame backward scan" : "backward scan");
        assertSqlCursors("SELECT ts FROM p" + where + desc, "SELECT ts FROM c" + where + desc);
    }
}
