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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * With the WITHIN LATEST ON optimisation on, a geohash WITHIN predicate is lifted out of the filter
 * into a prefix scan over the LATEST ON key index. LatestByAllIndexedRecordCursorFactory is the only
 * factory that reads those prefixes and it applies no filter of its own, so under any shape that
 * builds a different factory - several LATEST ON keys, a key predicate alongside the WITHIN, or any
 * other residual filter - lifting the predicate used to drop it from the query, returning rows
 * outside the geohash box.
 */
public class LatestOnWithinPrefixTest extends AbstractCairoTest {

    /**
     * The lone WITHIN over a single indexed SYMBOL key is the shape the optimisation exists for: it
     * must still prefix-scan rather than fall back to a per-row filter.
     */
    @Test
    public void testLoneWithinStillPrefixScans() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) LATEST ON ts PARTITION BY s1")
                    .withPlanContaining("LatestByAllIndexed", "index backward scan on: s1", "g within(")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            a\tp\tm\tdr5rsjutvshf\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    /**
     * Compiling a sub-query in the WHERE clause re-enters the table-query generator, so a
     * generator-wide prefix list would come back carrying the sub-query's WITHIN. The outer query
     * then either trips its own invariant check or, with assertions off, prefix-scans against
     * another table's geohash column.
     */
    @Test
    public void testNestedSubQueryDoesNotLeakPrefixes() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE flags (ts TIMESTAMP, g GEOHASH(8c), sym SYMBOL INDEX, b BOOLEAN) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE readings (ts TIMESTAMP, v LONG, sym SYMBOL INDEX) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO flags VALUES ('2026-01-01T00:00:00.000000Z', #sp05bcde, 'k', true)");
            execute("INSERT INTO readings VALUES ('2026-01-01T00:00:00.000000Z', 1, 'k')");

            assertQuery("SELECT * FROM readings " +
                    "WHERE (SELECT b FROM flags WHERE g WITHIN(#sp05) LATEST ON ts PARTITION BY sym) " +
                    "LATEST ON ts PARTITION BY sym")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tsym
                            2026-01-01T00:00:00.000000Z\t1\tk
                            """);
        });
    }

    /**
     * The optimisation is opt-in and off by default, so its off answer is the reference answer. Every
     * shape whose behaviour this fix changes must now agree with it - that equality is the claim the
     * fix rests on, and the other tests here only assert the on side of it.
     */
    @Test
    public void testOptimisationOffReturnsSameRows() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String matchingRow = """
                    s1\ts2\ts3\tg\tts
                    a\tp\tm\tdr5rsjutvshf\t2026-01-01T00:00:00.000000Z
                    """;
            final String noRows = """
                    s1\ts2\ts3\tg\tts
                    """;
            assertQuery("SELECT * FROM trips WHERE s1 = 'b' AND g WITHIN(#dr5) LATEST ON ts PARTITION BY s1")
                    .timestamp("ts")
                    .returns(noRows);
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) AND g WITHIN(#ez) LATEST ON ts PARTITION BY s1")
                    .timestamp("ts")
                    .returns(noRows);
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) LATEST ON ts PARTITION BY s1, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns(matchingRow);
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) AND s3 != 'z' LATEST ON ts PARTITION BY s1")
                    .timestamp("ts")
                    .expectSize()
                    .returns(matchingRow);
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) LATEST ON ts PARTITION BY s3")
                    .timestamp("ts")
                    .expectSize()
                    .returns(matchingRow);
        });
    }

    /**
     * Two WITHIN predicates cannot both become a prefix scan. The pre-extraction pass used to reject
     * the query outright; now they stay in the filter and intersect, which is what the same query
     * already did with the optimisation off.
     */
    @Test
    public void testTwoWithinsIntersectInFilter() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips " +
                    "WHERE g WITHIN(#dr5) AND g WITHIN(#ez) LATEST ON ts PARTITION BY s1")
                    .timestamp("ts")
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            """);
        });
    }

    /**
     * A timestamp bound alongside the WITHIN ends up in the interval model, not the filter, so the
     * prefix scan still applies - the fix must not give this case up.
     */
    @Test
    public void testWithinPlusIntervalStillPrefixScans() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips " +
                    "WHERE g WITHIN(#dr5) AND ts IN '2026-01-01' LATEST ON ts PARTITION BY s1")
                    .withPlanContaining("LatestByAllIndexed", "g within(", "Interval backward scan")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            a\tp\tm\tdr5rsjutvshf\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    /**
     * A key predicate sends the plan to an index-value cursor that never reads the prefixes.
     */
    @Test
    public void testWithinWithKeyPredicateIsApplied() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips " +
                    "WHERE s1 = 'b' AND g WITHIN(#dr5) LATEST ON ts PARTITION BY s1")
                    .timestamp("ts")
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            """);
        });
    }

    /**
     * Several LATEST ON keys build LatestByAllSymbolsFiltered, which filters and ignores prefixes.
     */
    @Test
    public void testWithinWithMultipleKeysIsApplied() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) LATEST ON ts PARTITION BY s1, s2")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            a\tp\tm\tdr5rsjutvshf\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    /**
     * A non-key predicate alongside the WITHIN leaves a residual filter, which turns off the prefix
     * scan. Both predicates must survive into that filter.
     */
    @Test
    public void testWithinWithResidualFilterIsApplied() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips " +
                    "WHERE g WITHIN(#dr5) AND s3 != 'z' LATEST ON ts PARTITION BY s1")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            a\tp\tm\tdr5rsjutvshf\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    /**
     * An unindexed LATEST ON key builds a deferred-values cursor that ignores prefixes.
     */
    @Test
    public void testWithinWithUnindexedKeyIsApplied() throws Exception {
        configOverrideUseWithinLatestByOptimisation();
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT * FROM trips WHERE g WITHIN(#dr5) LATEST ON ts PARTITION BY s3")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            s1\ts2\ts3\tg\tts
                            a\tp\tm\tdr5rsjutvshf\t2026-01-01T00:00:00.000000Z
                            """);
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE trips (" +
                "s1 SYMBOL INDEX, " +
                "s2 SYMBOL INDEX, " +
                "s3 SYMBOL, " +
                "g GEOHASH(12c), " +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO trips VALUES " +
                "('a','p','m', #dr5rsjutvshf, '2026-01-01T00:00:00.000000Z')," +
                "('b','q','n', #ezzzzzzzzzzz, '2026-01-01T00:00:01.000000Z')");
    }
}
