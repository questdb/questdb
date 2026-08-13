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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reject-side regression corpus: a sub-query reading a source outside the database must never be
 * accepted into a materialized view, no matter how deeply it is buried in the factory tree.
 * <p>
 * The external-source property is fail-open and propagates through {@code getBaseFactory()}, which
 * returns a single child. Two-child shapes (set operations, joins) therefore need explicit
 * propagation, and this corpus is what proves it: without it, wrapping {@code read_parquet()} in a
 * UNION or a JOIN silently made the view legal again.
 *
 * @see MatViewSubQuerySupportTest for the matching accept-side corpus
 */
public class MatViewExternalSourceRejectionTest extends AbstractCairoTest {
    private static final String EXPECTED = "non-deterministic function cannot be used in materialized view";
    private static final AtomicInteger VIEW_SEQ = new AtomicInteger();

    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testRejectsExternalSourceUnderAggregation() throws Exception {
        assertRejected(
                "ts > (SELECT max(value) FROM read_parquet('ext.parquet'))",
                "ts > (SELECT min(value) FROM read_parquet('ext.parquet'))",
                "ts > (SELECT max(value) FROM read_parquet('ext.parquet') GROUP BY value LIMIT 1)",
                "ts > (SELECT max(value) FROM read_parquet('ext.parquet') WHERE value > 0)"
        );
    }

    @Test
    public void testRejectsExternalSourceUnderJoin() throws Exception {
        assertRejected(
                "ts > (SELECT max(p.value) FROM read_parquet('ext.parquet') p JOIN cfg c ON c.lim = p.value)",
                "ts > (SELECT max(c.lim) FROM cfg c JOIN read_parquet('ext.parquet') p ON c.lim = p.value)",
                "ts > (SELECT max(p.value) FROM read_parquet('ext.parquet') p CROSS JOIN cfg c)",
                "ts > (SELECT max(c.lim) FROM cfg c CROSS JOIN read_parquet('ext.parquet') p)",
                "ts > (SELECT max(c.lim) FROM cfg c LEFT JOIN read_parquet('ext.parquet') p ON c.lim = p.value)"
        );
    }

    @Test
    public void testRejectsExternalSourceUnderNesting() throws Exception {
        assertRejected(
                "ts > (SELECT value FROM (SELECT value FROM read_parquet('ext.parquet')))",
                "ts > (SELECT value FROM (SELECT value FROM (SELECT value FROM read_parquet('ext.parquet'))))",
                "ts > (SELECT max(value) FROM (SELECT value FROM read_parquet('ext.parquet') ORDER BY value LIMIT 10))",
                "ts IN (SELECT value FROM read_parquet('ext.parquet'))"
        );
    }

    @Test
    public void testRejectsExternalSourceUnderSetOperations() throws Exception {
        assertRejected(
                "ts > (SELECT value FROM read_parquet('ext.parquet') UNION SELECT value FROM read_parquet('ext.parquet'))",
                "ts > (SELECT lim FROM cfg UNION SELECT value FROM read_parquet('ext.parquet'))",
                "ts > (SELECT lim FROM cfg UNION ALL SELECT value FROM read_parquet('ext.parquet') LIMIT 1)",
                "ts > (SELECT lim FROM cfg EXCEPT SELECT value FROM read_parquet('ext.parquet'))",
                "ts > (SELECT lim FROM cfg INTERSECT SELECT value FROM read_parquet('ext.parquet'))"
        );
    }

    @Test
    public void testRejectsPlainExternalSource() throws Exception {
        assertRejected(
                "ts > (SELECT value FROM read_parquet('ext.parquet'))",
                "ts > (SELECT value FROM read_parquet('ext.parquet') LIMIT 1)",
                "ts > (SELECT value FROM read_parquet('ext.parquet') ORDER BY value LIMIT 1)",
                "ts > (SELECT DISTINCT value FROM read_parquet('ext.parquet'))",
                "ts > (SELECT value FROM read_parquet('ext.parquet') WHERE value > 0)"
        );
    }

    private static void encodeTable(CharSequence tableName, CharSequence fileName) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(fileName);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }

    private void assertRejected(String... predicates) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ext AS (SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP AS value FROM long_sequence(3))");
            encodeTable("ext", "ext.parquet");
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE cfg (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cfg VALUES ('2024-01-01T00:00:00Z', 'a', '2024-01-01T00:00:00Z')");
            drainWalQueue();
            for (String predicate : predicates) {
                final String view = "mv_ext_" + VIEW_SEQ.incrementAndGet();
                final String sql = "CREATE MATERIALIZED VIEW " + view + " AS SELECT ts, sum(v) AS s FROM base WHERE "
                        + predicate + " SAMPLE BY 1h";
                try {
                    execute(sql);
                    throw new AssertionError(
                            "materialized view must reject a sub-query reading an external source; "
                                    + "the external-source property most likely failed to propagate "
                                    + "through a wrapping factory.\n  predicate: " + predicate);
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable e) {
                    final String message = String.valueOf(e.getMessage());
                    if (!message.contains(EXPECTED)) {
                        throw new AssertionError(
                                "expected the materialized-view guard to reject this predicate, but it "
                                        + "failed for an unrelated reason.\n  predicate: " + predicate
                                        + "\n  error: " + message, e);
                    }
                }
            }
        });
    }
}
