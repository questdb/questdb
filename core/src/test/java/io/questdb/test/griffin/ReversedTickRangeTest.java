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

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.TestTimestampType;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ReversedTickRangeTest extends AbstractCairoTest {
    private static final String REVERSED_NOW = "2026-03-15T12:00:00.000000Z";
    private static final String VALID_NOW = "2026-04-15T12:00:00.000000Z";
    private final byte partitionFormat;
    private final TestTimestampType timestampType;

    public ReversedTickRangeTest(byte partitionFormat, TestTimestampType timestampType) {
        this.partitionFormat = partitionFormat;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "format={0},ts={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {PartitionFormat.NATIVE, TestTimestampType.MICRO},
                {PartitionFormat.NATIVE, TestTimestampType.NANO},
                {PartitionFormat.PARQUET, TestTimestampType.MICRO},
                {PartitionFormat.PARQUET, TestTimestampType.NANO}
        });
    }

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Test
    public void testCachedRangeReversal() throws Exception {
        assertMemoryLeak(() -> {
            createTable("MONTH");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp(VALID_NOW));
            final String predicate = " WHERE ts IN '$now-1M..$now-30d'";
            try (
                    RecordCursorFactory forward = select("SELECT v FROM x" + predicate);
                    RecordCursorFactory backward = select("SELECT v FROM x" + predicate + " ORDER BY ts DESC");
                    RecordCursorFactory count = select("SELECT count() FROM x" + predicate)
            ) {
                QueryAssertion forwardAssertion = new QueryAssertion(engine, forward).withContext(sqlExecutionContext);
                QueryAssertion backwardAssertion = new QueryAssertion(engine, backward).withContext(sqlExecutionContext);
                QueryAssertion countAssertion = new QueryAssertion(engine, count).withContext(sqlExecutionContext).expectSize().noRandomAccess();

                forwardAssertion.returns("v\n7\n8\n9\n");
                backwardAssertion.returns("v\n9\n8\n7\n");
                countAssertion.returns("count\n3\n");

                // The same factories now evaluate [Feb 15, Feb 13], with rows between the bounds.
                setCurrentMicros(MicrosFormatUtils.parseTimestamp(REVERSED_NOW));
                forwardAssertion.returns("v\n");
                backwardAssertion.returns("v\n");
                countAssertion.returns("count\n0\n");

                setCurrentMicros(MicrosFormatUtils.parseTimestamp(VALID_NOW));
                forwardAssertion.returns("v\n7\n8\n9\n");
                backwardAssertion.returns("v\n9\n8\n7\n");
                countAssertion.returns("count\n3\n");
            }
        });
    }

    @Test
    public void testEqualBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            assertRange("ts IN '$now..$now'", "v\n7\n", "v\n7\n", 1);
            assertRange("ts IN '$now-1h..$now'", "v\n5\n6\n7\n", "v\n7\n6\n5\n", 3);
        });
    }

    @Test
    public void testNegatedReversedBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            assertRange("ts NOT IN '$now..$now-1h'", "v\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", "v\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n", 10);
            assertRange("ts NOT IN '$now..$now-1h' AND ts IN '2026-03-15'", "v\n4\n5\n6\n7\n8\n", "v\n8\n7\n6\n5\n4\n", 5);
            assertRange("ts IN '$now..$now-1h' AND ts NOT IN '$now..$now-2h'", "v\n", "v\n", 0);
            assertRange("ts NOT IN '$now..$now-2h' AND ts IN '$now..$now-1h'", "v\n", "v\n", 0);
        });
    }

    @Test
    public void testReversedBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            assertRange("ts IN '$now..$now-1h'", "v\n", "v\n", 0);
        });
    }

    @Test
    public void testReversedSetOperations() throws Exception {
        assertMemoryLeak(() -> {
            createTable("DAY");
            assertRange("ts IN '[$now-2h, $now..$now-1h, $now+1h]'", "v\n4\n8\n", "v\n8\n4\n", 2);
            assertRange("ts IN '$now..$now-1h' AND ts > '2026-03-01'", "v\n", "v\n", 0);
        });
    }

    private void assertRange(String predicate, String expectedForward, String expectedBackward, long expectedCount) throws Exception {
        assertQuery("SELECT v FROM x WHERE " + predicate)
                .noLeakCheck()
                .withPlanContaining("Interval forward scan")
                .returns(expectedForward);
        assertQuery("SELECT v FROM x WHERE " + predicate + " ORDER BY ts DESC")
                .noLeakCheck()
                .withPlanContaining("Interval backward scan")
                .returns(expectedBackward);
        assertQuery("SELECT count() FROM x WHERE " + predicate)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expectedCount + "\n");
    }

    private void createTable(String partitionBy) throws Exception {
        setCurrentMicros(MicrosFormatUtils.parseTimestamp(REVERSED_NOW));
        execute("CREATE TABLE x (v INT, ts " + timestampType.getTypeName() + ") TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
        execute("""
                INSERT INTO x VALUES
                    (1, '2026-02-13T12:00:00.000000Z'),
                    (2, '2026-02-14T12:00:00.000000Z'),
                    (3, '2026-02-15T12:00:00.000000Z'),
                    (4, '2026-03-15T10:00:00.000000Z'),
                    (5, '2026-03-15T11:00:00.000000Z'),
                    (6, '2026-03-15T11:30:00.000000Z'),
                    (7, '2026-03-15T12:00:00.000000Z'),
                    (8, '2026-03-15T13:00:00.000000Z'),
                    (9, '2026-03-16T12:00:00.000000Z'),
                    (10, '2026-04-01T00:00:00.000000Z')
                """);
        if (partitionFormat == PartitionFormat.PARQUET) {
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2026-04-01'");
        }
    }
}
