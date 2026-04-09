/*+******************************************************************************
 *  Copyright (c) 2019-2026 QuestDB
 *  Licensed under the Apache License, Version 2.0
 ******************************************************************************/

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SampleByFillTest extends AbstractCairoTest {

    @Test
    public void testFillNullNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            // Data at 00:00, 02:00, 04:00 — gaps at 01:00, 03:00
            assertSql(
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            null\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            null\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillPrevNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                    "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            assertSql(
                    """
                            sum\tts
                            1.0\t2024-01-01T00:00:00.000000Z
                            1.0\t2024-01-01T01:00:00.000000Z
                            2.0\t2024-01-01T02:00:00.000000Z
                            2.0\t2024-01-01T03:00:00.000000Z
                            3.0\t2024-01-01T04:00:00.000000Z
                            """,
                    "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testFillNullKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::DOUBLE AS val, " +
                    "       ('k' || (x % 2))::SYMBOL AS key, " +
                    "       timestamp_sequence('2024-01-01', 3_600_000_000) AS ts " +
                    "FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY");
            // k1 at 00:00, 02:00; k0 at 01:00, 03:00
            printSql("EXPLAIN SELECT ts, key, sum(val) FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR");
            System.out.println("=== PLAN ===");
            System.out.println(sink);

            printSql("SELECT ts, key, sum(val) FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR");
            System.out.println("=== RESULT ===");
            System.out.println(sink);
        });
    }
}
