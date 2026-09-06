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

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code FORMAT PARQUET} at CREATE on a composite table -- the DDL entry point.
 * <p>
 * This class used to assert a REFUSAL, and its history is the more useful half. FORMAT PARQUET on a
 * composite table was first ACCEPTED at CREATE and then suspended the table on the first INSERT
 * through a writer-side gate: a user got a successful DDL and a table that held zero rows while
 * {@code SHOW CREATE TABLE} still advertised FORMAT PARQUET. The fix at the time was to move the
 * refusal to the statement, which is where a refusal belongs -- an apply-time throw on a WAL table
 * arrives detached from the statement that caused it.
 * <p>
 * The feature is now supported, so the refusal is gone and this class asserts the DDL is accepted and
 * the table actually takes rows. Deep behaviour over born-parquet cells -- O3 merge, DROP PARTITION,
 * DEDUP upsert, CONVERT TO NATIVE, ADD COLUMN, each against a plain FORMAT PARQUET twin -- lives in
 * {@link CompositeFormatParquetTest}. What is covered HERE is only that the statement is accepted and
 * is not accepted vacuously.
 */
public class CompositeFormatParquetCreateTest extends AbstractCairoTest {

    /**
     * A composite table declared FORMAT PARQUET is created and works.
     * <p>
     * The insert and count matter as much as the CREATE: accepting the DDL and then failing on the
     * first row is the exact defect this class was originally written about, so "no exception from
     * CREATE" alone would re-admit it.
     */
    @Test(timeout = 60_000)
    public void testCreateCompositeWithFormatParquetIsAcceptedAndWorks() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch LAYOUT PLAIN FORMAT PARQUET WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','E1',2.0)");
            drainWalQueue();
            printSql("SELECT count() FROM c");
            Assert.assertEquals("count\n2\n", sink.toString());
        });
    }

    /**
     * The plain twin must be unaffected: FORMAT PARQUET without dimensions is a supported feature and
     * must keep working. Without this the test above could be satisfied by refusing FORMAT PARQUET
     * everywhere.
     */
    @Test(timeout = 60_000)
    public void testFormatParquetStillWorksOnAPlainTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY FORMAT PARQUET WAL");
            execute("INSERT INTO p VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0)");
            drainWalQueue();
            printSql("SELECT count() FROM p");
            Assert.assertEquals("count\n1\n", sink.toString());
        });
    }
}
