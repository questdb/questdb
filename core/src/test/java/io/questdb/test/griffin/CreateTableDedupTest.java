/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class CreateTableDedupTest extends AbstractGriffinTest {

    @Test
    public void testAlterTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            compile("create table a (ts timestamp, i int, s symbol, l long) timestamp(ts) partition by day wal");
            String alterPrefix = "alter table a";

            assertCreate(alterPrefix + " deduplicate UPSERT KEYS(l);", "UPSERT KEYS(l)", "deduplicate key column can only be INT or SYMBOL type");
            assertCreate(alterPrefix + " deduplicate UPSERT KEYS", "UPSERT KEYS", "deduplication column list expected");
            assertCreate(alterPrefix + " deduplicate UPSERT KEYS (;", "UPSERT KEYS (;", "literal expected");
            assertCreate(alterPrefix + " deduplicate UPSERT KEYS (a)", "UPSERT KEYS (a)", "deduplicate column not found ");
            assertCreate(alterPrefix + " deduplicate UPSERT KEYS (s)", "UPSERT KEYS (s)", "deduplicate key list must include dedicated timestamp column");
            assertCreate(alterPrefix + " deduplicate KEYS (s);", "deduplicate KEYS ", "expected 'upsert'");
            assertCreate(alterPrefix + " deduplicate UPSERT (s);", "deduplicate UPSERT (", "expected 'keys'");
            assertCreate(alterPrefix + " deduplicate UPSERT KEYS", "UPSERT KEYS", "column list expected");
        });
    }

    @Test
    public void testCreateTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            String createPrefix = "create table a (ts timestamp, i int, s symbol, l long)";
            assertCreate(createPrefix + " timestamp(ts) partition by day bypass wal deduplicate UPSERT KEYS(l);", "deduplicate ", "deduplication is possible only on WAL tables");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (l);", "KEYS (l", "deduplicate key column can only be INT or SYMBOL type [column=l, type=LONG]");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (;", "KEYS (;", "literal expected");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (a);", "KEYS (a", "deduplicate column not found [column=a]");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (s);", "KEYS (s)", "deduplicate key list must include dedicated timestamp column");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate KEYS (s);", "deduplicate ", "expected 'upsert'");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT (s);", "UPSERT (", "expected 'keys'");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS", "UPSERT KEYS", "column list expected");
        });
    }

    @Test
    public void testDedupEnabledTimestampOnly() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
            }
        });
    }

    @Test
    @Ignore
    public void testDeduplicationEnabledIntAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol, i int) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS (ts, i, s ) "
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
                Assert.assertTrue(writer.getMetadata().isDedupKey(3));
            }
        });
    }

    @Test
    public void testDeduplicationEnabledTimestampAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS(ts, s)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }
        });
    }

    @Test
    public void testDeduplicationEnabledTimestampOnly() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS (ts)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
            }
        });
    }

    @Test
    public void testDisableDedupOnTable() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts,s)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }

            compile("ALTER table " + tableName + " dedup disable");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertFalse(writer.isDeduplicationEnabled());
                Assert.assertFalse(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }

            compile("ALTER table " + tableName + " dedup UPSERT KEYS(ts)");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }

            compile("ALTER table " + tableName + " dedup disable");
            compile("ALTER table " + tableName + " drop column x");
            compile("ALTER table " + tableName + " dedup UPSERT KEYS(ts,s)");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }

            compile("ALTER table " + tableName + " dedup disable");
            compile("ALTER table " + tableName + " drop column s");
            try {
                compile("ALTER table " + tableName + " dedup UPSERT KEYS(ts,s)");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "deduplicate column not found [column=s]");
            }
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertFalse(writer.isDeduplicationEnabled());
                Assert.assertFalse(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }
        });
    }

    private static void assertCreate(String alterStmt, String errorAfter, String expected) {
        int pos = alterStmt.indexOf(errorAfter);
        assert pos > -1;
        int errorPos = pos + errorAfter.length();
        try {
            compile(alterStmt);
            Assert.fail("expected SQLException is not thrown");
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), expected);
            Assert.assertEquals(errorPos, ex.getPosition());
        }
    }
}
