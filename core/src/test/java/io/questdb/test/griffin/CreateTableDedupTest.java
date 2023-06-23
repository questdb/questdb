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
import org.junit.Test;

public class CreateTableDedupTest extends AbstractGriffinTest {

    @Test
    public void testAlterTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            String createPrefix = "create table a (ts timestamp, i int, s symbol, l long)";
            assertCreate(createPrefix + " timestamp(ts) partition by day bypass wal deduplicate(l);", "deduplication is possible only on WAL tables");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate(l);", "deduplicate key column can only be INT or SYMBOL type [column=l, type=LONG]");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate(;", "literal expected");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate(a);", "deduplicate column not found [column=a]");
        });
    }

    @Test
    public void testDedupEnabledTimestampOnly() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUP"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
            }
        });
    }

    @Test
    public void testDeduplicationEnabledIntAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol, i int) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE(i, s ) "
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
                            " PARTITION BY DAY WAL DEDUPLICATE(s)"
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
                            " PARTITION BY DAY WAL DEDUPLICATE"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
            }
        });
    }

    private static void assertCreate(String alterStmt, String expected) {
        try {
            compile(alterStmt);
            Assert.fail("expected SQLException is not thrown");
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), expected);
        }
    }
}
