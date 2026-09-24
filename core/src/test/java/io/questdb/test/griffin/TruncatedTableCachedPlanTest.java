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

package io.questdb.test.griffin;

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TruncatedTableCachedPlanTest extends AbstractCairoTest {

    @Test
    public void testNonWalTruncateInvalidatesCachedPlans() throws Exception {
        assertTruncateInvalidatesCachedPlans("BYPASS WAL", "");
    }

    @Test
    public void testNonWalTruncateKeepingSymbolMapsInvalidatesCachedPlans() throws Exception {
        assertTruncateInvalidatesCachedPlans("BYPASS WAL", " KEEP SYMBOL MAPS");
    }

    @Test
    public void testWalTruncateInvalidatesCachedPlans() throws Exception {
        assertTruncateInvalidatesCachedPlans("WAL", "");
    }

    private void assertTruncateInvalidatesCachedPlans(String walClause, String truncateClause) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL INDEX, status SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walClause);
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final String[] queries = {
                    "SELECT g, v FROM t",
                    "SELECT g, v FROM t WHERE g = 'aa'",
                    "SELECT g, v FROM t WHERE status = 'target'",
                    "SELECT g, v FROM t WHERE g = 'aa' LATEST ON ts PARTITION BY g",
                    "SELECT g, v FROM t WHERE g IN ('aa', 'bb') LATEST ON ts PARTITION BY g",
                    "SELECT g, v FROM t WHERE status = 'target' LATEST ON ts PARTITION BY g",
            };
            final String[] before = {
                    "g\tv\naa\t10\nbb\t20\n",
                    "g\tv\naa\t10\n",
                    "g\tv\naa\t10\n",
                    "g\tv\naa\t10\n",
                    "g\tv\naa\t10\nbb\t20\n",
                    "g\tv\naa\t10\n",
            };
            final String[] after = {
                    "g\tv\ncc\t30\ndd\t40\naa\t50\nbb\t60\n",
                    "g\tv\naa\t50\n",
                    "g\tv\naa\t50\nbb\t60\n",
                    "g\tv\naa\t50\n",
                    "g\tv\naa\t50\nbb\t60\n",
                    "g\tv\naa\t50\nbb\t60\n",
            };
            for (int i = 0; i < queries.length; i++) {
                execute("TRUNCATE TABLE t");
                execute("""
                        INSERT INTO t VALUES
                        ('aa', 'target', 10, '2024-01-01T00:00:01Z'),
                        ('bb', 'other', 20, '2024-01-01T00:00:02Z')
                        """);
                drainWalQueue();
                try (RecordCursorFactory factory = select(queries[i])) {
                    assertFactory(factory).withContext(sqlExecutionContext).sizeMayVary().returns(before[i]);
                    execute("TRUNCATE TABLE t" + truncateClause);
                    execute("""
                            INSERT INTO t VALUES
                            ('cc', 'other', 30, '2024-01-01T00:00:03Z'),
                            ('dd', 'other', 40, '2024-01-01T00:00:04Z'),
                            ('aa', 'target', 50, '2024-01-01T00:00:05Z'),
                            ('bb', 'target', 60, '2024-01-01T00:00:06Z')
                            """);
                    drainWalQueue();
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail(queries[i]);
                    } catch (TableReferenceOutOfDateException ignore) {
                    }
                    assertQuery(queries[i]).sizeMayVary().returns(after[i]);
                }
            }
        });
    }
}
