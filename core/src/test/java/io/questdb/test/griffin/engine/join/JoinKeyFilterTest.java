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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinKeyFilter;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The INNER hash join that reads a CROSS join as its master hands the CROSS join its hash table, see
 * {@link JoinKeyFilter}, and the CROSS join skips the master rows whose key the table does not have. The
 * hash join reads the rows of the CROSS join one by one, and the tests call the other methods of both
 * sides as a parent of either side may.
 */
public class JoinKeyFilterTest extends AbstractCairoTest {

    @Test
    public void testCrossJoinCountsAndSkipsRowsThatKeyFilterKeeps() throws Exception {
        assertMemoryLeak(() -> {
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("y", ColumnType.LONG));
            try (CrossJoinRecordCursorFactory factory = new CrossJoinRecordCursorFactory(
                    metadata,
                    select("SELECT x FROM long_sequence(4)"),
                    select("SELECT x y FROM long_sequence(3)"),
                    1
            )) {
                // the hash table holds the even keys: master rows 2 and 4 join each of the 3 slave rows
                factory.setJoinKeyFilter(record -> record.getLong(0) % 2 == 0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // the filter decides which rows the cursor returns as it reads them
                    Assert.assertEquals(-1, cursor.size());
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(6, counter.get());

                    // skipping 4 rows leaves the rows of master row 4 with slave rows 2 and 3
                    cursor.toTop();
                    counter.set(4);
                    cursor.skipRows(counter, Long.MAX_VALUE);
                    Assert.assertEquals(0, counter.get());
                    final StringSink rows = new StringSink();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        rows.put(record.getLong(0)).put(',').put(record.getLong(1)).put('\n');
                    }
                    TestUtils.assertEquals("4,2\n4,3\n", rows);

                    // a skip past the last row leaves the rows that the cursor does not have
                    cursor.toTop();
                    counter.set(10);
                    cursor.skipRows(counter, Long.MAX_VALUE);
                    Assert.assertEquals(4, counter.get());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testHashJoinMatchesEveryKeyAfterItSwapsSides() throws Exception {
        // The light hash join hashes the smaller side, so a master with fewer rows than the slave swaps the
        // sides, and the table holds the keys of the master. It then cannot rule a key out, and reads no
        // column of the record. SqlCodeGenerator hands the table only to a CROSS or nested loop LEFT join,
        // whose rows have no random access, so the join never swaps then.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT)");
            execute("CREATE TABLE b (x INT)");
            execute("INSERT INTO a VALUES (2)");
            execute("INSERT INTO b VALUES (1), (2), (3)");
            try (RecordCursorFactory factory = select("SELECT a.x, b.x FROM a JOIN b ON a.x = b.x")) {
                final JoinKeyFilter filter = ((HashJoinLightRecordCursorFactory) unwrap(factory)).getJoinKeyFilter();
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // the first row builds the table
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(filter.hasMatch(null));
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testHashJoinMatchesEveryKeyBeforeItReadsItsSlave() throws Exception {
        // The hash table is empty until the join reads its slave, so it cannot rule a key out yet. It reads
        // no column of the record then.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (x INT)");
            execute("CREATE TABLE b (x INT)");
            execute("INSERT INTO a VALUES (1), (2)");
            execute("INSERT INTO b VALUES (1)");
            for (boolean isFullFat : new boolean[]{false, true}) {
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    compiler.setFullFatJoins(isFullFat);
                    try (RecordCursorFactory factory = select(compiler, "SELECT a.x, b.x FROM a JOIN b ON a.x = b.x", sqlExecutionContext)) {
                        final RecordCursorFactory join = unwrap(factory);
                        final JoinKeyFilter filter;
                        if (isFullFat) {
                            filter = ((HashJoinRecordCursorFactory) join).getJoinKeyFilter();
                        } else {
                            filter = ((HashJoinLightRecordCursorFactory) join).getJoinKeyFilter();
                        }
                        Assert.assertTrue(filter.hasMatch(null));
                    }
                }
            }
        });
    }

    private static RecordCursorFactory unwrap(RecordCursorFactory factory) {
        RecordCursorFactory join = factory;
        while (join instanceof QueryProgress || join instanceof SelectedRecordCursorFactory) {
            join = join.getBaseFactory();
        }
        return join;
    }
}
