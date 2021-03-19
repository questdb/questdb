/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import org.junit.Assert;
import org.junit.Test;

public class OutOfOrderPartitionAccessTest extends AbstractGriffinTest {
    @Test
    public void testPartitionAccess() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as (select rnd_int() a, timestamp_sequence(0, 2000000) ts from long_sequence(100000)) timestamp(ts) partition by DAY", sqlExecutionContext);
            long timestamp = TimestampFormatUtils.parseTimestamp("1970-01-02T00:00:00.000000Z");
            long txn = -1;
            try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                // we should be able to lock partition if reader is absent
                Assert.assertTrue(w.reconcileAttachedPartitionsWithScoreboard());
                Assert.assertTrue(w.acquireWriterLock(timestamp, txn));
                w.releaseWriterLock(timestamp, txn);

                // likewise we should be able to lock partition when read is there but doesn't do anything
                try (TableReader ignored = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                    Assert.assertTrue(w.acquireWriterLock(timestamp, txn));
                    w.releaseWriterLock(timestamp, txn);
                }

                // now lets query that partition only
                try (RecordCursorFactory factory = compiler.compile("x where ts = '1970-01-02'", sqlExecutionContext).getRecordCursorFactory()) {
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                    long sum = 0;
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        sum += record.getInt(0);
                    }
                    Assert.assertTrue(sum != 0);

                    // we should not be able to lock this from writer
                    Assert.assertFalse(w.acquireWriterLock(timestamp, txn));

                    // but we should still be able to lock partition reader isn't using
                    Assert.assertTrue(w.acquireWriterLock(TimestampFormatUtils.parseTimestamp("1970-01-03T00:00:00.000000Z"), txn));
                    w.releaseWriterLock(timestamp, txn);

                    // when we close cursor we should be able to lock the partition again
                    cursor.close();
                    Assert.assertTrue(w.acquireWriterLock(timestamp, txn));
                    w.releaseWriterLock(timestamp, txn);

                    // now open cursor again and this should lock single partition
                    cursor = factory.getCursor(sqlExecutionContext);
                    while (cursor.hasNext()) {
                        sum += record.getInt(0);
                    }
                    Assert.assertTrue(sum != 0);

                    // we must not be able to lock it
                    Assert.assertFalse(w.acquireWriterLock(timestamp, txn));

                    // and if we close cursor again we should unlock it
                    cursor.close();
                    Assert.assertTrue(w.acquireWriterLock(timestamp, txn));
                    w.releaseWriterLock(timestamp, txn);
                }
            }
        });
    }
}
