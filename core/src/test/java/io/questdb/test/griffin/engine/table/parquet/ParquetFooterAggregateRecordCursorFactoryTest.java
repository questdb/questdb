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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.table.ParquetFooterAggregateRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ParquetFooterAggregateRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testMinMaxOverManyRowGroups() throws Exception {
        // Forces several row groups so the global-min logic has work to do
        // across more than the first one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (" +
                    "  SELECT timestamp_sequence(1_000_000_000L, 1_000L) AS ts, x AS id " +
                    "  FROM long_sequence(50_000)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            try (
                    Path path = new Path();
                    PartitionDescriptor pd = new PartitionDescriptor();
                    TableReader reader = engine.getReader("m")
            ) {
                path.of(root).concat("m.parquet");
                PartitionEncoder.populateFromTableReader(reader, pd, 0);
                PartitionEncoder.encode(pd, path);
                Assert.assertTrue(Files.exists(path.$()));

                final GenericRecordMetadata out = new GenericRecordMetadata();
                out.add(new TableColumnMetadata("min_ts", ColumnType.TIMESTAMP_MICRO));
                out.add(new TableColumnMetadata("max_ts", ColumnType.TIMESTAMP_MICRO));

                try (
                        RecordCursorFactory factory = new ParquetFooterAggregateRecordCursorFactory(
                                path, out, new boolean[]{false, true}
                        );
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertTrue("cursor must emit exactly one row", cursor.hasNext());
                    final Record rec = cursor.getRecord();
                    // 50 000 rows, step 1 000 us, start 1 000 000 000 us:
                    Assert.assertEquals(
                            "min_ts must equal the first timestamp",
                            1_000_000_000L, rec.getTimestamp(0)
                    );
                    Assert.assertEquals(
                            "max_ts must equal the last timestamp",
                            1_000_000_000L + 1_000L * (50_000 - 1), rec.getTimestamp(1)
                    );
                    Assert.assertFalse("cursor must not emit a second row", cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testMinOnlyAndMaxOnly() throws Exception {
        // aggregateKinds with a single column - both flavours.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s AS (" +
                    "  SELECT timestamp_sequence(2_000_000_000L, 500L) AS ts FROM long_sequence(10_000)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            try (
                    Path path = new Path();
                    PartitionDescriptor pd = new PartitionDescriptor();
                    TableReader reader = engine.getReader("s")
            ) {
                path.of(root).concat("s.parquet");
                PartitionEncoder.populateFromTableReader(reader, pd, 0);
                PartitionEncoder.encode(pd, path);

                // MIN only.
                {
                    final GenericRecordMetadata out = new GenericRecordMetadata();
                    out.add(new TableColumnMetadata("min_ts", ColumnType.TIMESTAMP_MICRO));
                    try (
                            RecordCursorFactory f = new ParquetFooterAggregateRecordCursorFactory(
                                    path, out, new boolean[]{false}
                            );
                            RecordCursor c = f.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertTrue(c.hasNext());
                        Assert.assertEquals(2_000_000_000L, c.getRecord().getTimestamp(0));
                        Assert.assertFalse(c.hasNext());
                    }
                }

                // MAX only.
                {
                    final GenericRecordMetadata out = new GenericRecordMetadata();
                    out.add(new TableColumnMetadata("max_ts", ColumnType.TIMESTAMP_MICRO));
                    try (
                            RecordCursorFactory f = new ParquetFooterAggregateRecordCursorFactory(
                                    path, out, new boolean[]{true}
                            );
                            RecordCursor c = f.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertTrue(c.hasNext());
                        Assert.assertEquals(
                                2_000_000_000L + 500L * (10_000 - 1),
                                c.getRecord().getTimestamp(0)
                        );
                        Assert.assertFalse(c.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testToTopReplaysSingleRow() throws Exception {
        // Cursor reuse: the planner / executor may toTop a cursor and replay
        // it. The single-row contract must hold across replays without
        // re-reading the file.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE r AS (" +
                    "  SELECT timestamp_sequence(0L, 100L) AS ts FROM long_sequence(1_000)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            try (
                    Path path = new Path();
                    PartitionDescriptor pd = new PartitionDescriptor();
                    TableReader reader = engine.getReader("r")
            ) {
                path.of(root).concat("r.parquet");
                PartitionEncoder.populateFromTableReader(reader, pd, 0);
                PartitionEncoder.encode(pd, path);

                final GenericRecordMetadata out = new GenericRecordMetadata();
                out.add(new TableColumnMetadata("min_ts", ColumnType.TIMESTAMP_MICRO));
                out.add(new TableColumnMetadata("max_ts", ColumnType.TIMESTAMP_MICRO));

                try (
                        RecordCursorFactory f = new ParquetFooterAggregateRecordCursorFactory(
                                path, out, new boolean[]{false, true}
                        );
                        RecordCursor c = f.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertTrue(c.hasNext());
                    final long firstMin = c.getRecord().getTimestamp(0);
                    final long firstMax = c.getRecord().getTimestamp(1);
                    Assert.assertFalse(c.hasNext());

                    c.toTop();
                    Assert.assertTrue("toTop must replay the single row", c.hasNext());
                    Assert.assertEquals(firstMin, c.getRecord().getTimestamp(0));
                    Assert.assertEquals(firstMax, c.getRecord().getTimestamp(1));
                    Assert.assertFalse(c.hasNext());
                }
            }
        });
    }

    @Test
    public void testSizeReportsExactlyOne() throws Exception {
        // Pins the size() contract: this is a 1-row factory; the outer
        // executor must be able to use that for buffer sizing without
        // iterating.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE z AS (" +
                    "  SELECT timestamp_sequence(0L, 100L) AS ts FROM long_sequence(100)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            try (
                    Path path = new Path();
                    PartitionDescriptor pd = new PartitionDescriptor();
                    TableReader reader = engine.getReader("z")
            ) {
                path.of(root).concat("z.parquet");
                PartitionEncoder.populateFromTableReader(reader, pd, 0);
                PartitionEncoder.encode(pd, path);

                final GenericRecordMetadata out = new GenericRecordMetadata();
                out.add(new TableColumnMetadata("min_ts", ColumnType.TIMESTAMP_MICRO));

                try (
                        RecordCursorFactory f = new ParquetFooterAggregateRecordCursorFactory(
                                path, out, new boolean[]{false}
                        );
                        RecordCursor c = f.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertEquals(1L, c.size());
                }
            }
        });
    }
}
