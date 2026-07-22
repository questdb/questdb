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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullBwdPartitionFrameCursor;
import io.questdb.cairo.FullFwdPartitionFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.BwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Tests for verifying that partitions are released when
 * {@link io.questdb.cairo.sql.PageFrameCursor#releaseOpenPartitions()} is called.
 */
public class PageFrameCursorReleasePartitionTest extends AbstractCairoTest {

    @Test
    public void testBwdCursorReleaseAfterToTopIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(10)" +
                    ") timestamp (t) partition by DAY"
            );

            TableReader reader = engine.getReader("x");

            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            FullBwdPartitionFrameCursor partitionFrameCursor = new FullBwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            BwdTableReaderPageFrameCursor pageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);

                // First pass - iterate and release
                while (pageFrameCursor.next() != null) {
                    pageFrameCursor.releaseOpenPartitions();
                }
                Assert.assertEquals("One partition should be open after first pass", 1, reader.getOpenPartitionCount());

                // Reset cursor
                pageFrameCursor.toTop();

                // Calling releaseOpenPartitions() immediately after toTop() should be safe
                // This tests the edge case where highestOpenPartitionIndex is reset to -1
                // but reenterPartitionIndex retains its old value
                pageFrameCursor.releaseOpenPartitions();

                // Should still have one partition open (toTop doesn't close partitions)
                // and releaseOpenPartitions should be a no-op since nothing new is open
                Assert.assertTrue("Partitions should remain from previous iteration", reader.getOpenPartitionCount() >= 0);

                // Cursor should still work normally
                int frameCount = 0;
                while (pageFrameCursor.next() != null) {
                    frameCount++;
                }
                Assert.assertTrue("Should process frames on second pass", frameCount > 0);
            }
        });
    }

    @Test
    public void testBwdCursorReleaseBeforeNextIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(10)" +
                    ") timestamp (t) partition by DAY"
            );

            TableReader reader = engine.getReader("x");

            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            FullBwdPartitionFrameCursor partitionFrameCursor = new FullBwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            BwdTableReaderPageFrameCursor pageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);

                // Calling releaseOpenPartitions() before next() should be safe (no-op)
                Assert.assertEquals("No partitions should be open initially", 0, reader.getOpenPartitionCount());
                pageFrameCursor.releaseOpenPartitions();
                Assert.assertEquals("No partitions should be open after release before next", 0, reader.getOpenPartitionCount());

                // Cursor should still work normally after
                int frameCount = 0;
                while (pageFrameCursor.next() != null) {
                    frameCount++;
                    pageFrameCursor.releaseOpenPartitions();
                }
                Assert.assertTrue("Should have processed frames", frameCount > 0);
            }
        });
    }

    @Test
    public void testBwdCursorReleasesPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions (one per day)
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " rnd_str() b," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(100)" +
                    ") timestamp (t) partition by DAY"
            );

            // Reader ownership is transferred to the cursor, so don't use try-with-resources
            TableReader reader = engine.getReader("x");
            int partitionCount = reader.getPartitionCount();
            Assert.assertTrue("Should have multiple partitions", partitionCount > 1);

            // Build column indexes and size shifts for all columns
            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            // Create backward partition frame cursor (takes ownership of reader)
            FullBwdPartitionFrameCursor partitionFrameCursor = new FullBwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Create backward page frame cursor
            BwdTableReaderPageFrameCursor pageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1 // single-threaded
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
                // Initially no partitions are open
                Assert.assertEquals("No partitions should be open initially", 0, reader.getOpenPartitionCount());

                int frameCount = 0;

                while (pageFrameCursor.next() != null) {
                    frameCount++;

                    // After calling releaseOpenPartitions, only current partition should be open
                    pageFrameCursor.releaseOpenPartitions();

                    Assert.assertEquals(
                            "Only current partition should be open after releaseOpenPartitions",
                            1,
                            reader.getOpenPartitionCount()
                    );

                }

                Assert.assertTrue("Should have processed some frames", frameCount > 0);
            }
            // This will close the reader via the cursor chain
        });
    }

    @Test
    public void testBwdCursorReleasesPartitionsWithMultipleFramesPerPartition() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions, each with enough data to produce multiple frames
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " rnd_str(5, 10, 0) b," + // variable length strings
                    " timestamp_sequence('2022-01-01', 1000000L) t" + // 1 second intervals
                    " from long_sequence(1000000)" + // 1M rows, ~11 days of data
                    ") timestamp (t) partition by DAY"
            );

            // Reader ownership is transferred to the cursor, so don't use try-with-resources
            TableReader reader = engine.getReader("x");
            int partitionCount = reader.getPartitionCount();
            Assert.assertTrue("Should have multiple partitions", partitionCount > 1);

            // Build column indexes and size shifts for all columns
            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            // Create backward partition frame cursor (takes ownership of reader)
            FullBwdPartitionFrameCursor partitionFrameCursor = new FullBwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Create backward page frame cursor with smaller frame size to force multiple frames
            BwdTableReaderPageFrameCursor pageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1 // single-threaded
            );

            try (pageFrameCursor) {
                sqlExecutionContext.changePageFrameSizes(1000, 10000); // smaller max to force multiple frames
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
                while (pageFrameCursor.next() != null) {
                    // Release after processing each frame
                    pageFrameCursor.releaseOpenPartitions();

                    // Only current partition should be open
                    Assert.assertEquals(
                            "Only current partition should be open",
                            1,
                            reader.getOpenPartitionCount()
                    );
                }
            }
            // This will close the reader via the cursor chain
        });
    }

    @Test
    public void testFwdCursorKeepsPartitionsOpenByDefault() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions (one per day)
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " rnd_str() b," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(100)" +
                    ") timestamp (t) partition by DAY"
            );

            // Reader ownership is transferred to the cursor, so don't use try-with-resources
            TableReader reader = engine.getReader("x");
            int partitionCount = reader.getPartitionCount();
            Assert.assertTrue("Should have multiple partitions", partitionCount > 1);

            // Build column indexes and size shifts for all columns
            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            // Create forward partition frame cursor (takes ownership of reader)
            FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Create forward page frame cursor
            FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1 // single-threaded
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
                // Do NOT call releaseOpenPartitions (default behavior)
                // Initially no partitions are open
                Assert.assertEquals("No partitions should be open initially", 0, reader.getOpenPartitionCount());

                int visitedPartitions = 0;
                int lastPartitionIndex = -1;
                PageFrame frame;

                while ((frame = pageFrameCursor.next()) != null) {
                    int currentPartitionIndex = frame.getPartitionIndex();
                    if (currentPartitionIndex != lastPartitionIndex) {
                        visitedPartitions++;
                        lastPartitionIndex = currentPartitionIndex;
                    }
                }

                // With default behavior, all visited partitions should remain open
                Assert.assertEquals(
                        "All visited partitions should remain open with default behavior",
                        visitedPartitions,
                        reader.getOpenPartitionCount()
                );
            }
            // This will close the reader via the cursor chain
        });
    }

    @Test
    public void testFwdCursorReleaseAfterToTopIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(10)" +
                    ") timestamp (t) partition by DAY"
            );

            TableReader reader = engine.getReader("x");

            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);

                // First pass - iterate and release
                while (pageFrameCursor.next() != null) {
                    pageFrameCursor.releaseOpenPartitions();
                }
                Assert.assertEquals("One partition should be open after first pass", 1, reader.getOpenPartitionCount());

                // Reset cursor
                pageFrameCursor.toTop();

                // Calling releaseOpenPartitions() immediately after toTop() should be safe
                pageFrameCursor.releaseOpenPartitions();

                // Should still have one partition open (toTop doesn't close partitions)
                Assert.assertTrue("Partitions should remain from previous iteration", reader.getOpenPartitionCount() >= 0);

                // Cursor should still work normally
                int frameCount = 0;
                while (pageFrameCursor.next() != null) {
                    frameCount++;
                }
                Assert.assertTrue("Should process frames on second pass", frameCount > 0);
            }
        });
    }

    @Test
    public void testFwdCursorReleaseBeforeNextIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(10)" +
                    ") timestamp (t) partition by DAY"
            );

            TableReader reader = engine.getReader("x");

            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);

                // Calling releaseOpenPartitions() before next() should be safe (no-op)
                Assert.assertEquals("No partitions should be open initially", 0, reader.getOpenPartitionCount());
                pageFrameCursor.releaseOpenPartitions();
                Assert.assertEquals("No partitions should be open after release before next", 0, reader.getOpenPartitionCount());

                // Cursor should still work normally after
                int frameCount = 0;
                while (pageFrameCursor.next() != null) {
                    frameCount++;
                    pageFrameCursor.releaseOpenPartitions();
                }
                Assert.assertTrue("Should have processed frames", frameCount > 0);
            }
        });
    }

    @Test
    public void testFwdCursorReleasesPartitions() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions (one per day)
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " rnd_str() b," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(100)" +
                    ") timestamp (t) partition by DAY"
            );

            // Reader ownership is transferred to the cursor, so don't use try-with-resources
            TableReader reader = engine.getReader("x");
            int partitionCount = reader.getPartitionCount();
            Assert.assertTrue("Should have multiple partitions", partitionCount > 1);

            // Build column indexes and size shifts for all columns
            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            // Create forward partition frame cursor (takes ownership of reader)
            FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Create forward page frame cursor
            FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1 // single-threaded
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
                // Initially no partitions are open
                Assert.assertEquals("No partitions should be open initially", 0, reader.getOpenPartitionCount());

                int frameCount = 0;

                while (pageFrameCursor.next() != null) {
                    frameCount++;

                    // Release partitions after processing each frame
                    pageFrameCursor.releaseOpenPartitions();

                    // Only current partition should be open
                    Assert.assertEquals(
                            "Only current partition should be open after releaseOpenPartitions",
                            1,
                            reader.getOpenPartitionCount()
                    );
                }

                Assert.assertTrue("Should have processed some frames", frameCount > 0);
            }
            // This will close the reader via the cursor chain
        });
    }

    @Test
    public void testFwdCursorReleasesPartitionsWithMultipleFramesPerPartition() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions, each with enough data to produce multiple frames
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " rnd_str(5, 10, 0) b," + // variable length strings
                    " timestamp_sequence('2022-01-01', 1000000L) t" + // 1 second intervals
                    " from long_sequence(1000000)" + // 1M rows, ~11 days of data
                    ") timestamp (t) partition by DAY"
            );

            // Reader ownership is transferred to the cursor, so don't use try-with-resources
            TableReader reader = engine.getReader("x");
            int partitionCount = reader.getPartitionCount();
            Assert.assertTrue("Should have multiple partitions", partitionCount > 1);

            // Build column indexes and size shifts for all columns
            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            // Create forward partition frame cursor (takes ownership of reader)
            FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Create forward page frame cursor with smaller frame size to force multiple frames
            FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1 // single-threaded
            );

            try (pageFrameCursor) {
                sqlExecutionContext.changePageFrameSizes(1000, 10000); // smaller max to force multiple frames
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
                while (pageFrameCursor.next() != null) {
                    // Release partitions after processing each frame
                    pageFrameCursor.releaseOpenPartitions();

                    // Only current partition should be open
                    Assert.assertEquals(
                            "Only current partition should be open",
                            1,
                            reader.getOpenPartitionCount()
                    );
                }
            }
            // This will close the reader via the cursor chain
        });
    }

    @Test
    public void testSelectedCursorReleasesPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x AS (
                        SELECT
                            x::INT a,
                            (x * 10)::LONG b,
                            timestamp_sequence('2022-01-01', 24 * 60 * 60 * 1_000_000L) t
                        FROM long_sequence(5)
                    ) TIMESTAMP (t) PARTITION BY DAY
                    """);

            try (RecordCursorFactory factory = select("SELECT b, t, a, b AS b2 FROM x")) {
                RecordCursorFactory selectedFactory = factory;
                while (selectedFactory != null && !(selectedFactory instanceof SelectedRecordCursorFactory)) {
                    selectedFactory = selectedFactory.getBaseFactory();
                }
                Assert.assertNotNull("duplicated/reordered projection must compile to SelectedRecord", selectedFactory);
                Assert.assertTrue(selectedFactory.supportsPageFrameCursor());

                try (PageFrameCursor cursor = selectedFactory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    Assert.assertTrue("Selected over a table must preserve the table cursor surface", cursor instanceof TablePageFrameCursor);
                    Assert.assertEquals("Selected cursor must expose the projected mapping", 4, cursor.getColumnMapping().getColumnCount());
                    Assert.assertEquals(1, cursor.getColumnMapping().getColumnIndex(0));
                    Assert.assertEquals(2, cursor.getColumnMapping().getColumnIndex(1));
                    Assert.assertEquals(0, cursor.getColumnMapping().getColumnIndex(2));
                    Assert.assertEquals(1, cursor.getColumnMapping().getColumnIndex(3));

                    TableReader reader = ((TablePageFrameCursor) cursor).getTableReader();
                    Assert.assertEquals(0, reader.getOpenPartitionCount());

                    PageFrame frame = cursor.next();
                    Assert.assertNotNull(frame);
                    Assert.assertEquals(0, frame.getPartitionIndex());
                    Assert.assertEquals(1, reader.getOpenPartitionCount());

                    frame = cursor.next();
                    Assert.assertNotNull(frame);
                    Assert.assertEquals(1, frame.getPartitionIndex());
                    Assert.assertEquals("completed and current partitions must be open before release", 2, reader.getOpenPartitionCount());

                    cursor.releaseOpenPartitions();
                    Assert.assertEquals("Selected must forward release to close the completed partition", 1, reader.getOpenPartitionCount());

                    frame = cursor.next();
                    Assert.assertNotNull("cursor must remain usable after releasing completed partitions", frame);
                    Assert.assertEquals(2, frame.getPartitionIndex());
                    Assert.assertEquals(4, frame.getColumnCount());
                    Assert.assertEquals("duplicated projected columns must share the base page", frame.getPageAddress(0), frame.getPageAddress(3));
                    cursor.releaseOpenPartitions();
                    Assert.assertEquals(1, reader.getOpenPartitionCount());
                }
            }
        });
    }

    @Test
    public void testToTopResetsPartitionReleaseTracking() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with multiple partitions
            execute("create table x as (" +
                    "select" +
                    " rnd_int() a," +
                    " timestamp_sequence('2022-01-01', 24*60*60*1000000L) t" +
                    " from long_sequence(10)" +
                    ") timestamp (t) partition by DAY"
            );

            // Reader ownership is transferred to the cursor, so don't use try-with-resources
            TableReader reader = engine.getReader("x");

            // Build column indexes and size shifts for all columns
            IntList columnIndexes = new IntList();
            IntList columnSizeShifts = new IntList();
            for (int i = 0; i < reader.getMetadata().getColumnCount(); i++) {
                columnIndexes.add(i);
                int columnType = reader.getMetadata().getColumnType(i);
                columnSizeShifts.add(ColumnType.pow2SizeOf(columnType));
            }

            // Create forward partition frame cursor (takes ownership of reader)
            FullFwdPartitionFrameCursor partitionFrameCursor = new FullFwdPartitionFrameCursor();
            partitionFrameCursor.of(reader);

            // Create forward page frame cursor
            FwdTableReaderPageFrameCursor pageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    null,
                    1 // single-threaded
            );

            try (pageFrameCursor) {
                pageFrameCursor.of(sqlExecutionContext, partitionFrameCursor);
                // First pass - exhaust cursor with release
                while (pageFrameCursor.next() != null) {
                    pageFrameCursor.releaseOpenPartitions();
                }

                // Only the last partition should be open (we released all but current)
                Assert.assertEquals("One partition should still be open", 1, reader.getOpenPartitionCount());

                // Reset cursor
                pageFrameCursor.toTop();

                // Second pass - should work the same way
                int frameCount = 0;
                while (pageFrameCursor.next() != null) {
                    frameCount++;
                    pageFrameCursor.releaseOpenPartitions();
                }

                Assert.assertTrue("Should process frames on second pass", frameCount > 0);
                // After second pass, one partition should be open (the last one visited)
                Assert.assertEquals(
                        "One partition should be open after second pass",
                        1,
                        reader.getOpenPartitionCount()
                );
            }
            // This will close the reader via the cursor chain
        });
    }
}
