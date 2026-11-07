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

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.LoopingRecordToRowCopier;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Helpers for the tests of commits into parquet partitions: a replace-range commit whose
 * rows come from a query, probes of a partition's {@code _txn}, {@code _pm}, directory and
 * index files, and the check that pq matches its native twin nat.
 */
public final class ParquetCommitTestUtils {
    private static final Log LOG = LogFactory.getLog(ParquetCommitTestUtils.class);

    private ParquetCommitTestUtils() {
    }

    /**
     * Asserts that neither twin is suspended and that pq returns the same rows, row count and timestamp bounds
     * as its native twin nat.
     */
    public static void assertTwinsEqual(CairoEngine engine, SqlExecutionContext executionContext) throws SqlException {
        Assert.assertFalse("nat suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("nat")));
        Assert.assertFalse("pq suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pq")));
        TestUtils.assertSqlCursors(engine, executionContext, "nat", "pq", LOG);
        TestUtils.assertSqlCursors(engine, executionContext, "SELECT count(), min(ts), max(ts) FROM nat", "SELECT count(), min(ts), max(ts) FROM pq", LOG);
    }

    /**
     * The index files of {@code columnName} in the partition's directory, one "name length hash" line each, by name.
     */
    public static String indexFiles(CairoEngine engine, String table, int partitionIndex, String columnName) throws IOException {
        final String partitionDir;
        try (TableReader reader = engine.getReader(table); Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(reader.getTableToken().getDirName());
            TableUtils.setPathForNativePartition(
                    path,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    reader.getPartitionTimestampByIndex(partitionIndex),
                    reader.getTxFile().getPartitionNameTxn(partitionIndex)
            );
            partitionDir = path.toString();
        }
        final String[] names = new File(partitionDir).list();
        Assert.assertNotNull(partitionDir, names);
        Arrays.sort(names);
        final StringBuilder files = new StringBuilder();
        for (String name : names) {
            if (name.startsWith(columnName + ".")) {
                final byte[] content = Files.readAllBytes(Paths.get(partitionDir, name));
                files.append(name).append(' ').append(content.length).append(' ').append(Arrays.hashCode(content)).append('\n');
            }
        }
        Assert.assertTrue("no key file in " + files, files.indexOf(columnName + ".pk") >= 0);
        return files.toString();
    }

    /**
     * A partition's parquet file size as committed in {@code _txn}.
     */
    public static long parquetFileSize(CairoEngine engine, String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionParquetFileSize(partitionIndex);
        }
    }

    /**
     * The row group count of a partition's parquet file, as its {@code _pm} records it.
     */
    public static int parquetRowGroupCount(CairoEngine engine, String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            reader.openPartition(partitionIndex);
            return reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getRowGroupCount();
        }
    }

    /**
     * The accumulated dead bytes in a partition's parquet file, as its {@code _pm} footer tracks them.
     */
    public static long parquetUnusedBytes(CairoEngine engine, String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            reader.openPartition(partitionIndex);
            return reader.getAndInitParquetPartitionDecoder(partitionIndex).metadata().getUnusedBytes();
        }
    }

    /**
     * Counts the table's directories for a day (the day and its txn-named versions).
     */
    public static int partitionDirCount(CairoEngine engine, String table, String day) {
        final String[] names = new File(engine.getConfiguration().getDbRoot().toString(), engine.verifyTableName(table).getDirName()).list();
        Assert.assertNotNull(names);
        int count = 0;
        for (String name : names) {
            if (name.startsWith(day)) {
                count++;
            }
        }
        return count;
    }

    /**
     * A partition's name txn as committed in {@code _txn}.
     */
    public static long partitionNameTxn(CairoEngine engine, String table, int partitionIndex) {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    /**
     * Commits the rows of {@code select} to {@code table} as one replace commit over the
     * inclusive range [{@code lo}, {@code hi}], in table timestamp units, for tests that need
     * a replace commit over any column set or type. Row {@code i} of the query gets timestamp
     * {@code timestamps[i]}; the query must not project the designated timestamp column, and
     * it must return exactly {@code timestamps.length} rows. Table columns the query does not
     * name stay NULL. SQL has no replace-range commit, so the rows go through a
     * {@link WalWriter}, and a {@link LoopingRecordToRowCopier} copies each row's values into
     * the table's columns by name, with the usual INSERT casts.
     */
    public static void replace(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            String table,
            CharSequence select,
            long lo,
            long hi,
            long... timestamps
    ) throws SqlException {
        final TableToken token = engine.verifyTableName(table);
        try (
                RecordCursorFactory factory = engine.select(select, executionContext);
                RecordCursor cursor = factory.getCursor(executionContext);
                TableReader reader = engine.getReader(token);
                WalWriter ww = engine.getWalWriter(token)
        ) {
            final RecordMetadata from = factory.getMetadata();
            final TableReaderMetadata to = reader.getMetadata();
            final ListColumnFilter columnFilter = new ListColumnFilter();
            for (int i = 0, n = from.getColumnCount(); i < n; i++) {
                final int columnIndex = to.getColumnIndex(from.getColumnName(i));
                Assert.assertNotEquals("designated timestamp in the query: " + select, to.getTimestampIndex(), columnIndex);
                // ListColumnFilter holds 1-based indexes
                columnFilter.add(columnIndex + 1);
            }
            final RecordToRowCopier copier = new LoopingRecordToRowCopier(from, to, columnFilter);
            final Record record = cursor.getRecord();
            int rowCount = 0;
            while (cursor.hasNext()) {
                Assert.assertTrue("query returned more rows than timestamps: " + select, rowCount < timestamps.length);
                final TableWriter.Row row = ww.newRow(timestamps[rowCount++]);
                copier.copy(executionContext, record, row);
                row.append();
            }
            Assert.assertEquals("query rows: " + select, timestamps.length, rowCount);
            ww.commitWithParams(lo, hi + 1, WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }
}
