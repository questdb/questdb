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
import org.junit.Assert;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Writes replace-range WAL commits whose rows come from a query, for tests that need a
 * replace commit over any column set or type. SQL has no replace-range commit, so the rows
 * go through a {@link WalWriter}, and a {@link LoopingRecordToRowCopier} copies each row's
 * values into the table's columns by name, with the usual INSERT casts.
 */
public final class ReplaceCommitTestUtils {

    private ReplaceCommitTestUtils() {
    }

    /**
     * Commits the rows of {@code select} to {@code table} as one replace commit over the
     * inclusive range [{@code lo}, {@code hi}], in table timestamp units. Row {@code i} of the
     * query gets timestamp {@code timestamps[i]}; the query must not project the designated
     * timestamp column, and it must return exactly {@code timestamps.length} rows. Table
     * columns the query does not name stay NULL.
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
