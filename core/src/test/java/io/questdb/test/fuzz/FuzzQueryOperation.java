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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.tools.TestUtils;

/**
 * Reads the table while other transactions may be applied to it in parallel.
 * <p>
 * Half of the time it scans the table, to verify that the column files from intermediate
 * txns are not corrupted. Otherwise, when the table has an indexed SYMBOL column, it looks
 * up a random existing value of that column through the index, forward or backward in time.
 * NULL is picked more often than other values: it takes the index's special null paths. The
 * query cannot know the right answer while commits land under it, but every row it returns
 * must hold the looked-up value, in timestamp order, and re-reading a row by its row id must
 * return the same row. A cursor whose index entries point at rows of another layout fails the
 * value check, and a cursor that serves a re-read from recycled memory fails the last one.
 */
public class FuzzQueryOperation implements FuzzTransactionOperation {
    // NULL is looked up with this probability, any existing value otherwise
    private static final double NULL_KEY_PROBABILITY = 0.3;
    private final int limit;

    public FuzzQueryOperation(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean apply(
            Rnd rnd,
            CairoEngine engine,
            TableWriterAPI tableWriter,
            int virtualTimestampIndex,
            LongList excludedTsIntervals
    ) {
        FailureFileFacade failureFileFacade = null;
        int failuresObserved = 0;
        if (engine.getConfiguration().getFilesFacade() instanceof FailureFileFacade) {
            failureFileFacade = (FailureFileFacade) engine.getConfiguration().getFilesFacade();
            failuresObserved = failureFileFacade.failureGenerated();
        }
        final TableToken tableToken = tableWriter.getTableToken();
        final StringSink sink = new StringSink();
        try (SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
            if (rnd.nextBoolean() || !queryIndex(rnd, engine, sqlExecutionContext, tableToken, sink)) {
                TestUtils.printSql(engine, sqlExecutionContext, tableToken.getTableName() + " LIMIT " + limit, sink);
            }
        } catch (SqlException e) {
            if (failureFileFacade != null && failureFileFacade.failureGenerated() > failuresObserved) {
                // Failure facade has generated an error either for us or a concurrent thread.
                // In any case, we can't trust this throwable.
                return false;
            }
            if (e.isTableDoesNotExist()) {
                return false; // drop table transactions are BAU
            }
            if (Chars.contains(e.getFlyweightMessage(), "too many cached query plan cannot be used because table schema has changed")) {
                // Just an unlucky live lock conflict, ignore
                return false;
            }
            if (Chars.contains(e.getFlyweightMessage(), "Invalid column")) {
                // A concurrent transaction dropped or renamed the column the index query names
                return false;
            }
            throw new RuntimeException(e);
        } catch (Throwable th) {
            if (failureFileFacade != null && failureFileFacade.failureGenerated() > failuresObserved) {
                return false;
            }
            if (th instanceof CairoException ce && ce.isTableDoesNotExist()) {
                return false;
            }
            if (th instanceof TableReferenceOutOfDateException) {
                return false;
            }
            throw th;
        }
        return false;
    }

    private static void appendQuoted(StringSink sql, CharSequence value) {
        sql.put('\'');
        for (int i = 0, n = value.length(); i < n; i++) {
            final char c = value.charAt(i);
            if (c == '\'') {
                sql.put('\'');
            }
            sql.put(c);
        }
        sql.put('\'');
    }

    private static String describeRow(Record record, int symbolColumnIndex, int timestampColumnIndex) {
        final CharSequence value = record.getSymA(symbolColumnIndex);
        return "[value=" + (value == null ? "null" : "'" + value + "'")
                + ", ts=" + record.getTimestamp(timestampColumnIndex)
                + ", rowId=" + record.getRowId() + ']';
    }

    private static boolean isSameValue(CharSequence expected, CharSequence actual) {
        return expected == null ? actual == null : actual != null && Chars.equals(expected, actual);
    }

    /**
     * Looks up a random existing value of a random indexed SYMBOL column and checks the rows the
     * query returns. Returns false when the table has no indexed SYMBOL column.
     */
    private boolean queryIndex(
            Rnd rnd,
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            TableToken tableToken,
            StringSink sql
    ) throws SqlException {
        final String columnName;
        final String timestampColumnName;
        final String value;
        try (TableReader reader = engine.getReader(tableToken)) {
            final TableReaderMetadata metadata = reader.getMetadata();
            if (metadata.getTimestampIndex() < 0) {
                return false;
            }
            final IntList indexedColumns = new IntList();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (ColumnType.isSymbol(metadata.getColumnType(i)) && metadata.isColumnIndexed(i)) {
                    indexedColumns.add(i);
                }
            }
            if (indexedColumns.size() == 0) {
                return false;
            }
            final int columnIndex = indexedColumns.getQuick(rnd.nextInt(indexedColumns.size()));
            columnName = metadata.getColumnName(columnIndex);
            timestampColumnName = metadata.getColumnName(metadata.getTimestampIndex());
            final int symbolCount = reader.getSymbolMapReader(columnIndex).getSymbolCount();
            if (symbolCount == 0 || rnd.nextDouble() < NULL_KEY_PROBABILITY) {
                value = null;
            } else {
                final CharSequence symbol = reader.getSymbolMapReader(columnIndex).valueOf(rnd.nextInt(symbolCount));
                value = symbol == null ? null : symbol.toString();
            }
        }

        final boolean isDescending = rnd.nextBoolean();
        sql.clear();
        sql.put("SELECT * FROM '").put(tableToken.getTableName()).put("' WHERE \"").put(columnName).put('"');
        if (value == null) {
            sql.put(" IS NULL");
        } else {
            sql.put(" = ");
            appendQuoted(sql, value);
        }
        if (isDescending) {
            sql.put(" ORDER BY \"").put(timestampColumnName).put("\" DESC");
        }
        sql.put(" LIMIT ").put(limit);
        final String query = sql.toString();

        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            final RecordMetadata metadata = factory.getMetadata();
            final int symbolColumnIndex = metadata.getColumnIndexQuiet(columnName);
            final int timestampColumnIndex = metadata.getColumnIndexQuiet(timestampColumnName);
            if (symbolColumnIndex < 0 || timestampColumnIndex < 0 || !ColumnType.isSymbol(metadata.getColumnType(symbolColumnIndex))) {
                // a concurrent transaction changed the column
                return true;
            }
            final boolean isRandomAccess = factory.recordCursorSupportsRandomAccess();
            final LongList rowIds = new LongList();
            final LongList timestamps = new LongList();
            final ObjList<String> values = new ObjList<>();
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                long prevTimestamp = isDescending ? Long.MAX_VALUE : Long.MIN_VALUE;
                long rowCount = 0;
                while (cursor.hasNext()) {
                    final CharSequence actual = record.getSymA(symbolColumnIndex);
                    final long timestamp = record.getTimestamp(timestampColumnIndex);
                    if (!isSameValue(value, actual)) {
                        throw new AssertionError("index query returned a row of another value [query=" + query
                                + ", row=" + rowCount + ' ' + describeRow(record, symbolColumnIndex, timestampColumnIndex) + ']');
                    }
                    if (isDescending ? timestamp > prevTimestamp : timestamp < prevTimestamp) {
                        throw new AssertionError("index query returned rows out of timestamp order [query=" + query
                                + ", row=" + rowCount + ' ' + describeRow(record, symbolColumnIndex, timestampColumnIndex)
                                + ", prevTs=" + prevTimestamp + ']');
                    }
                    prevTimestamp = timestamp;
                    if (isRandomAccess) {
                        rowIds.add(record.getRowId());
                        timestamps.add(timestamp);
                        values.add(actual == null ? null : actual.toString());
                    }
                    if (++rowCount == limit / 2) {
                        // Let the apply threads commit under the open cursor
                        Thread.yield();
                    }
                }
                for (int i = 0, n = rowIds.size(); i < n; i++) {
                    cursor.recordAt(record, rowIds.getQuick(i));
                    if (!isSameValue(values.getQuick(i), record.getSymA(symbolColumnIndex))
                            || record.getTimestamp(timestampColumnIndex) != timestamps.getQuick(i)) {
                        throw new AssertionError("re-reading a row by its row id returned another row [query=" + query
                                + ", row=" + i
                                + ", expectedValue=" + values.getQuick(i)
                                + ", expectedTs=" + timestamps.getQuick(i)
                                + ", actual=" + describeRow(record, symbolColumnIndex, timestampColumnIndex) + ']');
                    }
                }
            }
        }
        return true;
    }
}
