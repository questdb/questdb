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

package io.questdb.test.cutlass.qwp.load;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.ObjList;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Per-table oracle of expected rows. Producers add {@link QwpRow}s as they
 * publish; the assertion side walks the actual cursor in lockstep with the
 * snapshot sorted by ({@code tsMicros}, {@code id}) to match the on-disk
 * order under {@code DEDUP UPSERT KEYS(ts, id)}.
 */
public class QwpTable {

    private final ObjList<QwpRow> rows = new ObjList<>();
    private final String tableName;

    public QwpTable(String tableName) {
        this.tableName = tableName;
    }

    public synchronized void addRow(QwpRow row) {
        rows.add(row);
    }

    /**
     * Walks {@code cursor} once and asserts each cell against the oracle row
     * with the same (ts, id) ordinal. Fails on row count mismatch and on the
     * first cell mismatch.
     */
    public void assertCursor(RecordCursor cursor, RecordMetadata metadata, String idColumnName, String tsColumnName) {
        QwpRow[] expected = snapshotSorted();
        Record record = cursor.getRecord();
        long ordinal = 0;
        while (cursor.hasNext()) {
            if (ordinal >= expected.length) {
                Assert.fail("cursor has more rows than oracle (oracle.size=" + expected.length + ")");
            }
            expected[(int) ordinal].assertAgainst(metadata, record, idColumnName, tsColumnName, ordinal);
            ordinal++;
        }
        if (ordinal != expected.length) {
            Assert.fail("cursor has fewer rows than oracle: cursor=" + ordinal + " oracle=" + expected.length);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public synchronized int size() {
        return rows.size();
    }

    private synchronized QwpRow[] snapshotSorted() {
        QwpRow[] arr = new QwpRow[rows.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = rows.getQuick(i);
        }
        Arrays.sort(arr, Comparator
                .comparingLong(QwpRow::getTimestampMicros)
                .thenComparingLong(QwpRow::getId));
        return arr;
    }
}
