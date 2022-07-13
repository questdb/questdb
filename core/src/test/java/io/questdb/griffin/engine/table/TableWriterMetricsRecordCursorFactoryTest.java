/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class TableWriterMetricsRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testEmptyAndDisabled() throws Exception {
        try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor("total-commits\to3commits\trollbacks\tcommitted-rows\tphysically-written-rows\n" +
                    "0\t0\t0\t0\t0\n", cursor, factory.getMetadata(), true);
        }
    }

    @Test
    public void testSimple() throws Exception {
        int rows = 10;
        try (TableModel tm = new TableModel(configuration, "tab1", PartitionBy.NONE)) {
            tm.timestamp("ts").col("ID", ColumnType.INT);
            createPopulateTable(tm, rows, "2020-01-01", 1);
        }

        try (TableWriterMetricsRecordCursorFactory factory = new TableWriterMetricsRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            assertCursor("total-commits\to3commits\trollbacks\tcommitted-rows\tphysically-written-rows\n" +
                    "1\t0\t0\t" + rows + "\t" + rows + "\n", cursor, factory.getMetadata(), true);
        }
    }
}