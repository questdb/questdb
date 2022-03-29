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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionPurgeExecution;
import io.questdb.cairo.ColumnVersionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.mp.Sequence;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.tasks.ColumnVersionPurgeTask;
import org.junit.Test;

import java.io.IOException;

public class ColumnVersionPurgeJobTest extends AbstractGriffinTest {

    @Test
    public void testSavesDataToPurgeLogTable() throws SqlException, NumericException, IOException {
        currentMicros = IntervalUtils.parseFloorPartialDate("2022-02-24T05:00");
        try (ColumnVersionPurgeJob purgeJob = createColumnVersionPurgeJob()) {
            ColumnVersionPurgeTask task = createTask("tbl_name", "col", 1, ColumnType.INT, 43, 11, "2022-03-29", -1);
            task.appendColumnVersion(-1, IntervalUtils.parseFloorPartialDate("2022-04-05"), 2);
            appendTaskToQueue(task);

            ColumnVersionPurgeTask task2 = createTask("tbl_name2", "col2", 2, ColumnType.SYMBOL, 33, -1, "2022-02-13", 3);
            appendTaskToQueue(task2);

            purgeJob.run(0);
            assertSql(purgeJob.getLogTableName(), "ts\ttable_name\tcolumn_name\ttable_id\tcolumnType\ttable_partition_by\tupdated_txn\tcolumn_version\tpartition_timestamp\tpartition_name_txn\n" +
                    "2022-02-24T05:00:00.000000Z\ttbl_name\tcol\t1\t5\t3\t43\t11\t2022-03-29T00:00:00.000000Z\t-1\n" +
                    "2022-02-24T05:00:00.000000Z\ttbl_name\tcol\t1\t5\t3\t43\t-1\t2022-04-05T00:00:00.000000Z\t2\n" +
                    "2022-02-24T05:00:00.000001Z\ttbl_name2\tcol2\t2\t12\t3\t33\t-1\t2022-02-13T00:00:00.000000Z\t3\n");
        }
    }

    private void appendTaskToQueue(ColumnVersionPurgeTask task) {
        long cursor = -1L;
        Sequence pubSeq = engine.getMessageBus().getColumnVersionPurgePubSeq();
        while (cursor < 0) {
            cursor = pubSeq.next();
            if (cursor > -1L) {
                ColumnVersionPurgeTask queueTask = engine.getMessageBus().getColumnVersionPurgeQueue().get(cursor);
                queueTask.copyFrom(task);
                pubSeq.done(cursor);
            }
        }
    }

    private ColumnVersionPurgeExecution createColumnVersionCleanExecution() {
        return new ColumnVersionPurgeExecution();
    }

    private ColumnVersionPurgeJob createColumnVersionPurgeJob() throws SqlException {
        return new ColumnVersionPurgeJob(engine, null, createColumnVersionCleanExecution());
    }

    private ColumnVersionPurgeTask createTask(
            String tblName,
            String colName,
            int tableId,
            int columnType,
            long updateTxn,
            long columnVersion,
            String partitionTs,
            long partitionNameTxn
    ) throws NumericException {
        ColumnVersionPurgeTask tsk = new ColumnVersionPurgeTask();
        tsk.of(tblName, colName, tableId, columnType, PartitionBy.NONE, updateTxn, new LongList());
        tsk.appendColumnVersion(columnVersion, IntervalUtils.parseFloorPartialDate(partitionTs), partitionNameTxn);
        return tsk;
    }
}
