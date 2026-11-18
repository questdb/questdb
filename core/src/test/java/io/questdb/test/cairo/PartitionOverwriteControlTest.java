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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionOverwriteControl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PartitionOverwriteControlTest extends AbstractCairoTest {

    @Test
    public void testMutationOfColumnVersionsTheReaderMaps() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG)");
            execute("INSERT INTO t VALUES (1), (2), (3)");
            final TableToken token = engine.verifyTableName("t");
            final PartitionOverwriteControl control = engine.getPartitionOverwriteControl();
            Assert.assertTrue("the test suite turns the control on", control.isEnabled());
            final int errorCount = control.getErrorCount();
            try (TableReader reader = getReader("t")) {
                final long readerTxn = reader.getTxn();
                // the column files of unknown versions, and of versions the reader's txn published
                assertOverwrite(control, token, 0, Long.MIN_VALUE, readerTxn);
                assertOverwrite(control, token, 2, -1, readerTxn);
                assertOverwrite(control, token, 0, readerTxn - 1, readerTxn);
                Assert.assertEquals(errorCount + 3, control.getErrorCount());

                // the commit after the txn a version is named for publishes it: the reader maps none of these files
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, 0, readerTxn);
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, 0, readerTxn + 1);
                // the rows past those the reader sees
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, 3, -1);
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, 3);
                // another version of the partition
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, 5, 0, -1);
                Assert.assertEquals(errorCount + 3, control.getErrorCount());
            }
            // nothing reads the table
            control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, 0);
            Assert.assertEquals(errorCount + 3, control.getErrorCount());
        });
    }

    private static void assertOverwrite(
            PartitionOverwriteControl control,
            TableToken token,
            long mutateFromRow,
            long minColumnNameTxn,
            long readerTxn
    ) {
        try {
            if (minColumnNameTxn == Long.MIN_VALUE) {
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, mutateFromRow);
            } else {
                control.notifyPartitionMutates(token, ColumnType.TIMESTAMP, 0, -1, mutateFromRow, minColumnNameTxn);
            }
            Assert.fail("overwrite expected [mutateFromRow=" + mutateFromRow + ", minColumnNameTxn=" + minColumnNameTxn + ']');
        } catch (CairoException e) {
            TestUtils.assertContains(
                    e.getFlyweightMessage(),
                    "partition is overwritten while being in use by a reader [table=t, partition=1970-01-01T00:00:00.000000Z, partitionNameTxn=-1, readerTxn="
                            + readerTxn + ", mutateFromRow=" + mutateFromRow + ", visibleRows=3]"
            );
        }
    }
}
