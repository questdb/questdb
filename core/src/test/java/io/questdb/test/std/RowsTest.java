/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std;

import io.questdb.std.Rows;
import org.junit.Assert;
import org.junit.Test;

public class RowsTest {
    @Test
    public void testMaxBoundariesFitsIn() {
        int maxPartitionIndex = Rows.MAX_SAFE_PARTITION_INDEX;
        long maxRowId = Rows.MAX_SAFE_ROW_ID;

        long rowId = Rows.toRowID(maxPartitionIndex, maxRowId);
        int partitionIndex = Rows.toPartitionIndex(rowId);
        long localRowId = Rows.toLocalRowID(rowId);

        Assert.assertEquals(maxPartitionIndex, partitionIndex);
        Assert.assertEquals(maxRowId, localRowId);
    }

    @Test
    public void testMaxLocalRowIdIsNotUnnecessaryConservative() {
        int maxPartitionIndex = Rows.MAX_SAFE_PARTITION_INDEX;
        long maxRowId = Rows.MAX_SAFE_ROW_ID + 1;

        long rowId = Rows.toRowID(maxPartitionIndex, maxRowId);
        long localRowId = Rows.toLocalRowID(rowId);

        Assert.assertNotEquals(maxRowId, localRowId);
    }

    @Test
    public void testMaxPartitionIndexIsNotUnnecessaryConservative() {
        int maxPartitionIndex = Rows.MAX_SAFE_PARTITION_INDEX + 1;
        long maxRowId = Rows.MAX_SAFE_ROW_ID;

        long rowId = Rows.toRowID(maxPartitionIndex, maxRowId);
        int partitionIndex = Rows.toPartitionIndex(rowId);
        long localRowId = Rows.toLocalRowID(rowId);

        Assert.assertNotEquals(maxPartitionIndex, partitionIndex);
        Assert.assertEquals(maxRowId, localRowId);
    }
}
