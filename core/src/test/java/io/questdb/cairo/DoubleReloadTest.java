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

package io.questdb.cairo;

import org.junit.Assert;
import org.junit.Test;

public class DoubleReloadTest extends AbstractCairoTest {
    @Test
    public void testSingleColumn() {
        try (TableModel model = new TableModel(
                configuration,
                "int_test",
                PartitionBy.NONE
        ).col("x", ColumnType.INT)) {
            CairoTestUtils.create(model);
        }


        try (
                TableReader reader = newTableReader(configuration, "int_test");
                TableWriter w = newTableWriter(configuration, "int_test", metrics)
        ) {
            reader.reload();

            TableWriter.Row r = w.newRow();
            r.putInt(0, 10);
            r.append();
            w.commit();
            reader.reload();

            r = w.newRow();
            r.putInt(0, 10);
            r.append();
            w.commit();

            reader.reload();
            Assert.assertEquals(2, reader.size());
        }
    }
}
