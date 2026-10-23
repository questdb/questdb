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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.griffin.PurgingOperator;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PurgingOperatorTest extends AbstractCairoTest {

    @Test
    public void testClearReleasesColumnNames() throws Exception {
        assertMemoryLeak(() -> {
            // TableWriter keeps one operator for its whole lifetime and clears it
            // after every purge, so clear() must not leave column names behind.
            final PurgingOperator operator = new PurgingOperator(LOG, configuration, engine.getMessageBus());
            for (int i = 0; i < 3; i++) {
                operator.add(0, "sym", ColumnType.SYMBOL, IndexType.BITMAP, i, 0, -1);
                operator.add(1, "sym2", ColumnType.SYMBOL, IndexType.POSTING, i, 0, -1);
                Assert.assertEquals(2, operator.getColumnNameCount());
                operator.clear();
                Assert.assertEquals(0, operator.getColumnNameCount());
            }
        });
    }
}
