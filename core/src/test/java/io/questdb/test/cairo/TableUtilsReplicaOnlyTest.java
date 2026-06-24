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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TableUtilsReplicaOnlyTest extends AbstractCairoTest {

    @Test
    public void testReplicaOnlyFlagRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                int idx = m.getColumnIndex("s");
                Assert.assertTrue(m.isColumnIndexed(idx));
                Assert.assertTrue(m.isColumnReplicaOnlyIndex(idx));
            }
        });
    }
}
