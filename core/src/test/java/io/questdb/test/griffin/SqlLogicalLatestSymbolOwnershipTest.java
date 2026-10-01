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

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.SqlLogicalSymbolIndexOwnershipTest.ConstructionFailureEngine;
import org.junit.Test;

public class SqlLogicalLatestSymbolOwnershipTest extends AbstractCairoTest {
    @Test
    public void testIndexedKeyConstructionFailureClosesEveryInputOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (ConstructionFailureEngine failing = new ConstructionFailureEngine(temp.newFolder().getAbsolutePath())) {
                failing.createRows();
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s='A' AND lp_index_filter(id) LATEST ON ts PARTITION BY s");
                failing.assertConstructionFails("SELECT * FROM lp_index_keys WHERE s='A' LATEST ON ts PARTITION BY s");
                failing.context.getBindVariableService().setStr(0, "A");
                failing.assertConstructionFails("SELECT s,id,ts FROM lp_covering_keys WHERE s=$1 LATEST ON ts PARTITION BY s");
            }
        });
    }

    @Test
    public void testResolvedKeyConstructionFailureClosesEveryInputOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (ConstructionFailureEngine failing = new ConstructionFailureEngine(temp.newFolder().getAbsolutePath())) {
                failing.createRows();
                failing.assertConstructionFails("SELECT * FROM lp_latest_keys WHERE s='A' AND lp_index_filter(id) LATEST ON ts PARTITION BY s");
                failing.assertConstructionFails("SELECT * FROM lp_latest_keys WHERE s='A' LATEST ON ts PARTITION BY s");
            }
        });
    }
}
