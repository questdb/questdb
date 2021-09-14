/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.EngineMigrationTest.assertRemoveUpgradeFile;
import static io.questdb.cairo.EngineMigrationTest.replaceDbContent;

public class MigrationTableIDTest extends AbstractGriffinTest {

    @Test
    public void testAssignTableId() throws Exception {

        // do not add any more tests to this class

        engine.close();
        assertMemoryLeak(() -> {
            // This test has to run in a separate engine from the base test engine
            // because of removal of mapped file _tab_index.d with every test
            replaceDbContent("/migration/dbRoot.zip"); // last tableId is 16. 10 skipped + 7 tables generated
            // we need to remove "upgrade" file for the engine to upgrade tables
            // remember, this is the second instance of the engine
            assertRemoveUpgradeFile();

            try (CairoEngine engine2 = new CairoEngine(configuration)) {
                // check if constructor upgrades test
                try (TableReader reader = engine2.getReader(sqlExecutionContext.getCairoSecurityContext(), "y_416")) {
                    Assert.assertEquals(17, reader.getMetadata().getId());
                }
                try (TableReader reader = engine2.getReader(sqlExecutionContext.getCairoSecurityContext(), "y_419")) {
                    Assert.assertEquals(11, reader.getMetadata().getId());
                }
            }
        });
    }


}
