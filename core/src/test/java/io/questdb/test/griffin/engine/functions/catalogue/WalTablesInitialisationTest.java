/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class WalTablesInitialisationTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testWalTablesFunctionBeforeSeqTrackerInitialized() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;

            // Need many tables, single table will be initialized way too quickly on a background thread
            int tableCount = 100;
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                for (int i = 0; i < tableCount; i++) {
                    serverMain.getEngine().execute("create table A" + i + "(b byte, ts timestamp) timestamp(ts) partition by DAY WAL");
                }
            }

            // Check that the function is not initialized
            for (int i = 0; i < 5; i++) {
                try (final TestServerMain serverMain = startWithEnvVariables()) {
                    serverMain.start();
                    serverMain.assertSql(
                            "select max(name), min(name), min(sequencerTxn), max(writerTxn), count() from wal_tables()",
                            "max\tmin\tmin1\tmax1\tcount\n" +
                                    "A99\tA0\t0\t0\t" + tableCount + "\n"
                    );
                }
            }
        });
    }
}
