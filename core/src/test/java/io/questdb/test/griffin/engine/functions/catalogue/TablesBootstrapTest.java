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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class TablesBootstrapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testDesignatedTimestampAfterDropColumnAndRestart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                serverMain.execute("CREATE TABLE tab (sym SYMBOL, bar VARCHAR, ts TIMESTAMP, foo INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertTables(serverMain);

                serverMain.execute("ALTER TABLE tab DROP COLUMN foo");
                assertTables(serverMain);

                serverMain.execute("ALTER TABLE tab DROP COLUMN bar");
                assertTables(serverMain);
            }

            // restart
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                TestUtils.assertEventually(() -> assertTables(serverMain));

                serverMain.execute("ALTER TABLE tab DROP COLUMN sym");
                assertTables(serverMain);
            }
        });
    }

    private static void assertTables(TestServerMain serverMain) {
        serverMain.assertSql(
                "tables()",
                "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\twalEnabled\tdirectoryName\tdedup\tttlValue\tttlUnit\tmatView\n" +
                        "4\ttab\tts\tDAY\t500000\t600000000\ttrue\ttab~4\tfalse\t0\tHOUR\tfalse\n"
        );
    }
}
