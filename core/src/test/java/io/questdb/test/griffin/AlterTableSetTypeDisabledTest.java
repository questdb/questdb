/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.*;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.test.griffin.AlterTableSetTypeTest.WAL;
import static org.junit.Assert.assertFalse;

public class AlterTableSetTypeDisabledTest extends AbstractAlterTableSetTypeRestartTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration(
                    PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true",            // WAL enabled
                    PropertyKey.TABLE_TYPE_CONVERSION_ENABLED.getPropertyPath() + "=false"  // table type conversion is disabled
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSetTypeDisabled() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain questdb = new TestServerMain("-d", rootDir, Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain questdb = new TestServerMain("-d", rootDir, Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                questdb.start();

                final CairoEngine engine = questdb.getCairoEngine();
                final TableToken token = engine.getTableToken(tableName);

                // conversion is disabled so table is not converted, it is still non-WAL
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
                // pending conversion to WAL table is still on
                // can be removed with another ALTER statement
                assertConvertFileExists(engine, token);
            }
            validateShutdown(tableName);
        });
    }
}
