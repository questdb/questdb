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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CairoEngineMigrationTest extends AbstractGriffinTest {
    @Test
    public void testAssignTableId() throws Exception {
        assertMemoryLeak(() -> {
            // roll table id up
            for (int i = 0; i < 10; i++) {
                engine.getNextTableId();
            }
            String tableName = "test";
            // old table
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
            }

            try (TableModel model = new TableModel(configuration, "test2", PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                TableUtils.createTable(
                        model.getCairoCfg().getFilesFacade(),
                        model.getMem(),
                        model.getPath(),
                        model.getCairoCfg().getRoot(),
                        model,
                        model.getCairoCfg().getMkDirMode(),
                        ColumnType.VERSION,
                        (int) engine.getNextTableId()
                );
            }

            // we need to remove "upgrade" file for the engine to upgrade tables
            // remember, this is the second instance of the engine
            assertRemoveUpgradeFile();

            try (CairoEngine engine = new CairoEngine(configuration)) {
                // check if constructor upgrades test
                try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "test")) {
                    Assert.assertEquals(12, reader.getMetadata().getId());
                }
                try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "test2")) {
                    Assert.assertEquals(11, reader.getMetadata().getId());
                }
            }
        });
    }

    @Test
    public void testCannotReadMetadata() throws Exception {
        assertMemoryLeak(() -> {
            // roll table id up
            for (int i = 0; i < 10; i++) {
                engine.getNextTableId();
            }
            String tableName = "test";
            // old table
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("aaa", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.createTableWithVersion(model, 416);
            }

            FilesFacade ff = new FilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    return 0;
                }
            };

            // we need to remove "upgrade" file for the engine to upgrade tables
            // remember, this is the second instance of the engine

            assertRemoveUpgradeFile();

            try {
                new CairoEngine(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                });
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Could not update table");
            }
        });// /tmp/junit16870455038409769219/dbRoot/_upgrade.d
    }

    private static void assertRemoveUpgradeFile() {
        try (Path path = new Path()) {
            path.of(configuration.getRoot()).concat(TableUtils.UPGRADE_FILE_NAME).$();
            Assert.assertTrue(!FilesFacadeImpl.INSTANCE.exists(path) || FilesFacadeImpl.INSTANCE.remove(path));
        }
    }
}
