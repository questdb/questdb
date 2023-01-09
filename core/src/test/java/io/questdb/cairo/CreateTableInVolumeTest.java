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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

public class CreateTableInVolumeTest extends AbstractGriffinTest {

    private Path path;

    @Before
    public void setUp() {
        super.setUp();
        path = new Path();
    }

    @After
    public void tearDown() {
        super.tearDown();
        Misc.free(path);
    }

    @Test
    public void testVanilla() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        String tableName = "tab";
        String volume = temp.newFolder("NEW_VOLUME").getAbsolutePath();
        assertCompile("CREATE TABLE " + tableName + " (" +
                "a INT," +
                "s SYMBOL," +
                "t timestamp), INDEX(s CAPACITY 32) TIMESTAMP(t) PARTITION BY DAY IN VOLUME '" + volume + "'");
        try (TableModel src = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("1", ColumnType.INT)
                .col("s", ColumnType.SYMBOL).indexed(true, 32)
                .timestamp("t")
        ) {
            compile(
                    TestUtils.insertFromSelectPopulateTableStmt(src, 10000, "2022-10-17", 2),
                    sqlExecutionContext
            );
        }
        assertSql("SELECT min(t), max(t), count() FROM " + tableName,
                "min\tmax\tcount\n" +
                        "2022-10-17T00:00:17.279900Z\t2022-10-18T23:59:59.000000Z\t10000\n");
        FilesFacadeImpl.INSTANCE.isDirOrSoftLinkDir(path.of(configuration.getRoot()).concat(tableName).$());
        Assert.assertTrue(FilesFacadeImpl.INSTANCE.isSoftLink(path));
    }
}

