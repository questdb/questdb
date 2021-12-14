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

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

public class RebuildIndexTest extends AbstractCairoTest {
    protected static CharSequence root;
    private static SqlCompiler compiler;
    private static SqlExecutionContextImpl sqlExecutionContext;
    private final RebuildIndex rebuildIndex = new RebuildIndex();

    @BeforeClass
    public static void setUpStatic() {
        AbstractCairoTest.setUpStatic();
        compiler = new SqlCompiler(engine);
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();
    }

    @AfterClass
    public static void tearDownStatic() {
        compiler.close();
    }

    @After
    public void cleanup() {
        rebuildIndex.close();
    }

    @Test
    public void testPartitionedDaily() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(
                createTableSql,
                (tablePath) -> {
                    removeFileFirstParition("sym1.v", PartitionBy.DAY, tablePath);
                    removeFileFirstParition("sym2.k", PartitionBy.DAY, tablePath);
                },
                RebuildIndex::rebuildAll
        );
    }

    @Test
    public void testPartitionedOneColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileFirstParition("sym1.v", PartitionBy.DAY, tablePath);
                    removeFileFirstParition("sym1.k", PartitionBy.DAY, tablePath);
                },
                rebuildIndex -> rebuildIndex.rebuildColumn("sym1"));
    }

    @Test
    public void testRebuildOnePartition() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileFirstParition("sym1.v", PartitionBy.DAY, tablePath);
                    removeFileFirstParition("sym1.k", PartitionBy.DAY, tablePath);
                    removeFileFirstParition("sym2.k", PartitionBy.DAY, tablePath);
                },
                rebuildIndex -> rebuildIndex.rebuildPartition("1970-01-01"));
    }

    @Test
    public void testRebuildUnindexedColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "rnd_symbol(4,4,4,2) as sym3," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        try {
            checkRebuildIndexes(createTableSql,
                    tablePath -> {
                    },
                    rebuildIndex -> rebuildIndex.rebuildColumn("sym3"));
            Assert.fail();
        } catch (CairoException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), "Column is not indexed");
        }
    }

    @Test
    public void testPartitionedNone() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts)";

        checkRebuildIndexes(
                createTableSql,
                (tablePath) -> {
                    removeFileFirstParition("sym1.v", PartitionBy.NONE, tablePath);
                    removeFileFirstParition("sym2.k", PartitionBy.NONE, tablePath);
                },
                RebuildIndex::rebuildAll
        );
    }

    @Test
    public void testNonePartitionedOneColumn() throws Exception {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts)";

        checkRebuildIndexes(createTableSql,
                tablePath -> {
                    removeFileFirstParition("sym1.v", PartitionBy.NONE, tablePath);
                    removeFileFirstParition("sym1.k", PartitionBy.NONE, tablePath);
                },
                rebuildIndex -> rebuildIndex.rebuildColumn("sym1"));
    }

    private void checkRebuildIndexes(String createTableSql, Action<String> changeTable, Action<RebuildIndex> rebuildIndexAction) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(createTableSql, sqlExecutionContext);
            int sym1A = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C = countByFullScan("select * from xxx where sym1 = 'C'");

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            String tablePath = configuration.getRoot().toString() + Files.SEPARATOR + "xxx";
            changeTable.run(tablePath);

            rebuildIndex.clear();
            rebuildIndex.of(tablePath, configuration);
            rebuildIndexAction.run(rebuildIndex);

            int sym1A2 = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B2 = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C2 = countByFullScan("select * from xxx where sym1 = 'C'");

            Assert.assertEquals(sym1A, sym1A2);
            Assert.assertEquals(sym1B, sym1B2);
            Assert.assertEquals(sym1C, sym1C2);
        });
    }

    private void removeFileFirstParition(String fileName, int partitionBy, String tablePath) {
        try (Path path = new Path()) {
            path.concat(tablePath);
            path.put(Files.SEPARATOR);
            PartitionBy.setSinkForPartition(path, partitionBy, 0, false);
            path.concat(fileName);
            LOG.info().$("removing ").utf8(path).$();
            Assert.assertTrue(Files.remove(path.$()));
        }
    }

    private int countByFullScan(String sql) throws SqlException {
        int recordCount = 0;
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    recordCount++;
                }
            }
        }
        return recordCount;
    }

    @FunctionalInterface
    interface Action<T> {
        void run(T val);
    }
}
