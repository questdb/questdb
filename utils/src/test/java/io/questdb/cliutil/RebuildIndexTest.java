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

package io.questdb.cliutil;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RebuildIndex;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class RebuildIndexTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;
    private static DefaultCairoConfiguration configuration;
    private static CairoEngine engine;
    private static SqlCompiler compiler;
    private static SqlExecutionContextImpl sqlExecutionContext;

    public static void createTestPath(CharSequence root) {
        try (Path path = new Path().of(root).$()) {
            if (Files.exists(path)) {
                return;
            }
            Files.mkdirs(path.of(root).slash$(), 509);
        }
    }

    public static void removeTestPath(CharSequence root) {
        Path path = Path.getThreadLocal(root);
        Files.rmdir(path.slash$());
    }

    public static void setCairoStatic() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }
        configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
    }

    @BeforeClass
    public static void setUpStatic() {
        setCairoStatic();
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
        engine.close();
        compiler.close();
    }

    @Before
    public void setUp() {
        createTestPath(root);
        engine.openTableId();
        engine.resetTableId();
    }

    @After
    public void tearDown() {
        engine.freeTableId();
        engine.clear();
        removeTestPath(root);
    }

    @Test
    public void testPartitionedDaily() throws SqlException {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts) PARTITION BY DAY";

        checkRebuildIndexes(createTableSql, PartitionBy.DAY);
    }

    @Test
    public void testPartitionedNone() throws SqlException {
        String createTableSql = "create table xxx as (" +
                "select " +
                "rnd_symbol('A', 'B', 'C') as sym1," +
                "rnd_symbol(4,4,4,2) as sym2," +
                "x," +
                "timestamp_sequence(0, 100000000) ts " +
                "from long_sequence(10000)" +
                "), index(sym1), index(sym2) timestamp(ts)";

        checkRebuildIndexes(createTableSql, PartitionBy.NONE);
    }

    private void checkRebuildIndexes(String createTableSql, int partitionBy) throws SqlException {
        compiler.compile(createTableSql, sqlExecutionContext);
        int sym1A = countByFullScan("select * from xxx where sym1 = 'A'");
        int sym1B = countByFullScan("select * from xxx where sym1 = 'B'");
        int sym1C = countByFullScan("select * from xxx where sym1 = 'C'");

        engine.releaseAllReaders();
        engine.releaseAllWriters();

        String tablePath = root.toString() + Files.SEPARATOR + "xxx";
        try (Path path = new Path()) {
            path.concat(tablePath);
            path.put(Files.SEPARATOR);
            PartitionBy.setSinkForPartition(path, partitionBy, 0, false);
            path.concat("sym1.v");
            Assert.assertTrue(Files.remove(path.$()));
        }

        try (RebuildIndex ri = new RebuildIndex().of(tablePath, configuration)) {
            ri.rebuildAllPartitions();

            int sym1A2 = countByFullScan("select * from xxx where sym1 = 'A'");
            int sym1B2 = countByFullScan("select * from xxx where sym1 = 'B'");
            int sym1C2 = countByFullScan("select * from xxx where sym1 = 'C'");

            Assert.assertEquals(sym1A, sym1A2);
            Assert.assertEquals(sym1B, sym1B2);
            Assert.assertEquals(sym1C, sym1C2);
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
}
