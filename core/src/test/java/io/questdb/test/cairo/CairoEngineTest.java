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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class CairoEngineTest extends AbstractCairoTest {

    private static Path otherPath = new Path();
    private static Path path = new Path();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        otherPath = new Path();
        path = new Path();
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        otherPath = Misc.free(otherPath);
        path = Misc.free(path);
        AbstractCairoTest.tearDownStatic();
    }

    @Test
    public void testAncillaries() throws Exception {
        assertMemoryLeak(() -> {

            class MyListener implements PoolListener {
                int count = 0;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    count++;
                }
            }

            MyListener listener = new MyListener();

            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);

                engine.setPoolListener(listener);
                Assert.assertEquals(listener, engine.getPoolListener());

                TableReader reader = engine.getReader(x, -1);
                TableWriter writer = engine.getWriter(x, "test");
                Assert.assertEquals(1, engine.getBusyReaderCount());
                Assert.assertEquals(1, engine.getBusyWriterCount());

                reader.close();
                writer.close();

                Assert.assertEquals(4, listener.count);
                Assert.assertEquals(configuration, engine.getConfiguration());
            }
        });
    }

    @Test
    public void testCannotMapTableId() throws Exception {
        TestUtils.assertMemoryLeak(new TestUtils.LeakProneCode() {
            @Override
            public void run() {
                ff = new TestFilesFacadeImpl() {
                    private boolean failNextAlloc = false;
                    private int theFD = 0;

                    @Override
                    public boolean allocate(int fd, long size) {
                        if (failNextAlloc) {
                            failNextAlloc = false;
                            return false;
                        }
                        return super.allocate(fd, size);
                    }

                    @Override
                    public long length(int fd) {
                        if (theFD == fd) {
                            failNextAlloc = true;
                            theFD = 0;
                            return 0;
                        }
                        return super.length(fd);
                    }

                    @Override
                    public int openRW(LPSZ name, long opts) {
                        int fd = super.openRW(name, opts);
                        if (Chars.endsWith(name, TableUtils.TAB_INDEX_FILE_NAME)) {
                            theFD = fd;
                        }
                        return fd;
                    }
                };

                try {
                    //noinspection resource
                    new CairoEngine(configuration);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                } finally {
                    ff = null;
                }
                Path.clearThreadLocals();
            }
        });
    }

    @Test
    public void testDuplicateTableCreation() throws Exception {
        assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.INT)) {
                CreateTableTestUtils.create(model);
                try {
                    createTable(model);
                    fail("duplicated tables should not be permitted!");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table exists");
                }
            }
        });
    }

    @Test
    public void testExpiry() throws Exception {
        assertMemoryLeak(() -> {

            class MyListener implements PoolListener {
                int count = 0;

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    if (event == PoolListener.EV_EXPIRE) {
                        count++;
                    }
                }
            }

            MyListener listener = new MyListener();

            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);

                engine.setPoolListener(listener);

                assertWriter(engine, x);
                assertReader(engine, x);

                Job job = engine.getEngineMaintenanceJob();
                Assert.assertNotNull(job);

                Assert.assertTrue(job.run(0));
                Assert.assertFalse(job.run(0));

                Assert.assertEquals(2, listener.count);
            }
        });
    }

    @Test
    public void testLockBusyReader() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                try (TableReader reader = engine.getReader(x)) {
                    Assert.assertNotNull(reader);
                    Assert.assertEquals(CairoEngine.BUSY_READER, engine.lock(x, "testing"));
                    assertReader(engine, x);
                    assertWriter(engine, x);
                }
            }
        });
    }

    @Test
    public void testNewTableRename() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                createX(engine);
                try (MemoryMARW mem = Vm.getMARWInstance()) {
                    TableToken y = engine.rename(securityContext, path, mem, "x", otherPath, "y");
                    assertWriter(engine, y);
                    assertReader(engine, y);
                }
            }
        });
    }

    @Test
    public void testRemoveExisting() throws Exception {
        assertMemoryLeak(() -> {
            spinLockTimeout = 1;
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                assertReader(engine, x);
                assertWriter(engine, x);
                engine.drop(path, x);
                Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus(path, x));

                try {
                    engine.getReader(x);
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                try {
                    getWriter(x);
                    Assert.fail();
                } catch (CairoException ignored) {
                }
            }
        });
    }

    @Test
    public void testRemoveNewTable() {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            TableToken x = createX(engine);
            engine.drop(path, x);
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getTableStatus(path, x));
        }
    }

    @Test
    public void testRemoveNonExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                createY(engine);
                try {
                    engine.drop(path, engine.verifyTableName("x"));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
                }
            }
        });
    }

    @Test
    public void testRemoveWhenReaderBusy() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                try (TableReader reader = engine.getReader(x)) {
                    Assert.assertNotNull(reader);
                    try {
                        engine.drop(path, x);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testRemoveWhenWriterBusy() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                try (TableWriter writer = getWriter(engine, x.getTableName())) {
                    Assert.assertNotNull(writer);
                    try {
                        engine.drop(path, x);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testRenameExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                assertWriter(engine, x);
                assertReader(engine, x);


                try (MemoryMARW mem = Vm.getMARWInstance()) {
                    TableToken y = engine.rename(securityContext, path, mem, "x", otherPath, "y");

                    assertWriter(engine, y);
                    assertReader(engine, y);

                    Assert.assertTrue(engine.clear());
                }
            }
        });
    }

    @Test
    public void testRenameExternallyLockedTable() throws Exception {
        assertMemoryLeak(() -> {
            TableToken x = createX(engine);
            try (TableWriter ignored1 = newTableWriter(configuration, "x", metrics)) {

                try (CairoEngine engine = new CairoEngine(configuration)) {
                    try {
                        getWriter(x);
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    try (MemoryMARW mem = Vm.getMARWInstance()) {
                        engine.rename(securityContext, path, mem, "x", otherPath, "y");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=missing or owned by other process]");
                    }
                }
            }
        });
    }

    @Test
    public void testRenameFail() throws Exception {
        assertMemoryLeak(() -> {

            TestFilesFacade ff = new TestFilesFacade() {
                int counter = 1;

                @Override
                public int rename(LPSZ from, LPSZ to) {
                    return counter-- <= 0 && super.rename(from, to) == Files.FILES_RENAME_OK ? Files.FILES_RENAME_OK : Files.FILES_RENAME_ERR_OTHER;
                }

                @Override
                public boolean wasCalled() {
                    return counter < 1;
                }
            };
            AbstractCairoTest.ff = ff;

            try (CairoEngine engine = new CairoEngine(configuration); MemoryMARW mem = Vm.getMARWInstance()) {
                TableToken x = createX(engine);

                assertReader(engine, x);
                assertWriter(engine, x);

                try {
                    engine.rename(securityContext, path, mem, "x", otherPath, "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not rename");
                }

                assertReader(engine, x);
                assertWriter(engine, x);
                TableToken y = engine.rename(securityContext, path, mem, "x", otherPath, "y");
                assertReader(engine, y);
                assertWriter(engine, y);
            }

            Assert.assertTrue(ff.wasCalled());
        });
    }

    @Test
    public void testRenameNonExisting() throws Exception {
        assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "z", PartitionBy.NONE).col("a", ColumnType.INT)) {
                CreateTableTestUtils.create(model);
            }

            try (CairoEngine engine = new CairoEngine(configuration); MemoryMARW mem = Vm.getMARWInstance()) {
                engine.rename(securityContext, path, mem, "x", otherPath, "y");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "does not exist");
            }
        });
    }

    @Test
    public void testRenameToExistingTarget() throws Exception {
        assertMemoryLeak(() -> {

            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);
                TableToken y = createY(engine);

                assertWriter(engine, x);
                assertReader(engine, x);
                try (MemoryMARW mem = Vm.getMARWInstance()) {
                    engine.rename(securityContext, path, mem, "x", otherPath, "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "exists");
                }
                assertWriter(engine, x);
                assertReader(engine, x);

                assertReader(engine, y);
                assertWriter(engine, y);
            }
        });
    }

    @Test
    public void testWrongReaderVersion() throws Exception {

        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TableToken x = createX(engine);

                assertWriter(engine, x);
                try {
                    engine.getReader(x, 2);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
                Assert.assertTrue(engine.clear());
            }
        });
    }

    private void assertReader(CairoEngine engine, TableToken name) {
        try (TableReader reader = engine.getReader(name)) {
            Assert.assertNotNull(reader);
        }
    }

    private void assertWriter(CairoEngine engine, TableToken name) {
        try (TableWriter w = engine.getWriter(name, "testing")) {
            Assert.assertNotNull(w);
        }
    }

    private TableToken createX(CairoEngine engine) {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.INT)) {
            return TestUtils.create(model, engine);
        }
    }

    private TableToken createY(CairoEngine engine) {
        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE).col("b", ColumnType.INT)) {
            return TestUtils.create(model, engine);
        }
    }
}
