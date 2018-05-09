/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.pool.PoolListener;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.mp.Job;
import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.std.FilesFacade;
import com.questdb.std.ObjHashSet;
import com.questdb.std.str.LPSZ;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class EngineTest extends AbstractCairoTest {
    @Test
    public void testAncillaries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            class MyListener implements PoolListener {
                int count = 0;

                @Override
                public void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position) {
                    count++;
                }
            }

            MyListener listener = new MyListener();

            try (Engine engine = new Engine(configuration)) {
                engine.setPoolListener(listener);
                Assert.assertEquals(listener, engine.getPoolListener());

                TableReader reader = engine.getReader("x");
                TableWriter writer = engine.getWriter("x");
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
    public void testExpiry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            class MyListener implements PoolListener {
                int count = 0;

                @Override
                public void onEvent(byte factoryType, long thread, CharSequence name, short event, short segment, short position) {
                    if (event == PoolListener.EV_EXPIRE) {
                        count++;
                    }
                }
            }

            class MyWorkScheduler implements CairoWorkScheduler {
                final ObjHashSet<Job> jobs = new ObjHashSet<>();

                @Override
                public void addJob(Job job) {
                    jobs.add(job);
                }

                @Override
                public Sequence getIndexerPubSequence() {
                    return null;
                }

                @Override
                public RingQueue<ColumnIndexerEntry> getIndexerQueue() {
                    return null;
                }

                @Override
                public Sequence getIndexerSubSequence() {
                    return null;
                }
            }

            MyListener listener = new MyListener();
            MyWorkScheduler workScheduler = new MyWorkScheduler();

            try (Engine engine = new Engine(configuration, workScheduler)) {
                engine.setPoolListener(listener);

                assertWriter(engine, "x");
                assertReader(engine, "x");

                Assert.assertEquals(2, workScheduler.jobs.size());

                Job job = workScheduler.jobs.get(0);
                Assert.assertNotNull(job);

                Assert.assertTrue(job.run());
                Assert.assertFalse(job.run());

                Assert.assertEquals(2, listener.count);
            }
        });
    }

    @Test
    public void testLockBusyReader() throws Exception {

        createX();

        TestUtils.assertMemoryLeak(() -> {
            try (Engine engine = new Engine(configuration)) {
                try (TableReader reader = engine.getReader("x")) {
                    Assert.assertNotNull(reader);
                    Assert.assertFalse(engine.lock("x"));
                    assertReader(engine, "x");
                    assertWriter(engine, "x");
                }
            }
        });
    }

    @Test
    public void testNewTableRename() throws Exception {
        createX();

        TestUtils.assertMemoryLeak(() -> {
            try (Engine engine = new Engine(configuration)) {
                engine.rename("x", "y");

                assertWriter(engine, "y");
                assertReader(engine, "y");
            }
        });
    }

    @Test
    public void testRemoveExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            try (Engine engine = new Engine(configuration)) {
                assertReader(engine, "x");
                assertWriter(engine, "x");
                engine.remove("x");
                Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getStatus("x"));

                try {
                    engine.getReader("x");
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                try {
                    engine.getWriter("x");
                    Assert.fail();
                } catch (CairoException ignored) {
                }
            }
        });
    }

    @Test
    public void testRemoveNewTable() {

        createX();

        try (Engine engine = new Engine(configuration)) {
            engine.remove("x");
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getStatus("x"));
        }
    }

    @Test
    public void testRemoveNonExisting() throws Exception {
        createY(); // this will create root dir at least
        TestUtils.assertMemoryLeak(() -> {
            try (Engine engine = new Engine(configuration)) {
                try {
                    engine.remove("x");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "remove failed");
                }
            }
        });
    }

    @Test
    public void testRemoveWhenReaderBusy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            try (Engine engine = new Engine(configuration)) {
                try (TableReader reader = engine.getReader("x")) {
                    Assert.assertNotNull(reader);
                    try {
                        engine.remove("x");
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testRemoveWhenWriterBusy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            try (Engine engine = new Engine(configuration)) {
                try (TableWriter writer = engine.getWriter("x")) {
                    Assert.assertNotNull(writer);
                    try {
                        engine.remove("x");
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testRenameExisting() throws Exception {
        createX();

        TestUtils.assertMemoryLeak(() -> {
            try (Engine engine = new Engine(configuration)) {
                assertWriter(engine, "x");
                assertReader(engine, "x");

                engine.rename("x", "y");

                assertWriter(engine, "y");
                assertReader(engine, "y");

                Assert.assertTrue(engine.releaseAllReaders());
                Assert.assertTrue(engine.releaseAllWriters());
            }
        });
    }

    @Test
    public void testRenameExternallyLockedTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            try (TableWriter ignored1 = new TableWriter(configuration, "x")) {

                try (Engine engine = new Engine(configuration)) {
                    try {
                        engine.getWriter("x");
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    try {
                        engine.rename("x", "y");
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), "Cannot lock");
                    }
                }
            }
        });
    }

    @Test
    public void testRenameFail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            TestFilesFacade ff = new TestFilesFacade() {
                int counter = 1;

                @Override
                public boolean rename(LPSZ from, LPSZ to) {
                    return counter-- <= 0 && super.rename(from, to);
                }

                @Override
                public boolean wasCalled() {
                    return counter < 1;
                }
            };

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (Engine engine = new Engine(configuration)) {
                assertReader(engine, "x");
                assertWriter(engine, "x");
                try {
                    engine.rename("x", "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "Rename failed");
                }

                assertReader(engine, "x");
                assertWriter(engine, "x");
                engine.rename("x", "y");
                assertReader(engine, "y");
                assertWriter(engine, "y");
            }

            Assert.assertTrue(ff.wasCalled());
        });
    }

    @Test
    public void testRenameNonExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "z", PartitionBy.NONE)
                    .col("a", ColumnType.INT)) {
                CairoTestUtils.create(model);
            }

            try (Engine engine = new Engine(configuration)) {
                engine.rename("x", "y");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getMessage(), "does not exist");
            }
        });
    }

    @Test
    public void testRenameToExistingTarget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            createX();
            createY();

            try (Engine engine = new Engine(configuration)) {
                assertWriter(engine, "x");
                assertReader(engine, "x");
                try {
                    engine.rename("x", "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "exists");
                }
                assertWriter(engine, "x");
                assertReader(engine, "x");

                assertReader(engine, "y");
                assertWriter(engine, "y");
            }
        });
    }

    private void assertReader(Engine engine, String name) {
        try (TableReader reader = engine.getReader(name)) {
            Assert.assertNotNull(reader);
        }
    }

    private void assertWriter(Engine engine, String name) {
        try (TableWriter w = engine.getWriter(name)) {
            Assert.assertNotNull(w);
        }
    }

    private void createX() {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.INT)) {
            CairoTestUtils.create(model);
        }
    }

    private void createY() {
        try (TableModel model = new TableModel(configuration, "y", PartitionBy.NONE)
                .col("b", ColumnType.INT)) {
            CairoTestUtils.create(model);
        }
    }
}