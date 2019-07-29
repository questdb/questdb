/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.mp.Job;
import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.std.FilesFacade;
import com.questdb.std.ObjHashSet;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CairoEngineTest extends AbstractCairoTest {
    private final static Path path = new Path();
    private final static Path otherPath = new Path();

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

            try (CairoEngine engine = new CairoEngine(configuration)) {
                engine.setPoolListener(listener);
                Assert.assertEquals(listener, engine.getPoolListener());

                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", -1);
                TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x");
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

            try (CairoEngine engine = new CairoEngine(configuration, workScheduler)) {
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
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                    Assert.assertNotNull(reader);
                    Assert.assertFalse(engine.lock(AllowAllCairoSecurityContext.INSTANCE, "x"));
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
            try (CairoEngine engine = new CairoEngine(configuration)) {
                engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");

                assertWriter(engine, "y");
                assertReader(engine, "y");
            }
        });
    }

    @Test
    public void testRemoveExisting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createX();

            try (CairoEngine engine = new CairoEngine(configuration)) {
                assertReader(engine, "x");
                assertWriter(engine, "x");
                engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "x");
                Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, "x"));

                try {
                    engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION);
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                try {
                    engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x");
                    Assert.fail();
                } catch (CairoException ignored) {
                }
            }
        });
    }

    @Test
    public void testRemoveNewTable() {

        createX();

        try (CairoEngine engine = new CairoEngine(configuration)) {
            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "x");
            Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, "x"));
        }
    }

    @Test
    public void testRemoveNonExisting() throws Exception {
        createY(); // this will create root dir at least
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try {
                    engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "x");
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

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_VERSION)) {
                    Assert.assertNotNull(reader);
                    try {
                        engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "x");
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

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                    Assert.assertNotNull(writer);
                    try {
                        engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "x");
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
            try (CairoEngine engine = new CairoEngine(configuration)) {
                assertWriter(engine, "x");
                assertReader(engine, "x");

                engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");

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

                try (CairoEngine engine = new CairoEngine(configuration)) {
                    try {
                        engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x");
                        Assert.fail();
                    } catch (CairoException ignored) {
                    }

                    try {
                        engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");
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

            try (CairoEngine engine = new CairoEngine(configuration)) {
                assertReader(engine, "x");
                assertWriter(engine, "x");
                try {
                    engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "Rename failed");
                }

                assertReader(engine, "x");
                assertWriter(engine, "x");
                engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");
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

            try (CairoEngine engine = new CairoEngine(configuration)) {
                engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");
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

            try (CairoEngine engine = new CairoEngine(configuration)) {
                assertWriter(engine, "x");
                assertReader(engine, "x");
                try {
                    engine.rename(AllowAllCairoSecurityContext.INSTANCE, path, "x", otherPath, "y");
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

    @Test
    public void testWrongReaderVersion() throws Exception {
        createX();

        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                assertWriter(engine, "x");
                try {
                    engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", 2);
                    Assert.fail();
                } catch (ReaderOutOfDateException ignored) {
                }
                Assert.assertTrue(engine.releaseAllReaders());
                Assert.assertTrue(engine.releaseAllWriters());
            }
        });
    }

    private void assertReader(CairoEngine engine, String name) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, name, TableUtils.ANY_TABLE_VERSION)) {
            Assert.assertNotNull(reader);
        }
    }

    private void assertWriter(CairoEngine engine, String name) {
        try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, name)) {
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