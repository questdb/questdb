/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DataID;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DataIDTest extends AbstractBootstrapTest {
    @Test
    public void testChange() throws Exception {
        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testChange.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);
            Assert.assertFalse(id.isInitialized());

            // change() should fail if not initialized
            try {
                id.change(123L, 456L);
                Assert.fail("Expected CairoException");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot change DataID: not initialized");
            }

            // Initialize with first value
            final long initialLo = 111L;
            final long initialHi = 222L;
            Assert.assertTrue(id.initialize(initialLo, initialHi));
            Assert.assertTrue(id.isInitialized());
            Assert.assertEquals(initialLo, id.getLo());
            Assert.assertEquals(initialHi, id.getHi());

            // change() should succeed now and update the value
            final long newLo = 333L;
            final long newHi = 444L;
            id.change(newLo, newHi);
            Assert.assertEquals(newLo, id.getLo());
            Assert.assertEquals(newHi, id.getHi());

            // Verify the change persisted to disk
            final DataID reopened = DataID.open(config);
            Assert.assertTrue(reopened.isInitialized());
            Assert.assertEquals(newLo, reopened.getLo());
            Assert.assertEquals(newHi, reopened.getHi());
        });
    }

    /**
     * Green counterpart of the cross-call torn read for the SQL consumer. {@code current_data_id()}
     * ({@code CurrentDataIdFunctionFactory}) now reads both halves through {@link DataID#getSnapshot()},
     * which takes the pair under a single monitor acquisition. A reader hammering {@code getSnapshot()}
     * exactly while the one-shot replica publish ({@link DataID#initialize(long, long)}) lands must never
     * observe a torn pair -- one half still the NULL sentinel, the other published -- the value the SQL
     * function would otherwise surface as neither NULL nor the correct id.
     * <p>
     * Was RED against the pre-fix two-call read {@code new UuidConstant(id.getLo(), id.getHi())} (the
     * publish landing between the two separate accessor calls); GREEN now that the read site routes
     * through the atomic accessor.
     */
    @Test
    public void testConcurrentSnapshotStaysAtomicAcrossInitialize() throws Exception {
        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testSnapshotInit.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);
            Assert.assertFalse(id.isInitialized());

            // Deterministic boundary check: a pre-publish snapshot is fully unpublished.
            final Uuid before = id.getSnapshot();
            Assert.assertEquals(Numbers.LONG_NULL, before.getLo());
            Assert.assertEquals(Numbers.LONG_NULL, before.getHi());

            // Neither half equals the NULL sentinel (Long.MIN_VALUE), so a torn pair is unambiguous.
            final long realLo = 0x1122334455667788L;
            final long realHi = 0x0123456789abcdefL;

            final SOCountDownLatch ready = new SOCountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicLong tornLo = new AtomicLong();
            final AtomicLong tornHi = new AtomicLong();
            final AtomicBoolean torn = new AtomicBoolean(false);
            final AtomicBoolean published = new AtomicBoolean(false);

            // Reader hammers the atomic accessor the SQL function now uses, racing the one-shot publish.
            final Thread reader = new Thread(() -> {
                try {
                    ready.await();
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (System.nanoTime() - deadline < 0) {
                        final Uuid snap = id.getSnapshot();
                        final long lo = snap.getLo();
                        final long hi = snap.getHi();
                        if ((lo == Numbers.LONG_NULL) != (hi == Numbers.LONG_NULL)) {
                            tornLo.set(lo);
                            tornHi.set(hi);
                            torn.set(true);
                            break;
                        }
                        if (published.get() && lo != Numbers.LONG_NULL) {
                            break; // observed the fully-published value; the race window is closed
                        }
                    }
                } catch (Throwable t) {
                    error.set(t);
                }
            }, "dataid-snapshot-reader");

            // Writer models the replication downloader's one-shot initialize() JNI up-call.
            final Thread writer = new Thread(() -> {
                try {
                    ready.await();
                    Assert.assertTrue(id.initialize(realLo, realHi));
                    published.set(true);
                } catch (Throwable t) {
                    error.set(t);
                }
            }, "dataid-snapshot-writer");

            reader.start();
            writer.start();
            ready.countDown(); // release reader and writer together to maximise overlap
            reader.join();
            writer.join();

            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            Assert.assertFalse(
                    "getSnapshot() returned a torn pair across initialize(): lo=" + tornLo.get()
                            + " hi=" + tornHi.get(),
                    torn.get());

            // Sanity: the publish landed and the post-publish snapshot is exactly the value.
            final Uuid after = id.getSnapshot();
            Assert.assertEquals(realLo, after.getLo());
            Assert.assertEquals(realHi, after.getHi());
        });
    }

    /**
     * Symmetric green counterpart for the Rust consumer and the {@code change()} (point-in-time-recovery
     * re-stamp) publish path. The Rust {@code JavaDataId::get()} (qdb-ent/src/data_id.rs) now holds the
     * DataID monitor across its {@code getHi()}/{@code getLo()} JNI reads, so the pair is atomic and the
     * {@code QDB_NULL_UUID} guard never sees a torn value. This exercises the same Java-side invariant the
     * Rust monitor relies on, against {@link DataID#change(long, long)}, which overwrites an already
     * published value (real -&gt; real'): a two-call read could observe {@code (newHi, oldLo)} -- BOTH
     * non-null, so a NULL-sentinel guard would not even catch it -- whereas an atomic snapshot is always
     * either fully the old value or fully the new value.
     */
    @Test
    public void testConcurrentSnapshotStaysAtomicAcrossChange() throws Exception {
        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testSnapshotChange.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);

            final long oldLo = 0x1111111111111111L;
            final long oldHi = 0x2222222222222222L;
            final long newLo = 0x3333333333333333L;
            final long newHi = 0x4444444444444444L;
            Assert.assertTrue(id.initialize(oldLo, oldHi));

            final SOCountDownLatch ready = new SOCountDownLatch(1);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicLong badLo = new AtomicLong();
            final AtomicLong badHi = new AtomicLong();
            final AtomicBoolean torn = new AtomicBoolean(false);
            final AtomicBoolean changed = new AtomicBoolean(false);

            // Reader hammers getSnapshot() while change() re-stamps the value. Read hi then lo from the
            // snapshot to mirror the Rust JavaDataId::get() order; since the snapshot is an immutable
            // copy, both halves always belong to the same publish regardless of read order.
            final Thread reader = new Thread(() -> {
                try {
                    ready.await();
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (System.nanoTime() - deadline < 0) {
                        final Uuid snap = id.getSnapshot();
                        final long hi = snap.getHi();
                        final long lo = snap.getLo();
                        final boolean isOld = lo == oldLo && hi == oldHi;
                        final boolean isNew = lo == newLo && hi == newHi;
                        if (!isOld && !isNew) {
                            badLo.set(lo);
                            badHi.set(hi);
                            torn.set(true);
                            break;
                        }
                        if (changed.get() && isNew) {
                            break; // observed the fully re-stamped value; race window closed
                        }
                    }
                } catch (Throwable t) {
                    error.set(t);
                }
            }, "dataid-change-reader");

            final Thread writer = new Thread(() -> {
                try {
                    ready.await();
                    id.change(newLo, newHi);
                    changed.set(true);
                } catch (Throwable t) {
                    error.set(t);
                }
            }, "dataid-change-writer");

            reader.start();
            writer.start();
            ready.countDown();
            reader.join();
            writer.join();

            if (error.get() != null) {
                throw new AssertionError(error.get());
            }
            Assert.assertFalse(
                    "getSnapshot() returned a value belonging to neither the old nor the new id across "
                            + "change(): lo=" + badLo.get() + " hi=" + badHi.get(),
                    torn.get());

            final Uuid after = id.getSnapshot();
            Assert.assertEquals(newLo, after.getLo());
            Assert.assertEquals(newHi, after.getHi());
        });
    }

    @Test
    public void testDataIDSurvivesRestarts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            // Start.
            Uuid initDataId;
            try (ServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                initDataId = serverMain.getEngine().getDataID().get();
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
            }

            // Restart 1.
            Uuid restartUuid1;
            try (ServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                restartUuid1 = serverMain.getEngine().getDataID().get();
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
            }

            Assert.assertEquals(initDataId.getLo(), restartUuid1.getLo());
            Assert.assertEquals(initDataId.getHi(), restartUuid1.getHi());

            // Restart 2.
            Uuid restartUuid2;
            try (ServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                restartUuid2 = serverMain.getEngine().getDataID().get();
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
            }

            Assert.assertEquals(initDataId.getLo(), restartUuid2.getLo());
            Assert.assertEquals(initDataId.getHi(), restartUuid2.getHi());
        });
    }

    @Test
    public void testInvalidDataID() throws Exception {
        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testInvalidDataID.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            // Creates a file of 8 bytes instead of 16
            try (Path path = new Path()) {
                path.of(config.getDbRoot());
                path.concat(DataID.FILENAME);

                final FilesFacade ff = config.getFilesFacade();
                try (var mem = new MemoryCMARWImpl(ff, path.$(), 8, -1, MemoryTag.MMAP_DEFAULT, config.getWriterFileOpenOpts())) {
                    mem.putLong(0, 123);
                    mem.sync(false);
                }
            }

            DataID id = DataID.open(config);
            Assert.assertNotNull(id);
            Assert.assertFalse(id.isInitialized());
            Assert.assertEquals(Numbers.LONG_NULL, id.getLo());
            Assert.assertEquals(Numbers.LONG_NULL, id.getHi());
        });
    }

    @Test
    public void testOpenDataID() throws Exception {
        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testOpenDataID.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);
            Assert.assertNotNull(id);
            Assert.assertFalse(id.isInitialized());
            Assert.assertEquals(Numbers.LONG_NULL, id.getLo());
            Assert.assertEquals(Numbers.LONG_NULL, id.getHi());

            final Rnd rnd = new Rnd(config.getMicrosecondClock().getTicks(), config.getMillisecondClock().getTicks());
            final Uuid currentId = new Uuid();
            currentId.of(rnd.nextLong(), rnd.nextLong());
            id.initialize(currentId.getLo(), currentId.getHi());
            Assert.assertTrue(id.isInitialized());
            Assert.assertEquals(id.getLo(), currentId.getLo());
            Assert.assertEquals(id.getHi(), currentId.getHi());

            Assert.assertEquals(id.get().getLo(), id.getLo());
            Assert.assertEquals(id.get().getHi(), id.getHi());

            final DataID updatedId = DataID.open(config);
            Assert.assertTrue(updatedId.isInitialized());
            Assert.assertEquals(updatedId.getLo(), currentId.getLo());
            Assert.assertEquals(updatedId.getHi(), currentId.getHi());

            // Ensure that the data is still there
            final DataID updatedId2 = DataID.open(config);
            Assert.assertTrue(updatedId2.isInitialized());
            Assert.assertEquals(updatedId2.getLo(), currentId.getLo());
            Assert.assertEquals(updatedId2.getHi(), currentId.getHi());
        });
    }

    /**
     * This test checks that we write the data as per the standard RFC 4122 big endian binary representation.
     */
    @Test
    public void testSpecificValue() throws Exception {
        final Uuid specific = new Uuid();
        specific.of("14cec117-b3f0-487f-83af-55e3b9acf4da");
        final byte[] expected = {
                (byte) 20, (byte) -50, (byte) -63, (byte) 23,
                (byte) -77, (byte) -16, (byte) 72, (byte) 127,
                (byte) -125, (byte) -81, (byte) 85, (byte) -29,
                (byte) -71, (byte) -84, (byte) -12, (byte) -38
        };

        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testSpecificValue.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);
            Assert.assertFalse(id.isInitialized());
            id.initialize(specific.getLo(), specific.getHi());

            final java.io.File dataIdFile = new java.io.File(tmpDbRoot, DataID.FILENAME);
            final byte[] actual = java.nio.file.Files.readAllBytes(dataIdFile.toPath());

            Assert.assertArrayEquals(expected, Arrays.copyOf(actual, 16));
        });
    }
}