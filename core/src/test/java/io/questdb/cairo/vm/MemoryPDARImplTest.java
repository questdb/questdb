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

package io.questdb.cairo.vm;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;

public class MemoryPDARImplTest extends AbstractCairoTest {

    @Test
    public void testAppendAndRead() {
        try (
                Path path = new Path().of(root).concat("x.d").$();
                MemoryPDARImpl mem = new MemoryPDARImpl(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        FilesFacadeImpl._16M,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_ASYNC | CairoConfiguration.O_DIRECT
                );

                MemoryCMR rmem = Vm.getCMARWInstance(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        FilesFacadeImpl._16M,
                        0,
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                )
        ) {
            int n = 10_000_000;
            Rnd rnd = new Rnd();
            // useless jump, making sure we're dealing with it
            mem.jumpTo(0);
            for (long i = 0; i < n; i++) {
                mem.putLong(rnd.nextLong());
            }
            mem.flush();

            rmem.resize(n * 8);
            rnd.reset();
            for (long i = 0; i < n; i++) {
                Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
            }
        }
    }

    @Test
    public void testAppendMidPage() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (
                    MemoryPDARImpl mem = new MemoryPDARImpl(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            32 * 1024,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_DIRECT
                    );

                    MemoryCMR rmem = Vm.getCMARWInstance(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            FilesFacadeImpl._16M,
                            0,
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                // this is half the page size
                int n = 2048;
                Rnd rnd = new Rnd();
                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }
                // this is unnecessary but we should check this is tolerated
                mem.jumpTo(n * 8);

                // deliberately avoid flush() call, re-purposing the memory must flush()

                // re-open mem and set append position where we left off
                mem.of(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        32 * 1024,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_DIRECT
                );

                mem.jumpTo(n * 8L);

                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }

                mem.flush();


                rmem.resize(2 * n * 8);
                rnd.reset();
                for (long i = 0; i < 2 * n; i++) {
                    Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                }
            }
            Assert.assertEquals(Files.PAGE_SIZE, Files.length(path));
        }
    }

    @Test
    public void testAppendNextPageMid() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (
                    MemoryPDARImpl mem = new MemoryPDARImpl(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            32 * 1024,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_DIRECT
                    );

                    MemoryCMR rmem = Vm.getCMARWInstance(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            FilesFacadeImpl._16M,
                            0,
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                // append half page
                int n = 3096;
                Rnd rnd = new Rnd();
                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }
                // this is unnecessary but we should check this is tolerated
                mem.jumpTo(n * 8);

                // deliberately avoid flush() call, re-purposing the memory must flush()

                // re-open mem and set append position where we left off
                mem.of(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        32 * 1024,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_DIRECT
                );

                mem.jumpTo(2 * n * 8L);

                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }

                mem.flush();

                rmem.resize(n * 8);
                rnd.reset();
                for (long i = 0; i < n; i++) {
                    Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                }

                // check that area we skipped over is zero
                for (long i = n; i < 2 * n; i++) {
                    Assert.assertEquals(0, rmem.getLong(i * 8));
                }

                // check that second page we've written is correct
                for (long i = 2 * n; i < 3 * n; i++) {
                    Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                }
            }
            Assert.assertEquals(2 * Files.PAGE_SIZE, Files.length(path));
        }
    }

    @Test
    public void testAppendNextPageStart() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (
                    MemoryPDARImpl mem = new MemoryPDARImpl(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            32 * 1024,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_DIRECT
                    );

                    MemoryCMR rmem = Vm.getCMARWInstance(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            FilesFacadeImpl._16M,
                            0,
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                // append half page
                int n = 2048;
                Rnd rnd = new Rnd();
                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }
                // this is unnecessary but we should check this is tolerated
                mem.jumpTo(n * 8);

                // deliberately avoid flush() call, re-purposing the memory must flush()

                // re-open mem and set append position where we left off
                mem.of(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        32 * 1024,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_DIRECT
                );

                mem.jumpTo(2 * n * 8L);

                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }

                mem.flush();

                rmem.resize(n * 8);
                rnd.reset();
                for (long i = 0; i < n; i++) {
                    Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                }

                // check that area we skipped over is zero
                for (long i = n; i < 2 * n; i++) {
                    Assert.assertEquals(0, rmem.getLong(i * 8));
                }

                // check that second page we've written is correct
                for (long i = 2 * n; i < 3 * n; i++) {
                    Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                }
            }
            Assert.assertEquals(Files.PAGE_SIZE, Files.length(path));
        }
    }

    @Test
    public void testAppendPageEdge() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (
                    MemoryPDARImpl mem = new MemoryPDARImpl(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            32 * 1024,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_DIRECT
                    );

                    MemoryCMR rmem = Vm.getCMARWInstance(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            FilesFacadeImpl._16M,
                            0,
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                // exactly full page
                int n = 4096;
                Rnd rnd = new Rnd();
                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }
                // this is unnecessary but we should check this is tolerated
                mem.jumpTo(n * 8);

                // deliberately avoid flush() call, re-purposing the memory must flush()

                // re-open mem and set append position where we left off
                mem.of(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        32 * 1024,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_DIRECT
                );

                mem.jumpTo(n * 8L);

                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }

                mem.flush();

                rmem.resize(2 * n * 8);
                rnd.reset();
                for (long i = 0; i < 2 * n; i++) {
                    Assert.assertEquals("" + n, rnd.nextLong(), rmem.getLong(i * 8));
                }
            }
            Assert.assertEquals(Files.PAGE_SIZE, Files.length(path));
        }
    }

    @Test
    public void testDefaultConstructor() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (MemoryPDARImpl mem = new MemoryPDARImpl()) {

                // ensure flush() is no op on closed memory
                mem.flush();

                mem.of(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        2048,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_ASYNC
                );

                try (MemoryCMR rmem = Vm.getCMARWInstance(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        FilesFacadeImpl._16M,
                        0,
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                )
                ) {
                    int n = 100;
                    Rnd rnd = new Rnd();
                    for (long i = 0; i < n; i++) {
                        mem.putLong(rnd.nextLong());
                    }
                    mem.flush();

                    rmem.resize(n * 8);
                    rnd.reset();
                    for (long i = 0; i < n; i++) {
                        Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                    }
                }
            }
            Assert.assertEquals(Files.PAGE_SIZE, Files.length(path));
        }
    }

    public void testMemMap(int c, long n) {
        // write simple long
        try (
                Path path = new Path();
                MemoryPMARImpl mem = new MemoryPMARImpl(
                        FilesFacadeImpl.INSTANCE,
                        path.of(root).concat("x.d" + c).$(),
                        FilesFacadeImpl._16M,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                )
        ) {
            Rnd rnd = new Rnd();
            long t = System.nanoTime();
            for (long i = 0; i < n; i++) {
                mem.putLong(rnd.nextLong());
            }
            System.out.println(System.nanoTime() - t);
        }
    }

    @Test
    @Ignore
    public void testMulti1() {

        int n = 2;

        CyclicBarrier barrier = new CyclicBarrier(n);
        SOCountDownLatch latch = new SOCountDownLatch(n);
        for (int i = 0; i < n; i++) {
            int c = i;
            new Thread(() -> {
                TestUtils.await(barrier);
                testMemMap(c, 1_000_000_000);
                latch.countDown();
            }).start();
        }

        latch.await();
    }

    @Test
    public void testOpenExistingFileForAppend() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (MemoryPDARImpl mem = new MemoryPDARImpl()) {

                // ensure flush() is no op on closed memory
                mem.flush();

                mem.of(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        2048,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                );

                try (MemoryCMR rmem = Vm.getCMARWInstance(
                        FilesFacadeImpl.INSTANCE,
                        path,
                        FilesFacadeImpl._16M,
                        0,
                        MemoryTag.MMAP_DEFAULT,
                        configuration.getWriterFileOpenOpts()
                )
                ) {
                    int n = 100;
                    Rnd rnd = new Rnd();
                    for (long i = 0; i < n; i++) {
                        mem.putLong(rnd.nextLong());
                    }
                    mem.flush();

                    rmem.resize(n * 8);
                    rnd.reset();
                    for (long i = 0; i < n; i++) {
                        Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                    }
                }
            }
            Assert.assertEquals(Files.PAGE_SIZE, Files.length(path));
        }
    }

    public void testSimple(int c, long n) {
        // write simple long
        try (
                Path path = new Path();
                MemoryPDARImpl mem = new MemoryPDARImpl(
                        FilesFacadeImpl.INSTANCE,
                        path.of(root).concat("x.d" + c).$(),
                        FilesFacadeImpl._16M,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_DIRECT
                )
        ) {
            Rnd rnd = new Rnd();
            long t = System.nanoTime();
            for (long i = 0; i < n; i++) {
                mem.putLong(rnd.nextLong());
            }
            System.out.println(System.nanoTime() - t);
        }
    }

    @Test
    public void testTruncateToPageSize() {
        try (Path path = new Path().of(root).concat("x.d").$()) {
            try (
                    MemoryPDARImpl mem = new MemoryPDARImpl(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            FilesFacadeImpl._16M,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_DIRECT
                    );

                    MemoryCMR rmem = Vm.getCMARWInstance(
                            FilesFacadeImpl.INSTANCE,
                            path,
                            FilesFacadeImpl._16M,
                            0,
                            MemoryTag.MMAP_DEFAULT,
                            configuration.getWriterFileOpenOpts()
                    )
            ) {
                int n = 100;
                Rnd rnd = new Rnd();
                for (long i = 0; i < n; i++) {
                    mem.putLong(rnd.nextLong());
                }
                mem.flush();

                rmem.resize(n * 8);
                rnd.reset();
                for (long i = 0; i < n; i++) {
                    Assert.assertEquals(rnd.nextLong(), rmem.getLong(i * 8));
                }
            }
            Assert.assertEquals(Files.PAGE_SIZE, Files.length(path));
        }
    }
}