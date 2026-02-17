/*******************************************************************************
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class MemRemappedFileTest extends AbstractTest {
    private static final int NCYCLES = 4;
    private static final int NPAGES = 1000;
    private static final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
    private static final long MAPPING_PAGE_SIZE = ff.getPageSize();
    private static int nFile = 0;
    private static CharSequence root;
    private final Path path = new Path(1_000_000);

    @AfterClass
    public static void afterClass() {
        LOG.info().$("Finished").$();
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        LOG.info().$("Starting").$();
        root = temp.newFolder("root").getAbsolutePath();
    }

    @Test
    public void testExtendableOnePageMemory() {
        LOG.info().$("ExtendableOnePageMemory starting").$();
        double micros = test(new MemoryCMRImpl());
        LOG.info().$("ExtendableOnePageMemory took ").$(micros).$("ms").$();
    }

    @Test
    public void testReadOnlyMemory() {
        LOG.info().$("ReadOnlyMemory starting").$();
        double micros = test(new MemoryCMRImpl());
        LOG.info().$("ReadOnlyMemory took ").$(micros).$("ms").$();
    }

    private double test(MemoryMR readMem) {
        long nanos = 0;
        try (MemoryMA appMem = Vm.getPMARInstance(null)) {
            for (int cycle = 0; cycle < NCYCLES; cycle++) {
                path.trimTo(0).concat(root).concat("file" + nFile).$();
                nFile++;
                Rnd rnd = new Rnd();
                long expectedTotal = 0;

                nanos = System.nanoTime();
                long actualTotal = 0;
                long offset = 0;
                for (int nPage = 0; nPage < NPAGES; nPage++) {
                    long newSize = MAPPING_PAGE_SIZE * (nPage + 1);
                    appMem.of(ff, path.$(), newSize, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                    appMem.skip(newSize - MAPPING_PAGE_SIZE);
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        byte b = (byte) rnd.nextInt();
                        appMem.putByte(b);
                        expectedTotal += b;
                    }
                    if (nPage == 0) {
                        readMem.smallFile(ff, path.$(), MemoryTag.MMAP_DEFAULT);
                    } else {
                        readMem.extend(newSize);
                    }
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        actualTotal += readMem.getByte(offset);
                        offset++;
                    }
                }

                nanos = System.nanoTime() - nanos;
                Assert.assertEquals(expectedTotal, actualTotal);
                appMem.close();
                readMem.close();
                ff.remove(path.$());
            }
            readMem.close();
            return nanos / 1000000.0;
        }
    }
}
