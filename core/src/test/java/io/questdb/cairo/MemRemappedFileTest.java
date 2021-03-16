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

package io.questdb.cairo;

import java.io.IOException;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;

public class MemRemappedFileTest {
    private static final int NPAGES = 1000;
    private static final int NCYCLES = 4;
    private static final Log LOG = LogFactory.getLog(MemRemappedFileTest.class);
    private static final FilesFacade ff = FilesFacadeImpl.INSTANCE;
    private static final long MAPPING_PAGE_SIZE = ff.getPageSize();
    private static CharSequence root;
    private static int nFile = 0;

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private final Path path = new Path(1_000_000);

    @Test
    public void testReadOnlyMemory() {
        LOG.info().$("ReadOnlyMemory starting").$();
        double micros = test(new ReadOnlyMemory());
        LOG.info().$("ReadOnlyMemory took ").$(micros).$("ms").$();
    }

    @Test
    public void testExtendableOnePageMemory() {
        LOG.info().$("ExtendableOnePageMemory starting").$();
        double micros = test(new ExtendableOnePageMemory());
        LOG.info().$("ExtendableOnePageMemory took ").$(micros).$("ms").$();
    }

    private double test(ReadOnlyColumn readMem) {
        long nanos = 0;
        try (AppendMemory appMem = new AppendMemory()) {
            for (int cycle = 0; cycle < NCYCLES; cycle++) {
                path.trimTo(0).concat(root).put(Files.SEPARATOR).concat("file" + nFile).$();
                nFile++;
                Random rand = new Random(0);
                long expectedTotal = 0;

                nanos = System.nanoTime();
                long actualTotal = 0;
                long offset = 0;
                for (int nPage = 0; nPage < NPAGES; nPage++) {
                    long newSize = MAPPING_PAGE_SIZE * (nPage + 1);
                    appMem.of(ff, path, newSize);
                    appMem.jumpTo(newSize - MAPPING_PAGE_SIZE);
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        byte b = (byte) rand.nextInt();
                        appMem.putByte(b);
                        expectedTotal += b;
                    }
                    if (nPage == 0) {
                        readMem.of(ff, path, MAPPING_PAGE_SIZE, newSize);
                    } else {
                        readMem.grow(newSize);
                    }
                    for (int i = 0; i < MAPPING_PAGE_SIZE; i++) {
                        actualTotal += readMem.getByte(offset);
                        offset++;
                    }
                }

                nanos = System.nanoTime() - nanos;
                Assert.assertEquals(expectedTotal, actualTotal);

                ff.remove(path);
            }
            readMem.close();
            return nanos / 1000000.0;
        }
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        LOG.info().$("Starting").$();
        root = temp.newFolder("root").getAbsolutePath();
    }

    @AfterClass
    public static void afterClass() {
        LOG.info().$("Finished").$();
    }
}
