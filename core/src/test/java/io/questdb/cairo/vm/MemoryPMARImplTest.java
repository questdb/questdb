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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.api.MemoryM;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.ConcurrentLinkedQueue;

public class MemoryPMARImplTest {
    private static final Log LOG = LogFactory.getLog(MemoryPMARImplTest.class);

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testJumpChangesActivePage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long pageSize = Files.PAGE_SIZE;
            ConcurrentLinkedQueue<Throwable> allErrors = new ConcurrentLinkedQueue<>();
            ObjList<Thread> threads = new ObjList<>();
            FilesFacade ff = FilesFacadeImpl.INSTANCE;

            for (int thread = 0; thread < 10; thread++) {
                Thread th = new Thread(() -> {

                    try (Path path = new Path().of(temp.newFile().getAbsolutePath()).$()) {

                        LOG.info().$(path).$();
                        try (MemoryPARWImpl mem = new MemoryPMARImpl(ff, path, pageSize, MemoryTag.NATIVE_DEFAULT, CairoConfiguration.O_NONE)) {
                            long pos = 0;

                            mem.jumpTo(0);
                            String value = "abcdef";
                            mem.putStr(value);
                            pos = mem.getAppendOffset();
                            Assert.assertEquals('f', mem.getChar(pos - 2));

                            mem.jumpTo(2 * pageSize);
                            mem.jumpTo(0);

                            Assert.assertEquals(0, mem.getAppendOffset());
                            Assert.assertEquals(0, mem.pageIndex(mem.getAppendOffset()));

                            long addr = ((MemoryM) mem).map(0, 4);
                            long pageAddress = mem.getPageAddress(0);

                            Assert.assertEquals(pageAddress, addr);
                        }
                    } catch (Throwable e) {
                        allErrors.add(e);
                    }
                });
                th.start();
                threads.add(th);
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                try {
                    threads.getQuick(i).join();
                } catch (InterruptedException e) {
                }
            }

            if (!allErrors.isEmpty()) {
                throw new RuntimeException(allErrors.poll());
            }
        });
    }
}
