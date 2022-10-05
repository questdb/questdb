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

import io.questdb.cairo.vm.api.MemoryM;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.ConcurrentLinkedQueue;

import static io.questdb.cairo.CairoConfiguration.O_DIRECT;

public class MemoryPMARImplTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testJumpChangesActivePage() {
        long pageSize = Files.PAGE_SIZE;
        ConcurrentLinkedQueue<Throwable> allErrors = new ConcurrentLinkedQueue<>();
        ObjList<Thread> threads = new ObjList<>();

        for (int thread = 0; thread < 10; thread++) {
            final int threadNum = thread;

            Thread th = new Thread(() -> {

                try (Path path = new Path()) {
                    FilesFacade ff = FilesFacadeImpl.INSTANCE;
                    path.of(temp.newFolder("root" + threadNum).getAbsolutePath());

                    path.chop$().concat("testJumpChangesActivePage");
                    if (!ff.touch(path.$())) {
                        throw new RuntimeException("Cannot create file: " + path + " errno=" + ff.errno());
                    }

                    try (MemoryPARWImpl mem = new MemoryPMARImpl(ff, path, pageSize, MemoryTag.NATIVE_DEFAULT, O_DIRECT)) {
                        long pos = 0;
                        int page = 0;

                        mem.jumpTo(page * pageSize);
                        String value = "abcdef";

                        Assert.assertEquals(16, Vm.getStorageLength(value));
                        mem.putStr(value);

                        while (mem.pageIndex(mem.getAppendOffset()) < page + 1) {
                            if (mem.pageIndex(mem.getAppendOffset()) == page) {
                                pos = mem.getAppendOffset();
                                mem.getChar(pos - 2);
                            }
                            mem.putStr(value);
                        }

                        mem.jumpTo(pos);
                        Assert.assertEquals(pos, mem.getAppendOffset());
                        Assert.assertEquals(page, mem.pageIndex(mem.getAppendOffset()));

                        long addr = ((MemoryM) mem).map(pos, 4);
                        long pageAddress = mem.getPageAddress(page);

                        Assert.assertEquals(pageAddress + pos - page * pageSize, addr);
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
    }
}
