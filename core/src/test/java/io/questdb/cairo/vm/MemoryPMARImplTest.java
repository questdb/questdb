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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static io.questdb.cairo.CairoConfiguration.O_DIRECT;

public class MemoryPMARImplTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testJumpChangesActivePage() throws IOException {
        long pageSize = 8 * Files.PAGE_SIZE;
        try (Path path = new Path()) {
            path.of(temp.newFolder("root").getAbsolutePath()).concat("testJumpChangesActivePage");
            FilesFacade ff = FilesFacadeImpl.INSTANCE;
            ff.touch(path.$());

            try (MemoryPARWImpl mem = new MemoryPMARImpl(ff, path, pageSize, MemoryTag.NATIVE_DEFAULT, O_DIRECT)) {

                long pos = 0;
                int page = 0;

                mem.putStr("abcd");
                mem.getChar(mem.getAppendOffset() - 2);

                while (mem.pageIndex(mem.getAppendOffset()) < page + 5) {
                    if (mem.pageIndex(mem.getAppendOffset()) == page) {
                        pos = mem.getAppendOffset();
                    }
                    mem.putStr("abcd");
                }

                mem.jumpTo(pos);
                Assert.assertEquals(pos, mem.getAppendOffset());

                long addr = ((MemoryM) mem).map(pos, 4);
                long pageAddress = mem.getPageAddress(page);
                Assert.assertTrue((addr - pageAddress < pageSize) && addr > pageAddress);

                mem.putStr("abcd");
            }
        }
    }
}
