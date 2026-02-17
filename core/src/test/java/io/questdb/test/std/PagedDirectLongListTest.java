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

package io.questdb.test.std;

import io.questdb.std.MemoryTag;
import io.questdb.std.PagedDirectLongList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PagedDirectLongListTest extends AbstractTest {
    @Test
    public void testAllocateExceedsCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (PagedDirectLongList pagedList = new PagedDirectLongList(MemoryTag.NATIVE_O3)) {
                int blockSize = 513;
                pagedList.setBlockSize(blockSize);

                int count = 1000;
                for (int i = 0; i < count; i++) {
                    long addr = pagedList.allocateBlock();
                    for (int j = 0; j < blockSize / 8; j += 8) {
                        Unsafe.getUnsafe().putLong(addr + (long) j * Long.BYTES, i + j);
                    }
                }

                Assert.assertEquals(count, getCount(pagedList, blockSize));

                count *= 2;
                blockSize = 514;
                pagedList.clear();
                pagedList.setBlockSize(blockSize);

                for (int i = 0; i < count; i++) {
                    long addr = pagedList.allocateBlock();
                    for (int j = 0; j < blockSize / 8; j += 8) {
                        Unsafe.getUnsafe().putLong(addr + (long) j * Long.BYTES, i + j);
                    }
                }

                Assert.assertEquals(count, getCount(pagedList, blockSize));

                try {
                    pagedList.setBlockSize(23);
                    Assert.fail();
                } catch (UnsupportedOperationException e) {
                    Assert.assertEquals("list must be clear when changing block size", e.getMessage());
                }
            }
        });
    }

    private static int getCount(PagedDirectLongList pagedList, int blockSize) {
        long blockIndex = -1;
        int c = 0;
        while ((blockIndex = pagedList.nextBlockIndex(blockIndex)) > -1L) {
            long memAddr = pagedList.getBlockAddress(blockIndex);
            for (int j = 0; j < blockSize / 8; j += 8) {
                Assert.assertEquals(c + j, Unsafe.getUnsafe().getLong(memAddr + (long) j * Long.BYTES));
            }
            c++;
        }
        return c;
    }
}
