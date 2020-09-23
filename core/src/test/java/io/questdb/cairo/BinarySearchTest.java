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

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.Test;

public class BinarySearchTest extends AbstractCairoTest {

    @Test
    public void testFindForward() {
        try (Path path = new Path()) {
            path.of(root).concat("binsearch.d").$();
            try (AppendMemory appendMem = new AppendMemory(FilesFacadeImpl.INSTANCE, path, 4096)) {
                for (int i = 0; i < 100; i++) {
                    appendMem.putLong(i);
                    appendMem.putLong(i);
                    appendMem.putLong(i);
                    appendMem.putLong(i);
                    appendMem.putLong(i);
                }

                try (OnePageMemory mem = new OnePageMemory(FilesFacadeImpl.INSTANCE, path.of(root).concat("binsearch.d"), 400 * Long.BYTES)) {
                    System.out.println(BinarySearch.find(mem, 20, 0, 400, BinarySearch.SCAN_DOWN));
                }
            }
        }
    }
}