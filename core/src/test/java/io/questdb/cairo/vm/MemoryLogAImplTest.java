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

package io.questdb.cairo.vm;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

public class MemoryLogAImplTest extends AbstractCairoTest {
    @Test
    public void testLogWorkflow() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    MemoryMAR log = Vm.getMARInstance();
                    MemoryMAR main = Vm.getMARInstance();
                    MemoryMR reader = Vm.getMRInstance()
            ) {
                final FilesFacade ff = FilesFacadeImpl.INSTANCE;

                log.of(ff, path.of(root).concat("col.log").$(), ff.getMapPageSize(), Long.MAX_VALUE);
                main.of(ff, path.of(root).concat("col.d").$(), ff.getMapPageSize(), Long.MAX_VALUE);

                MemoryLogAImpl fork = new MemoryLogAImpl();

                fork.of(log, main);
                final Rnd rnd = new Rnd();

                for (int i = 0; i < 1_000_000; i++) {
                    fork.putLong(rnd.nextLong());
                }

                main.truncate();

                for (int i = 0; i < 1_000_000; i++) {
                    fork.putLong(rnd.nextLong());
                }

                Assert.assertEquals(8_000_000, main.size());
                Assert.assertEquals(16_000_000, log.size());

                rnd.reset();

                reader.wholeFile(ff, path.of(root).concat("col.log").$());
                for (int i = 0; i < 2_000_000; i++) {
                    Assert.assertEquals(rnd.nextLong(), reader.getLong(i * 8));
                }

                rnd.reset();
                // skip 1M record, we truncated the file and overwrote them
                for (int i = 0; i < 1_000_000; i++) {
                    rnd.nextLong();
                }

                reader.wholeFile(ff, path.of(root).concat("col.d").$());
                for (int i = 0; i < 1_000_000; i++) {
                    Assert.assertEquals(rnd.nextLong(), reader.getLong(i * 8));
                }
            }
        });
    }
}