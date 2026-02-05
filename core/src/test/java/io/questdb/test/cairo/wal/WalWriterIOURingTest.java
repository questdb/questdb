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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.std.IOURingFacadeImpl;
import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * Runs the full WalWriterTest suite with io_uring enabled.
 * All tests are inherited from WalWriterTest -- the only difference
 * is that WAL columns use MemoryPURImpl (io_uring pwrite) instead of
 * MemoryPMARImpl (mmap).
 */
public class WalWriterIOURingTest extends WalWriterTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        Assume.assumeTrue("io_uring not available", IOURingFacadeImpl.INSTANCE.isAvailable());
        setProperty(PropertyKey.CAIRO_IO_URING_ENABLED, "true");
        WalWriterTest.setUpStatic();
    }
}
