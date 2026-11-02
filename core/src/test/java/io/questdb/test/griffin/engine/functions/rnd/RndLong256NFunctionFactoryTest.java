/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class RndLong256NFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testDrawsFromPoolOfGivenSize() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                SELECT count_distinct(a) a, count_distinct(b) b
                FROM (SELECT rnd_long256(1) a, rnd_long256(3) b FROM long_sequence(1_000))
                """)
                .noLeakCheck()
                .returnsOnce("""
                        a\tb
                        1\t3
                        """));
    }

    @Test
    public void testInvalidCount() throws Exception {
        assertMemoryLeak(() -> {
            assertException("SELECT rnd_long256(0)", 19, "invalid long256 count");
            assertException("SELECT rnd_long256(-1)", 19, "invalid long256 count");
            assertException("SELECT rnd_long256(null::int)", 23, "invalid long256 count");
            assertException("SELECT rnd_long256(536_870_912)", 19, "invalid long256 count");
        });
    }
}
