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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class PGOidsTest extends AbstractTest {

    // 5000 is safely above max oids, even if a new type is added. yet it runs in a few ms
    private static final int MAX_OIDS = 5_000;

    @Test
    public void testAttTypModEndianityMatches() {
        for (int i = 0; i < MAX_OIDS; i++) {
            int le = PGOids.getAttTypMod(i);
            int be = PGOids.getXAttTypMod(i);

            Assert.assertEquals("failure for oid " + i + ", le: " + le + ", be: " + be + ", did you add a typmod for a new type? make sure you update both Big and Little endian methods!", le, Numbers.bswap(be));
        }
    }

    @Test
    public void testPgTypeToSizeEndianityMatches() {
        for (int i = 0; i < MAX_OIDS; i++) {
            short le = PGOids.PG_TYPE_TO_SIZE_MAP.get(i);
            short be = PGOids.X_PG_TYPE_TO_SIZE_MAP.get(i);

            Assert.assertEquals("failure for oid " + i + ", le: " + le + ", be: " + be + ", did you add a size for a new type? make sure you update both Big and Little endian methods!", le, Numbers.bswap(be));
        }
    }
}
