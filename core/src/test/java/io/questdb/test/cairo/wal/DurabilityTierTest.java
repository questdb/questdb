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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class DurabilityTierTest {
    @Test
    public void testFromHeaderValue() {
        Assert.assertEquals(DurabilityTier.DEFAULT, DurabilityTier.fromHeaderValue(new Utf8String("true")));
        Assert.assertEquals(DurabilityTier.DEFAULT, DurabilityTier.fromHeaderValue(new Utf8String("TRUE")));
        Assert.assertEquals(DurabilityTier.LOCAL, DurabilityTier.fromHeaderValue(new Utf8String("local")));
        Assert.assertEquals(DurabilityTier.REPLICATED, DurabilityTier.fromHeaderValue(new Utf8String("replicated")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(new Utf8String("bogus")));
        Assert.assertEquals(DurabilityTier.NONE, DurabilityTier.fromHeaderValue(null));
    }

    @Test
    public void testResponseToken() {
        Assert.assertEquals("local", DurabilityTier.responseToken(DurabilityTier.LOCAL).toString());
        Assert.assertEquals("replicated", DurabilityTier.responseToken(DurabilityTier.REPLICATED).toString());
        Assert.assertNull(DurabilityTier.responseToken(DurabilityTier.NONE));
        Assert.assertNull(DurabilityTier.responseToken(DurabilityTier.DEFAULT));
    }
}
