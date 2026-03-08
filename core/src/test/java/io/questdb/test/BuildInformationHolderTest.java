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

package io.questdb.test;

import io.questdb.BuildInformationHolder;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BuildInformationHolderTest {

    @Test
    public void testAsCharSequence() {
        TestUtils.assertEquals("[DEVELOPMENT]:unknown:unknown", new BuildInformationHolder());
        BuildInformationHolder holder = new BuildInformationHolder("a", "b", "c", "d");
        TestUtils.assertEquals("a:b:c", holder);
        Assert.assertEquals("a", holder.getSwVersion());
        Assert.assertEquals("b", holder.getCommitHash());
        Assert.assertEquals("c", holder.getJdkVersion());
        Assert.assertEquals("d", holder.getSwName());
        char[] expected = {'a', ':', 'b', ':', 'c'};
        Assert.assertEquals(expected.length, holder.length());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], holder.charAt(i));
        }
        Assert.assertEquals("a:b:c", holder.subSequence(0, 5));
    }

    @Test
    public void testAsCharSequenceDefault() {
        TestUtils.assertEquals("[DEVELOPMENT]:unknown:unknown", new BuildInformationHolder());
    }
}
