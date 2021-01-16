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

package io.questdb.std;

import org.junit.Assert;
import org.junit.Test;

public class IntListTest {
    @Test
    public void testBinarySearch() {
        IntList list = new IntList();
        for (int i = 0; i < 65; i++) {
            list.add(i);
        }
        list.add(76);
        list.add(974);
        list.add(1115);
        list.add(2094);
        Assert.assertEquals(-66, list.binarySearch(70));
        Assert.assertEquals(65, list.binarySearch(76));
        Assert.assertEquals(-67, list.binarySearch(950));
        Assert.assertEquals(-70, list.binarySearch(2500));
    }
}