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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexReader;
import org.junit.Assert;
import org.junit.Test;

public class PostingIndexReaderContractTest {

    @Test
    public void testContractDeclaresOnlyTheSeamMethods() {
        final java.util.Set<String> declared = new java.util.TreeSet<>();
        for (java.lang.reflect.Method m : PostingIndexReader.class.getDeclaredMethods()) {
            declared.add(m.getName());
        }
        Assert.assertEquals("[countMatchesClamped, getEntryMaxValue, populateCacheForKey, selectKthMatch]", declared.toString());
    }

    @Test
    public void testNativeReaderSatisfiesTheContract() {
        Assert.assertTrue(PostingIndexReader.class.isAssignableFrom(PostingIndexFwdReader.class));
        Assert.assertTrue(PostingIndexReader.class.isAssignableFrom(PostingIndexBwdReader.class));
    }
}
