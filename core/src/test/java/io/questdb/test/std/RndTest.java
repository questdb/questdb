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

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

public class RndTest extends AbstractTest {

    @Test
    public void testGenerateArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            try (DirectArray arrayView = new DirectArray(new DefaultCairoConfiguration(root))) {
                for (int i = 0; i < 1000; i++) {
                    rnd.nextDoubleArray(2, arrayView, 0, 8, 1);
                    Assert.assertTrue(arrayView.getFlatViewLength() > 0);
                    arrayView.clear();
                }
            }
        });
    }

    @Test
    public void testGeneratesDecodableUtf8() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        Utf8StringSink utf8Sink = new Utf8StringSink();
        StringSink utf16Sink = new StringSink();
        for (int i = 0; i < 100; i++) {
            utf8Sink.clear();
            rnd.nextUtf8Str(rnd.nextInt(i + 1) + 1, utf8Sink);
            utf16Sink.clear();
            Assert.assertTrue("generation failed for " + i + " chars", Utf8s.utf8ToUtf16(utf8Sink, utf16Sink));
        }
    }

    @Test
    public void testShuffleArrayDeterministic() {
        Rnd rnd = new Rnd(123, 456);
        Integer[] array = {1, 2, 3, 4, 5};
        rnd.shuffle(array);

        Rnd rnd2 = new Rnd(123, 456);
        Integer[] array2 = {1, 2, 3, 4, 5};
        rnd2.shuffle(array2);

        Assert.assertArrayEquals("Same seed should produce same shuffle", array, array2);
    }

    @Test
    public void testShuffleArrayDistribution() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int[] firstPositionCounts = new int[5];
        for (int i = 0; i < 1000; i++) {
            Integer[] test = {0, 1, 2, 3, 4};
            rnd.shuffle(test);
            firstPositionCounts[test[0]]++;
        }

        for (int count : firstPositionCounts) {
            Assert.assertTrue("Each element should appear in first position at least once", count > 0);
        }
    }

    @Test
    public void testShuffleArrayEmpty() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        Integer[] empty = {};
        rnd.shuffle(empty);
        Assert.assertEquals("Empty array should remain empty", 0, empty.length);
    }

    @Test
    public void testShuffleArrayPreservesElements() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        Integer[] original = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        Integer[] shuffled = Arrays.copyOf(original, original.length);
        rnd.shuffle(shuffled);

        Arrays.sort(shuffled);
        Assert.assertArrayEquals("All elements should be preserved", original, shuffled);
    }

    @Test
    public void testShuffleArrayProducesDifferentResults() {
        Rnd rnd1 = new Rnd(123, 456);
        Integer[] array1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        rnd1.shuffle(array1);

        Rnd rnd2 = new Rnd(789, 101112);
        Integer[] array2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        rnd2.shuffle(array2);

        Assert.assertFalse("Different seeds should produce different shuffles", Arrays.equals(array1, array2));
    }

    @Test
    public void testShuffleArraySingleElement() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        Integer[] single = {42};
        rnd.shuffle(single);
        Assert.assertEquals("Single element should remain unchanged", Integer.valueOf(42), single[0]);
    }

    @Test
    public void testShuffleArrayStringType() {
        Rnd rnd = new Rnd(123, 456);
        String[] array = {"apple", "banana", "cherry", "date", "elderberry"};
        String[] original = Arrays.copyOf(array, array.length);
        rnd.shuffle(array);

        HashSet<String> originalSet = new HashSet<>(Arrays.asList(original));
        HashSet<String> shuffledSet = new HashSet<>(Arrays.asList(array));

        Assert.assertEquals("All elements should be preserved", originalSet, shuffledSet);
        Assert.assertFalse("Array should be shuffled", Arrays.equals(original, array));
    }
}
