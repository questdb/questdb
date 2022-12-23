/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class HashTest {

    @Test
    public void testHashMem32EnglishWordsCorpus() throws URISyntaxException, IOException {
        testHashMemEnglishWordsCorpus(Hash::hashMem32);
    }

    @Test
    public void testHashMem32RandomCorpus() {
        testHashMemRandomCorpus(Hash::hashMem32);
    }

    @Test
    public void testHashMem64EnglishWordsCorpus() throws URISyntaxException, IOException {
        testHashMemEnglishWordsCorpus(Hash::hashMem64);
    }

    @Test
    public void testHashMem64RandomCorpus() {
        testHashMemRandomCorpus(Hash::hashMem64);
    }

    private void testHashMemEnglishWordsCorpus(HashFunction hashFunction) throws URISyntaxException, IOException {
        final int maxLen = 128;
        LongHashSet hashes = new LongHashSet(500000);

        Path p = Paths.get(HashTest.class.getResource("/hash/words.txt").toURI());
        long address = Unsafe.malloc(maxLen, MemoryTag.NATIVE_DEFAULT);
        try {
            Files.lines(p)
                    .flatMap(line -> Arrays.stream(line.split(",")))
                    .forEach(str -> {
                        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                        for (int i = 0; i < bytes.length; i++) {
                            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
                        }
                        hashes.add(hashFunction.hash(address, bytes.length));
                    });
            // 466189 is the number of unique values of String#hashCode() on the same corpus.
            Assert.assertTrue("hash function distribution on English words corpus dropped", hashes.size() >= 466189);
        } finally {
            Unsafe.free(address, maxLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void testHashMemRandomCorpus(HashFunction hashFunction) {
        final int len = 15;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(100000);

        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 100000; i++) {
                rnd.nextChars(address, len / 2);
                hashes.add(hashFunction.hash(address, len));
            }
            Assert.assertTrue("Hash function distribution dropped", hashes.size() > 99990);
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private interface HashFunction {
        long hash(long p, long len);
    }
}
