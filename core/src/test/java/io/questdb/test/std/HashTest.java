/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertEquals;

public class HashTest {

    @Test
    public void testHashMemEnglishWordsCorpus() throws IOException {
        final int maxLen = 128;
        LongHashSet hashes = new LongHashSet(500000);

        String file = Files.getResourcePath(getClass().getResource("/hash/words.zip"));
        long address = Unsafe.malloc(maxLen, MemoryTag.NATIVE_DEFAULT);
        try (
                ZipFile zipFile = new ZipFile(file);
                InputStream input = zipFile.getInputStream(zipFile.entries().nextElement());
                BufferedReader br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(address + i, bytes[i]);
                }
                // Use only 32 LSBs for the unique value check since that's where we want entropy.
                hashes.add((int) Hash.hashMem64(address, bytes.length));
            }
            // 466189 is the number of unique values of String#hashCode() on the same corpus.
            Assert.assertTrue("hash function distribution on English words corpus dropped", hashes.size() >= 466189);
        } finally {
            Unsafe.free(address, maxLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHashMemRandomCorpus() {
        final int len = 15;
        Rnd rnd = new Rnd();
        LongHashSet hashes = new LongHashSet(100000);

        long address = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 100000; i++) {
                rnd.nextChars(address, len / 2);
                // Use only 32 LSBs for the unique value check since that's where we want entropy.
                hashes.add((int) Hash.hashMem64(address, len));
            }
            Assert.assertTrue("Hash function distribution dropped", hashes.size() > 99990);
        } finally {
            Unsafe.free(address, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMurmur3ToLongForIntKey() {
        // The expected values have been obtained from the original implementation of MurmurHash3,
        // available at: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
        assertEquals(-1633616987925480281L, Hash.murmur3ToLong(-1205269188));
        assertEquals(-5166452714297686332L, Hash.murmur3ToLong(287961467));
        assertEquals(370098364460170807L, Hash.murmur3ToLong(43976175));
        assertEquals(337284429664094377L, Hash.murmur3ToLong(1071024900));
        assertEquals(-7391378269516181578L, Hash.murmur3ToLong(-46715208));
    }

    @Test
    public void testMurmur3ToLongForLongKey() {
        // The expected values have been obtained from the original implementation of MurmurHash3,
        // available at: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
        assertEquals(1586830184839932339L, Hash.murmur3ToLong(2769845405872435875L));
        assertEquals(5667271150751524839L, Hash.murmur3ToLong(8467001914150166941L));
        assertEquals(-6164039929522353948L, Hash.murmur3ToLong(3116016319545714670L));
        assertEquals(5404083732375145584L, Hash.murmur3ToLong(-3505607450965693221L));
        assertEquals(-2748674767479114199L, Hash.murmur3ToLong(-1442442454180049685L));
    }
}
