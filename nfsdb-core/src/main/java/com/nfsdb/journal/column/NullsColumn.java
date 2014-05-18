/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.column;

import com.nfsdb.journal.utils.BitSetAccessor;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class NullsColumn extends FixedColumn {
    private final BitSet bitSet;
    private final int wordCount;
    private long cachedRowID;

    public NullsColumn(MappedFile mappedFile, int size, int nullCount) {
        // convert nullCount, which would be a number of columns in journal
        // to count of 64bit words needed to represent each null with 1 bit.
        // once we have number of words we convert it to number of bytes
        super(mappedFile, size);
        this.bitSet = new BitSet(nullCount);
        this.wordCount = size / 8;
        this.cachedRowID = -1;
    }

    public BitSet getBitSet(long localRowID) {
        if (localRowID != cachedRowID) {
            getBitSet(localRowID, bitSet);
        }
        return bitSet;
    }

    public void putBitSet(BitSet bitSet) {
        ByteBuffer bb = getBuffer();
        long[] words = BitSetAccessor.getWords(bitSet);
        for (int i = 0; i < wordCount; i++) {
            bb.putLong(words[i]);
        }
        cachedRowID = -1;
    }

    private void getBitSet(long localRowID, BitSet bs) {
        ByteBuffer bb = getBuffer(localRowID);
        long words[] = BitSetAccessor.getWords(bs);
        if (words == null || words.length < wordCount) {
            words = new long[wordCount];
            BitSetAccessor.setWords(bs, words);
        }
        BitSetAccessor.setWordsInUse(bs, wordCount);

        for (int j = 0; j < wordCount; j++) {
            words[j] = bb.getLong();
        }
    }
}
