/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.ql.impl;

import com.nfsdb.ql.KeyCursor;
import com.nfsdb.ql.KeySource;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.utils.Hash;

public class SingleStringHashKeySource implements KeySource, KeyCursor {
    private final String column;
    private final VirtualColumn value;
    private int bucketCount = -1;
    private boolean hasNext;

    public SingleStringHashKeySource(String column, VirtualColumn value) {
        this.column = column;
        this.value = value;
    }

    @Override
    public KeyCursor cursor(PartitionSlice slice) {
        if (bucketCount == -1) {
            bucketCount = slice.partition.getJournal().getMetadata().getColumn(column).distinctCountHint;
        }
        this.hasNext = true;
        return this;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public int next() {
        hasNext = false;
        return Hash.boundedHash(value.getFlyweightStr(), bucketCount);
    }

    @Override
    public void reset() {
        bucketCount = -1;
    }

    @Override
    public int size() {
        return 1;
    }
}
