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

package com.nfsdb.lang.cst.impl.ksrc;

import com.nfsdb.lang.cst.KeyCursor;
import com.nfsdb.lang.cst.KeySource;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.impl.ref.StringRef;
import com.nfsdb.utils.Checksum;

import java.util.List;

public class StringHashKeySource implements KeySource, KeyCursor {
    private final StringRef column;
    private final List<String> values;
    private int bucketCount = -1;
    private int valueIndex;

    public StringHashKeySource(StringRef column, List<String> values) {
        this.column = column;
        this.values = values;
    }

    @Override
    public KeyCursor cursor(PartitionSlice slice) {
        if (bucketCount == -1) {
            bucketCount = slice.partition.getJournal().getMetadata().getColumnMetadata(column.value).distinctCountHint;
        }
        this.valueIndex = 0;
        return this;
    }

    @Override
    public boolean hasNext() {
        return valueIndex < values.size();
    }

    @Override
    public int next() {
        return Checksum.hash(values.get(valueIndex++), bucketCount);
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public void reset() {
        bucketCount = -1;
    }
}
