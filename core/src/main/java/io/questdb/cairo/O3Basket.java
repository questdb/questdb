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

package io.questdb.cairo;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

public class O3Basket implements Mutable {
    private final ObjList<BitmapIndexWriter> indexers = new ObjList<BitmapIndexWriter>();
    private final ObjList<AtomicInteger> partCounters = new ObjList<AtomicInteger>();
    private int indexerPointer;
    private int partCounterPointer;
    private int columnCount;
    private int indexCount;

    @Override
    public void clear() {
        indexerPointer = 0;
        partCounterPointer = 0;
    }

    public void ensureCapacity(int columnCount, int indexCount) {
        if (this.columnCount == columnCount && this.indexCount == indexCount) {
            return;
        }
        ensureCapacity0(columnCount, indexCount);
    }

    public BitmapIndexWriter nextIndexer() {
        return indexers.getQuick(indexerPointer++);
    }

    public AtomicInteger nextPartCounter() {
        return partCounters.getQuick(partCounterPointer++);
    }

    private void ensureCapacity0(int columnCount, int indexCount) {
        if (this.columnCount < columnCount) {
            for (int i = this.columnCount; i < columnCount; i++) {
                partCounters.add(new O3MutableAtomicInteger());
            }
        } else {
            for (int i = columnCount; i < this.columnCount; i++) {
                partCounters.setQuick(i, null);
            }
            partCounters.setPos(columnCount);
        }
        this.columnCount = columnCount;

        if (this.indexCount < indexCount) {
            for (int i = this.indexCount; i < indexCount; i++) {
                indexers.add(new BitmapIndexWriter());
            }
        } else {
            for (int i = indexCount; i < this.indexCount; i++) {
                indexers.setQuick(i, null);
            }
            indexers.setPos(indexCount);
        }
        this.indexCount = indexCount;
    }
}
