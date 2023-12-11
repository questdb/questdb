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

package io.questdb.cairo;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;

public class ArrayColumnTypes implements ColumnTypes, Mutable {
    private final IntList types = new IntList();

    public ArrayColumnTypes add(int type) {
        types.add(type);
        return this;
    }

    public ArrayColumnTypes add(int index, int type) {
        types.extendAndSet(index, type);
        return this;
    }

    public ArrayColumnTypes addAll(ArrayColumnTypes that) {
        types.addAll(that.types);
        return this;
    }

    public void clear() {
        types.clear();
    }

    @Override
    public int getColumnCount() {
        return types.size();
    }

    @Override
    public int getColumnType(int columnIndex) {
        return types.getQuick(columnIndex);
    }
}
