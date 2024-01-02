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

package io.questdb.cairo.map;

import io.questdb.cairo.Reopenable;
import io.questdb.std.Mutable;

import java.io.Closeable;

public interface Map extends Mutable, Closeable, Reopenable {

    @Override
    void close();

    MapRecordCursor getCursor();

    MapRecord getRecord();

    default void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        throw new UnsupportedOperationException();
    }

    void restoreInitialCapacity();

    default void setKeyCapacity(int keyCapacity) {
        throw new UnsupportedOperationException();
    }

    long size();

    MapValue valueAt(long address);

    MapKey withKey();
}
