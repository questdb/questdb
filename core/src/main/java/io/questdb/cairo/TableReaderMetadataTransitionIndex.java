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

package io.questdb.cairo;

import io.questdb.std.IntList;

public class TableReaderMetadataTransitionIndex {
    private final IntList actions = new IntList();

    public void clear() {
        actions.setAll(actions.capacity(), 0);
        actions.clear();
    }

    public boolean closeColumn(int index) {
        return actions.get(index * 2) == -1;
    }

    public int getCopyFromIndex(int index) {
        return actions.get(index * 2 + 1);
    }

    public boolean replaceWithNew(int index) {
        return actions.get(index * 2 + 1) != Integer.MIN_VALUE && actions.get(index * 2 + 1) < 0;
    }

    void markCopyFrom(int index, int newIndex) {
        actions.extendAndSet(index * 2 + 1, -newIndex - 1);
    }

    void markDeleted(int index) {
        actions.extendAndSet(index * 2, -1);
        actions.extendAndSet(index * 2 + 1, Integer.MIN_VALUE);
    }

    void markReplaced(int index) {
        actions.extendAndSet(index * 2 + 1, Integer.MIN_VALUE);
    }

    void markReusedAction(int index, int oldIndex) {
        actions.extendAndSet(index * 2 + 1, oldIndex);
    }
}
