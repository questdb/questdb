/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.SimpleReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Holds the list of mat view tokens for the given base table (or base mat view).
 * The list is protected with a R/W mutex, so it can be read concurrently.
 */
public class MatViewDependencyList {
    private final ReadWriteLock lock = new SimpleReadWriteLock();
    private final ObjList<TableToken> matViews = new ObjList<>();

    ReadOnlyObjList<TableToken> lockForRead() {
        lock.readLock().lock();
        return matViews;
    }

    ObjList<TableToken> lockForWrite() {
        lock.writeLock().lock();
        return matViews;
    }

    void unlockAfterRead() {
        lock.readLock().unlock();
    }

    void unlockAfterWrite() {
        lock.writeLock().unlock();
    }
}
