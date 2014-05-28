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

package com.nfsdb.journal.iterators;

public class JournalRow<T> {
    private final T object;
    private long rowID;

    public JournalRow(T object) {
        this.object = object;
    }

    public T getObject() {
        return object;
    }

    public long getRowID() {
        return rowID;
    }

    void setRowID(long rowID) {
        this.rowID = rowID;
    }
}
