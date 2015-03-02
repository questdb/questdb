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

package com.nfsdb.ha.model;

import com.nfsdb.JournalKey;

public class IndexedJournalKey {
    private final JournalKey key;
    private final int index;

    public IndexedJournalKey(int index, JournalKey key) {
        this.key = key;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public JournalKey getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + index;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexedJournalKey)) return false;

        IndexedJournalKey that = (IndexedJournalKey) o;

        return index == that.index && key.equals(that.key);

    }
}
