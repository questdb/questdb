/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha.model;

import com.questdb.store.JournalKey;

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
        return 31 * key.hashCode() + index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexedJournalKey)) return false;

        IndexedJournalKey that = (IndexedJournalKey) o;

        return index == that.index && key.equals(that.key);

    }
}
