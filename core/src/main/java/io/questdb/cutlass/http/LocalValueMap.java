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

package io.questdb.cutlass.http;

import io.questdb.std.Misc;
import io.questdb.std.Mutable;

import java.io.Closeable;

public class LocalValueMap implements Closeable, Mutable {

    private static final int INITIAL_CAPACITY = 32;
    private int size;
    private Entry[] table;
    private int threshold;

    public LocalValueMap() {
        table = new Entry[INITIAL_CAPACITY];
        size = 0;
        setThreshold(INITIAL_CAPACITY);
    }

    @Override
    public void clear() {
        for (int i = 0, n = table.length; i < n; i++) {
            Entry e = table[i];
            if (e != null && e.value instanceof Mutable) {
                ((Mutable) e.value).clear();
            }
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = table.length; i < n; i++) {
            Entry e = table[i];
            if (e != null) {
                e.value = Misc.freeIfCloseable(e.value);
                e.k = null;
            }
            table[i] = null;
        }
    }

    public void disconnect() {
        for (int i = 0, n = table.length; i < n; i++) {
            Entry e = table[i];
            if (e != null && e.value instanceof ConnectionAware) {
                ((ConnectionAware) e.value).onDisconnected();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(LocalValue<T> key) {
        int i = key.hashCode & (table.length - 1);
        Entry e = table[i];
        return e != null && e.k == key ? (T) e.value : get0(key, i, e);
    }

    public <T> void set(LocalValue<T> key, T value) {
        Entry[] tab = table;
        int len = tab.length;
        int i = key.hashCode & (len - 1);

        for (Entry e = tab[i];
             e != null;
             e = tab[i = nextIndex(i, len)]) {
            LocalValue<?> k = e.k;

            if (k == key) {
                Misc.freeIfCloseable(e.value);
                e.value = value;
                return;
            }

            if (k == null) {
                replaceNull(key, value, i);
                return;
            }
        }

        tab[i] = new Entry(key, value);
        int sz = ++size;
        if (!removeNullKeys(i, sz) && sz >= threshold) {
            rehash();
        }
    }

    private static int nextIndex(int i, int mod) {
        return ((i + 1 < mod) ? i + 1 : 0);
    }

    private static int prevIndex(int i, int mod) {
        return ((i - 1 > -1) ? i - 1 : mod - 1);
    }

    @SuppressWarnings("unchecked")
    private <T> T get0(LocalValue<T> key, int i, Entry e) {
        Entry[] tab = table;
        int len = tab.length;

        while (e != null) {
            LocalValue<?> k = e.k;
            if (k == key) {
                return (T) e.value;
            }
            if (k == null) {
                removeNull(i);
            } else {
                i = nextIndex(i, len);
            }
            e = tab[i];
        }
        return null;
    }

    private void rehash() {
        removeNulls();
        if (size >= threshold - threshold / 4) {
            resize();
        }
    }

    private int removeNull(int index) {
        Entry[] tab = table;
        int len = tab.length;

        tab[index].value = Misc.freeIfCloseable(tab[index].value);
        tab[index] = null;
        size--;

        Entry e;
        int i;
        for (i = nextIndex(index, len); (e = tab[i]) != null; i = nextIndex(i, len)) {
            LocalValue<?> k = e.k;
            if (k == null) {
                e.value = Misc.freeIfCloseable(e.value);
                tab[i] = null;
                size--;
            } else {
                int h = k.hashCode & (len - 1);
                if (h != i) {
                    tab[i] = null;
                    while (tab[h] != null) {
                        h = nextIndex(h, len);
                    }
                    tab[h] = e;
                }
            }
        }
        return i;
    }

    private boolean removeNullKeys(int i, int n) {
        boolean removed = false;
        Entry[] tab = table;
        int len = tab.length;
        do {
            i = nextIndex(i, len);
            Entry e = tab[i];
            if (e != null && e.k == null) {
                n = len;
                removed = true;
                i = removeNull(i);
            }
        } while ((n >>>= 1) != 0);
        return removed;
    }

    private void removeNulls() {
        Entry[] tab = table;
        int len = tab.length;
        for (int j = 0; j < len; j++) {
            Entry e = tab[j];
            if (e != null && e.k == null) {
                removeNull(j);
            }
        }
    }

    private void replaceNull(LocalValue<?> key, Object value, int index) {
        Entry[] tab = table;
        int len = tab.length;
        Entry e;

        int slotToExpunge = index;
        for (int i = prevIndex(index, len);
             (e = tab[i]) != null;
             i = prevIndex(i, len))
            if (e.k == null) {
                slotToExpunge = i;
            }

        for (int i = nextIndex(index, len); (e = tab[i]) != null; i = nextIndex(i, len)) {
            LocalValue<?> k = e.k;

            if (k == key) {
                Misc.freeIfCloseable(e.value);
                e.value = value;

                tab[i] = tab[index];
                tab[index] = e;

                if (slotToExpunge == index) {
                    slotToExpunge = i;
                }
                removeNullKeys(removeNull(slotToExpunge), len);
                return;
            }

            if (k == null && slotToExpunge == index) {
                slotToExpunge = i;
            }
        }

        tab[index].value = Misc.freeIfCloseable(tab[index].value);
        tab[index] = new Entry(key, value);

        if (slotToExpunge != index) {
            removeNullKeys(removeNull(slotToExpunge), len);
        }
    }

    private void resize() {
        Entry[] oldTab = table;
        int oldLen = oldTab.length;
        int newLen = oldLen * 2;
        Entry[] newTab = new Entry[newLen];
        int count = 0;

        for (int j = 0; j < oldLen; ++j) {
            Entry e = oldTab[j];
            if (e != null) {
                LocalValue<?> k = e.k;
                if (k == null) {
                    e.value = Misc.freeIfCloseable(e.value);
                } else {
                    int h = k.hashCode & (newLen - 1);
                    while (newTab[h] != null) {
                        h = nextIndex(h, newLen);
                    }
                    newTab[h] = e;
                    count++;
                }
            }
        }

        setThreshold(newLen);
        size = count;
        table = newTab;
    }

    private void setThreshold(int len) {
        threshold = len * 2 / 3;
    }

    static class Entry {
        LocalValue<?> k;
        Object value;

        Entry(LocalValue<?> k, Object v) {
            value = v;
            this.k = k;
        }
    }
}
