/*+*****************************************************************************
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

package io.questdb.std;

abstract class AbstractLowerCaseCharSequenceHashMap extends AbstractLowerCaseCharSequenceHashSet {
    private final ObjList<CharSequence> list;

    protected AbstractLowerCaseCharSequenceHashMap(int initialCapacity, double loadFactor) {
        super(initialCapacity, loadFactor);
        list = new ObjList<>(capacity);
    }

    @Override
    public void clear() {
        super.clear();
        list.clear();
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    @Override
    public void removeAt(int index) {
        if (index < 0) {
            final CharSequence key = keys[-index - 1];
            super.removeAt(index);
            list.remove(key);
        }
    }

    // Removes a present key in O(1), where removeAt() scans the key list for it: the caller passes the key's
    // position in keys(), and the last key of the list moves into it. A loop that removes while it walks keys()
    // must therefore walk it from the end.
    public void removeAtQuick(int keyIndex, int listIndex) {
        assert keyIndex < 0 : "removeAtQuick expects a present key (negative keyIndex)";
        if (keyIndex < 0) {
            assert list.getQuick(listIndex) == keys[-keyIndex - 1];
            super.removeAt(keyIndex);
            final CharSequence lastKey = list.popLast();
            if (listIndex < list.size()) {
                list.setQuick(listIndex, lastKey);
            }
        }
    }

    protected void putAt0(int index, CharSequence key) {
        keys[index] = key;
        list.add(key);
        if (--free == 0) {
            rehash();
        }
    }

    protected abstract void rehash();
}
