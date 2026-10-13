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

package io.questdb.griffin.plan.logical;

import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

/**
 * Column names taken in any case, handing out each requested name, or the name with the lowest numeric suffix that
 * no taken name has. Each base name keeps the suffix below which every suffixed name is taken, so a name costs
 * amortised constant time.
 */
public final class UniqueColumnNames implements Mutable {
    private static final int MAX_SUFFIX_DIGITS = 9;
    private final LowerCaseCharSequenceIntHashMap nextSuffixes = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceHashSet takenNames = new LowerCaseCharSequenceHashSet();

    @Override
    public void clear() {
        nextSuffixes.clear();
        takenNames.clear();
    }

    /**
     * Takes the names of every column of the output, visible or not, in place of the names taken so far.
     */
    public UniqueColumnNames of(OutputSchema output) {
        clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            takenNames.add(output.getColumnName(i));
        }
        return this;
    }

    /**
     * Frees the name, so that a later {@link #take} can hand it out again.
     */
    public void release(CharSequence name) {
        if (takenNames.remove(name) < 0) {
            return;
        }
        final int length = name.length();
        int suffix = 0;
        int scale = 1;
        for (int lo = length - 1; lo >= 0 && length - lo <= MAX_SUFFIX_DIGITS; lo--) {
            final char c = name.charAt(lo);
            if (c < '0' || c > '9') {
                return;
            }
            suffix += (c - '0') * scale;
            scale *= 10;
            if (c != '0') {
                final StringSink prefix = Misc.getThreadLocalSink();
                prefix.put(name, 0, lo);
                final int index = nextSuffixes.keyIndex(prefix);
                if (index < 0 && nextSuffixes.valueAt(index) > suffix) {
                    nextSuffixes.putAt(index, prefix, suffix);
                }
            }
        }
    }

    /**
     * Takes and returns the name, or the name with the lowest numeric suffix from 1, that no taken name has in any
     * case.
     */
    public CharSequence take(CharSequence name, CharacterStore store) {
        int index = takenNames.keyIndex(name);
        if (index > -1) {
            takenNames.addAt(index, name);
            return name;
        }
        final int suffixIndex = nextSuffixes.keyIndex(name);
        int suffix = Math.max(1, nextSuffixes.valueAt(suffixIndex));
        final CharacterStoreEntry entry = store.newEntry();
        final int start = entry.length();
        CharSequence candidate;
        while (true) {
            entry.trimTo(start);
            entry.put(name).put(suffix);
            candidate = entry.toImmutable();
            index = takenNames.keyIndex(candidate);
            if (index > -1) {
                break;
            }
            suffix++;
        }
        takenNames.addAt(index, candidate);
        nextSuffixes.putAt(suffixIndex, name, suffix + 1);
        return candidate;
    }
}
