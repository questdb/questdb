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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.str.DirectString;

/**
 * The key-to-value view a cast-to-symbol function hands out from {@code newSymbolTable()}.
 * <p>
 * It is deliberately not a {@link io.questdb.cairo.sql.Function}. Every cast-to-symbol function
 * used to answer {@code newSymbolTable()} with a clone of itself, and that clone held - and on
 * close released - the very argument the live function was still reading. A caller that frees what
 * {@code newSymbolTable()} handed it therefore closed the projection's own argument: with a
 * resource-owning argument such as {@code json_extract}, every later read of the column came back
 * NULL. Handing out something that owns nothing removes that hazard rather than documenting it.
 * <p>
 * The view copies nothing. It reads the function's live {@link CastToSymbolDictionary} into its own
 * A/B flyweights, so it resolves every key to what the function's own valueOf() answers at the time
 * of the call: a key minted after the hand-out, the re-minted value of a key after the function is
 * initialized again, and null for {@link SymbolTable#VALUE_IS_NULL}, for a key not minted yet, and
 * for every key once the execution releases the dictionary or the function closes. The text it
 * returns points into append-only chunks, so it stays valid across later minting, up to that
 * release.
 * <p>
 * Reads are serial, or concurrent with no key being minted: minting may move the descriptor block
 * that a concurrent read follows. No worker thread reads a cast's view, because casts report
 * supportsParallelism() == false, and parallel operators take their symbol tables from page frames.
 */
public final class CastToSymbolTable implements SymbolTable {
    private final CastToSymbolDictionary dictionary;
    private final DirectString viewA = new DirectString();
    private final DirectString viewB = new DirectString();

    CastToSymbolTable(CastToSymbolDictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public CharSequence valueBOf(int key) {
        return dictionary.valueOf(key, viewB);
    }

    @Override
    public CharSequence valueOf(int key) {
        return dictionary.valueOf(key, viewA);
    }
}
