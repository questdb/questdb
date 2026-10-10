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

package io.questdb.griffin;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.str.Utf16Sink;

/**
 * Renders an expression as the base of a non-literal generated column alias without storing the whole rendering.
 * For a base longer than {@code maxLength}, {@link SqlUtil#createExprColumnAlias} reads only its first
 * {@code maxLength + 2} characters, its last two characters, and whether it has a dot outside double quotes, or
 * inside them. The sink stores the first {@code maxLength + 2} characters, replaces the middle with at most five
 * characters that keep those facts, and stores the last two characters, so the alias matches the one the whole
 * rendering yields.
 */
public class AliasBaseSink implements Utf16Sink {
    private CharacterStoreEntry entry;
    private boolean hasDotInQuotes;
    private boolean hasDotOutsideQuotes;
    private int headLimit;
    private boolean isInQuotesAfterHead;
    private boolean isInQuotesBeforeTail;
    private int length;
    private char tail0;
    private char tail1;

    @Override
    public Utf16Sink put(char c) {
        if (length < headLimit) {
            entry.put(c);
            if (c == '"') {
                isInQuotesAfterHead = !isInQuotesAfterHead;
                isInQuotesBeforeTail = isInQuotesAfterHead;
            }
        } else {
            if (length >= headLimit + 2) {
                skip(tail0);
            }
            tail0 = tail1;
            tail1 = c;
        }
        length++;
        return this;
    }

    /**
     * Writes the bounded base of {@code ast} to a new entry of {@code store} and returns it.
     */
    public CharSequence render(CharacterStore store, ExpressionNode ast, int maxLength) {
        entry = store.newEntry();
        headLimit = maxLength + 2;
        length = 0;
        hasDotInQuotes = false;
        hasDotOutsideQuotes = false;
        isInQuotesAfterHead = false;
        isInQuotesBeforeTail = false;
        ast.toSink(this);

        boolean isInQuotes = isInQuotesAfterHead;
        if (hasDotOutsideQuotes) {
            if (isInQuotes) {
                entry.put('"');
                isInQuotes = false;
            }
            entry.put('.');
        }
        if (hasDotInQuotes) {
            if (!isInQuotes) {
                entry.put('"');
                isInQuotes = true;
            }
            entry.put('.');
        }
        if (isInQuotes != isInQuotesBeforeTail) {
            entry.put('"');
        }
        final int tailLength = length - headLimit;
        if (tailLength > 1) {
            entry.put(tail0);
        }
        if (tailLength > 0) {
            entry.put(tail1);
        }
        final CharSequence base = entry.toImmutable();
        entry = null;
        return base;
    }

    @Override
    public int[] ryuScratch() {
        return entry.ryuScratch();
    }

    private void skip(char c) {
        if (c == '"') {
            isInQuotesBeforeTail = !isInQuotesBeforeTail;
        } else if (c == '.') {
            if (isInQuotesBeforeTail) {
                hasDotInQuotes = true;
            } else {
                hasDotOutsideQuotes = true;
            }
        }
    }
}
