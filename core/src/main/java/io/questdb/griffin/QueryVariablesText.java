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

import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

/**
 * The text the parser reads the variables a client declared outside a statement's text from, see
 * {@link SqlExecutionContext#getQueryVariables()}: blanks up to
 * {@link SqlParser#QUERY_VARIABLES_POSITION}, then the variables. The blanks take no memory, and
 * the parser never reads them: it starts at the variables. So everything parsed from the
 * variables takes a position at or past {@link SqlParser#QUERY_VARIABLES_POSITION}, which no other
 * text a statement parses reaches.
 * <p>
 * {@link #toString()} and {@link #subSequence(int, int)} leave the blanks out: they stand for no
 * text, and a string of them would not fit the heap.
 */
public final class QueryVariablesText implements CharSequence {
    private final StringSink variables = new StringSink();

    @Override
    public char charAt(int index) {
        return index < SqlParser.QUERY_VARIABLES_POSITION ? ' ' : variables.charAt(index - SqlParser.QUERY_VARIABLES_POSITION);
    }

    /**
     * Returns the variables, without the blanks ahead of them.
     */
    public CharSequence getVariables() {
        return variables;
    }

    @Override
    public int length() {
        return SqlParser.QUERY_VARIABLES_POSITION + variables.length();
    }

    /**
     * Sets the text to a copy of the variables, which outlives the parse of the statement: a
     * sub-query in a value is parsed again from this text for each read past the first.
     */
    public void of(CharSequence variables) {
        this.variables.clear();
        this.variables.put(variables);
    }

    @Override
    public @NotNull CharSequence subSequence(int start, int end) {
        return variables.subSequence(
                Math.max(start - SqlParser.QUERY_VARIABLES_POSITION, 0),
                Math.max(end - SqlParser.QUERY_VARIABLES_POSITION, 0)
        );
    }

    @Override
    public @NotNull String toString() {
        return variables.toString();
    }
}
