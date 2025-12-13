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

package io.questdb.test.fuzz.sql;

import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Represents a SQL query as a list of tokens.
 * <p>
 * This class is used for:
 * 1. Building queries token-by-token during generation
 * 2. Manipulating queries for corruption (drop, swap, insert tokens)
 * 3. Serializing back to SQL string with proper spacing
 * <p>
 * By operating at the token level rather than character level, we can perform
 * meaningful corruptions without breaking things like string literals or operators.
 */
public final class TokenizedQuery {

    private final ObjList<SqlToken> tokens;
    private final StringSink sink;

    public TokenizedQuery() {
        this.tokens = new ObjList<>();
        this.sink = new StringSink();
    }

    /**
     * Creates a new TokenizedQuery with a copy of the given tokens.
     */
    public TokenizedQuery(ObjList<SqlToken> tokens) {
        this.tokens = new ObjList<>(tokens.size());
        for (int i = 0; i < tokens.size(); i++) {
            this.tokens.add(tokens.get(i));
        }
        this.sink = new StringSink();
    }

    // --- Token addition ---

    /**
     * Adds a keyword token.
     */
    public TokenizedQuery addKeyword(String value) {
        tokens.add(SqlToken.keyword(value));
        return this;
    }

    /**
     * Adds an identifier token.
     */
    public TokenizedQuery addIdentifier(String value) {
        tokens.add(SqlToken.identifier(value));
        return this;
    }

    /**
     * Adds a literal token.
     */
    public TokenizedQuery addLiteral(String value) {
        tokens.add(SqlToken.literal(value));
        return this;
    }

    /**
     * Adds an operator token.
     */
    public TokenizedQuery addOperator(String value) {
        tokens.add(SqlToken.operator(value));
        return this;
    }

    /**
     * Adds a punctuation token.
     */
    public TokenizedQuery addPunctuation(String value) {
        tokens.add(SqlToken.punctuation(value));
        return this;
    }

    /**
     * Adds a token.
     */
    public TokenizedQuery add(SqlToken token) {
        tokens.add(token);
        return this;
    }

    /**
     * Adds all tokens from another TokenizedQuery.
     */
    public TokenizedQuery addAll(TokenizedQuery other) {
        for (int i = 0; i < other.size(); i++) {
            tokens.add(other.get(i));
        }
        return this;
    }

    // --- Token access ---

    /**
     * Returns the token at the given index.
     */
    public SqlToken get(int index) {
        return tokens.get(index);
    }

    /**
     * Returns the number of tokens.
     */
    public int size() {
        return tokens.size();
    }

    /**
     * Returns true if there are no tokens.
     */
    public boolean isEmpty() {
        return tokens.size() == 0;
    }

    /**
     * Clears all tokens.
     */
    public void clear() {
        tokens.clear();
    }

    /**
     * Returns a copy of the internal token list.
     */
    public ObjList<SqlToken> getTokens() {
        ObjList<SqlToken> copy = new ObjList<>(tokens.size());
        for (int i = 0; i < tokens.size(); i++) {
            copy.add(tokens.get(i));
        }
        return copy;
    }

    // --- Token manipulation for corruption ---

    /**
     * Returns a new TokenizedQuery with the token at index removed.
     */
    public TokenizedQuery removeToken(int index) {
        if (index < 0 || index >= tokens.size()) {
            return new TokenizedQuery(tokens);
        }
        ObjList<SqlToken> newTokens = new ObjList<>(tokens.size() - 1);
        for (int i = 0; i < tokens.size(); i++) {
            if (i != index) {
                newTokens.add(tokens.get(i));
            }
        }
        return new TokenizedQuery(newTokens);
    }

    /**
     * Returns a new TokenizedQuery with tokens in range [start, end) removed.
     */
    public TokenizedQuery removeRange(int start, int end) {
        if (start < 0 || start >= tokens.size() || end <= start) {
            return new TokenizedQuery(tokens);
        }
        end = Math.min(end, tokens.size());
        ObjList<SqlToken> newTokens = new ObjList<>(tokens.size() - (end - start));
        for (int i = 0; i < tokens.size(); i++) {
            if (i < start || i >= end) {
                newTokens.add(tokens.get(i));
            }
        }
        return new TokenizedQuery(newTokens);
    }

    /**
     * Returns a new TokenizedQuery with tokens at i and j swapped.
     */
    public TokenizedQuery swapTokens(int i, int j) {
        if (i < 0 || i >= tokens.size() || j < 0 || j >= tokens.size() || i == j) {
            return new TokenizedQuery(tokens);
        }
        ObjList<SqlToken> newTokens = new ObjList<>(tokens.size());
        for (int k = 0; k < tokens.size(); k++) {
            if (k == i) {
                newTokens.add(tokens.get(j));
            } else if (k == j) {
                newTokens.add(tokens.get(i));
            } else {
                newTokens.add(tokens.get(k));
            }
        }
        return new TokenizedQuery(newTokens);
    }

    /**
     * Returns a new TokenizedQuery with a token inserted at index.
     */
    public TokenizedQuery insertToken(int index, SqlToken token) {
        if (index < 0 || index > tokens.size()) {
            return new TokenizedQuery(tokens);
        }
        ObjList<SqlToken> newTokens = new ObjList<>(tokens.size() + 1);
        for (int i = 0; i < tokens.size(); i++) {
            if (i == index) {
                newTokens.add(token);
            }
            newTokens.add(tokens.get(i));
        }
        if (index == tokens.size()) {
            newTokens.add(token);
        }
        return new TokenizedQuery(newTokens);
    }

    /**
     * Returns a new TokenizedQuery with the token at index replaced.
     */
    public TokenizedQuery replaceToken(int index, SqlToken token) {
        if (index < 0 || index >= tokens.size()) {
            return new TokenizedQuery(tokens);
        }
        ObjList<SqlToken> newTokens = new ObjList<>(tokens.size());
        for (int i = 0; i < tokens.size(); i++) {
            if (i == index) {
                newTokens.add(token);
            } else {
                newTokens.add(tokens.get(i));
            }
        }
        return new TokenizedQuery(newTokens);
    }

    /**
     * Returns a new TokenizedQuery with the token at index duplicated.
     */
    public TokenizedQuery duplicateToken(int index) {
        if (index < 0 || index >= tokens.size()) {
            return new TokenizedQuery(tokens);
        }
        return insertToken(index + 1, tokens.get(index));
    }

    /**
     * Returns a new TokenizedQuery truncated at the given index.
     */
    public TokenizedQuery truncateAt(int index) {
        if (index < 0 || index >= tokens.size()) {
            return new TokenizedQuery(tokens);
        }
        ObjList<SqlToken> newTokens = new ObjList<>(index);
        for (int i = 0; i < index; i++) {
            newTokens.add(tokens.get(i));
        }
        return new TokenizedQuery(newTokens);
    }

    // --- Serialization ---

    /**
     * Serializes the tokens back to a SQL string with appropriate spacing.
     */
    public String serialize() {
        sink.clear();

        for (int i = 0; i < tokens.size(); i++) {
            SqlToken token = tokens.get(i);

            // Add space before token if needed
            if (i > 0 && needsSpaceBetween(tokens.get(i - 1), token)) {
                sink.put(' ');
            }

            sink.put(token.value());
        }

        return sink.toString();
    }

    /**
     * Determines if a space is needed between two tokens.
     */
    private static boolean needsSpaceBetween(SqlToken prev, SqlToken curr) {
        // No space after open paren
        if (prev.value().equals("(")) {
            return false;
        }

        // No space before close paren
        if (curr.value().equals(")")) {
            return false;
        }

        // No space before comma or semicolon
        if (curr.value().equals(",") || curr.value().equals(";")) {
            return false;
        }

        // No space around dot (member access)
        if (prev.value().equals(".") || curr.value().equals(".")) {
            return false;
        }

        // No space before open paren only after identifier (function call)
        if (curr.value().equals("(") && prev.isIdentifier()) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return serialize();
    }
}
