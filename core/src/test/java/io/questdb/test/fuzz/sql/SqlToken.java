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

import java.util.Objects;

/**
 * Represents a single token in a SQL query.
 * Used for token-based corruption strategies where we need to manipulate
 * the query at the token level rather than as raw text.
 */
public final class SqlToken {

    /**
     * Token types for classification.
     */
    public enum Type {
        /**
         * SQL keywords like SELECT, FROM, WHERE, JOIN, etc.
         */
        KEYWORD,

        /**
         * Identifiers like table names, column names, aliases.
         */
        IDENTIFIER,

        /**
         * Literal values: numbers, strings, booleans, etc.
         */
        LITERAL,

        /**
         * Operators: +, -, *, /, =, !=, AND, OR, etc.
         */
        OPERATOR,

        /**
         * Punctuation: (, ), ,, ;, .
         */
        PUNCTUATION
    }

    private final String value;
    private final Type type;

    /**
     * Creates a new token with the given value and type.
     */
    public SqlToken(String value, Type type) {
        this.value = Objects.requireNonNull(value, "value cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
    }

    /**
     * Creates a keyword token.
     */
    public static SqlToken keyword(String value) {
        return new SqlToken(value, Type.KEYWORD);
    }

    /**
     * Creates an identifier token.
     */
    public static SqlToken identifier(String value) {
        return new SqlToken(value, Type.IDENTIFIER);
    }

    /**
     * Creates a literal token.
     */
    public static SqlToken literal(String value) {
        return new SqlToken(value, Type.LITERAL);
    }

    /**
     * Creates an operator token.
     */
    public static SqlToken operator(String value) {
        return new SqlToken(value, Type.OPERATOR);
    }

    /**
     * Creates a punctuation token.
     */
    public static SqlToken punctuation(String value) {
        return new SqlToken(value, Type.PUNCTUATION);
    }

    public String value() {
        return value;
    }

    public Type type() {
        return type;
    }

    /**
     * Returns true if this is a keyword token.
     */
    public boolean isKeyword() {
        return type == Type.KEYWORD;
    }

    /**
     * Returns true if this is an identifier token.
     */
    public boolean isIdentifier() {
        return type == Type.IDENTIFIER;
    }

    /**
     * Returns true if this is a literal token.
     */
    public boolean isLiteral() {
        return type == Type.LITERAL;
    }

    /**
     * Returns true if this is an operator token.
     */
    public boolean isOperator() {
        return type == Type.OPERATOR;
    }

    /**
     * Returns true if this is a punctuation token.
     */
    public boolean isPunctuation() {
        return type == Type.PUNCTUATION;
    }

    /**
     * Returns true if this token requires a space before it when serializing.
     * Punctuation like ( and . don't need space before.
     */
    public boolean needsSpaceBefore() {
        if (type == Type.PUNCTUATION) {
            return false;  // No space before (, ), ,, ;, .
        }
        return true;
    }

    /**
     * Returns true if this token requires a space after it when serializing.
     * Punctuation like . and ( don't need space after.
     */
    public boolean needsSpaceAfter() {
        if (type == Type.PUNCTUATION) {
            // No space after ( or .
            return !value.equals("(") && !value.equals(".");
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlToken sqlToken = (SqlToken) o;
        return value.equals(sqlToken.value) && type == sqlToken.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }

    @Override
    public String toString() {
        return type + ":" + value;
    }
}
