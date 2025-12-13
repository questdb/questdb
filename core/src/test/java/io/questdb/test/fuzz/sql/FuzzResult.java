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

/**
 * Classification of fuzz test results.
 * <p>
 * A successful fuzz test means the parser either:
 * <ul>
 *   <li>PARSED_OK - accepted the query</li>
 *   <li>SYNTAX_ERROR - rejected with proper SqlException</li>
 * </ul>
 * <p>
 * Failures indicate bugs in the parser:
 * <ul>
 *   <li>TIMEOUT - parser hung (did not complete within timeout)</li>
 *   <li>CRASH - unexpected exception thrown</li>
 * </ul>
 */
public final class FuzzResult {

    /**
     * Result type classification.
     */
    public enum Type {
        /**
         * Parser accepted the query - this is expected for valid SQL.
         */
        PARSED_OK,

        /**
         * Parser threw SqlException - this is expected for invalid SQL.
         */
        SYNTAX_ERROR,

        /**
         * Parser did not complete within timeout - this is a bug!
         */
        TIMEOUT,

        /**
         * Parser threw an unexpected exception - this is a bug!
         */
        CRASH
    }

    // Pre-allocated singleton instances for non-failure cases
    private static final FuzzResult PARSED_OK_INSTANCE = new FuzzResult(Type.PARSED_OK, null, null);
    private static final FuzzResult SYNTAX_ERROR_INSTANCE = new FuzzResult(Type.SYNTAX_ERROR, null, null);
    private static final FuzzResult TIMEOUT_INSTANCE = new FuzzResult(Type.TIMEOUT, null, null);

    private final Type type;
    private final Throwable exception;
    private final String errorMessage;

    private FuzzResult(Type type, Throwable exception, String errorMessage) {
        this.type = type;
        this.exception = exception;
        this.errorMessage = errorMessage;
    }

    /**
     * Returns a PARSED_OK result (parser accepted the query).
     */
    public static FuzzResult parsedOk() {
        return PARSED_OK_INSTANCE;
    }

    /**
     * Returns a SYNTAX_ERROR result (parser threw SqlException).
     */
    public static FuzzResult syntaxError() {
        return SYNTAX_ERROR_INSTANCE;
    }

    /**
     * Returns a SYNTAX_ERROR result with error message.
     */
    public static FuzzResult syntaxError(String errorMessage) {
        return new FuzzResult(Type.SYNTAX_ERROR, null, errorMessage);
    }

    /**
     * Returns a TIMEOUT result (parser did not complete within timeout).
     */
    public static FuzzResult timeout() {
        return TIMEOUT_INSTANCE;
    }

    /**
     * Returns a CRASH result with the exception that caused the crash.
     */
    public static FuzzResult crash(Throwable exception) {
        return new FuzzResult(Type.CRASH, exception, null);
    }

    public Type type() {
        return type;
    }

    public Throwable exception() {
        return exception;
    }

    public String errorMessage() {
        return errorMessage;
    }

    /**
     * Returns true if this result represents a successful parse
     * (either parsed OK or expected syntax error).
     */
    public boolean isSuccess() {
        return type == Type.PARSED_OK || type == Type.SYNTAX_ERROR;
    }

    /**
     * Returns true if this result represents a failure
     * (timeout or crash - indicates a parser bug).
     */
    public boolean isFailure() {
        return type == Type.TIMEOUT || type == Type.CRASH;
    }

    /**
     * Returns true if the parser accepted the query.
     */
    public boolean isParsedOk() {
        return type == Type.PARSED_OK;
    }

    /**
     * Returns true if the parser threw SqlException.
     */
    public boolean isSyntaxError() {
        return type == Type.SYNTAX_ERROR;
    }

    /**
     * Returns true if the parser timed out.
     */
    public boolean isTimeout() {
        return type == Type.TIMEOUT;
    }

    /**
     * Returns true if the parser crashed with an unexpected exception.
     */
    public boolean isCrash() {
        return type == Type.CRASH;
    }

    @Override
    public String toString() {
        if (exception != null) {
            return type + ": " + exception.getClass().getSimpleName() + ": " + exception.getMessage();
        }
        if (errorMessage != null) {
            return type + ": " + errorMessage;
        }
        return type.toString();
    }
}
