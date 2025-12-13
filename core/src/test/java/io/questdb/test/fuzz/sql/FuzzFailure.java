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

import io.questdb.std.str.StringSink;

/**
 * Records details of a fuzz test failure for debugging and reproduction.
 * <p>
 * Each failure captures:
 * <ul>
 *   <li>Iteration number - which iteration failed</li>
 *   <li>Seeds - both s0 and s1 for exact reproduction with QuestDB's Rnd class</li>
 *   <li>SQL - the query that caused the failure</li>
 *   <li>Result - the failure type (TIMEOUT or CRASH) with exception if applicable</li>
 *   <li>Generation mode - whether the query was VALID, CORRUPT, or GARBAGE</li>
 * </ul>
 */
public final class FuzzFailure {

    /**
     * Generation mode that produced the failing query.
     */
    public enum GenerationMode {
        VALID,
        CORRUPT,
        GARBAGE
    }

    private final int iteration;
    private final long seed0;
    private final long seed1;
    private final String sql;
    private final FuzzResult result;
    private final GenerationMode generationMode;

    /**
     * Creates a new failure record.
     *
     * @param iteration      the iteration number where the failure occurred
     * @param seed0          the first seed (s0) for reproduction
     * @param seed1          the second seed (s1) for reproduction
     * @param sql            the SQL query that caused the failure
     * @param result         the failure result (should be TIMEOUT or CRASH)
     * @param generationMode the mode used to generate the query
     */
    public FuzzFailure(
            int iteration,
            long seed0,
            long seed1,
            String sql,
            FuzzResult result,
            GenerationMode generationMode
    ) {
        this.iteration = iteration;
        this.seed0 = seed0;
        this.seed1 = seed1;
        this.sql = sql;
        this.result = result;
        this.generationMode = generationMode;
    }

    public int iteration() {
        return iteration;
    }

    public long seed0() {
        return seed0;
    }

    public long seed1() {
        return seed1;
    }

    /**
     * Returns both seeds as an array [seed0, seed1].
     */
    public long[] seeds() {
        return new long[]{seed0, seed1};
    }

    public String sql() {
        return sql;
    }

    public FuzzResult result() {
        return result;
    }

    public GenerationMode generationMode() {
        return generationMode;
    }

    /**
     * Returns true if this failure was caused by a timeout.
     */
    public boolean isTimeout() {
        return result.isTimeout();
    }

    /**
     * Returns true if this failure was caused by a crash.
     */
    public boolean isCrash() {
        return result.isCrash();
    }

    /**
     * Returns the exception that caused the crash, or null if this was a timeout.
     */
    public Throwable exception() {
        return result.exception();
    }

    /**
     * Generates Java code that can reproduce this failure.
     * <p>
     * Example output:
     * <pre>
     * // Reproduction for failure #42
     * // Mode: VALID
     * // Result: CRASH: NullPointerException: null
     * Rnd rnd = new Rnd(123456789L, 987654321L);
     * String sql = "SELECT a FROM t";
     * </pre>
     */
    public String toReproductionCode() {
        StringSink sink = new StringSink();

        sink.put("// Reproduction for failure #").put(iteration).put('\n');
        sink.put("// Mode: ").put(generationMode.name()).put('\n');
        sink.put("// Result: ").put(result.toString()).put('\n');
        sink.put("Rnd rnd = new Rnd(").put(seed0).put("L, ").put(seed1).put("L);\n");

        // Escape the SQL string for Java
        sink.put("String sql = \"");
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            switch (c) {
                case '"':
                    sink.put("\\\"");
                    break;
                case '\\':
                    sink.put("\\\\");
                    break;
                case '\n':
                    sink.put("\\n");
                    break;
                case '\r':
                    sink.put("\\r");
                    break;
                case '\t':
                    sink.put("\\t");
                    break;
                default:
                    if (c < 32 || c > 126) {
                        // Non-printable or non-ASCII: use unicode escape
                        sink.put("\\u").put(String.format("%04x", (int) c));
                    } else {
                        sink.put(c);
                    }
            }
        }
        sink.put("\";\n");

        return sink.toString();
    }

    /**
     * Returns a concise summary of this failure suitable for logging.
     */
    public String toSummary() {
        StringSink sink = new StringSink();
        sink.put("Failure #").put(iteration);
        sink.put(" [").put(generationMode.name()).put("]");
        sink.put(" seeds=(").put(seed0).put("L, ").put(seed1).put("L)");
        sink.put(" result=").put(result.type().name());
        if (result.exception() != null) {
            sink.put(" (").put(result.exception().getClass().getSimpleName()).put(")");
        }
        return sink.toString();
    }

    @Override
    public String toString() {
        StringSink sink = new StringSink();
        sink.put("FuzzFailure{");
        sink.put("iteration=").put(iteration);
        sink.put(", seed0=").put(seed0);
        sink.put(", seed1=").put(seed1);
        sink.put(", mode=").put(generationMode.name());
        sink.put(", result=").put(result.type().name());
        sink.put(", sql='");
        // Truncate long SQL
        if (sql.length() > 100) {
            sink.put(sql, 0, 100).put("...");
        } else {
            sink.put(sql);
        }
        sink.put("'}");
        return sink.toString();
    }
}
