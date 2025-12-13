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

import io.questdb.std.Rnd;
import io.questdb.test.fuzz.sql.generators.SelectGenerator;

/**
 * Main orchestrator for SQL fuzz generation.
 * <p>
 * Generates SQL queries in three modes:
 * <ul>
 *   <li>VALID - structurally correct SQL</li>
 *   <li>CORRUPT - valid SQL with random corruptions applied</li>
 *   <li>GARBAGE - random token sequences</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * Rnd rnd = new Rnd(seed0, seed1);
 * SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());
 *
 * for (int i = 0; i < iterations; i++) {
 *     String sql = generator.generate();
 *     long[] seeds = generator.getLastSeeds();
 *     // Parse and test...
 * }
 * </pre>
 */
public final class SqlFuzzGenerator {

    /**
     * Generation mode.
     */
    public enum Mode {
        VALID,
        CORRUPT,
        GARBAGE
    }

    private final Rnd rnd;
    private final GeneratorConfig config;
    private final GeneratorContext ctx;

    // Last generation state (for reproduction)
    private long lastSeed0;
    private long lastSeed1;
    private Mode lastMode;

    /**
     * Creates a new generator with the given random source and configuration.
     */
    public SqlFuzzGenerator(Rnd rnd, GeneratorConfig config) {
        this.rnd = rnd;
        this.config = config;
        this.ctx = new GeneratorContext(rnd, config);
    }

    /**
     * Generates a SQL query string.
     * <p>
     * Before generation, the current RNG state is captured for reproduction.
     * After generation, use {@link #getLastSeeds()} to retrieve the seeds
     * and {@link #getLastMode()} to retrieve the generation mode.
     *
     * @return generated SQL query
     */
    public String generate() {
        // Capture seeds before generation
        lastSeed0 = rnd.getSeed0();
        lastSeed1 = rnd.getSeed1();

        // Select generation mode
        lastMode = selectMode();

        // Generate based on mode
        switch (lastMode) {
            case VALID:
                return generateValid();
            case CORRUPT:
                return generateCorrupt();
            case GARBAGE:
                return generateGarbage();
            default:
                throw new IllegalStateException("Unknown mode: " + lastMode);
        }
    }

    /**
     * Generates a tokenized query.
     * <p>
     * Useful for tests that want to inspect or manipulate the token stream.
     *
     * @return generated tokenized query
     */
    public TokenizedQuery generateTokenized() {
        lastSeed0 = rnd.getSeed0();
        lastSeed1 = rnd.getSeed1();
        lastMode = selectMode();

        switch (lastMode) {
            case VALID:
                return generateValidTokenized();
            case CORRUPT:
                return generateCorruptTokenized();
            case GARBAGE:
                return generateGarbageTokenized();
            default:
                throw new IllegalStateException("Unknown mode: " + lastMode);
        }
    }

    /**
     * Returns the seeds captured before the last generation.
     * Use these to reproduce the exact same query.
     *
     * @return array of [seed0, seed1]
     */
    public long[] getLastSeeds() {
        return new long[]{lastSeed0, lastSeed1};
    }

    /**
     * Returns the first seed from the last generation.
     */
    public long getLastSeed0() {
        return lastSeed0;
    }

    /**
     * Returns the second seed from the last generation.
     */
    public long getLastSeed1() {
        return lastSeed1;
    }

    /**
     * Returns the generation mode used for the last query.
     */
    public Mode getLastMode() {
        return lastMode;
    }

    /**
     * Maps the generation mode to FuzzFailure.GenerationMode.
     */
    public FuzzFailure.GenerationMode getLastGenerationMode() {
        switch (lastMode) {
            case VALID:
                return FuzzFailure.GenerationMode.VALID;
            case CORRUPT:
                return FuzzFailure.GenerationMode.CORRUPT;
            case GARBAGE:
                return FuzzFailure.GenerationMode.GARBAGE;
            default:
                throw new IllegalStateException("Unknown mode: " + lastMode);
        }
    }

    /**
     * Returns the configuration used by this generator.
     */
    public GeneratorConfig config() {
        return config;
    }

    // --- Mode selection ---

    private Mode selectMode() {
        double totalWeight = config.totalModeWeight();
        double roll = rnd.nextDouble() * totalWeight;

        double cumulative = 0;
        cumulative += config.validModeWeight();
        if (roll < cumulative) {
            return Mode.VALID;
        }

        cumulative += config.corruptModeWeight();
        if (roll < cumulative) {
            return Mode.CORRUPT;
        }

        return Mode.GARBAGE;
    }

    // --- Valid generation ---

    private String generateValid() {
        return generateValidTokenized().serialize();
    }

    private TokenizedQuery generateValidTokenized() {
        ctx.reset();
        generateSelectStatement(ctx);
        return ctx.tokens();
    }

    // --- Corrupt generation ---

    private String generateCorrupt() {
        return generateCorruptTokenized().serialize();
    }

    private TokenizedQuery generateCorruptTokenized() {
        // First generate a valid query
        TokenizedQuery valid = generateValidTokenized();

        // Then apply random corruptions
        return applyCorruptions(valid);
    }

    // --- Garbage generation ---

    private String generateGarbage() {
        return generateGarbageTokenized().serialize();
    }

    private TokenizedQuery generateGarbageTokenized() {
        ctx.reset();
        int tokenCount = 1 + rnd.nextInt(20);  // 1-20 random tokens

        for (int i = 0; i < tokenCount; i++) {
            ctx.tokens().add(randomToken());
        }

        return ctx.tokens();
    }

    // --- Statement generation ---

    /**
     * Generates a SELECT statement using the SelectGenerator.
     */
    private void generateSelectStatement(GeneratorContext ctx) {
        SelectGenerator.generate(ctx);
    }

    // --- Corruption (placeholder for Phase 4) ---

    /**
     * Applies random corruptions to a valid query.
     * Placeholder implementation - will be expanded in Phase 4.
     */
    private TokenizedQuery applyCorruptions(TokenizedQuery query) {
        if (query.isEmpty()) {
            return query;
        }

        // Apply 1-3 random corruptions
        int corruptionCount = 1 + rnd.nextInt(3);
        TokenizedQuery result = query;

        for (int i = 0; i < corruptionCount; i++) {
            result = applyOneCorruption(result);
        }

        return result;
    }

    private TokenizedQuery applyOneCorruption(TokenizedQuery query) {
        if (query.isEmpty()) {
            return query;
        }

        int corruptionType = rnd.nextInt(6);
        int pos = rnd.nextInt(query.size());

        switch (corruptionType) {
            case 0:  // Drop token
                return query.removeToken(pos);
            case 1:  // Duplicate token
                return query.duplicateToken(pos);
            case 2:  // Swap adjacent tokens
                if (pos < query.size() - 1) {
                    return query.swapTokens(pos, pos + 1);
                }
                return query;
            case 3:  // Insert random token
                return query.insertToken(pos, randomToken());
            case 4:  // Replace with random token
                return query.replaceToken(pos, randomToken());
            case 5:  // Truncate
                return query.truncateAt(pos);
            default:
                return query;
        }
    }

    // --- Token generation helpers ---

    private static final String[] KEYWORDS = {
            "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR",
            "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
            "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
            "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "ASOF", "LT",
            "SAMPLE", "FILL", "ALIGN", "LATEST", "PARTITION",
            "UNION", "EXCEPT", "INTERSECT", "ALL", "DISTINCT",
            "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "IN",
            "BETWEEN", "LIKE", "IS", "NULL", "NOT", "TRUE", "FALSE"
    };

    private static final String[] OPERATORS = {
            "+", "-", "*", "/", "%",
            "=", "!=", "<>", "<", ">", "<=", ">=",
            "AND", "OR", "NOT",
            "||", "&", "|", "^", "~",
            "<<", ">>"
    };

    private SqlToken randomToken() {
        int tokenType = rnd.nextInt(5);

        switch (tokenType) {
            case 0:  // Keyword
                return SqlToken.keyword(KEYWORDS[rnd.nextInt(KEYWORDS.length)]);
            case 1:  // Identifier
                return SqlToken.identifier(ctx.randomColumnName());
            case 2:  // Literal
                return randomLiteral();
            case 3:  // Operator
                return SqlToken.operator(OPERATORS[rnd.nextInt(OPERATORS.length)]);
            case 4:  // Punctuation
                return randomPunctuation();
            default:
                return SqlToken.keyword("SELECT");
        }
    }

    private SqlToken randomLiteral() {
        int literalType = rnd.nextInt(4);

        switch (literalType) {
            case 0:  // Integer
                return SqlToken.literal(String.valueOf(rnd.nextInt(1000)));
            case 1:  // Negative integer
                return SqlToken.literal(String.valueOf(-rnd.nextInt(1000)));
            case 2:  // Float
                return SqlToken.literal(String.format("%.2f", rnd.nextDouble() * 1000));
            case 3:  // String
                return SqlToken.literal("'" + ctx.randomColumnName() + "'");
            default:
                return SqlToken.literal("0");
        }
    }

    private SqlToken randomPunctuation() {
        String[] puncts = {"(", ")", ",", ";", "."};
        return SqlToken.punctuation(puncts[rnd.nextInt(puncts.length)]);
    }
}
