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
import io.questdb.test.fuzz.sql.corruption.CorruptGenerator;
import io.questdb.test.fuzz.sql.corruption.GarbageGenerator;
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

    // --- Garbage generation using GarbageGenerator ---

    private String generateGarbage() {
        // Sometimes use raw garbage, sometimes tokenized
        if (rnd.nextDouble() < 0.3) {
            String raw = GarbageGenerator.generateRaw(rnd);
            // If raw is empty, fall back to tokenized
            if (raw == null || raw.isEmpty()) {
                return generateGarbageTokenized().serialize();
            }
            return raw;
        }
        return generateGarbageTokenized().serialize();
    }

    private TokenizedQuery generateGarbageTokenized() {
        return GarbageGenerator.generate(rnd);
    }

    // --- Statement generation ---

    /**
     * Generates a SELECT statement using the SelectGenerator.
     */
    private void generateSelectStatement(GeneratorContext ctx) {
        SelectGenerator.generate(ctx);
    }

    // --- Corruption using CorruptGenerator ---

    /**
     * Applies random corruptions to a valid query using CorruptGenerator.
     */
    private TokenizedQuery applyCorruptions(TokenizedQuery query) {
        return CorruptGenerator.corruptRandom(query, rnd);
    }

}
