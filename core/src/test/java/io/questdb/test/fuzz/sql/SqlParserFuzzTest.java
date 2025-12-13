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

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fuzz test for QuestDB's SQL parser.
 * <p>
 * Generates random SQL queries and verifies the parser either:
 * <ul>
 *   <li>Parses successfully (PARSED_OK)</li>
 *   <li>Throws SqlException (SYNTAX_ERROR)</li>
 * </ul>
 * <p>
 * The parser must NEVER:
 * <ul>
 *   <li>Throw unexpected exceptions (CRASH)</li>
 *   <li>Hang indefinitely (TIMEOUT)</li>
 * </ul>
 * <p>
 * Configuration via system properties:
 * <ul>
 *   <li>questdb.fuzz.iterations - number of iterations (default: 10000)</li>
 *   <li>questdb.fuzz.timeout - per-query timeout in ms (default: 1000)</li>
 * </ul>
 */
public class SqlParserFuzzTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(SqlParserFuzzTest.class);

    // System properties for configuration
    private static final String PROP_ITERATIONS = "questdb.fuzz.iterations";
    private static final String PROP_TIMEOUT = "questdb.fuzz.timeout";

    // Defaults
    private static final int DEFAULT_ITERATIONS = 10_000;
    private static final long DEFAULT_TIMEOUT_MS = 1000;

    /**
     * Main fuzz test - generates and parses random SQL queries.
     */
    @Test
    public void testFuzzSqlParser() throws Exception {
        final int iterations = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS);
        final long timeoutMs = Long.getLong(PROP_TIMEOUT, DEFAULT_TIMEOUT_MS);

        LOG.info().$("Starting SQL parser fuzz test")
                .$(" iterations=").$(iterations)
                .$(" timeout=").$(timeoutMs).$("ms")
                .$();

        Rnd rnd = TestUtils.generateRandom(LOG);
        GeneratorConfig config = GeneratorConfig.defaults();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        runFuzzTest(generator, iterations, timeoutMs);
    }

    /**
     * Fuzz test with QuestDB-focused configuration.
     */
    @Test
    public void testFuzzSqlParserQuestdbFocused() throws Exception {
        final int iterations = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS) / 10;  // Shorter
        final long timeoutMs = Long.getLong(PROP_TIMEOUT, DEFAULT_TIMEOUT_MS);

        LOG.info().$("Starting QuestDB-focused SQL parser fuzz test")
                .$(" iterations=").$(iterations)
                .$();

        Rnd rnd = TestUtils.generateRandom(LOG);
        GeneratorConfig config = GeneratorConfig.questdbFocused();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        runFuzzTest(generator, iterations, timeoutMs);
    }

    /**
     * Fuzz test with valid-only queries.
     * These should mostly parse successfully.
     */
    @Test
    public void testFuzzValidOnly() throws Exception {
        final int iterations = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS) / 10;
        final long timeoutMs = Long.getLong(PROP_TIMEOUT, DEFAULT_TIMEOUT_MS);

        LOG.info().$("Starting valid-only SQL parser fuzz test")
                .$(" iterations=").$(iterations)
                .$();

        Rnd rnd = TestUtils.generateRandom(LOG);
        GeneratorConfig config = GeneratorConfig.builder().validOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        runFuzzTest(generator, iterations, timeoutMs);
    }

    /**
     * Fuzz test with corrupt queries.
     * These should mostly throw SqlException.
     */
    @Test
    public void testFuzzCorruptOnly() throws Exception {
        final int iterations = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS) / 10;
        final long timeoutMs = Long.getLong(PROP_TIMEOUT, DEFAULT_TIMEOUT_MS);

        LOG.info().$("Starting corrupt-only SQL parser fuzz test")
                .$(" iterations=").$(iterations)
                .$();

        Rnd rnd = TestUtils.generateRandom(LOG);
        GeneratorConfig config = GeneratorConfig.builder().corruptOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        runFuzzTest(generator, iterations, timeoutMs);
    }

    /**
     * Fuzz test with garbage queries.
     * These should all throw SqlException.
     */
    @Test
    public void testFuzzGarbageOnly() throws Exception {
        final int iterations = Integer.getInteger(PROP_ITERATIONS, DEFAULT_ITERATIONS) / 10;
        final long timeoutMs = Long.getLong(PROP_TIMEOUT, DEFAULT_TIMEOUT_MS);

        LOG.info().$("Starting garbage-only SQL parser fuzz test")
                .$(" iterations=").$(iterations)
                .$();

        Rnd rnd = TestUtils.generateRandom(LOG);
        GeneratorConfig config = GeneratorConfig.builder().garbageOnly().build();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        runFuzzTest(generator, iterations, timeoutMs);
    }

    /**
     * Basic integration test for Phase 2 generators.
     * Runs with a small iteration count suitable for CI.
     * Verifies that generated queries either parse or throw SqlException (no crashes).
     */
    @Test
    public void testBasicGeneratorIntegration() throws Exception {
        final int iterations = 1000;  // Small count for CI
        final long timeoutMs = DEFAULT_TIMEOUT_MS;

        LOG.info().$("Starting basic generator integration test")
                .$(" iterations=").$(iterations)
                .$();

        Rnd rnd = TestUtils.generateRandom(LOG);
        // Use simple config with valid only to test the generators
        GeneratorConfig config = GeneratorConfig.simple();
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, config);

        runFuzzTest(generator, iterations, timeoutMs);
    }

    /**
     * Test that a specific seed can be reproduced.
     * Use this test to debug failures by setting SEED0 and SEED1.
     */
    @Test
    public void testReproduceFromSeed() throws Exception {
        // Set these values from a failure report to reproduce
        final long SEED0 = 0L;  // Replace with failure seed0
        final long SEED1 = 0L;  // Replace with failure seed1

        if (SEED0 == 0 && SEED1 == 0) {
            // Skip if no seeds specified
            return;
        }

        LOG.info().$("Reproducing from seeds: ").$(SEED0).$("L, ").$(SEED1).$("L").$();

        Rnd rnd = new Rnd(SEED0, SEED1);
        SqlFuzzGenerator generator = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());
        String sql = generator.generate();

        LOG.info().$("Generated SQL: ").$(sql).$();

        FuzzResult result = testParse(sql);
        LOG.info().$("Result: ").$(result).$();

        Assert.assertTrue("Parser crashed: " + result, result.isSuccess());
    }

    // --- Core test runner ---

    private void runFuzzTest(SqlFuzzGenerator generator, int iterations, long timeoutMs) throws Exception {
        assertMemoryLeak(() -> {
            int parsedOk = 0;
            int syntaxErrors = 0;
            ObjList<FuzzFailure> failures = new ObjList<>();

            final int progressInterval = Math.max(1, iterations / 10);

            for (int i = 0; i < iterations; i++) {
                // Generate query
                String sql = generator.generate();
                long seed0 = generator.getLastSeed0();
                long seed1 = generator.getLastSeed1();
                FuzzFailure.GenerationMode mode = generator.getLastGenerationMode();

                // Parse and classify
                FuzzResult result = testParse(sql);

                switch (result.type()) {
                    case PARSED_OK:
                        parsedOk++;
                        break;
                    case SYNTAX_ERROR:
                        syntaxErrors++;
                        break;
                    case TIMEOUT:
                    case CRASH:
                        failures.add(new FuzzFailure(i, seed0, seed1, sql, result, mode));
                        break;
                }

                // Progress logging
                if ((i + 1) % progressInterval == 0) {
                    LOG.info().$("Progress: ").$(i + 1).$("/").$(iterations)
                            .$(" ok=").$(parsedOk)
                            .$(" errors=").$(syntaxErrors)
                            .$(" failures=").$(failures.size())
                            .$();
                }
            }

            // Final report
            LOG.info().$("=== Fuzz Test Results ===").$();
            LOG.info().$("Iterations: ").$(iterations).$();
            LOG.info().$("Parsed OK: ").$(parsedOk).$();
            LOG.info().$("Syntax Errors: ").$(syntaxErrors).$();
            LOG.info().$("FAILURES: ").$(failures.size()).$();

            // Report failures
            if (failures.size() > 0) {
                LOG.error().$("=== FAILURES ===").$();
                for (int i = 0; i < failures.size(); i++) {
                    FuzzFailure f = failures.get(i);
                    LOG.error().$(f.toSummary()).$();
                    LOG.error().$("SQL: ").$(f.sql()).$();
                    if (f.exception() != null) {
                        LOG.error().$(f.exception()).$();
                    }
                    LOG.info().$("Reproduction code:").$();
                    LOG.info().$(f.toReproductionCode()).$();
                }
            }

            Assert.assertEquals(
                    "Parser crashed on " + failures.size() + " inputs. See log for details.",
                    0,
                    failures.size()
            );
        });
    }

    // --- Parsing ---

    /**
     * Parses the given SQL and returns the result.
     */
    private FuzzResult testParse(String sql) {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try {
                compiler.testCompileModel(sql, sqlExecutionContext);
                return FuzzResult.parsedOk();
            } catch (SqlException e) {
                return FuzzResult.syntaxError(e.getMessage());
            } catch (Throwable t) {
                return FuzzResult.crash(t);
            }
        }
    }
}
