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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Tests for plugin function parsing and execution.
 * Verifies that qualified plugin function names (plugin_name.function_name)
 * are correctly parsed and not confused with table.column references.
 */
public class PluginFunctionTest extends AbstractCairoTest {

    private static final String PLUGIN_NAME = "questdb-plugin-example-1.0.0";
    private static final String PLUGIN_JAR = PLUGIN_NAME + ".jar";

    @Before
    public void setUp() {
        super.setUp();
        setupPluginDirectory();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Unload any loaded plugins to prevent state leakage between tests
        if (engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME)) {
            try {
                engine.getPluginManager().unloadPlugin(PLUGIN_NAME);
            } catch (Exception e) {
                // Ignore - plugin might not be loaded
            }
        }
        // Must call parent tearDown to properly clean up engine state
        super.tearDown();
    }

    @Test
    public void testPluginDiscovery() throws Exception {
        // First verify that the plugin is correctly discovered
        assertMemoryLeak(() -> {
            io.questdb.std.ObjList<CharSequence> available = engine.getPluginManager().getAvailablePlugins();
            org.junit.Assert.assertTrue(
                    "Plugin should be available. Found: " + available,
                    containsIgnoreCase(available, PLUGIN_NAME)
            );
        });
    }

    @Test
    public void testPluginFunctionIsRecognizedAsGroupBy() throws Exception {
        // Test that the function factory cache correctly identifies plugin GROUP BY functions
        assertMemoryLeak(() -> {
            // Load the plugin
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Verify the function is recognized as a GROUP BY function
            final String qualifiedName = PLUGIN_NAME + ".example_weighted_avg";
            assert engine.getFunctionFactoryCache().isGroupBy(qualifiedName) :
                    "Plugin GROUP BY function should be recognized: " + qualifiedName;

            // Also test with quoted name (as it appears in SQL)
            final String quotedName = "\"" + PLUGIN_NAME + "\".example_weighted_avg";
            assert engine.getFunctionFactoryCache().isGroupBy(quotedName) :
                    "Plugin GROUP BY function with quotes should be recognized: " + quotedName;
        });
    }

    @Test
    public void testPluginGroupByFunctionWithSubquery() throws Exception {
        // Test plugin GROUP BY function with a subquery (UNION ALL)
        assertMemoryLeak(() -> {
            // Load the plugin
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Weighted average: (5*0.2 + 2*0.3 + 3*0.7) / (0.2 + 0.3 + 0.7) = (1 + 0.6 + 2.1) / 1.2 = 3.7 / 1.2 = 3.0833...
            assertSql(
                    "example_weighted_avg\n" +
                            "3.083333333333333\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_weighted_avg(x, y)\n" +
                            "FROM (\n" +
                            "    SELECT 5 as x, 0.2 as y\n" +
                            "    UNION ALL\n" +
                            "    SELECT 2 as x, 0.3 as y\n" +
                            "    UNION ALL\n" +
                            "    SELECT 3 as x, 0.7 as y\n" +
                            ")"
            );
        });
    }

    @Test
    public void testPluginGroupByFunctionWithTable() throws Exception {
        // Test that plugin GROUP BY functions work with a table and GROUP BY clause
        assertMemoryLeak(() -> {
            // Load the plugin
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Create a test table
            execute("CREATE TABLE test_data (category SYMBOL, value DOUBLE, weight DOUBLE)");
            execute("INSERT INTO test_data VALUES ('A', 10.0, 2.0)");
            execute("INSERT INTO test_data VALUES ('A', 20.0, 3.0)");
            execute("INSERT INTO test_data VALUES ('B', 5.0, 1.0)");
            execute("INSERT INTO test_data VALUES ('B', 15.0, 4.0)");

            // Test plugin GROUP BY function with GROUP BY clause
            assertSql(
                    "category\tweighted_avg\n" +
                            "A\t16.0\n" +
                            "B\t13.0\n",
                    "SELECT category, \"" + PLUGIN_NAME + "\".example_weighted_avg(value, weight) as weighted_avg " +
                            "FROM test_data GROUP BY category ORDER BY category"
            );
        });
    }

    @Test
    public void testPluginGroupByFunctionWithoutTable() throws Exception {
        // Test that plugin GROUP BY functions work without a FROM clause
        // This was causing StackOverflowError due to circular JoinRecord reference
        assertMemoryLeak(() -> {
            // Load the plugin
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Verify plugin is loaded
            org.junit.Assert.assertTrue(
                    "Plugin should be loaded",
                    engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME)
            );

            // Test plugin GROUP BY function without a table
            // The quoted syntax is needed because the plugin name contains hyphens
            // Note: Weighted avg of single value (5 with weight 2) = 5
            assertSql(
                    "example_weighted_avg\n" +
                            "5.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_weighted_avg(5, 2)"
            );
        });
    }

    @Test
    public void testPluginLoadIdempotent() throws Exception {
        // Test that loading the same plugin twice is idempotent
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'"); // Should not throw
            assert engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME);
        });
    }

    @Test
    public void testPluginLoadViaApi() throws Exception {
        // Test loading via the PluginManager API directly
        assertMemoryLeak(() -> {
            // Verify plugin is available
            io.questdb.std.ObjList<CharSequence> available = engine.getPluginManager().getAvailablePlugins();
            org.junit.Assert.assertTrue(
                    "Plugin should be available. Found: " + available,
                    containsIgnoreCase(available, PLUGIN_NAME)
            );

            // Load via API
            engine.getPluginManager().loadPlugin(PLUGIN_NAME);

            // Verify it's loaded
            io.questdb.std.ObjList<CharSequence> loaded = engine.getPluginManager().getLoadedPlugins();
            org.junit.Assert.assertTrue(
                    "Plugin should be loaded. Found: " + loaded,
                    containsIgnoreCase(loaded, PLUGIN_NAME)
            );

            // Verify via FunctionFactoryCache
            org.junit.Assert.assertTrue(
                    "FunctionFactoryCache should show plugin loaded",
                    engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME)
            );
        });
    }

    @Test
    public void testPluginUnload() throws Exception {
        // Test that plugins can be unloaded
        assertMemoryLeak(() -> {
            // Load the plugin
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            assert engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME);

            // Unload the plugin
            execute("UNLOAD PLUGIN '" + PLUGIN_NAME + "'");
            assert !engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME);
        });
    }

    @Test
    public void testShowPlugins() throws Exception {
        // Test the SHOW PLUGINS command
        assertMemoryLeak(() -> {
            // Before loading, plugin should be available but not loaded
            assertSql(
                    "name\tloaded\n" +
                            PLUGIN_NAME + "\tfalse\n",
                    "SHOW PLUGINS"
            );

            // Load the plugin
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // After loading, plugin should show as loaded
            assertSql(
                    "name\tloaded\n" +
                            PLUGIN_NAME + "\ttrue\n",
                    "SHOW PLUGINS"
            );

            // Unload the plugin
            execute("UNLOAD PLUGIN '" + PLUGIN_NAME + "'");

            // After unloading, plugin should show as not loaded
            assertSql(
                    "name\tloaded\n" +
                            PLUGIN_NAME + "\tfalse\n",
                    "SHOW PLUGINS"
            );
        });
    }

    @Test
    public void testLoadPluginAuthorizationDenied() throws Exception {
        // Test that LOAD PLUGIN is denied for read-only security context
        assertMemoryLeak(() -> {
            try {
                ReadOnlySecurityContext.INSTANCE.authorizePluginLoad(PLUGIN_NAME);
                Assert.fail("Expected CairoException for authorization denied");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("Write permission denied"));
            }
        });
    }

    @Test
    public void testUnloadPluginAuthorizationDenied() throws Exception {
        // Test that UNLOAD PLUGIN is denied for read-only security context
        assertMemoryLeak(() -> {
            try {
                ReadOnlySecurityContext.INSTANCE.authorizePluginUnload(PLUGIN_NAME);
                Assert.fail("Expected CairoException for authorization denied");
            } catch (CairoException e) {
                Assert.assertTrue(e.getMessage().contains("Write permission denied"));
            }
        });
    }

    @Test
    public void testPluginScalarFunction() throws Exception {
        // Test the scalar example_square function
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test basic squaring
            assertSql(
                    "example_square\n" +
                            "16.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_square(4.0)"
            );

            // Test with negative number
            assertSql(
                    "example_square\n" +
                            "9.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_square(-3.0)"
            );

            // Test with zero
            assertSql(
                    "example_square\n" +
                            "0.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_square(0.0)"
            );
        });
    }

    @Test
    public void testPluginScalarFunctionWithTable() throws Exception {
        // Test scalar function with table data
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            execute("CREATE TABLE numbers (val DOUBLE)");
            execute("INSERT INTO numbers VALUES (2.0), (3.0), (4.0)");

            assertSql(
                    "val\tsquared\n" +
                            "2.0\t4.0\n" +
                            "3.0\t9.0\n" +
                            "4.0\t16.0\n",
                    "SELECT val, \"" + PLUGIN_NAME + "\".example_square(val) as squared FROM numbers"
            );
        });
    }

    @Test
    public void testPluginStringFunction() throws Exception {
        // Test the string example_reverse function
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test basic reversal
            assertSql(
                    "example_reverse\n" +
                            "olleh\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_reverse('hello')"
            );

            // Test with longer string
            assertSql(
                    "example_reverse\n" +
                            "BDtseuQ\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_reverse('QuestDB')"
            );

            // Test with single character
            assertSql(
                    "example_reverse\n" +
                            "a\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_reverse('a')"
            );
        });
    }

    @Test
    public void testPluginStringFunctionWithTable() throws Exception {
        // Test string function with table data
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            execute("CREATE TABLE words (word STRING)");
            execute("INSERT INTO words VALUES ('abc'), ('hello'), ('QuestDB')");

            assertSql(
                    "word\treversed\n" +
                            "abc\tcba\n" +
                            "hello\tolleh\n" +
                            "QuestDB\tBDtseuQ\n",
                    "SELECT word, \"" + PLUGIN_NAME + "\".example_reverse(word) as reversed FROM words"
            );
        });
    }

    @Test
    public void testPluginFunctionsInFunctionsList() throws Exception {
        // Test that plugin functions appear in the functions() output
        // This includes both direct FunctionFactory implementations AND
        // functions discovered via getFunctions() method (simplified UDF API)
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Query functions() and filter for plugin functions
            // Should include both traditional and simplified UDF functions
            assertSql(
                    "name\n" +
                            PLUGIN_NAME + ".example_reverse\n" +
                            PLUGIN_NAME + ".example_square\n" +
                            PLUGIN_NAME + ".example_weighted_avg\n" +
                            PLUGIN_NAME + ".simple_abs\n" +
                            PLUGIN_NAME + ".simple_add_days\n" +
                            PLUGIN_NAME + ".simple_avg\n" +
                            PLUGIN_NAME + ".simple_ceil\n" +
                            PLUGIN_NAME + ".simple_coalesce\n" +
                            PLUGIN_NAME + ".simple_concat\n" +
                            PLUGIN_NAME + ".simple_concat_all\n" +
                            PLUGIN_NAME + ".simple_count_notnull\n" +
                            PLUGIN_NAME + ".simple_cube\n" +
                            PLUGIN_NAME + ".simple_date_add_months\n" +
                            PLUGIN_NAME + ".simple_date_day\n" +
                            PLUGIN_NAME + ".simple_date_month\n" +
                            PLUGIN_NAME + ".simple_date_year\n" +
                            PLUGIN_NAME + ".simple_day\n" +
                            PLUGIN_NAME + ".simple_floor\n" +
                            PLUGIN_NAME + ".simple_hour\n" +
                            PLUGIN_NAME + ".simple_len\n" +
                            PLUGIN_NAME + ".simple_ln\n" +
                            PLUGIN_NAME + ".simple_lower\n" +
                            PLUGIN_NAME + ".simple_lpad\n" +
                            PLUGIN_NAME + ".simple_max\n" +
                            PLUGIN_NAME + ".simple_max_of\n" +
                            PLUGIN_NAME + ".simple_min\n" +
                            PLUGIN_NAME + ".simple_min_of\n" +
                            PLUGIN_NAME + ".simple_mod\n" +
                            PLUGIN_NAME + ".simple_not\n" +
                            PLUGIN_NAME + ".simple_power\n" +
                            PLUGIN_NAME + ".simple_require_nonnull\n" +
                            PLUGIN_NAME + ".simple_reverse\n" +
                            PLUGIN_NAME + ".simple_sqrt\n" +
                            PLUGIN_NAME + ".simple_square\n" +
                            PLUGIN_NAME + ".simple_sum\n" +
                            PLUGIN_NAME + ".simple_throw_on_negative\n" +
                            PLUGIN_NAME + ".simple_trim\n" +
                            PLUGIN_NAME + ".simple_upper\n" +
                            PLUGIN_NAME + ".simple_year\n",
                    "SELECT name FROM functions() WHERE name LIKE '" + PLUGIN_NAME + "%' ORDER BY name"
            );
        });
    }

    @Test
    public void testSimplifiedUDFScalarFunction() throws Exception {
        // Test a simplified UDF scalar function (discovered via getFunctions())
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test simple_square (from SimpleUDFExamples)
            assertSql(
                    "simple_square\n" +
                            "25.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_square(5.0)"
            );

            // Test simple_cube
            assertSql(
                    "simple_cube\n" +
                            "27.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_cube(3.0)"
            );

            // Test simple_sqrt
            assertSql(
                    "simple_sqrt\n" +
                            "4.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_sqrt(16.0)"
            );
        });
    }

    @Test
    public void testSimplifiedUDFStringFunction() throws Exception {
        // Test simplified UDF string functions
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test simple_upper
            assertSql(
                    "simple_upper\n" +
                            "HELLO\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_upper('hello')"
            );

            // Test simple_lower
            assertSql(
                    "simple_lower\n" +
                            "world\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_lower('WORLD')"
            );

            // Test simple_reverse
            assertSql(
                    "simple_reverse\n" +
                            "cba\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_reverse('abc')"
            );

            // Test simple_len
            assertSql(
                    "simple_len\n" +
                            "5\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_len('hello')"
            );
        });
    }

    @Test
    public void testSimplifiedUDFBinaryFunction() throws Exception {
        // Test simplified UDF binary (two-argument) functions
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test simple_power
            assertSql(
                    "simple_power\n" +
                            "8.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_power(2.0, 3.0)"
            );

            // Test simple_concat
            assertSql(
                    "simple_concat\n" +
                            "helloworld\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_concat('hello', 'world')"
            );

            // Test simple_mod
            assertSql(
                    "simple_mod\n" +
                            "1\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_mod(10, 3)"
            );
        });
    }

    @Test
    public void testSimplifiedUDFAggregateFunction() throws Exception {
        // Test simplified UDF aggregate functions
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            execute("CREATE TABLE test_data (value DOUBLE)");
            execute("INSERT INTO test_data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)");

            // Test simple_sum
            assertSql(
                    "simple_sum\n" +
                            "15.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_sum(value) FROM test_data"
            );

            // Test simple_avg
            assertSql(
                    "simple_avg\n" +
                            "3.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_avg(value) FROM test_data"
            );

            // Test simple_min
            assertSql(
                    "simple_min\n" +
                            "1.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_min(value) FROM test_data"
            );

            // Test simple_max
            assertSql(
                    "simple_max\n" +
                            "5.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_max(value) FROM test_data"
            );
        });
    }

    @Test
    public void testSimplifiedUDFTimestampFunction() throws Exception {
        // Test simplified UDF timestamp functions
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test simple_hour - timestamp '2023-06-15T14:30:00.000000Z' should return hour 14
            assertSql(
                    "simple_hour\n" +
                            "14\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_hour('2023-06-15T14:30:00.000000Z'::timestamp)"
            );

            // Test simple_day - should return day 15
            assertSql(
                    "simple_day\n" +
                            "15\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_day('2023-06-15T14:30:00.000000Z'::timestamp)"
            );

            // Test simple_year - should return year 2023
            assertSql(
                    "simple_year\n" +
                            "2023\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_year('2023-06-15T14:30:00.000000Z'::timestamp)"
            );

            // Test simple_add_days - add 10 days to a timestamp
            // 2023-06-15 + 10 days = 2023-06-25
            assertSql(
                    "simple_day\n" +
                            "25\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_day(\"" + PLUGIN_NAME + "\".simple_add_days('2023-06-15T00:00:00.000000Z'::timestamp, 10))"
            );
        });
    }

    @Test
    public void testSimplifiedUDFTimestampFunctionWithTable() throws Exception {
        // Test timestamp functions with table data
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            execute("CREATE TABLE events (ts TIMESTAMP, name STRING)");
            execute("INSERT INTO events VALUES ('2023-01-01T08:00:00.000000Z'::timestamp, 'morning')");
            execute("INSERT INTO events VALUES ('2023-06-15T14:30:00.000000Z'::timestamp, 'afternoon')");
            execute("INSERT INTO events VALUES ('2023-12-31T23:59:00.000000Z'::timestamp, 'night')");

            // Test extracting year, day, and hour from timestamp column
            assertSql(
                    "name\tyear\tday\thour\n" +
                            "morning\t2023\t1\t8\n" +
                            "afternoon\t2023\t15\t14\n" +
                            "night\t2023\t31\t23\n",
                    "SELECT name, " +
                            "\"" + PLUGIN_NAME + "\".simple_year(ts) as year, " +
                            "\"" + PLUGIN_NAME + "\".simple_day(ts) as day, " +
                            "\"" + PLUGIN_NAME + "\".simple_hour(ts) as hour " +
                            "FROM events ORDER BY ts"
            );
        });
    }

    @Test
    public void testSimplifiedUDFDateFunction() throws Exception {
        // Test simplified UDF date functions
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test simple_date_year - date '2023-06-15' should return year 2023
            assertSql(
                    "simple_date_year\n" +
                            "2023\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_date_year(to_date('2023-06-15', 'yyyy-MM-dd'))"
            );

            // Test simple_date_month - should return month 6
            assertSql(
                    "simple_date_month\n" +
                            "6\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_date_month(to_date('2023-06-15', 'yyyy-MM-dd'))"
            );

            // Test simple_date_day - should return day 15
            assertSql(
                    "simple_date_day\n" +
                            "15\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_date_day(to_date('2023-06-15', 'yyyy-MM-dd'))"
            );

            // Test simple_date_add_months - add 3 months to a date
            // 2023-06-15 + 3 months = 2023-09-15
            assertSql(
                    "simple_date_month\n" +
                            "9\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_date_month(" +
                            "\"" + PLUGIN_NAME + "\".simple_date_add_months(to_date('2023-06-15', 'yyyy-MM-dd'), 3))"
            );
        });
    }

    @Test
    public void testSimplifiedUDFVarargsFunction() throws Exception {
        // Test simplified UDF variadic (N-ary) functions
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test simple_max_of - maximum of multiple values
            assertSql(
                    "simple_max_of\n" +
                            "7.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_max_of(1.0, 5.0, 3.0, 7.0, 2.0)"
            );

            // Test simple_min_of - minimum of multiple values
            assertSql(
                    "simple_min_of\n" +
                            "1.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_min_of(1.0, 5.0, 3.0, 7.0, 2.0)"
            );

            // Test simple_concat_all - concatenate multiple strings
            assertSql(
                    "simple_concat_all\n" +
                            "HelloWorld!\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_concat_all('Hello', 'World', '!')"
            );

            // Test simple_coalesce - return first non-null
            assertSql(
                    "simple_coalesce\n" +
                            "3.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_coalesce(null, null, 3.0, 5.0)"
            );

            // Test with two arguments (edge case)
            assertSql(
                    "simple_max_of\n" +
                            "10.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_max_of(5.0, 10.0)"
            );
        });
    }

    @Test
    public void testSimplifiedUDFErrorHandling() throws Exception {
        // Test that UDF exceptions are properly caught and reported
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Test that positive values work normally
            assertSql(
                    "simple_throw_on_negative\n" +
                            "5.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".simple_throw_on_negative(5.0)"
            );

            // Test that negative values throw a meaningful error
            try {
                assertSql(
                        "simple_throw_on_negative\n" +
                                "-1.0\n",
                        "SELECT \"" + PLUGIN_NAME + "\".simple_throw_on_negative(-1.0)"
                );
                org.junit.Assert.fail("Expected exception for negative value");
            } catch (io.questdb.cairo.CairoException e) {
                // Verify the error message contains useful information
                String message = e.getMessage();
                org.junit.Assert.assertTrue("Error should mention UDF name",
                        message.contains("simple_throw_on_negative"));
                org.junit.Assert.assertTrue("Error should mention the exception",
                        message.contains("Negative value not allowed"));
            }
        });
    }

    private static boolean containsIgnoreCase(io.questdb.std.ObjList<CharSequence> list, String value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (io.questdb.std.Chars.equalsIgnoreCase(list.getQuick(i), value)) {
                return true;
            }
        }
        return false;
    }

    private void setupPluginDirectory() {
        try {
            // Get the plugins directory path from configuration
            final String pluginRoot = configuration.getPluginRoot().toString();
            final Path pluginDir = Paths.get(pluginRoot);

            // Create plugins directory if it doesn't exist
            Files.createDirectories(pluginDir);

            // Copy plugin jar from test resources to plugin directory
            try (InputStream is = getClass().getResourceAsStream("/plugins/" + PLUGIN_JAR)) {
                if (is != null) {
                    final Path targetJar = pluginDir.resolve(PLUGIN_JAR);
                    Files.copy(is, targetJar, StandardCopyOption.REPLACE_EXISTING);

                    // Re-scan plugins to discover the newly added plugin
                    engine.getPluginManager().scanPlugins();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup plugin directory", e);
        }
    }
}
