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
import io.questdb.cairo.PluginInfo;
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
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");

            // Query functions() and filter for plugin functions
            assertSql(
                    "name\n" +
                            PLUGIN_NAME + ".example_reverse\n" +
                            PLUGIN_NAME + ".example_square\n" +
                            PLUGIN_NAME + ".example_weighted_avg\n",
                    "SELECT name FROM functions() WHERE name LIKE '" + PLUGIN_NAME + "%' ORDER BY name"
            );
        });
    }

    @Test
    public void testPluginUnloadAndReload() throws Exception {
        // Test that plugins can be unloaded and reloaded
        assertMemoryLeak(() -> {
            // Load
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            assertSql("example_square\n9.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_square(3.0)");

            // Unload
            execute("UNLOAD PLUGIN '" + PLUGIN_NAME + "'");
            Assert.assertFalse(engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME));

            // Reload
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            Assert.assertTrue(engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME));

            // Verify function works again
            assertSql("example_square\n16.0\n",
                    "SELECT \"" + PLUGIN_NAME + "\".example_square(4.0)");
        });
    }

    @Test
    public void testPluginInfoVersionComparison() {
        // Test PluginInfo.compareVersions()
        Assert.assertEquals(0, PluginInfo.compareVersions("1.0.0", "1.0.0"));
        Assert.assertTrue(PluginInfo.compareVersions("1.0.1", "1.0.0") > 0);
        Assert.assertTrue(PluginInfo.compareVersions("1.0.0", "1.0.1") < 0);
        Assert.assertTrue(PluginInfo.compareVersions("2.0.0", "1.9.9") > 0);
        Assert.assertTrue(PluginInfo.compareVersions("1.10.0", "1.9.0") > 0);
        Assert.assertEquals(0, PluginInfo.compareVersions("1.0", "1.0.0"));
        Assert.assertEquals(0, PluginInfo.compareVersions("1.0.0-SNAPSHOT", "1.0.0")); // SNAPSHOT suffix is stripped
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
