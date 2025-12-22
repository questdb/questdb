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

import io.questdb.test.AbstractCairoTest;
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

    private static boolean containsIgnoreCase(io.questdb.std.ObjList<CharSequence> list, String value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (io.questdb.std.Chars.equalsIgnoreCase(list.getQuick(i), value)) {
                return true;
            }
        }
        return false;
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
    public void testPluginLoadIdempotent() throws Exception {
        // Test that loading the same plugin twice is idempotent
        assertMemoryLeak(() -> {
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'");
            execute("LOAD PLUGIN '" + PLUGIN_NAME + "'"); // Should not throw
            assert engine.getFunctionFactoryCache().isPluginLoaded(PLUGIN_NAME);
        });
    }
}
