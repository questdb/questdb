/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.plugin;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;

/**
 * Context provided to {@link PluginLifecycle#onLoad(PluginContext)}.
 * Gives the plugin access to server resources and per-plugin configuration.
 * <p>
 * Plugin configuration is resolved from two sources (env vars take precedence):
 * <ol>
 *   <li>{@code <plugin_root>/<plugin-name>.conf} — Java properties file</li>
 *   <li>Environment variables prefixed with {@code QDB_PLUGIN_<NORMALIZED_NAME>_}
 *       where the plugin name has dots/hyphens replaced with underscores and is uppercased.
 *       For example, plugin {@code questdb-plugin-jsonl-1.0.0} uses prefix
 *       {@code QDB_PLUGIN_QUESTDB_PLUGIN_JSONL_1_0_0_}.</li>
 * </ol>
 */
public interface PluginContext {

    /**
     * Executes a SQL statement (DDL or DML). Runs with full privileges.
     * <p>
     * Plugins can use this to create tables, insert data, or run any SQL during
     * lifecycle callbacks. Example:
     * <pre>
     *   context.executeSql("CREATE TABLE IF NOT EXISTS my_plugin_log (ts TIMESTAMP, msg STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
     *   context.executeSql("INSERT INTO my_plugin_log VALUES (now(), 'plugin started')");
     * </pre>
     *
     * @param sql the SQL statement to execute
     * @throws SqlException if the statement is invalid or execution fails
     */
    void executeSql(CharSequence sql) throws SqlException;

    /**
     * Returns the CairoEngine instance for advanced operations.
     * <p>
     * For simple DDL/DML, prefer {@link #executeSql(CharSequence)}.
     * Use the engine directly for table readers/writers, configuration access, etc.
     */
    CairoEngine getEngine();

    /**
     * Returns the canonical name of this plugin (e.g., "questdb-plugin-example-1.0.0").
     */
    CharSequence getPluginName();

    /**
     * Returns a plugin configuration property, or {@code defaultValue} if not set.
     * <p>
     * Resolution order:
     * <ol>
     *   <li>Environment variable {@code QDB_PLUGIN_<NORMALIZED_NAME>_<KEY>}
     *       (key is uppercased, dots/hyphens become underscores)</li>
     *   <li>{@code <plugin_root>/<plugin-name>.conf} properties file</li>
     *   <li>{@code defaultValue}</li>
     * </ol>
     *
     * @param key          the property key (e.g., "port", "bind.address")
     * @param defaultValue returned if the key is not found in any source
     * @return the resolved value or defaultValue
     */
    String getProperty(String key, String defaultValue);
}
