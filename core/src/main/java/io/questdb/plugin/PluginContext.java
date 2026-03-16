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
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
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
     * Validates the given username and password against QuestDB's user store
     * (admin credentials and password store) and returns a {@link SecurityContext}
     * for the authenticated user.
     * <p>
     * This mirrors how the ILP HTTP endpoint authenticates Basic credentials.
     * Use this when the plugin receives user-supplied credentials (e.g., HTTP Basic auth).
     * <p>
     * Example:
     * <pre>
     *   SecurityContext userCtx = context.authenticate("alice", "secret", SecurityContextFactory.HTTP);
     *   context.executeSql("INSERT INTO my_table VALUES (...)", userCtx);
     * </pre>
     *
     * @param username    the user name
     * @param password    the password
     * @param interfaceId one of {@link SecurityContextFactory#HTTP},
     *                    {@link SecurityContextFactory#PGWIRE}, {@link SecurityContextFactory#ILP}
     * @return a security context enforcing that user's permissions
     * @throws io.questdb.cairo.CairoException if the credentials are invalid
     */
    SecurityContext authenticate(CharSequence username, CharSequence password, byte interfaceId);

    /**
     * Creates a {@link SecurityContext} for the given principal, using
     * {@link SecurityContext#AUTH_TYPE_CREDENTIALS} and no group memberships.
     * <p>
     * This is a convenience wrapper around {@link #getSecurityContextFactory()}
     * that hides the internal {@link io.questdb.cairo.security.PrincipalContext} type.
     * <p>
     * Use this when the plugin handles its own authentication (e.g., OIDC, API keys)
     * and only needs to map a known principal to a {@link SecurityContext}.
     * For password-based authentication, use {@link #authenticate(CharSequence, CharSequence, byte)} instead.
     * <p>
     * Example:
     * <pre>
     *   SecurityContext userCtx = context.createSecurityContext("alice", SecurityContextFactory.HTTP);
     *   context.executeSql("INSERT INTO my_table VALUES (...)", userCtx);
     * </pre>
     *
     * @param principal   the authenticated user name
     * @param interfaceId one of {@link SecurityContextFactory#HTTP},
     *                    {@link SecurityContextFactory#PGWIRE}, {@link SecurityContextFactory#ILP}
     * @return a security context enforcing that user's permissions
     */
    SecurityContext createSecurityContext(CharSequence principal, byte interfaceId);

    /**
     * Executes a SQL statement (DDL or DML). Runs with full privileges.
     * <p>
     * Intended for plugin bootstrap (creating tables, inserting seed data) during
     * lifecycle callbacks. For operations on behalf of an authenticated user, use
     * {@link #executeSql(CharSequence, SecurityContext)} instead.
     * <p>
     * Example:
     * <pre>
     *   context.executeSql("CREATE TABLE IF NOT EXISTS my_plugin_log (ts TIMESTAMP, msg STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
     * </pre>
     *
     * @param sql the SQL statement to execute
     * @throws SqlException if the statement is invalid or execution fails
     */
    void executeSql(CharSequence sql) throws SqlException;

    /**
     * Executes a SQL statement with the given security context, enforcing that
     * user's permissions. Use this when executing SQL on behalf of an authenticated
     * user (e.g., from a plugin HTTP endpoint).
     * <p>
     * Obtain a {@link SecurityContext} via {@link #getSecurityContextFactory()}.
     *
     * @param sql             the SQL statement to execute
     * @param securityContext the authenticated user's security context
     * @throws SqlException if the statement is invalid, execution fails, or the
     *                      user lacks the required permissions
     */
    void executeSql(CharSequence sql, SecurityContext securityContext) throws SqlException;

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
     * Returns the server's {@link SecurityContextFactory} for creating user-specific
     * security contexts. Plugins that accept external requests (e.g., HTTP endpoints)
     * should use this to authenticate users and enforce permissions.
     * <p>
     * Example: creating a security context for a known user:
     * <pre>
     *   SecurityContextFactory factory = context.getSecurityContextFactory();
     *   SecurityContext userCtx = factory.getInstance(principalContext, SecurityContextFactory.HTTP);
     *   context.executeSql("INSERT INTO my_table VALUES (...)", userCtx);
     * </pre>
     *
     * @return the security context factory, never null
     * @see SecurityContextFactory
     * @see io.questdb.cairo.security.PrincipalContext
     */
    SecurityContextFactory getSecurityContextFactory();

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
