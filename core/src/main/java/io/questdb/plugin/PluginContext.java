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
import io.questdb.cairo.CheckpointListener;
import io.questdb.cairo.DdlListener;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cutlass.http.HttpRequestHandlerFactory;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;

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
     * Returns a plugin-scoped metrics registry. Metrics created through this
     * registry are automatically prefixed with the plugin name and scraped
     * alongside QuestDB's built-in metrics at the {@code /metrics} endpoint.
     * <p>
     * All metrics become no-ops after the plugin is unloaded.
     * <p>
     * Example:
     * <pre>
     *   Counter requests = context.getMetricsRegistry().newCounter("requests");
     *   requests.inc();
     * </pre>
     *
     * @return the plugin-scoped metrics registry, never null
     */
    PluginMetricsRegistry getMetricsRegistry();

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
     * Registers a {@link Job} to run on the server's shared worker pool.
     * <p>
     * The job's {@link Job#run(int, Job.RunStatus)} method will be polled
     * alongside other engine jobs (mat view refresh, WAL apply, etc.) using
     * QuestDB's adaptive sleep/backoff worker loop.
     * <p>
     * <b>Threading:</b> the job will be called from multiple worker threads
     * concurrently. If the job should execute on only one worker at a time,
     * use a CAS-based latch (see {@code SynchronizedJob}) or an
     * {@code AtomicBoolean} guard inside the job.
     * <p>
     * <b>Lifecycle:</b> jobs are automatically removed when the plugin is
     * unloaded. The plugin's {@link PluginLifecycle#onUnload()} method
     * <em>must</em> cause registered jobs to return {@code false} promptly
     * (e.g., by setting a volatile shutdown flag) because a worker thread may
     * still be executing the job when unload proceeds.
     * <p>
     * Example — a plugin that runs periodic export work:
     * <pre>
     *   context.registerJob((workerId, runStatus) -&gt; {
     *       if (runStatus.isTerminating() || shuttingDown) return false;
     *       return doExportCycleIfReady();
     *   });
     * </pre>
     *
     * @param job the job to register; must not be null
     */
    void registerJob(Job job);

    /**
     * Registers a timer that fires a callback at a fixed interval.
     * <p>
     * The callback runs on the shared worker pool thread and must return
     * promptly. Long-running work should be enqueued to a registered
     * {@link Job} instead.
     * <p>
     * The first firing occurs approximately {@code intervalMicros} after
     * registration. If a callback is still executing when the next firing
     * is due, the next firing is deferred (no concurrent executions).
     * <p>
     * <b>Lifecycle:</b> automatically cancelled when the plugin is unloaded.
     * <p>
     * Example — flush pending data every 30 seconds:
     * <pre>
     *   handle = context.registerTimer(30_000_000L, this::flushPendingData);
     * </pre>
     *
     * @param intervalMicros interval between firings in microseconds; must be positive
     * @param callback       the task to run on each firing; must not be null
     * @return a handle that can be used to cancel the timer early
     */
    PluginTimerHandle registerTimer(long intervalMicros, Runnable callback);

    /**
     * Registers a calendar-aligned scheduled task using QuestDB's interval
     * syntax (e.g., {@code "1h"}, {@code "1d"}, {@code "1M"}).
     * <p>
     * Unlike {@link #registerTimer(long, Runnable)} which uses a fixed
     * microsecond interval, this method aligns firings to calendar boundaries
     * (e.g., midnight for daily, first of month for monthly) and handles
     * timezone transitions (DST) correctly.
     * <p>
     * The callback runs on the shared worker pool thread and must return
     * promptly.
     * <p>
     * <b>Supported intervals:</b> {@code s} (seconds), {@code m} (minutes),
     * {@code h} (hours), {@code d} (days), {@code M} (months), {@code y} (years).
     * Examples: {@code "5m"}, {@code "1d"}, {@code "1M"}, {@code "6h"}.
     * <p>
     * <b>Lifecycle:</b> automatically cancelled when the plugin is unloaded.
     * <p>
     * Example — run daily at 2:00 AM US/Eastern:
     * <pre>
     *   // startMicros = 2 hours past the Unix epoch, defining the alignment origin.
     *   // For a daily interval this means "fire at 2 AM". The sampler rounds to the
     *   // nearest interval boundary relative to this origin.
     *   long twoAmOffset = 2L * 60 * 60 * 1_000_000L;
     *   handle = context.registerCalendarTask("1d", "US/Eastern", twoAmOffset, this::runDailyExport);
     * </pre>
     *
     * @param interval    QuestDB interval string (e.g., {@code "5m"}, {@code "1d"})
     * @param timezone    IANA timezone (e.g., {@code "US/Eastern"}), or null/empty for UTC
     * @param startMicros microseconds since 1970-01-01 00:00:00 UTC that defines the
     *                    alignment origin. For example, {@code 2 * 3600 * 1_000_000L}
     *                    means "align to 2 hours past the epoch". For a daily interval
     *                    this produces firings at 2:00 AM.
     * @param callback    the task to run on each firing; must not be null
     * @return a handle that can be used to cancel the timer early
     * @throws SqlException if the interval string or timezone is invalid
     */
    PluginTimerHandle registerCalendarTask(CharSequence interval, CharSequence timezone, long startMicros, Runnable callback) throws SqlException;

    /**
     * Registers a {@link CheckpointListener} to receive callbacks when checkpoints
     * are created or restored.
     * <p>
     * <b>Threading:</b> {@link CheckpointListener#onCheckpointReleased} fires on the
     * thread executing the CHECKPOINT RELEASE SQL command.
     * {@link CheckpointListener#onCheckpointRestoreComplete()} fires on the recovery
     * thread during startup.
     * <p>
     * <b>Lifecycle:</b> automatically removed when the plugin is unloaded.
     *
     * @param listener the checkpoint listener; must not be null
     */
    void registerCheckpointListener(CheckpointListener listener);

    /**
     * Registers a {@link DdlListener} to receive callbacks when tables, views,
     * materialized views, or columns are created, dropped, or renamed.
     * <p>
     * <b>Threading:</b> callbacks fire on the SQL compiler thread executing the DDL.
     * Implementations must be thread-safe if multiple concurrent DDL statements are
     * possible.
     * <p>
     * <b>Lifecycle:</b> automatically removed when the plugin is unloaded.
     *
     * @param listener the DDL listener; must not be null
     */
    void registerDdlListener(DdlListener listener);

    /**
     * Registers an {@link HttpRequestHandlerFactory} that maps one or more URL paths
     * to an HTTP request handler on QuestDB's HTTP server.
     * <p>
     * If the HTTP server has not yet started when this method is called (e.g., during
     * {@link PluginLifecycle#onLoad(PluginContext)}), the registration is buffered and
     * applied once the server is ready.
     * <p>
     * <b>Threading:</b> the factory's {@link HttpRequestHandlerFactory#newInstance()} is
     * called once per HTTP worker thread. Each returned handler instance is private to
     * that worker.
     * <p>
     * <b>Lifecycle:</b> handlers are automatically unbound when the plugin is unloaded.
     * <p>
     * Example:
     * <pre>
     *   context.registerHttpEndpoint(new HttpRequestHandlerFactory() {
     *       public ObjHashSet&lt;String&gt; getUrls() {
     *           ObjHashSet&lt;String&gt; urls = new ObjHashSet&lt;&gt;();
     *           urls.add("/my-plugin/api");
     *           return urls;
     *       }
     *       public HttpRequestHandler newInstance() {
     *           return header -&gt; new MyProcessor();
     *       }
     *   });
     * </pre>
     *
     * @param factory the handler factory; must not be null
     */
    void registerHttpEndpoint(HttpRequestHandlerFactory factory);

    /**
     * Registers a {@link WalListener} to receive callbacks for WAL events:
     * data/non-data transactions committed, segments closed, tables created/dropped/renamed,
     * and WAL closed.
     * <p>
     * <b>Threading:</b> callbacks fire on WAL apply worker threads. Implementations
     * must be thread-safe and return quickly — blocking delays WAL application.
     * Offload heavy work to a registered {@link Job}.
     * <p>
     * <b>Lifecycle:</b> automatically removed when the plugin is unloaded.
     * <p>
     * Example:
     * <pre>
     *   context.registerWalListener(new DefaultWalListener() {
     *       public void dataTxnCommitted(TableToken tableToken, long txn,
     *               long timestamp, int walId, int segmentId, int segmentTxn) {
     *           pendingWork.add(tableToken);
     *       }
     *   });
     * </pre>
     *
     * @param listener the WAL listener; must not be null
     */
    void registerWalListener(WalListener listener);
}
