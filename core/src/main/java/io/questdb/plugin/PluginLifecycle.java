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

/**
 * Optional lifecycle interface for QuestDB plugins.
 * <p>
 * Plugin JARs may include a {@code META-INF/services/io.questdb.plugin.PluginLifecycle}
 * file listing a single implementation class. If present, the plugin manager calls
 * {@link #onLoad(PluginContext)} after registering the plugin's functions and
 * {@link #onUnload()} before removing them.
 * <p>
 * Implementations must return promptly from both methods. Long-running work should
 * be delegated to background threads spawned in {@link #onLoad(PluginContext)} and
 * joined in {@link #onUnload()}.
 * <p>
 * If {@link #onLoad(PluginContext)} throws, the plugin load is rolled back: functions
 * are removed and the classloader is closed. If {@link #onUnload()} throws, the error
 * is logged and the unload continues.
 */
public interface PluginLifecycle {

    /**
     * Called after the plugin's functions have been registered.
     * The plugin may use the provided context to access the {@link CairoEngine}
     * and other server resources.
     *
     * @param context plugin context providing access to server resources
     * @throws Exception if initialization fails; the plugin load will be rolled back
     */
    void onLoad(PluginContext context) throws Exception;

    /**
     * Called when the server is shutting down, before worker pools are halted.
     * <p>
     * This method is <b>not</b> called on plugin reload — only on server shutdown.
     * Use it to flush pending work, execute final SQL, or perform graceful
     * finalization that requires server resources to still be available.
     * <p>
     * After this method returns for all plugins, worker pools are halted
     * and {@link #onUnload()} is called.
     * <p>
     * The default implementation is a no-op, so existing plugins that do not
     * need shutdown-specific behavior do not need to implement this method.
     *
     * @param context plugin context; server resources are still available
     * @throws Exception if shutdown work fails; the error is logged and shutdown continues
     */
    default void onShutdown(PluginContext context) throws Exception {}

    /**
     * Called before the plugin's functions are removed and classloader is closed.
     * Must release all resources acquired in {@link #onLoad(PluginContext)}.
     *
     * @throws Exception if cleanup fails; the error is logged and unload continues
     */
    void onUnload() throws Exception;
}
