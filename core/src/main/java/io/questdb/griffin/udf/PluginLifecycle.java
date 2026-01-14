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
 ******************************************************************************/

package io.questdb.griffin.udf;

import io.questdb.cairo.CairoConfiguration;

/**
 * Interface for plugin lifecycle management.
 * <p>
 * Classes annotated with {@link PluginFunctions} can optionally implement this interface
 * to receive callbacks when the plugin is loaded or unloaded.
 * <p>
 * Example usage:
 * <pre>{@code
 * @PluginFunctions(description = "My custom functions")
 * public class MyPluginFunctions implements PluginLifecycle {
 *     private static MyService service;
 *
 *     @Override
 *     public void onLoad(CairoConfiguration configuration) {
 *         // Initialize resources when plugin is loaded
 *         service = new MyService(configuration);
 *     }
 *
 *     @Override
 *     public void onUnload() {
 *         // Clean up resources when plugin is unloaded
 *         if (service != null) {
 *             service.close();
 *             service = null;
 *         }
 *     }
 *
 *     public static ObjList<FunctionFactory> getFunctions() {
 *         return UDFRegistry.functions(
 *             UDFRegistry.scalar("my_func", Double.class, Double.class, x -> service.process(x))
 *         );
 *     }
 * }
 * }</pre>
 */
public interface PluginLifecycle {

    /**
     * Called when the plugin is loaded.
     * <p>
     * This method is called after the plugin JAR is loaded but before any functions
     * are registered. Use this to initialize any resources needed by the plugin.
     *
     * @param configuration the QuestDB configuration, can be used to access
     *                      configuration settings and paths
     */
    default void onLoad(CairoConfiguration configuration) {
        // Default implementation does nothing
    }

    /**
     * Called when the plugin is being unloaded.
     * <p>
     * Use this to clean up any resources allocated in {@link #onLoad(CairoConfiguration)}.
     * This method should not throw exceptions.
     */
    default void onUnload() {
        // Default implementation does nothing
    }
}
