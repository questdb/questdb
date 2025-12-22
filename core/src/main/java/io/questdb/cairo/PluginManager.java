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

package io.questdb.cairo;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Manages plugin lifecycle - discovery, loading, and unloading of plugin JARs.
 * Each plugin JAR is loaded with an isolated URLClassLoader to prevent classpath conflicts.
 * Plugin functions are stored in a separate namespace in FunctionFactoryCache and accessed
 * using qualified names (plugin_name.function_name).
 */
public class PluginManager implements Closeable {
    private static final Log LOG = LogFactory.getLog(PluginManager.class);

    private final CairoConfiguration configuration;
    private final FunctionFactoryCache functionFactoryCache;
    private final FilesFacade ff;

    // Maps plugin name -> path to plugin JAR file (stored as String for URL conversion)
    private final LowerCaseCharSequenceObjHashMap<String> availablePlugins = new LowerCaseCharSequenceObjHashMap<>();

    // Maps plugin name -> list of FunctionFactory instances
    private final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactory>> pluginFactories = new LowerCaseCharSequenceObjHashMap<>();

    // Maps plugin name -> URLClassLoader (for cleanup on unload)
    private final LowerCaseCharSequenceObjHashMap<URLClassLoader> pluginClassLoaders = new LowerCaseCharSequenceObjHashMap<>();

    // Reusable path for file operations
    private final Path path = new Path();
    // Reusable for reading native file names
    private final DirectUtf8StringZ fileName = new DirectUtf8StringZ();
    // Reusable sink for normalized plugin names
    private final StringSink pluginNameSink = new StringSink();

    public PluginManager(
            @NotNull CairoConfiguration configuration,
            @NotNull FunctionFactoryCache functionFactoryCache
    ) {
        this.configuration = configuration;
        this.functionFactoryCache = functionFactoryCache;
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Scans the plugin directory and indexes available plugins.
     * This does NOT load the plugins - they are only indexed for later loading.
     * Plugins are loaded on-demand when LOAD PLUGIN command is executed.
     *
     * @throws CairoException if plugin directory exists but is not readable
     */
    public void scanPlugins() throws CairoException {
        final CharSequence pluginRootPath = configuration.getPluginRoot();
        if (pluginRootPath == null || pluginRootPath.length() == 0) {
            LOG.info().$("Plugin root not configured").$();
            return;
        }

        path.of(pluginRootPath);

        // If directory doesn't exist, that's ok - just log and return
        if (!ff.exists(path.$())) {
            LOG.info().$("Plugin directory does not exist: ").$(path).$();
            return;
        }

        // Scan for .jar files using FilesFacade
        final int pathLen = path.size();
        final long pFind = ff.findFirst(path.$());
        if (pFind == 0) {
            return;
        }

        try {
            do {
                final long namePtr = ff.findName(pFind);
                final int type = ff.findType(pFind);

                // Skip directories and non-files
                if (type != Files.DT_FILE) {
                    continue;
                }

                // Get the file name using DirectUtf8StringZ
                fileName.of(namePtr);

                // Check if it's a .jar file
                if (!Utf8s.endsWithAscii(fileName, ".jar")) {
                    continue;
                }

                // Extract plugin name (strip .jar suffix)
                final String pluginName = Utf8s.toString(fileName, 0, fileName.size() - 4, (byte) 0);

                // Check for duplicates
                if (availablePlugins.keyIndex(pluginName) < 0) {
                    LOG.error().$("Duplicate plugin name: ").$(pluginName).$();
                    continue;
                }

                // Store full path as String for later URL conversion
                path.trimTo(pathLen).concat(fileName).$();
                final String jarPathStr = path.toString();
                availablePlugins.put(pluginName, jarPathStr);
                LOG.info().$("Discovered plugin: ").$(pluginName).$(", JAR: ").$(jarPathStr).$();
            } while (ff.findNext(pFind) > 0);
        } finally {
            ff.findClose(pFind);
        }
    }

    /**
     * Loads a plugin and registers its functions in the FunctionFactoryCache.
     * The plugin name is normalized (strips .jar suffix if present).
     * If the plugin is already loaded, this is a no-op (idempotent).
     *
     * @param name the plugin name (with or without .jar suffix)
     * @throws SqlException if plugin is not found or fails to load
     */
    public synchronized void loadPlugin(@NotNull final CharSequence name) throws SqlException {
        try {
            // Normalize plugin name (strip .jar suffix if present) into reusable sink
            normalizePluginName(name, pluginNameSink);

            // Check if already loaded
            if (functionFactoryCache.isPluginLoaded(pluginNameSink)) {
                LOG.info().$("Plugin already loaded: ").$(pluginNameSink).$();
                return;
            }

            // Check if plugin exists
            final int index = availablePlugins.keyIndex(pluginNameSink);
            if (index >= 0) {
                throw SqlException.position(0).put("Plugin not found: ").put(pluginNameSink);
            }
            final String jarPathStr = availablePlugins.valueAtQuick(index);

            // Create persistent string for storage (only when we know plugin exists)
            final String pluginName = Chars.toString(pluginNameSink);

            // Create isolated ClassLoader for this plugin
            final java.io.File jarFile = new java.io.File(jarPathStr);
            final URL pluginJarUrl = jarFile.toURI().toURL();
            final URLClassLoader classLoader = new URLClassLoader(
                    new URL[]{pluginJarUrl},
                    Thread.currentThread().getContextClassLoader()
            );

            try {
                // Discover FunctionFactory implementations in the JAR
                final ObjList<FunctionFactory> factories = discoverFunctionFactories(jarPathStr, classLoader);

                LOG.info().$("Loading plugin: ").$(pluginName)
                        .$(", found ").$(factories.size()).$(" functions").$();

                // Register functions in cache (namespaced by plugin name)
                if (factories.size() > 0) {
                    functionFactoryCache.addPluginFunctions(pluginName, factories);
                    pluginFactories.put(pluginName, factories);
                } else {
                    LOG.info().$("Plugin ").$(pluginName).$(" has no FunctionFactory implementations").$();
                }

                // Store classloader for cleanup on unload
                pluginClassLoaders.put(pluginName, classLoader);

                LOG.info().$("Successfully loaded plugin: ").$(pluginName).$();
            } catch (final Exception e) {
                // Close classloader if loading failed
                try {
                    classLoader.close();
                } catch (final IOException closeEx) {
                    LOG.error().$("Failed to close classloader after failed plugin load: ").$(closeEx).$();
                }
                throw e;
            }
        } catch (final SqlException e) {
            throw e;
        } catch (final Exception e) {
            throw SqlException.position(0).put("Failed to load plugin: ").put(name).put(": ").put(e.getMessage());
        }
    }

    /**
     * Unloads a plugin and removes its functions from the FunctionFactoryCache.
     * Closes the plugin's isolated ClassLoader to prevent memory leaks.
     *
     * @param name the plugin name
     * @throws SqlException if plugin is not loaded
     */
    public synchronized void unloadPlugin(@NotNull final CharSequence name) throws SqlException {
        try {
            // Normalize plugin name into reusable sink
            normalizePluginName(name, pluginNameSink);

            if (!functionFactoryCache.isPluginLoaded(pluginNameSink)) {
                throw SqlException.position(0).put("Plugin not loaded: ").put(pluginNameSink);
            }

            // Remove functions from cache
            functionFactoryCache.removePluginFunctions(pluginNameSink);

            // Clean up classloader (CRITICAL for preventing memory leaks)
            final int clIndex = pluginClassLoaders.keyIndex(pluginNameSink);
            if (clIndex < 0) {
                final URLClassLoader classLoader = pluginClassLoaders.valueAtQuick(clIndex);
                pluginClassLoaders.removeAt(clIndex);
                if (classLoader != null) {
                    try {
                        classLoader.close();
                    } catch (final IOException e) {
                        LOG.error().$("Failed to close classloader for plugin ").$(pluginNameSink).$(": ").$(e).$();
                    }
                }
            }

            // Clean up factories
            final int factoriesIndex = pluginFactories.keyIndex(pluginNameSink);
            if (factoriesIndex < 0) {
                pluginFactories.removeAt(factoriesIndex);
            }

            LOG.info().$("Successfully unloaded plugin: ").$(pluginNameSink).$();
        } catch (final SqlException e) {
            throw e;
        } catch (final Exception e) {
            throw SqlException.position(0).put("Failed to unload plugin: ").put(name).put(": ").put(e.getMessage());
        }
    }

    /**
     * Returns a list of discovered (but not necessarily loaded) plugins.
     *
     * @return list of plugin names
     */
    @NotNull
    public ObjList<CharSequence> getAvailablePlugins() {
        final ObjList<CharSequence> result = new ObjList<>(availablePlugins.size());
        for (int i = 0, n = availablePlugins.size(); i < n; i++) {
            result.add(availablePlugins.keys().getQuick(i));
        }
        return result;
    }

    /**
     * Returns a list of currently loaded plugins.
     *
     * @return list of loaded plugin names
     */
    @NotNull
    public ObjList<CharSequence> getLoadedPlugins() {
        final ObjList<CharSequence> result = new ObjList<>(pluginClassLoaders.size());
        for (int i = 0, n = pluginClassLoaders.size(); i < n; i++) {
            result.add(pluginClassLoaders.keys().getQuick(i));
        }
        return result;
    }

    /**
     * Closes the plugin manager and unloads all loaded plugins.
     * This is called during CairoEngine shutdown.
     */
    @Override
    public void close() {
        // Unload all plugins
        final ObjList<CharSequence> loadedPlugins = getLoadedPlugins();
        for (int i = 0, n = loadedPlugins.size(); i < n; i++) {
            try {
                unloadPlugin(loadedPlugins.getQuick(i));
            } catch (final Exception e) {
                LOG.error().$("Failed to unload plugin ").$(loadedPlugins.getQuick(i)).$(": ").$(e.getMessage()).$();
            }
        }
        path.close();
    }

    /**
     * Discovers FunctionFactory implementations in a plugin JAR.
     * Returns an empty list if the JAR contains no FunctionFactory implementations.
     *
     * @param jarPath path to the JAR file as String
     * @param classLoader isolated classloader for loading classes from the JAR
     * @return list of discovered FunctionFactory instances
     */
    private ObjList<FunctionFactory> discoverFunctionFactories(
            @NotNull final String jarPath,
            @NotNull final URLClassLoader classLoader
    ) throws IOException {
        final ObjList<FunctionFactory> factories = new ObjList<>();

        try (final JarFile jarFile = new JarFile(jarPath)) {
            final Enumeration<JarEntry> entries = jarFile.entries();

            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();

                if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
                    final String className = entry.getName()
                            .replace('/', '.')
                            .replace(".class", "");

                    try {
                        final Class<?> clazz = classLoader.loadClass(className);

                        // Check if this class implements FunctionFactory
                        if (FunctionFactory.class.isAssignableFrom(clazz) &&
                                !clazz.isInterface()) {

                            // Instantiate the factory
                            final FunctionFactory factory = (FunctionFactory) clazz.getDeclaredConstructor()
                                    .newInstance();
                            factories.add(factory);

                            LOG.debug().$("Discovered function factory: ").$(className).$();
                        }
                    } catch (final ClassNotFoundException e) {
                        // Ignore classes not in the JAR
                    } catch (final Exception e) {
                        LOG.debug().$("Failed to instantiate function factory ").$(className)
                                .$(": ").$(e.getMessage()).$();
                    }
                }
            }
        }

        return factories;
    }

    /**
     * Normalizes a plugin name by removing the .jar suffix if present.
     * Writes the normalized name to the provided sink.
     *
     * @param name the plugin name
     * @param sink the sink to write the normalized name to
     */
    private static void normalizePluginName(@NotNull final CharSequence name, @NotNull final StringSink sink) {
        sink.clear();
        if (Chars.endsWith(name, ".jar")) {
            sink.put(name, 0, name.length() - 4);
        } else {
            sink.put(name);
        }
    }
}
