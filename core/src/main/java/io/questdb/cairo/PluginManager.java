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
import org.jetbrains.annotations.Nullable;

import io.questdb.griffin.udf.PluginFunctions;
import io.questdb.griffin.udf.PluginLifecycle;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSigner;
import java.security.cert.Certificate;
import java.util.Collection;
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
    // Maps plugin name -> path to plugin JAR file (stored as String for URL conversion)
    private final LowerCaseCharSequenceObjHashMap<String> availablePlugins = new LowerCaseCharSequenceObjHashMap<>();
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    // Reusable for reading native file names
    private final DirectUtf8StringZ fileName = new DirectUtf8StringZ();
    private final FunctionFactoryCache functionFactoryCache;
    // Reusable path for file operations
    private final Path path = new Path();
    // Maps plugin name -> URLClassLoader (for cleanup on unload)
    private final LowerCaseCharSequenceObjHashMap<URLClassLoader> pluginClassLoaders = new LowerCaseCharSequenceObjHashMap<>();
    // Maps plugin name -> list of FunctionFactory instances
    private final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactory>> pluginFactories = new LowerCaseCharSequenceObjHashMap<>();
    // Maps plugin name -> list of PluginLifecycle instances for cleanup
    private final LowerCaseCharSequenceObjHashMap<ObjList<PluginLifecycle>> pluginLifecycles = new LowerCaseCharSequenceObjHashMap<>();
    // Maps plugin name -> PluginInfo (version, author, description, etc.)
    private final LowerCaseCharSequenceObjHashMap<PluginInfo> pluginInfoMap = new LowerCaseCharSequenceObjHashMap<>();
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
     * Closes the plugin manager and unloads all loaded plugins.
     * This is called during CairoEngine shutdown.
     */
    @Override
    public synchronized void close() {
        // Get list of loaded plugins
        final ObjList<CharSequence> loadedPlugins = new ObjList<>(pluginClassLoaders.size());
        for (int i = 0, n = pluginClassLoaders.size(); i < n; i++) {
            loadedPlugins.add(pluginClassLoaders.keys().getQuick(i));
        }
        // Unload all plugins
        for (int i = 0, n = loadedPlugins.size(); i < n; i++) {
            try {
                unloadPluginInternal(loadedPlugins.getQuick(i));
            } catch (final Exception e) {
                LOG.error().$("Failed to unload plugin ").$(loadedPlugins.getQuick(i)).$(": ").$(e.getMessage()).$();
            }
        }
        path.close();
    }

    /**
     * Returns a list of discovered (but not necessarily loaded) plugins.
     *
     * @return list of plugin names
     */
    @NotNull
    public synchronized ObjList<CharSequence> getAvailablePlugins() {
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
    public synchronized ObjList<CharSequence> getLoadedPlugins() {
        final ObjList<CharSequence> result = new ObjList<>(pluginClassLoaders.size());
        for (int i = 0, n = pluginClassLoaders.size(); i < n; i++) {
            result.add(pluginClassLoaders.keys().getQuick(i));
        }
        return result;
    }

    /**
     * Returns metadata about a loaded plugin including version, author, and description.
     *
     * @param name the plugin name
     * @return PluginInfo if the plugin is loaded and has metadata, null otherwise
     */
    @Nullable
    public synchronized PluginInfo getPluginInfo(@NotNull CharSequence name) {
        normalizePluginName(name, pluginNameSink);
        final int index = pluginInfoMap.keyIndex(pluginNameSink);
        if (index < 0) {
            return pluginInfoMap.valueAtQuick(index);
        }
        return null;
    }

    /**
     * Returns metadata for all loaded plugins.
     *
     * @return list of PluginInfo for all loaded plugins
     */
    @NotNull
    public synchronized ObjList<PluginInfo> getAllPluginInfo() {
        final ObjList<PluginInfo> result = new ObjList<>(pluginInfoMap.size());
        for (int i = 0, n = pluginInfoMap.size(); i < n; i++) {
            result.add(pluginInfoMap.valueAtQuick(i));
        }
        return result;
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

            // Verify JAR signature if enabled
            if (configuration.isPluginSignatureVerificationEnabled()) {
                LOG.info().$("Verifying signature for plugin: ").$(pluginName).$();
                verifyJarSignature(jarPathStr, pluginName);
                LOG.info().$("Signature verified for plugin: ").$(pluginName).$();
            }

            // Create isolated ClassLoader for this plugin
            final java.io.File jarFile = new java.io.File(jarPathStr);
            final URL pluginJarUrl = jarFile.toURI().toURL();
            final URLClassLoader classLoader = new URLClassLoader(
                    new URL[]{pluginJarUrl},
                    Thread.currentThread().getContextClassLoader()
            );

            try {
                // Discover FunctionFactory implementations and lifecycle handlers in the JAR
                final ObjList<FunctionFactory> factories = new ObjList<>();
                final ObjList<PluginLifecycle> lifecycles = new ObjList<>();
                final PluginInfo pluginInfo = discoverPluginClasses(jarPathStr, pluginName, classLoader, factories, lifecycles);

                LOG.info().$("Loading plugin: ").$(pluginName)
                        .$(", found ").$(factories.size()).$(" functions").$();

                // Log version info if available
                if (pluginInfo.getVersion() != null) {
                    LOG.info().$("Plugin version: ").$(pluginInfo.getVersion()).$();
                }
                if (pluginInfo.getAuthor() != null) {
                    LOG.info().$("Plugin author: ").$(pluginInfo.getAuthor()).$();
                }

                // Call onLoad for all lifecycle handlers
                for (int i = 0, n = lifecycles.size(); i < n; i++) {
                    try {
                        lifecycles.getQuick(i).onLoad(configuration);
                    } catch (Exception e) {
                        LOG.error().$("Plugin ").$(pluginName).$(" onLoad failed: ").$(e.getMessage()).$();
                    }
                }

                // Register functions in cache (namespaced by plugin name)
                if (factories.size() > 0) {
                    functionFactoryCache.addPluginFunctions(pluginName, factories);
                    pluginFactories.put(pluginName, factories);
                } else {
                    LOG.info().$("Plugin ").$(pluginName).$(" has no FunctionFactory implementations").$();
                }

                // Store lifecycle handlers for cleanup on unload
                if (lifecycles.size() > 0) {
                    pluginLifecycles.put(pluginName, lifecycles);
                }

                // Store plugin info
                pluginInfoMap.put(pluginName, pluginInfo);

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
     * Scans the plugin directory and indexes available plugins.
     * This does NOT load the plugins - they are only indexed for later loading.
     * Plugins are loaded on-demand when LOAD PLUGIN command is executed.
     *
     * @throws CairoException if plugin directory exists but is not readable
     */
    public synchronized void scanPlugins() throws CairoException {
        final CharSequence pluginRootPath = configuration.getPluginRoot();
        //noinspection ConstantValue
        assert pluginRootPath != null && pluginRootPath.length() > 0;

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
     * Unloads a plugin and removes its functions from the FunctionFactoryCache.
     * Closes the plugin's isolated ClassLoader to prevent memory leaks.
     *
     * @param name the plugin name
     * @throws SqlException if plugin is not loaded
     */
    public synchronized void unloadPlugin(@NotNull final CharSequence name) throws SqlException {
        unloadPluginInternal(name);
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

    /**
     * Discovers FunctionFactory implementations and PluginLifecycle handlers in a plugin JAR.
     * Also collects plugin metadata from @PluginFunctions annotations.
     * <p>
     * This method discovers:
     * <ol>
     *   <li>Classes that directly implement {@link FunctionFactory}</li>
     *   <li>Classes with a static {@code getFunctions()} method that returns a collection
     *       of FunctionFactory instances (for use with the simplified UDF API)</li>
     *   <li>Classes that implement {@link PluginLifecycle} for lifecycle callbacks</li>
     * </ol>
     *
     * @param jarPath     path to the JAR file as String
     * @param pluginName  name of the plugin being loaded
     * @param classLoader isolated classloader for loading classes from the JAR
     * @param factories   list to add discovered FunctionFactory instances to
     * @param lifecycles  list to add discovered PluginLifecycle instances to
     * @return PluginInfo containing metadata about the plugin
     */
    private PluginInfo discoverPluginClasses(
            @NotNull final String jarPath,
            @NotNull final String pluginName,
            @NotNull final URLClassLoader classLoader,
            @NotNull final ObjList<FunctionFactory> factories,
            @NotNull final ObjList<PluginLifecycle> lifecycles
    ) throws IOException {
        int classCount = 0;
        int errorCount = 0;
        String lastErrorClass = null;
        String lastErrorMessage = null;

        // Holders for plugin metadata (collected from @PluginFunctions annotations)
        final String[] version = {null};
        final String[] description = {null};
        final String[] author = {null};
        final String[] url = {null};
        final String[] license = {null};

        try (final JarFile jarFile = new JarFile(jarPath)) {
            final Enumeration<JarEntry> entries = jarFile.entries();

            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();

                if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
                    classCount++;
                    final String className = entry.getName()
                            .replace('/', '.')
                            .replace(".class", "");

                    try {
                        final Class<?> clazz = classLoader.loadClass(className);

                        // Check if this class implements FunctionFactory directly
                        if (FunctionFactory.class.isAssignableFrom(clazz) &&
                                !clazz.isInterface()) {

                            // Instantiate the factory
                            final FunctionFactory factory = (FunctionFactory) clazz.getDeclaredConstructor()
                                    .newInstance();
                            factories.add(factory);

                            LOG.info().$("Discovered function factory: ").$(className)
                                    .$(" [signature=").$(factory.getSignature()).$("]").$();
                        }

                        // Check for static getFunctions() method (simplified UDF API)
                        // and track lifecycle if class implements PluginLifecycle
                        // Also collect metadata from @PluginFunctions annotation
                        discoverFunctionsFromMethod(clazz, factories, lifecycles, version, description, author, url, license);

                    } catch (final ClassNotFoundException e) {
                        // Expected for dependency classes not bundled in JAR - ignore silently
                    } catch (final NoClassDefFoundError e) {
                        // Missing dependency - log at debug level, might be optional
                        LOG.debug().$("Class ").$(className).$(" has missing dependency: ").$(e.getMessage()).$();
                    } catch (final Exception e) {
                        errorCount++;
                        lastErrorClass = className;
                        lastErrorMessage = e.getMessage();
                        LOG.debug().$("Failed to load class ").$(className)
                                .$(": ").$(e.getClass().getSimpleName())
                                .$(": ").$(e.getMessage()).$();
                    }
                }
            }
        }

        // Log summary if there were errors
        if (errorCount > 0) {
            LOG.info().$("Plugin discovery: scanned ").$(classCount).$(" classes, ")
                    .$(errorCount).$(" failed to load").$();
            if (errorCount == 1) {
                LOG.info().$("  Failed class: ").$(lastErrorClass).$(": ").$(lastErrorMessage).$();
            } else {
                LOG.info().$("  Last failed: ").$(lastErrorClass).$(": ").$(lastErrorMessage).$();
                LOG.info().$("  Enable DEBUG logging for full details").$();
            }
        }

        // Return collected plugin info
        return new PluginInfo(pluginName, version[0], description[0], author[0], url[0], license[0], factories.size());
    }

    /**
     * Discovers functions from a class's static getFunctions() method and
     * tracks PluginLifecycle implementations for lifecycle callbacks.
     * Also collects metadata from @PluginFunctions annotation.
     * <p>
     * This supports the simplified UDF API where plugin authors can define functions
     * using lambdas and the UDFRegistry helper class.
     *
     * @param clazz       the class to check for getFunctions() method
     * @param factories   the list to add discovered factories to
     * @param lifecycles  the list to add discovered lifecycle handlers to
     * @param version     holder for version string from annotation
     * @param description holder for description string from annotation
     * @param author      holder for author string from annotation
     * @param url         holder for url string from annotation
     * @param license     holder for license string from annotation
     */
    @SuppressWarnings("unchecked")
    private void discoverFunctionsFromMethod(
            Class<?> clazz,
            ObjList<FunctionFactory> factories,
            ObjList<PluginLifecycle> lifecycles,
            String[] version,
            String[] description,
            String[] author,
            String[] url,
            String[] license
    ) {
        try {
            // Look for static getFunctions() method
            final Method method = clazz.getMethod("getFunctions");

            // Must be static
            if (!Modifier.isStatic(method.getModifiers())) {
                return;
            }

            // Check return type is compatible (Collection or ObjList of FunctionFactory)
            final Class<?> returnType = method.getReturnType();
            if (!Collection.class.isAssignableFrom(returnType) &&
                    !ObjList.class.isAssignableFrom(returnType)) {
                return;
            }

            // Invoke the method
            final Object result = method.invoke(null);

            if (result instanceof ObjList) {
                final ObjList<FunctionFactory> list = (ObjList<FunctionFactory>) result;
                for (int i = 0, n = list.size(); i < n; i++) {
                    factories.add(list.getQuick(i));
                }
                LOG.debug().$("Discovered ").$(list.size()).$(" functions from ")
                        .$(clazz.getName()).$(".getFunctions()").$();
            } else if (result instanceof Collection) {
                final Collection<FunctionFactory> collection = (Collection<FunctionFactory>) result;
                for (FunctionFactory factory : collection) {
                    factories.add(factory);
                }
                LOG.debug().$("Discovered ").$(collection.size()).$(" functions from ")
                        .$(clazz.getName()).$(".getFunctions()").$();
            }

            // Collect metadata from @PluginFunctions annotation
            if (clazz.isAnnotationPresent(PluginFunctions.class)) {
                final PluginFunctions annotation = clazz.getAnnotation(PluginFunctions.class);
                // Only set if not already set (first annotation wins)
                if (version[0] == null && !annotation.version().isEmpty()) {
                    version[0] = annotation.version();
                }
                if (description[0] == null && !annotation.description().isEmpty()) {
                    description[0] = annotation.description();
                }
                if (author[0] == null && !annotation.author().isEmpty()) {
                    author[0] = annotation.author();
                }
                if (url[0] == null && !annotation.url().isEmpty()) {
                    url[0] = annotation.url();
                }
                if (license[0] == null && !annotation.license().isEmpty()) {
                    license[0] = annotation.license();
                }
            }

            // Check if class implements PluginLifecycle for lifecycle callbacks
            if (PluginLifecycle.class.isAssignableFrom(clazz) && !clazz.isInterface()) {
                try {
                    final PluginLifecycle lifecycle = (PluginLifecycle) clazz.getDeclaredConstructor().newInstance();
                    lifecycles.add(lifecycle);
                    LOG.debug().$("Discovered lifecycle handler: ").$(clazz.getName()).$();
                } catch (Exception e) {
                    LOG.debug().$("Failed to instantiate lifecycle handler ").$(clazz.getName())
                            .$(": ").$(e.getMessage()).$();
                }
            }

        } catch (NoSuchMethodException e) {
            // Class doesn't have getFunctions() method - that's fine
        } catch (Exception e) {
            LOG.debug().$("Failed to invoke getFunctions() on ").$(clazz.getName())
                    .$(": ").$(e.getMessage()).$();
        }
    }

    /**
     * Internal method to unload a plugin.
     * Caller must hold the monitor (synchronized).
     */
    private void unloadPluginInternal(@NotNull final CharSequence name) throws SqlException {
        try {
            // Normalize plugin name into reusable sink
            normalizePluginName(name, pluginNameSink);

            if (!functionFactoryCache.isPluginLoaded(pluginNameSink)) {
                throw SqlException.position(0).put("Plugin not loaded: ").put(pluginNameSink);
            }

            // Call onUnload for all lifecycle handlers
            final int lifecycleIndex = pluginLifecycles.keyIndex(pluginNameSink);
            if (lifecycleIndex < 0) {
                final ObjList<PluginLifecycle> lifecycles = pluginLifecycles.valueAtQuick(lifecycleIndex);
                for (int i = 0, n = lifecycles.size(); i < n; i++) {
                    try {
                        lifecycles.getQuick(i).onUnload();
                    } catch (Exception e) {
                        LOG.error().$("Plugin ").$(pluginNameSink).$(" onUnload failed: ").$(e.getMessage()).$();
                    }
                }
                pluginLifecycles.removeAt(lifecycleIndex);
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

            // Clean up plugin info
            final int infoIndex = pluginInfoMap.keyIndex(pluginNameSink);
            if (infoIndex < 0) {
                pluginInfoMap.removeAt(infoIndex);
            }

            LOG.info().$("Successfully unloaded plugin: ").$(pluginNameSink).$();
        } catch (final SqlException e) {
            throw e;
        } catch (final Exception e) {
            throw SqlException.position(0).put("Failed to unload plugin: ").put(name).put(": ").put(e.getMessage());
        }
    }

    /**
     * Verifies that a JAR file is signed with a valid certificate.
     * <p>
     * To verify a signed JAR, we must read all entries completely - this triggers
     * signature verification by the JarFile implementation. If any entry fails
     * verification, a SecurityException is thrown.
     *
     * @param jarPath    path to the JAR file
     * @param pluginName name of the plugin (for error messages)
     * @throws SqlException if the JAR is not signed or signature verification fails
     */
    private void verifyJarSignature(String jarPath, String pluginName) throws SqlException {
        try (JarFile jarFile = new JarFile(jarPath, true)) { // true = verify signatures
            boolean hasSignedEntries = false;
            byte[] buffer = new byte[8192];

            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();

                // Skip directory entries and signature-related files
                if (entry.isDirectory() || entry.getName().startsWith("META-INF/")) {
                    continue;
                }

                // Read the entire entry to trigger signature verification
                try (InputStream is = jarFile.getInputStream(entry)) {
                    while (is.read(buffer) != -1) {
                        // Just reading to trigger verification
                    }
                }

                // Check if this entry was signed
                CodeSigner[] signers = entry.getCodeSigners();
                if (signers != null && signers.length > 0) {
                    hasSignedEntries = true;

                    // Log certificate info at debug level
                    for (CodeSigner signer : signers) {
                        Certificate[] certs = signer.getSignerCertPath().getCertificates().toArray(new Certificate[0]);
                        if (certs.length > 0) {
                            LOG.debug().$("Plugin ").$(pluginName)
                                    .$(" signed by: ").$(certs[0].toString().substring(0, Math.min(100, certs[0].toString().length())))
                                    .$("...").$();
                        }
                    }
                }
            }

            if (!hasSignedEntries) {
                throw SqlException.position(0)
                        .put("Plugin JAR is not signed: ")
                        .put(pluginName)
                        .put(". Enable unsigned plugins by setting cairo.plugin.signature.verification=false");
            }

            LOG.info().$("Plugin ").$(pluginName).$(" signature verified successfully").$();

        } catch (SecurityException e) {
            // Signature verification failed
            throw SqlException.position(0)
                    .put("Plugin signature verification failed for: ")
                    .put(pluginName)
                    .put(": ")
                    .put(e.getMessage());
        } catch (IOException e) {
            throw SqlException.position(0)
                    .put("Failed to verify plugin signature: ")
                    .put(pluginName)
                    .put(": ")
                    .put(e.getMessage());
        }
    }
}
