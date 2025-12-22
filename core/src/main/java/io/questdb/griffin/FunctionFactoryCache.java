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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.engine.functions.ArgSwappingFunctionFactory;
import io.questdb.griffin.engine.functions.NegatingFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FunctionFactoryCache {

    static final IntHashSet invalidFunctionNameChars = new IntHashSet();
    static final CharSequenceHashSet invalidFunctionNames = new CharSequenceHashSet();
    private static final Log LOG = LogFactory.getLog(FunctionFactoryCache.class);
    private final LowerCaseCharSequenceHashSet cursorFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceHashSet groupByFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceHashSet runtimeConstantFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceHashSet windowFunctionNames = new LowerCaseCharSequenceHashSet();

    // Plugin function support - namespaced plugin functions separate from core functions
    // Structure: pluginName -> functionName -> List<FunctionFactoryDescriptor>
    private final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>>> pluginFunctions =
            new LowerCaseCharSequenceObjHashMap<>();

    // Track loaded plugins (case-insensitive)
    private final LowerCaseCharSequenceHashSet loadedPlugins = new LowerCaseCharSequenceHashSet();

    // Thread safety for plugin function modifications (read lock for lookups, write lock for add/remove)
    private final ReentrantReadWriteLock pluginLock = new ReentrantReadWriteLock();

    public FunctionFactoryCache(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        boolean enableTestFactories = configuration.enableTestFactories();
        LOG.info().$("loading functions [test=").$(enableTestFactories).$(']').$();
        for (FunctionFactory factory : functionFactories) {
            if (!factory.getClass().getName().contains("io.questdb.griffin.engine.functions.test.") || enableTestFactories) {
                try {
                    final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(factory);
                    final String name = descriptor.getName();
                    addFactoryToList(factories, descriptor);

                    // Add != counterparts to equality function factories
                    if (factory.isBoolean()) {
                        switch (name) {
                            case "=":
                                addFactoryToList(factories, createNegatingFactory("!=", factory));
                                addFactoryToList(factories, createNegatingFactory("<>", factory));
                                if (descriptor.getArgTypeWithFlags(0) != descriptor.getArgTypeWithFlags(1)) {
                                    FunctionFactory swappingFactory = createSwappingFactory("=", factory);
                                    addFactoryToList(factories, swappingFactory);
                                    addFactoryToList(factories, createNegatingFactory("!=", swappingFactory));
                                    addFactoryToList(factories, createNegatingFactory("<>", swappingFactory));
                                }
                                break;
                            case "<":
                                // `a < b` == `a >= b`
                                addFactoryToList(factories, createNegatingFactory(">=", factory));
                                FunctionFactory greaterThan = createSwappingFactory(">", factory);
                                // `a < b` == `b > a`
                                addFactoryToList(factories, greaterThan);
                                // `b > a` == !(`b <= a`)
                                addFactoryToList(factories, createNegatingFactory("<=", greaterThan));
                                break;
                            case ">":
                                // `a > b` == `a <= b`
                                addFactoryToList(factories, createNegatingFactory("<=", factory));
                                FunctionFactory lessThan = createSwappingFactory("<", factory);
                                // `a > b` == `b < a`
                                addFactoryToList(factories, lessThan);
                                // `b < a` == !(`b >= a`)
                                addFactoryToList(factories, createNegatingFactory(">=", lessThan));
                                break;
                        }
                    } else if (factory.isGroupBy()) {
                        groupByFunctionNames.add(name);
                    } else if (factory.isWindow()) {
                        windowFunctionNames.add(name);
                    } else if (factory.isCursor()) {
                        cursorFunctionNames.add(name);
                    } else if (factory.isRuntimeConstant()) {
                        runtimeConstantFunctionNames.add(name);
                    } else if (factory.shouldSwapArgs() && descriptor.getSigArgCount() == 2 &&
                            descriptor.getArgTypeWithFlags(0) != descriptor.getArgTypeWithFlags(1)
                    ) {
                        FunctionFactory swappingFactory = createSwappingFactory(name, factory);
                        addFactoryToList(factories, swappingFactory);
                    }
                } catch (SqlException e) {
                    LOG.error().$((Sinkable) e)
                            .$(" [signature=").$safe(factory.getSignature())
                            .$(", class=").$safe(factory.getClass().getName())
                            .I$();
                }
            }
        }
    }

    @TestOnly
    public LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> getFactories() {
        return factories;
    }

    public int getFunctionCount() {
        return factories.size();
    }

    public ObjList<FunctionFactoryDescriptor> getOverloadList(CharSequence token) {
        return factories.get(token);
    }

    public boolean isCursor(CharSequence name) {
        if (name == null) {
            return false;
        }
        pluginLock.readLock().lock();
        try {
            return cursorFunctionNames.contains(name);
        } finally {
            pluginLock.readLock().unlock();
        }
    }

    public boolean isGroupBy(CharSequence name) {
        if (name == null) {
            return false;
        }
        pluginLock.readLock().lock();
        try {
            if (groupByFunctionNames.contains(name)) {
                return true;
            }
            // For qualified plugin function names, try unquoting the plugin name part
            final int lastDot = Chars.lastIndexOf(name, 0, name.length(), '.');
            if (lastDot > 0 && lastDot < name.length() - 1) {
                String pluginName = name.subSequence(0, lastDot).toString();
                if (pluginName.startsWith("\"") && pluginName.endsWith("\"")) {
                    pluginName = pluginName.substring(1, pluginName.length() - 1);
                }
                final String functionName = name.subSequence(lastDot + 1, name.length()).toString();
                final String qualifiedName = pluginName + "." + functionName;
                return groupByFunctionNames.contains(qualifiedName);
            }
            return false;
        } finally {
            pluginLock.readLock().unlock();
        }
    }

    public boolean isRuntimeConstant(CharSequence name) {
        if (name == null) {
            return false;
        }
        pluginLock.readLock().lock();
        try {
            return runtimeConstantFunctionNames.contains(name);
        } finally {
            pluginLock.readLock().unlock();
        }
    }

    public boolean isValidNoArgFunction(ExpressionNode node) {
        final ObjList<FunctionFactoryDescriptor> overload = getOverloadList(node.token);
        if (overload == null) {
            return false;
        }

        for (int i = 0, n = overload.size(); i < n; i++) {
            FunctionFactoryDescriptor ffd = overload.getQuick(i);
            if (ffd.getSigArgCount() == 0) {
                return true;
            }
        }

        return false;
    }

    public boolean isWindow(CharSequence name) {
        if (name == null) {
            return false;
        }
        pluginLock.readLock().lock();
        try {
            return windowFunctionNames.contains(name);
        } finally {
            pluginLock.readLock().unlock();
        }
    }

    private void addFactoryToList(LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> list, FunctionFactory factory) throws SqlException {
        addFactoryToList(list, new FunctionFactoryDescriptor(factory));
    }

    private void addFactoryToList(LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> list, FunctionFactoryDescriptor descriptor) {
        String name = descriptor.getName();
        int index = list.keyIndex(name);
        ObjList<FunctionFactoryDescriptor> overload;
        if (index < 0) {
            overload = list.valueAtQuick(index);
        } else {
            overload = new ObjList<>(4);
            list.putAt(index, name, overload);
        }
        overload.add(descriptor);
    }

    private FunctionFactory createNegatingFactory(String name, FunctionFactory factory) throws SqlException {
        return new NegatingFunctionFactory(name, factory);
    }

    private FunctionFactory createSwappingFactory(String name, FunctionFactory factory) throws SqlException {
        return new ArgSwappingFunctionFactory(name, factory);
    }

    /**
     * Adds plugin functions to the cache under a namespaced plugin name.
     * Functions are stored separately from core functions to allow namespace isolation.
     * Thread-safe with write lock.
     *
     * @param pluginName the plugin name (normalized, no .jar suffix)
     * @param factories list of FunctionFactory implementations from the plugin
     * @throws SqlException if plugin name is invalid or already loaded
     */
    public void addPluginFunctions(String pluginName, ObjList<FunctionFactory> factories) throws SqlException {
        pluginLock.writeLock().lock();
        try {
            // Validate plugin name
            if (pluginName == null || pluginName.isEmpty()) {
                throw SqlException.position(0).put("Invalid plugin name: empty");
            }
            if (pluginName.length() > 64) {
                throw SqlException.position(0).put("Plugin name too long (max 64 chars): ").put(pluginName);
            }

            // Check if already loaded (idempotent - just return)
            if (loadedPlugins.contains(pluginName)) {
                LOG.info().$("Plugin already loaded: ").$(pluginName).$();
                return;
            }

            // Create function map for this plugin
            final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> pluginFactoryMap =
                    new LowerCaseCharSequenceObjHashMap<>();

            // Add each factory to the plugin's function map
            for (int i = 0, n = factories.size(); i < n; i++) {
                final FunctionFactory factory = factories.getQuick(i);
                try {
                    final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(factory);
                    final CharSequence name = descriptor.getName();
                    addFactoryToList(pluginFactoryMap, descriptor);

                    // Track plugin GROUP BY functions with qualified name (plugin_name.function_name)
                    if (factory.isGroupBy()) {
                        final String qualifiedName = pluginName + "." + name;
                        groupByFunctionNames.add(qualifiedName);
                    } else if (factory.isCursor()) {
                        final String qualifiedName = pluginName + "." + name;
                        cursorFunctionNames.add(qualifiedName);
                    } else if (factory.isWindow()) {
                        final String qualifiedName = pluginName + "." + name;
                        windowFunctionNames.add(qualifiedName);
                    } else if (factory.isRuntimeConstant()) {
                        final String qualifiedName = pluginName + "." + name;
                        runtimeConstantFunctionNames.add(qualifiedName);
                    }
                } catch (SqlException e) {
                    LOG.error().$("Failed to register plugin function: ")
                            .$("[signature=").$safe(factory.getSignature())
                            .$(", plugin=").$(pluginName)
                            .$(", class=").$safe(factory.getClass().getName())
                            .$("]").$((Sinkable) e).$();
                }
            }

            // Store plugin's function map and mark as loaded
            if (pluginFactoryMap.size() > 0) {
                final int index = pluginFunctions.keyIndex(pluginName);
                if (index >= 0) {
                    pluginFunctions.putAt(index, pluginName, pluginFactoryMap);
                }
            }

            loadedPlugins.add(pluginName);
            LOG.info().$("Added plugin functions for: ").$(pluginName).$(", count=")
                    .$(pluginFactoryMap.size()).$();
        } finally {
            pluginLock.writeLock().unlock();
        }
    }

    /**
     * Removes all functions for a plugin from the cache.
     * Thread-safe with write lock.
     *
     * @param pluginName the plugin name
     * @throws SqlException if plugin is not loaded
     */
    public void removePluginFunctions(CharSequence pluginName) throws SqlException {
        pluginLock.writeLock().lock();
        try {
            if (!loadedPlugins.contains(pluginName)) {
                throw SqlException.position(0).put("Plugin not loaded: ").put(pluginName);
            }

            // Get plugin's function map before removing
            final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> pluginFactoryMap =
                    pluginFunctions.get(pluginName);

            // Remove qualified names from function type sets
            if (pluginFactoryMap != null) {
                final ObjList<CharSequence> keys = pluginFactoryMap.keys();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    final CharSequence functionName = keys.getQuick(i);
                    final ObjList<FunctionFactoryDescriptor> overloads = pluginFactoryMap.get(functionName);
                    if (overloads != null && overloads.size() > 0) {
                        // Check the first overload to determine the function type
                        final FunctionFactory factory = overloads.getQuick(0).getFactory();
                        final String qualifiedName = pluginName + "." + functionName;
                        if (factory.isGroupBy()) {
                            groupByFunctionNames.remove(qualifiedName);
                        } else if (factory.isCursor()) {
                            cursorFunctionNames.remove(qualifiedName);
                        } else if (factory.isWindow()) {
                            windowFunctionNames.remove(qualifiedName);
                        } else if (factory.isRuntimeConstant()) {
                            runtimeConstantFunctionNames.remove(qualifiedName);
                        }
                    }
                }
            }

            // Remove plugin's function map
            pluginFunctions.remove(pluginName);
            loadedPlugins.remove(pluginName);

            LOG.info().$("Removed plugin functions for: ").$(pluginName).$();
        } finally {
            pluginLock.writeLock().unlock();
        }
    }

    /**
     * Returns whether a plugin is currently loaded.
     * Thread-safe with read lock.
     *
     * @param pluginName the plugin name
     * @return true if the plugin is loaded
     */
    public boolean isPluginLoaded(CharSequence pluginName) {
        pluginLock.readLock().lock();
        try {
            return loadedPlugins.contains(pluginName);
        } finally {
            pluginLock.readLock().unlock();
        }
    }

    /**
     * Gets the function overload list for a qualified plugin function name.
     * Thread-safe with read lock.
     *
     * @param pluginName the plugin name
     * @param functionName the function name
     * @return the overload list, or null if plugin/function not found
     */
    public ObjList<FunctionFactoryDescriptor> getPluginFunction(String pluginName, CharSequence functionName) {
        pluginLock.readLock().lock();
        try {
            final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> pluginFactoryMap =
                    pluginFunctions.get(pluginName);

            if (pluginFactoryMap == null) {
                return null;
            }

            return pluginFactoryMap.get(functionName);
        } finally {
            pluginLock.readLock().unlock();
        }
    }
}
