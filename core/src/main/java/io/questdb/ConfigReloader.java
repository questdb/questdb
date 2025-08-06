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

package io.questdb;

import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjHashSet;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

public interface ConfigReloader {

    /**
     * Reload known dynamic properties from the configuration source.
     *
     * @return true if the config was reloaded
     */
    boolean reload();

    void unwatch(int watchId);

    /**
     * Register a listener for specific config keys.
     *
     * @param listener the listener to notify
     * @return watch ID for later unregistration
     */
    int watch(ConfigReloader.Listener listener);

    interface Listener {
        /**
         * The configuration has changed.
         */
        void configChanged();

        /**
         * Gets the list of config keys that the listener wants to be notified about.
         */
        @NotNull ConfigPropertyKey[] getWatchedConfigKeys();
    }

    class WatchRegistry {
        public static final int UNREGISTERED = -1;
        private final AtomicInteger nextWatchId = new AtomicInteger(UNREGISTERED + 1);
        private final IntObjHashMap<Listener> watchers = new IntObjHashMap<>();

        /**
         * Notify all watchers whose config keys intersect with the changed keys.
         * Each watcher is notified at most once per call, even if multiple
         * config keys they are watching have changed.
         *
         * @param changedKeys the set of config keys that changed
         */
        public synchronized void notifyWatchers(ObjHashSet<String> changedKeys) {
            final int noEntryKeyValue = watchers.getNoEntryKey();
            final int[] keys = watchers.getKeys();
            final Listener[] values = watchers.getValues();
            for (int slotIndex = 0, n = values.length; slotIndex < n; slotIndex++) {
                if (keys[slotIndex] == noEntryKeyValue) {
                    continue;
                }
                final Listener listener = values[slotIndex];
                final ConfigPropertyKey[] watchedKeys = listener.getWatchedConfigKeys();
                if (hasIntersection(watchedKeys, changedKeys)) {
                    listener.configChanged();
                }
            }
        }

        /**
         * Unregister a listener by watch ID.
         *
         * @param watchId the ID returned from watch()
         */
        public synchronized void unwatch(int watchId) {
            watchers.remove(watchId);
        }

        /**
         * Register a listener for specific config keys.
         *
         * @param listener the listener to notify
         * @return watch ID for later unregistration
         */
        public synchronized int watch(ConfigReloader.Listener listener) {
            final int watchId = nextWatchId.getAndIncrement();
            watchers.put(watchId, listener);
            return watchId;
        }

        private static boolean hasIntersection(ConfigPropertyKey[] watchedKeys, ObjHashSet<String> changedKeys) {
            for (ConfigPropertyKey watchedKey : watchedKeys) {
                if (changedKeys.contains(watchedKey.getPropertyPath())) {
                    return true;
                }
            }
            return false;
        }
    }
}
