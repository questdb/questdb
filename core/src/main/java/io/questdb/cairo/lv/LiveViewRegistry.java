/*+*****************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

import java.util.Map;

/**
 * Thread-safe registry of live view instances.
 * <p>
 * TODO(live-view): zero-GC — {@link #getViewsForBaseTable}, {@link #invalidateViewsForBaseTable} and
 *  {@link #hasViewsForBaseTable} do a linear scan over {@code viewsByName.values()} on every call. The enhanced-for
 *  iteration allocates an Iterator via {@link ConcurrentHashMap}'s JDK-style {@code values()}, and all three methods
 *  are called on the WAL notification path. Maintain a secondary {@code baseTable -> ObjList<LiveViewInstance>}
 *  index (mirroring {@code MatViewGraph}'s base-table index) so lookups are O(1) and allocation-free.
 */
public class LiveViewRegistry implements QuietCloseable {
    private final ConcurrentHashMap<LiveViewInstance> viewsByName = new ConcurrentHashMap<>();

    @Override
    public void close() {
        clear();
    }

    public void clear() {
        for (Map.Entry<CharSequence, LiveViewInstance> entry : viewsByName.entrySet()) {
            Misc.free(entry.getValue());
        }
        viewsByName.clear();
    }

    public LiveViewInstance getViewInstance(CharSequence name) {
        return viewsByName.get(name);
    }

    /**
     * Collects all live view instances into the given sink.
     */
    public void getViews(ObjList<LiveViewInstance> sink) {
        sink.clear();
        for (LiveViewInstance instance : viewsByName.values()) {
            sink.add(instance);
        }
    }

    /**
     * Collects all live view instances that depend on the given base table.
     * Linear scan — acceptable for the V1 draft.
     */
    public void getViewsForBaseTable(CharSequence baseTableName, ObjList<LiveViewInstance> sink) {
        sink.clear();
        for (LiveViewInstance instance : viewsByName.values()) {
            if (instance.getDefinition().getBaseTableName().contentEquals(baseTableName)) {
                sink.add(instance);
            }
        }
    }

    /**
     * Invalidates all live view instances that depend on the given base table.
     */
    public void invalidateViewsForBaseTable(CharSequence baseTableName, String reason) {
        for (LiveViewInstance instance : viewsByName.values()) {
            if (instance.getDefinition().getBaseTableName().contentEquals(baseTableName)) {
                instance.invalidate(reason);
            }
        }
    }

    public boolean hasView(CharSequence name) {
        return viewsByName.get(name) != null;
    }

    public boolean hasViewsForBaseTable(CharSequence baseTableName) {
        for (LiveViewInstance instance : viewsByName.values()) {
            if (instance.getDefinition().getBaseTableName().contentEquals(baseTableName)) {
                return true;
            }
        }
        return false;
    }

    public void registerView(LiveViewInstance instance) {
        viewsByName.put(instance.getDefinition().getViewName(), instance);
    }

    public LiveViewInstance removeView(CharSequence name) {
        return viewsByName.remove(name);
    }
}
