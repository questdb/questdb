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

package io.questdb.cairo.map;

import io.questdb.cairo.RecordSinkSPI;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Read-only, re-targetable probe over a frozen {@link Map}, so that many threads can look up
 * keys in one map at the same time. A map's own {@link MapKey} cannot do that: it stages the
 * searched key in memory the map owns and hands every caller the same value flyweight. A view
 * stages the key privately and carries its own value flyweight, so two views share nothing but
 * the frozen map they read.
 * <p>
 * The shape is stage once, probe many: {@link #withKey()} starts a fresh key, a
 * {@link io.questdb.cairo.RecordSink} copies the key columns in, and {@link #findValue()} looks
 * the key up. Binding a view to a map is the one thing this interface leaves out, since it takes
 * the concrete map type: see {@code OrderedMap.ProbeView.of} and
 * {@code Unordered8Map.ProbeView.of}.
 * <p>
 * A view reads a map that nothing is mutating: between binding and the last
 * {@link #findValue()} the map must take no put, clear, resize, rehash or close.
 */
public interface MapProbeView extends RecordSinkSPI, QuietCloseable {

    /**
     * Looks the staged key up in the bound map and returns its value, or null when the map holds
     * no such key. The returned value is this view's own flyweight and stays valid until the next
     * call.
     */
    MapValue findValue();

    /** Allocated native bytes, including unused capacity. */
    long getSizeInBytes();

    /** Binds the tracker that charges whatever native memory the view stages keys in. */
    void setMemoryTracker(@Nullable MemoryTracker tracker);

    /** Discards the staged key and starts a new one. The view must already be bound to a map. */
    MapProbeView withKey();
}
