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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.io.InputStream;

/**
 * Aggregates one joined row into its group, on behalf of the fused reducers.
 * <p>
 * The key sink and the aggregate updater are classes that each query generates for itself. A
 * shared method that calls them collects one type profile for every query the JVM has run, so C2
 * compiles it for the queries that came first: with two receivers it inlines both and may run out
 * of its inlining budget before the current query's aggregates, and with more it calls both per
 * row through the interface. Either way, the whole JVM stays in that state until the older
 * queries' classes unload. {@link #newInstance()} therefore defines a hidden class of its own from
 * {@link HashJoinGroupByRowUpdaterTemplate}'s bytecode for every fused factory, so that each
 * factory's copy profiles only that factory's classes.
 */
abstract class HashJoinGroupByRowUpdater {
    private static final byte[] TEMPLATE_BYTECODE = readTemplateBytecode();

    /**
     * Returns a row updater whose class no other factory shares, or the shared template when the
     * JVM cannot define hidden classes.
     */
    static HashJoinGroupByRowUpdater newInstance() {
        if (TEMPLATE_BYTECODE != null) {
            final Class<?> clazz = Unsafe.defineAnonymousClass(HashJoinGroupByRowUpdaterTemplate.class, TEMPLATE_BYTECODE);
            if (clazz != null) {
                try {
                    return (HashJoinGroupByRowUpdater) clazz.getDeclaredConstructor().newInstance();
                } catch (ReflectiveOperationException ignore) {
                    // fall back to the shared template
                }
            }
        }
        return new HashJoinGroupByRowUpdaterTemplate();
    }

    private static byte[] readTemplateBytecode() {
        try (InputStream in = HashJoinGroupByRowUpdaterTemplate.class.getResourceAsStream(
                HashJoinGroupByRowUpdaterTemplate.class.getSimpleName() + ".class")) {
            return in != null ? in.readAllBytes() : null;
        } catch (IOException e) {
            return null;
        }
    }

    abstract void update(
            AsyncHashJoinGroupByAtom.Slot slot,
            GroupByMapFragment fragment,
            Map map,
            RecordSink sink,
            GroupByFunctionsUpdater updater,
            HashJoinGroupByRecord record,
            Function filter,
            long rowId
    );
}
