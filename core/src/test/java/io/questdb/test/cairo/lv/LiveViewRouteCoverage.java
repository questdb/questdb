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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.log.Log;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Sums the lifetime counters of every refresh job a fuzz test runs, so the test can assert
 * which repair routes and counters it reached. Each job carries its own
 * {@link LiveViewRefreshStats}, and a restart replaces the job and its counters with it, so
 * the test absorbs every job right before it frees it.
 * <p>
 * The counters stay per job in production. This class only adds them up on the test side.
 */
final class LiveViewRouteCoverage {
    private static final ObjList<String> STAT_NAMES = new ObjList<>();
    private static final int STAT_COUNT;

    static {
        // Every public int constant of LiveViewRefreshStats names one counter slot. The slot
        // count is private, so it comes from the highest slot rather than a copy of the number.
        int maxSlot = -1;
        for (Field field : LiveViewRefreshStats.class.getFields()) {
            final int modifiers = field.getModifiers();
            if (field.getType() == int.class && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)) {
                try {
                    final int slot = field.getInt(null);
                    STAT_NAMES.extendAndSet(slot, field.getName());
                    maxSlot = Math.max(maxSlot, slot);
                } catch (IllegalAccessException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
        }
        STAT_COUNT = maxSlot + 1;
        for (int i = 0; i < STAT_COUNT; i++) {
            Assert.assertNotNull("LiveViewRefreshStats slot " + i + " has no name", STAT_NAMES.getQuick(i));
        }
    }

    private final long[] routes = new long[LiveViewCheckpointRepairRoute.ROUTE_COUNT];
    private final long[] stats = new long[STAT_COUNT];
    private int absorbedJobs;

    /**
     * Adds {@code job}'s counters to the totals. A null job adds nothing, so a caller can
     * absorb whatever it is about to free.
     */
    void absorb(@Nullable LiveViewRefreshJob job) {
        if (job == null) {
            return;
        }
        final LiveViewRefreshStats jobStats = job.getStatsForTest();
        for (int i = 0; i < STAT_COUNT; i++) {
            final long value = jobStats.get(i);
            if (i == LiveViewRefreshStats.OUTPUT_UNIQUENESS_MAX_GROUP_ROWS) {
                stats[i] = Math.max(stats[i], value);
            } else {
                stats[i] += value;
            }
        }
        for (int route = 0; route < routes.length; route++) {
            routes[route] += jobStats.getRouteCount(route);
        }
        absorbedJobs++;
    }

    void assertReached(int route) {
        if (routes[route] == 0) {
            Assert.fail("the fuzz never published a repair on the "
                    + LiveViewCheckpointRepairRoute.routeName(route) + " route\n" + table());
        }
    }

    void assertReachedStat(int stat) {
        if (stats[stat] == 0) {
            Assert.fail("the fuzz never moved " + STAT_NAMES.getQuick(stat) + "\n" + table());
        }
    }

    long get(int stat) {
        return stats[stat];
    }

    long getRouteCount(int route) {
        return routes[route];
    }

    void log(Log log, CharSequence label) {
        log.info().$("live view route coverage [label=").$(label).$("]\n").$(table()).$();
    }

    /**
     * One line per route, then one per counter, in index order.
     */
    String table() {
        final StringSink sink = new StringSink();
        sink.put("jobs: ").put(absorbedJobs).put('\n');
        for (int route = 1; route < routes.length; route++) {
            sink.put("route ").put(LiveViewCheckpointRepairRoute.routeName(route)).put(": ").put(routes[route]).put('\n');
        }
        for (int i = 0; i < STAT_COUNT; i++) {
            sink.put(STAT_NAMES.getQuick(i)).put(": ").put(stats[i]).put('\n');
        }
        return sink.toString();
    }
}
