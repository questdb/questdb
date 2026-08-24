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

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCompiledEncodingIdentityProbe;
import io.questdb.cairo.lv.LiveViewCompiledEncodingIdentityProbe.Snapshot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LiveViewCompiledEncodingIdentityTest extends AbstractLiveViewTest {

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testBuildersAndResultRootsBorrowThenClearAcrossFailureAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (Path dir = new Path(); Path metaDir = new Path()) {
                dir.of(configuration.getDbRoot()).concat("lv_compiled_encoding_identity_checkpoints");
                metaDir.of(dir).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
                configuration.getFilesFacade().mkdirs(metaDir, configuration.getMkDirMode());
                LiveViewCompiledEncodingIdentityProbe.assertBuilderFailureReuse(configuration, dir);
            }
        });
    }

    @Test
    public void testCompiledOwnersKeepExactBackingAcrossFreezeAndRestore() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tx (ts timestamp, account symbol, amount double) timestamp(ts) partition by day wal");
            execute("create live view lv_a flush every 100ms start from beginning as "
                    + "select ts, account, sum(amount) over window_a s from tx "
                    + "window window_a as (partition by account order by ts anchor daily '00:00')");
            execute("create live view lv_b flush every 100ms start from beginning as "
                    + "select ts, amount, count() over window_b c from tx "
                    + "window window_b as (partition by amount order by ts anchor daily '00:00')");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv_a");
                driveSeedToCompletion(job, "lv_b");
                insert(job, "2026-01-01T00:00:01.000000Z", "acct-a", 1.0);

                final LiveViewInstance a = instance("lv_a");
                final LiveViewInstance b = instance("lv_b");
                final Snapshot aBytes = LiveViewCompiledEncodingIdentityProbe.capture(a);
                final Snapshot bBytes = LiveViewCompiledEncodingIdentityProbe.capture(b);
                LiveViewCompiledEncodingIdentityProbe.assertDistinct(aBytes, bBytes);

                insert(job, "2026-01-01T00:00:02.000000Z", "acct-b", 2.0);
                LiveViewCompiledEncodingIdentityProbe.assertSameOwners(aBytes, a, true);
                LiveViewCompiledEncodingIdentityProbe.assertSameOwners(bBytes, b, true);
                restoreTwice(a);
                restoreTwice(b);
                LiveViewCompiledEncodingIdentityProbe.assertSameOwners(aBytes, a, true);
                LiveViewCompiledEncodingIdentityProbe.assertSameOwners(bBytes, b, true);

                Assert.assertFalse(a.getAnchorWindow().bindCheckpointWindowStatePlan(null));
                insert(job, "2026-01-01T00:00:03.000000Z", "acct-a", 3.0);
                LiveViewCompiledEncodingIdentityProbe.assertSameOwners(aBytes, a, false);
                restoreTwice(a);
                LiveViewCompiledEncodingIdentityProbe.assertSameOwners(aBytes, a, false);
            }
            LiveViewCompiledEncodingIdentityProbe.assertMalformedUnicodeGoldenBytesAndEmptySchemaSingleton(configuration);
        });
    }

    private static LiveViewInstance instance(String name) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(name);
        Assert.assertNotNull(instance);
        return instance;
    }

    private void insert(LiveViewRefreshJob job, String ts, String account, double amount) throws Exception {
        execute("insert into tx values ('" + ts + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private static void restoreTwice(LiveViewInstance instance) {
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
        try (Path dir = checkpointsDir(instance); LiveViewCheckpointTimelineStoreReader reader = new LiveViewCheckpointTimelineStoreReader(configuration)) {
            reader.of(dir);
            reader.restoreLatest(instance.getLiveViewToken().getTableId(), functions, instance.getAnchorWindow());
            reader.restoreLatest(instance.getLiveViewToken().getTableId(), functions, instance.getAnchorWindow());
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }
}
