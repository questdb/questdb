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
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The order a seal walks the keys it freezes in, read off the state pages it wrote.
 * <p>
 * A seal looks every key it freezes up in the partition map the boundary below
 * published, and that map's reader memoises one decoded node per depth. Consecutive
 * lookups that land in one leaf therefore decode it once, while lookups that arrive
 * in a runtime map cursor's order - the order its entries were created in, or the
 * hash-slot order a narrow key's unordered map walks - decode a leaf each. Walking
 * the keys in the tree's own byte order instead makes the lookup cost the leaves the
 * key set touches rather than the keys it holds.
 * <p>
 * Nothing in a checkpoint's content records that order - the root builder sorts the
 * puts it is handed, so a seal that walked backwards would publish the same tree -
 * which is exactly why it needs a case of its own. What the order does leave behind
 * is the sequence its state pages were appended to the data segment in, and a
 * partition map hands its entries back in key order, so a boundary whose entries'
 * page offsets ascend with their keys is one whose seal walked in the tree's order.
 * The view below keeps that reading honest on both sides: a bounded ROWS frame is
 * whole-state per key, so every key names exactly one page of its own, and every
 * commit moves every key, so no key can reuse the page the boundary below wrote for
 * it and land an older offset in the middle of the sequence.
 * <p>
 * The keys are SYMBOLs, which the view keys by an LV-private id rather than by the
 * string, and the discriminator is the NULL key. An id is minted the first time the
 * view sees its string, so an id-ordered tree and a map that walks its entries in the
 * order they were created agree on every ordinary key and no arrangement of the rows
 * can pull them apart. NULL has no id: its key is {@code VALUE_IS_NULL}, which leads
 * with the sign byte and therefore sorts after every id however early the row that
 * carried it arrived. The commits below put that row first, so the key the map hands
 * over first is the key the tree holds last, and a seal that walked its map instead
 * of the tree writes that key's state page ahead of all the others rather than behind
 * them.
 */
public class LiveViewCheckpointSealWalkOrderTest extends AbstractLiveViewTest {

    // Named keys, plus the NULL key the commits lead with. Few enough that all of
    // them stay inside one 64-entry leaf, so what the case reads is one page's
    // entries in one order rather than a tree traversal's.
    private static final int KEYS = 23;
    private static final String VIEW_SQL = "SELECT ts, sym, sum(x) OVER (" +
            "PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
            ") AS s FROM base";

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so each commit below is exactly one seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testEverySealWritesItsStatePagesInTheTreesKeyOrder() throws Exception {
        assertMemoryLeak(() -> {
            createView();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The first seal freezes every key out of the function's own state map
                // with no predecessor to look anything up in. It still walks in the
                // tree's order, and its pages say so.
                commitEveryKey(job, 10);
                assertStatePagesAscendWithKeys("the first seal");

                // The second has a predecessor, so this is the walk whose order decides
                // how many leaves the lookups decode. Every key moves again, so every
                // key writes a fresh page rather than naming the one below it.
                commitEveryKey(job, 11);
                assertStatePagesAscendWithKeys("the second seal");

                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * Asserts the newest boundary names one state page per live key and that those
     * pages ascend, in the partition map's own key order, by the position they were
     * appended to the data store at.
     */
    private void assertStatePagesAscendWithKeys(String what) {
        final LongList segmentIds = new LongList();
        final LongList offsets = new LongList();
        readNewestBoundaryStatePages(segmentIds, offsets);
        Assert.assertEquals(what + ": one entry per key, the NULL key included", KEYS + 1, offsets.size());
        for (int i = 1; i < offsets.size(); i++) {
            final long previousSegment = segmentIds.getQuick(i - 1);
            final long segment = segmentIds.getQuick(i);
            final boolean ascends = segment > previousSegment
                    || (segment == previousSegment && offsets.getQuick(i) > offsets.getQuick(i - 1));
            Assert.assertTrue(
                    what + ": key " + i + " wrote its state page at " + segment + "/" + offsets.getQuick(i)
                            + ", behind key " + (i - 1) + " at " + previousSegment + "/"
                            + offsets.getQuick(i - 1) + " - the seal did not walk in the tree's order",
                    ascends
            );
        }
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + VIEW_SQL + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    // One row for the NULL key and one for every named key, at one designated
    // timestamp, plus a refresh turn. The NULL row leads, so it is the first entry
    // the function's map holds and the last the partition map does.
    private void commitEveryKey(LiveViewRefreshJob job, int second) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final String rowTs = "2026-01-01T00:00:" + String.format("%02d", second) + ".000000Z";
        final StringBuilder sql = new StringBuilder("INSERT INTO base (ts, sym, x) VALUES ");
        sql.append("('").append(rowTs).append("', NULL, 0)");
        for (int k = 0; k < KEYS; k++) {
            sql.append(", ('").append(rowTs).append("', '").append(key(k)).append("', ").append(k + 1).append(')');
        }
        execute(sql.toString());
        drainWalQueue();
        drainJob(job);
        drainWalQueue();
    }

    private Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private void createView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
    }

    private String key(int index) {
        return String.format("k%02d", index);
    }

    /**
     * Fills the two lists with the segment id and offset of the state page every live
     * key of the newest published boundary names, in the partition map's key order.
     */
    private void readNewestBoundaryStatePages(LongList segmentIdsOut, LongList offsetsOut) {
        final LiveViewInstance instance = viewInstance();
        try (
                Path dir = checkpointsDir(instance);
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointPageRef newestRootRef = new LiveViewCheckpointPageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> newestRootRef.of(
                        entry.rootRef.getSegmentId(),
                        entry.rootRef.getOffset(),
                        entry.rootRef.getLength()
                ));
                Assert.assertFalse("the view must have published a boundary", newestRootRef.isNull());

                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                root.of(dir, newestRootRef);
                root.getFunctionDirectoryRef(functionDirectoryRef);
                functions.of(dir, functionDirectoryRef);
                Assert.assertEquals("the view declares exactly one window function", 1, functions.size());
                functions.getRootRef(0, functionRootRef);
                functionRoot.of(dir, functionRootRef);
                functionRoot.getPartitionMapRootRef(partitionMapRoot);
                segmentIdsOut.clear();
                offsetsOut.clear();
                partitions.iterateAll(partitionMapRoot, partition -> {
                    Assert.assertEquals(
                            "a whole-state entry holds exactly one page",
                            1,
                            partition.getStatePageCount()
                    );
                    segmentIdsOut.add(partition.getStatePageRef(0).getSegmentId());
                    offsetsOut.add(partition.getStatePageRef(0).getOffset());
                });
            }
        }
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
