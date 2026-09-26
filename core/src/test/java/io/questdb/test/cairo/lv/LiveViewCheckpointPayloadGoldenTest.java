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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointCaptureLedger;
import io.questdb.cairo.lv.LiveViewCheckpointCompaction;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointSealState;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Golden bytes and elision decisions for every shape of per-key checkpoint payload, pinned
 * on the build that still stages payloads on the Java heap. The payload conversion moves
 * those payloads into native arenas phase by phase, and it must change neither the bytes a
 * root persists nor which entries a seal writes and which it elides. The expected literals
 * below were captured from that build, so they are the oracle every later phase is held to:
 * no phase may edit them, and only {@link #scalarHex} may change, to read the scalar through
 * whatever accessor replaces {@code getScalarState()}.
 * <p>
 * Each case asserts three things:
 * <ul>
 *     <li>the SHA-256 of every file under the view's {@code _checkpoints/meta} and
 *     {@code _checkpoints/data}, sorted by name - the raw bytes the timeline persists;</li>
 *     <li>a decoded dump of every logical boundary's roots: per partition entry, its key,
 *     its scalar payload and its state page references. A root a boundary shares with the
 *     boundary below it is marked unchanged, and a rewritten root lists only the entries that
 *     differ from the boundary below, which also records which roots each seal rewrote;</li>
 *     <li>an elision log: per seal, how many entries reached a root builder
 *     ({@code getLastBoundaryPartitionPuts()}) and what the capture ledger says the freeze
 *     walked, imaged and probed. An equal put is dropped one layer down anyway, so an elision
 *     compare that stopped matching would leave the bytes alone; the log is what sees it.</li>
 * </ul>
 * The cases pin what seals, repairs and compaction write and elide. Beyond the fused window
 * state a repair restores to replay from, they read no image back into a runtime: the rest
 * of the restore side - a function root's inline image, the fused overlay, grouped members
 * and private projections - is pinned by the rest of the {@code io.questdb.test.cairo.lv}
 * suite, whose restarts, repairs and round trips fail when a restored image changes.
 * <p>
 * Every input is fixed: the clock is pinned, the table ids restart per test, one boundary is
 * sealed per commit and a single refresh job drives the view. Nothing here depends on the OS
 * page size: segment files are truncated to their append offset.
 */
public class LiveViewCheckpointPayloadGoldenTest extends AbstractLiveViewTest {

    private static final String DAILY_WINDOW = " WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')";
    private static final long DEFINITION_TXN = 17;
    private static final String FUSED_VIEW = "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
            + "SELECT created_at, account_id, sum(amount) OVER w AS s, max(amount) OVER w AS m FROM tx" + DAILY_WINDOW;
    private static final int GROUPED_SUMS = 16;
    private static final long LIFECYCLE_IDENTITY = 503;
    private static final String STUB_DIR = "lv_payload_golden";

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // One logical boundary per commit, so every commit below is exactly one seal.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testChainedResumeRepair() throws Exception {
        // (i) An anchor resume re-versions every boundary above its anchor as a chain: each
        // boundary seeded from the one below it, the chained predecessor answering from the
        // capture's own frozen payloads. The last boundary of the chain takes a NULL row for
        // acct-1, so both its fused window payload and its DECIMAL sum's inline image come
        // out byte-equal to what the chain staged for acct-1 two boundaries down, and unlike
        // what the published root below the chain holds. Both are elided only when the
        // chained predecessor answers from the staged images: an answer from the published
        // root would put them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE, d DECIMAL(38,2)) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER w AS s, max(amount) OVER w AS m, "
                    + "sum(d) OVER w AS ds FROM tx" + DAILY_WINDOW);
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                golden.commit(job, "d1 acct-1", decimalRow("2026-01-01T09:00", "acct-1", "1.0"));
                golden.commit(job, "d1 acct-2", decimalRow("2026-01-01T09:10", "acct-2", "2.0"));
                golden.commit(job, "d2 acct-1", decimalRow("2026-01-02T09:00", "acct-1", "4.0"));
                golden.commit(job, "d2 acct-1", decimalRow("2026-01-02T09:10", "acct-1", "8.0"));
                golden.commit(job, "d3 acct-1", decimalRow("2026-01-03T09:00", "acct-1", "16.0"));
                golden.commit(job, "d3 acct-2", decimalRow("2026-01-03T09:10", "acct-2", "32.0"));
                golden.commit(job, "d3 acct-1", decimalRow("2026-01-03T09:20", "acct-1", "128.0"));
                golden.commit(job, "d3 acct-2", decimalRow("2026-01-03T09:30", "acct-2", "256.0"));
                golden.commit(job, "d3 acct-1 NULL", decimalRow("2026-01-03T09:40", "acct-1", "NULL"));
                final LiveViewInstance instance = viewInstance();
                // Four boundaries above the anchor at 09:00, so the chain carries the
                // payloads it froze at one boundary into the next one's predecessor. The late
                // amount is small against the sum the resume restores, so a restored state
                // that is off in its lowest bit does not round away at the first boundary.
                golden.commit(job, "late d3 acct-1", decimalRow("2026-01-03T09:05", "acct-1", "0.25"));
                golden.recordRoute();
                Assert.assertTrue("the correction must resume from the anchor", instance.getO3ResumeReplayRows() > 0);
                Assert.assertTrue("the resume must splice its roots", instance.getCheckpointRepairRootsVersioned() > 0);
                Assert.assertEquals(
                        "the chain's last boundary must elide acct-1 against the images the chain staged",
                        0,
                        checkpointTimelineStoreWriter(job).getLastBoundaryPartitionPuts()
                );
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    meta/m.0000000000000001 355 161dc846d23d745494b9a40849ce0ed306622d721b87f54b37a81060c1b5960a
                    meta/m.0000000000000002 326 0037d0f6d484a381795d1588d3f95611c7ac87a5fb71430dd7835469ff3f4863
                    meta/m.0000000000000003 281 e99161744eaf37e2267142423863e111726af6607bd4dded484f035ba06bf68d
                    meta/m.0000000000000007 415 a24600059c8a75ec12ab6daa383e4d6016d699b68e63e61a8b02729efc45e81a
                    meta/m.0000000000000008 387 87586d80376681d6114be0bd94712c3b98b44b7b2221778f92da65e6aab1668c
                    meta/m.0000000000000009 281 7ca62e4daf35100b1f06983902eacb23de0e48aaad72a572d66860032082faae
                    meta/m.0000000000000013 415 de25a84c050e2f2d5b952a0b2668b38b263869bdadd23e2b367b013ec6ba0d9f
                    meta/m.0000000000000014 387 a7c5f4eb3e0e5e390bb13028cb1c0b56a4caf9542e3f365bacf7d873182741e1
                    meta/m.0000000000000015 281 9151a67e1acfb5ea18526720b66b36d6efca83f406f88c952b1c37a566556a19
                    meta/m.0000000000000019 415 210289a226b0ee8a41a9c965b27e9b371ad4a70309df2ef2b12b410d1e355d90
                    meta/m.0000000000000020 387 6f7529d6801a1a368272d217a41e2e248a7f703b20f6425bc8f8ecac09f76f14
                    meta/m.0000000000000021 281 ff772c305bf13769dc27d44cb04896f25c61ade8b9432278cd017e45b31e7114
                    meta/m.0000000000000025 415 24755293482214a9992d47b6be46ebfd80afb003f3e71ab5e90c71b778794b90
                    meta/m.0000000000000026 387 6a27bda179e1663901e8ba7222e1eedca05bce0bde96cfb50831a47cb4f9e90e
                    meta/m.0000000000000027 281 619660038a5c2be49d509ac0d8970ecaa4730e5fe3b4bc02d3a7a95d93060a35
                    meta/m.0000000000000031 415 aee71dd77b5e14a723e9c36fdc3677595a011582c5bef17d9692d28a2c62fc60
                    meta/m.0000000000000032 387 4e81c77ad1b13144df148eb49850f78cfe8d1ee063efc8bc1fddf73e6ff514f9
                    meta/m.0000000000000033 281 37cd18b5809cdcc18b6d29e5a4886a3db2c0754ab760d8408772f721926e6394
                    meta/m.0000000000000037 415 ab00a70a2a8d600c6e691a705d19bb4aa0838c60f3a2c321c1ec6772d9317150
                    meta/m.0000000000000038 387 1405fb3cf1c1046f87c30daf7252490b68f888848e5a035c7eaeb929d2912945
                    meta/m.0000000000000039 281 ea18d66e69af671d6a0551122cd1cd5014b5e6ff1cd0b5c730669255619c00c0
                    meta/m.0000000000000043 415 58bc9d1a39519730b27ea1b0db1c1629e1c316cc56daabee4554ca9252146d77
                    meta/m.0000000000000044 387 f7624a811328dc2d84670546439d19840bbfcfecf32626f9b44fac3355e4db03
                    meta/m.0000000000000045 281 b453af7a896c4e8a7c01d1a50ead3acc3c325805f2c7409e0447afb9de79d59b
                    meta/m.0000000000000049 291 2e035a6824bab85d4390fa600cda507d9a2818c74159c5f4b0e6556d49b7dd40
                    meta/m.0000000000000050 261 d90d777fc905139446701d234c01353d9f046347eaf2924a905a476a6a7bcb8b
                    meta/m.0000000000000051 297 e4d98a3f45e8a33880b394742edc93bae4a86990d7e2c041c84f3e7f7baab842
                    meta/m.0000000000000052 580 8a7ae0d51c777e7b1d8a3c08545fd32821bd853355f49c840de8f5d14bc5ea3a
                    meta/m.0000000000000053 1320 750be015c106ca8d135db3a5e012ce32143efd286cc13564f3fee34df311f612
                    meta/m.0000000000000055 415 aa076e3e5f89b5a7713891f78df89efd921cf16f0a8f2d001bb83063f9952894
                    meta/m.0000000000000056 387 74f40585679a68f605fd4aaf4e1d34b683c5f3035af179c7540f78441203387a
                    meta/m.0000000000000057 281 9e6dc20e8bb81104850e9c8278635abc91c125e618a42d4f0dce7f2650bd3ace
                    meta/m.0000000000000058 415 8bcee366e4a3a69c62be9eef4f58ebb706016f921fc983a708afa3449ab2473b
                    meta/m.0000000000000059 387 8aeaa62d5e9c864601e2c4486e59f1da186dc3c1786710c37e6ee62f04fb4741
                    meta/m.0000000000000060 281 4fff9c8e7961217171b9689b39d8db602506c556ad942f807092470388635773
                    meta/m.0000000000000061 415 dcef9be78ff1e3e724f58793520ec06f12e0e717687bf3bc01e590f7e2dc95db
                    meta/m.0000000000000062 387 3301ebbd8d85ffbd25459778346cc4c00980328b13a99ee9ffee677372c984d5
                    meta/m.0000000000000063 281 4ba631e8bfee44af5a681c505617d8dd0f3310f6e5b1272971ea8a6c6515b725
                    meta/m.0000000000000064 291 d4a978647c6623a107bf31ef858f4bbe1a365b4dc29191c0f0954f3ffc787998
                    meta/m.0000000000000065 261 ef537c65c23039381cf8cbf343b80803a8396c7770c600655b81a9dde1592fb5
                    meta/m.0000000000000066 297 ba7f1ceb11ecb46491f16b7b3803eae1a9552f01bfca64ed9f93a4aac33c46dc
                    meta/m.0000000000000067 580 3d6f293e66d4da063279b48889f41bf18cbaca45a6e4937559b7849b3ce597cc
                    meta/m.0000000000000068 1880 aeca9ce97fb65b44b98c825f03678122c633ed74d29f9edb26172b059dea0c5d
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258000000000 root=3/173/108
                      window 1/104/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=1/24/80 entries=1
                        + 0600000061006300630074002d003100 -> 0040204648470600000000000000f03f0100000000000000000000000000f03f refs=0
                      function 0 2/105/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=2/24/81 entries=1
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000640000000000000000 refs=0
                    boundary checkpoint=1 maxTs=1767258600000000 root=9/173/108
                      window 7/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=7/24/140 entries=2
                        + 0600000061006300630074002d003200 -> 0040204648470600000000000000004001000000000000000000000000000040 refs=0
                      function 0 8/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=8/24/142 entries=2
                        + 0600000061006300630074002d003200 -> 000000000000000000000000000000000000000000000000c80000000000000000 refs=0
                    boundary checkpoint=2 maxTs=1767344400000000 root=15/173/108
                      window 13/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=13/24/140 entries=2
                        + 0600000061006300630074002d003100 -> 00a0f7635c470600000000000000104001000000000000000000000000001040 refs=0
                      function 0 14/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=14/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000900100000000000000 refs=0
                    boundary checkpoint=3 maxTs=1767345000000000 root=21/173/108
                      window 19/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=19/24/140 entries=2
                        + 0600000061006300630074002d003100 -> 00a0f7635c470600000000000000284002000000000000000000000000002040 refs=0
                      function 0 20/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=20/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000b00400000000000000 refs=0
                    boundary checkpoint=4 maxTs=1767430800000000 root=27/173/108
                      window 25/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=25/24/140 entries=2
                        + 0600000061006300630074002d003100 -> 0000cf8170470600000000000000304001000000000000000000000000003040 refs=0
                      function 0 26/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=26/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000400600000000000000 refs=0
                    boundary checkpoint=5 maxTs=1767431400000000 root=57/173/108
                      window 55/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=55/24/140 entries=2
                        + 0600000061006300630074002d003100 -> 0000cf8170470600000000000040304002000000000000000000000000003040 refs=0
                        + 0600000061006300630074002d003200 -> 0000cf8170470600000000000000404001000000000000000000000000004040 refs=0
                      function 0 56/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=56/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000590600000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 000000000000000000000000000000000000000000000000800c00000000000000 refs=0
                    boundary checkpoint=6 maxTs=1767432000000000 root=60/173/108
                      window 58/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=58/24/140 entries=2
                        + 0600000061006300630074002d003100 -> 0000cf8170470600000000000008624003000000000000000000000000006040 refs=0
                      function 0 59/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=59/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000593800000000000000 refs=0
                    boundary checkpoint=7 maxTs=1767432600000000 root=63/173/108
                      window 61/164/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=61/24/140 entries=2
                        + 0600000061006300630074002d003200 -> 0000cf8170470600000000000000724002000000000000000000000000007040 refs=0
                      function 0 62/166/221 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=62/24/142 entries=2
                        + 0600000061006300630074002d003200 -> 000000000000000000000000000000000000000000000000807000000000000000 refs=0
                    boundary checkpoint=8 maxTs=1767433200000000 root=66/173/124
                      window 64/24/267 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=61/24/140 entries=2
                      function 0 65/24/237 identity=105:2de93262d9893551 version=1 keySchema=8:09da8010bedba854 map=62/24/142 entries=2
                    """,
                    """
                    d1 acct-1: puts=2 window roots=1 incremental=0 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=0 visited=1 imaged=1
                    d1 acct-2: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=1 visited=1 imaged=1
                    d2 acct-1: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=1 visited=1 imaged=1
                    d2 acct-1: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=1 visited=1 imaged=1
                    d3 acct-1: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=1 visited=1 imaged=1
                    d3 acct-2: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=1 visited=1 imaged=1
                    d3 acct-1: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=1 visited=1 imaged=1
                    d3 acct-2: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=1 visited=1 imaged=1
                    d3 acct-1 NULL: puts=0 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=1 visited=1 imaged=1
                    late d3 acct-1: puts=0 window roots=4 incremental=4 visited=5 imaged=5 removed=0 probes=4 | function roots=4 incremental=4 visited=5 imaged=5
                    route: checkpoint_repair_plan,checkpoint_repair_last_disposition,checkpoint_repair_last_denial|anchor,resume from anchor,resume cheaper|
                    """
            );
        });
    }

    @Test
    public void testCompactionRedirectsRingState() throws Exception {
        // Compaction's redirect. The correction at 23 s re-versions boundaries 2 to 5 into
        // one capture segment, and the one at 33 s re-versions 3 to 5 again, so the first
        // capture segment keeps live pages for boundary 2 alone. Compaction moves those
        // pages into a fresh segment and rewrites boundary 2's function root: each entry it
        // redirects must keep its ring scalar byte for byte and change only its page refs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT created_at, account_id, "
                    + "sum(amount) OVER (PARTITION BY account_id ORDER BY created_at "
                    + "RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW) AS s FROM tx");
            final Golden golden = new Golden();
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    LiveViewCheckpointTimelineStoreWriter compactionWriter = new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                driveSeedToCompletion(job, "lv");
                for (int second = 10; second <= 60; second += 10) {
                    golden.commit(job, "second " + second, ringRows(second, second + 1, 2));
                }
                final LiveViewInstance instance = viewInstance();
                golden.commit(job, "correction 23", ringRows(23, 24, 2));
                golden.recordRoute();
                golden.commit(job, "correction 33", ringRows(33, 34, 2));
                golden.recordRoute();
                Assert.assertTrue("the corrections must resume from an anchor", instance.getO3ResumeReplayRows() > 0);
                final LiveViewCheckpointCompaction.Result result;
                try (Path dir = new Path()) {
                    // The loosest policy: any referenced segment with a dead byte qualifies.
                    result = LiveViewCheckpointCompaction.compact(
                            configuration,
                            checkpointsDir(dir, instance),
                            compactionWriter,
                            instance.getLiveViewToken().getTableId(),
                            0,
                            instance.getLifecycleIdentity(),
                            true,
                            null,
                            100,
                            1,
                            64
                    );
                }
                Assert.assertTrue("the overlapping corrections must leave a segment to compact", result.isPublished());
                Assert.assertTrue("the compaction must rewrite a root", result.getRootsRewritten() > 0);
                golden.recordCompaction(result);
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    data/d.0000000000000000 32 7fd29c426a72d17eb53f552a88b84c0b19e4b330eb85dfe7f3d98d7ad8bb76a8
                    data/d.0000000000000005 64 dc5c1da4086d9bc9a57f0a6587c6d215a4dfe11366f04aa1016ced469a35ca80
                    data/d.0000000000000030 414 946d77ddef3b9b41dadf4082d754f7e0633e7aeb9a9e6a429fd2f3ea296946fb
                    data/d.0000000000000041 340 7a2f0161f3ece7b54d833897f941e6c6e179b2957d023aeda5cf0c43a1f53ad9
                    data/d.0000000000000050 98 cba74cce0c32a722543728c6bea939d22896cfc5a11a682927c3db2712578175
                    meta/m.0000000000000001 554 3e5f957657eeabf70ee6f41a51150f07f398fca65841c5b0bb70b0eb47584f90
                    meta/m.0000000000000002 278 b8ec83a6f96eca6ecce212f460a13323d1e92a5d9b76dbc9c524faa503e3fa2b
                    meta/m.0000000000000006 554 d4c3f3f384d019f0308f0fc84c98474acdc87c4bac451de736a892f9b64bc8c3
                    meta/m.0000000000000007 278 43e87e939fc57634d413f42cf7b265328bdea2449345cef55881e7e015a0fc07
                    meta/m.0000000000000031 554 afdd4b4b34baaa01756be1226c5396db919075e7e35f180b365b9288b9cb23f7
                    meta/m.0000000000000032 278 2b6386eb71c6b346a4c416fe05a6a337b6e7d8d6459dbe6871ffb1d707bb4416
                    meta/m.0000000000000033 554 0082a546b57c6488ccd9198a8936d50b14de1b76169cb5af0fda6fdebc236466
                    meta/m.0000000000000034 278 700f5b926937436b3e7c6eed514d6b7109e54480b648ecfbd8496b1d8f7acef6
                    meta/m.0000000000000035 554 bc688447a02ae677976f41786aa9bfe1961c70fa3d25d0f01e1ad461d94de4fc
                    meta/m.0000000000000036 278 bb7d0318419158334adbc908012da13166faa770dd15ef1a842fd504fa9f1f59
                    meta/m.0000000000000037 554 c7438f80325ce49ba75516a46fc88a47347d4e8a6a25e19af13af93f671d5b90
                    meta/m.0000000000000038 278 f3720cc0c896e541e703154abb609f1e72d691ffd5bbc1afe7d7a3279c26f20c
                    meta/m.0000000000000039 400 d2316d0b04d78e39548269dcc80057685743dc634fe0d156bf983718d9af0c7f
                    meta/m.0000000000000040 1400 8c8a3728fbe2ff88b9b46b49f5cd40e027a26d39aa9619e7f83f9cce41eb8e39
                    meta/m.0000000000000042 554 60b915fe8ede4f5435bbe21a286a4e1645a046ba1165507dd9171f9f3989dddd
                    meta/m.0000000000000043 278 c5aacf6329612d5061daf0929e336db0be62748a98aebe6aaccfe4fde6fb98e3
                    meta/m.0000000000000044 554 009ed50c6b35f782ca12a813d52592e9c7c43dece2c5f878bce5cbd34236c386
                    meta/m.0000000000000045 278 b9a86208cf0a12ab0697b9af778ed15016b8129c4af716c81f2d894f1c0b24aa
                    meta/m.0000000000000046 554 2bc76153d14249913abf0d6a538f4071cceb3a354ba3813a595cf6120f331a8a
                    meta/m.0000000000000047 278 991e130dccfe5cc1c6b6a894fe2a131542eca763fb1dc3d64883005e216e3155
                    meta/m.0000000000000048 400 09cbb412ee55e8580175e79fbad5d2ccaa0d9293108b6d94a2ef3ae9736924b3
                    meta/m.0000000000000049 1760 da7b70e08ed06de05c07243dd28d0cc80313d285149e3a42894d13d13215ee3a
                    meta/m.0000000000000051 554 ecd0c7927d303d655f773a96351696d56b4e795d338f90b61b4392863a9a5ba1
                    meta/m.0000000000000052 278 4fb4bb300dbdfc068e88a37a5b879c6aa593e3e8d9b96c198c6fb5f7aba8adf0
                    meta/m.0000000000000053 400 59bae7421ed44936a60b6a57b77185e5c2cf074220427399a651a002d2477792
                    meta/m.0000000000000054 1960 03a03005ee97259040cd645356703d148688f246e4f9dec4652571537feedb37
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258010000000 root=2/170/108
                      function 0 1/320/234 identity=102:013f4d34264b99a2 version=1 keySchema=8:09da8010bedba854 map=1/24/296 entries=2
                        + 010000006100 -> 0100000100000000010000000000000000000000000025400100000000000000809ae9d14f470600 refs=2 0/0/8/8/k33/c0/r1/f0 0/8/8/8/k34/c0/r1/f0
                        + 010000006200 -> 010000010000000001000000000000000000000000948f400100000000000000809ae9d14f470600 refs=2 0/16/8/8/k33/c0/r1/f0 0/24/8/8/k34/c0/r1/f0
                    boundary checkpoint=1 maxTs=1767258020000000 root=7/170/108
                      function 0 6/320/234 identity=102:013f4d34264b99a2 version=1 keySchema=8:09da8010bedba854 map=6/24/296 entries=2
                        + 010000006100 -> 010000010000000002000000000000000000000000003f400200000000000000003182d24f470600 refs=2 5/0/16/16/k33/c0/r2/f0 5/16/16/16/k34/c0/r2/f0
                        + 010000006200 -> 010000010000000002000000000000000000000000bc9f400200000000000000003182d24f470600 refs=2 5/32/16/16/k33/c0/r2/f0 5/48/16/16/k34/c0/r2/f0
                    boundary checkpoint=2 maxTs=1767258030000000 root=52/170/108
                      function 0 51/320/234 identity=102:013f4d34264b99a2 version=1 keySchema=8:09da8010bedba854 map=51/24/296 entries=2
                        + 010000006100 -> 010000010000000004000000000000000000000000405540040000000000000080c71ad34f470600 refs=2 50/0/26/32/k33/c1/r4/f0 50/26/23/32/k34/c2/r4/f0
                        + 010000006200 -> 010000010000000004000000000000000000000000eaaf40040000000000000080c71ad34f470600 refs=2 50/49/26/32/k33/c1/r4/f0 50/75/23/32/k34/c2/r4/f0
                    boundary checkpoint=3 maxTs=1767258040000000 root=43/170/108
                      function 0 42/320/234 identity=102:013f4d34264b99a2 version=1 keySchema=8:09da8010bedba854 map=42/24/296 entries=2
                        + 010000006100 -> 010000010000000006000000000000000000000000e063400600000000000000005eb3d34f470600 refs=2 41/0/32/48/k33/c1/r6/f0 41/32/26/48/k34/c2/r6/f0
                        + 010000006200 -> 0100000100000000060000000000000000000000000fb8400600000000000000005eb3d34f470600 refs=2 41/58/32/48/k33/c1/r6/f0 41/90/26/48/k34/c2/r6/f0
                    boundary checkpoint=4 maxTs=1767258050000000 root=45/170/108
                      function 0 44/320/234 identity=102:013f4d34264b99a2 version=1 keySchema=8:09da8010bedba854 map=44/24/296 entries=2
                        + 010000006100 -> 010000010000000006000000000000000000000000e06840060000000000000080f44bd44f470600 refs=2 41/116/32/48/k33/c1/r6/f0 41/148/26/48/k34/c2/r6/f0
                        + 010000006200 -> 01000001000000000600000000000000000000000037b840060000000000000080f44bd44f470600 refs=2 41/174/32/48/k33/c1/r6/f0 41/206/26/48/k34/c2/r6/f0
                    boundary checkpoint=5 maxTs=1767258060000000 root=47/170/108
                      function 0 46/320/234 identity=102:013f4d34264b99a2 version=1 keySchema=8:09da8010bedba854 map=46/24/296 entries=2
                        + 010000006100 -> 010000010000000005000000000000000000000000f06a400500000000000000008be4d44f470600 refs=2 41/232/29/40/k33/c1/r5/f0 41/261/25/40/k34/c2/r5/f0
                        + 010000006200 -> 0100000100000000050000000000000000000000805fb4400500000000000000008be4d44f470600 refs=2 41/286/29/40/k33/c1/r5/f0 41/315/25/40/k34/c2/r5/f0
                    """,
                    """
                    second 10: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    second 20: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    second 30: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    second 40: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    second 50: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    second 60: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    correction 23: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=4 incremental=0 visited=8 imaged=8
                    route: checkpoint_repair_plan,checkpoint_repair_last_disposition,checkpoint_repair_last_denial|range,resume from anchor,resume cheaper|
                    correction 33: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=3 incremental=0 visited=6 imaged=6
                    route: checkpoint_repair_plan,checkpoint_repair_last_disposition,checkpoint_repair_last_denial|range,resume from anchor,resume cheaper|
                    compaction: roots rewritten=1 target=50 generation=9
                    """
            );
        });
    }

    @Test
    public void testDecimalState() throws Exception {
        // (g) A DECIMAL sum twice: fused into the anchored window's leaf, and on a bounded
        // ROWS frame that keeps a function root of its own.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DECIMAL(38,2)) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER w AS s, sum(amount) OVER "
                    + "(PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r FROM tx"
                    + DAILY_WINDOW);
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                golden.commit(job, "complete", "('2026-01-01T09:00:00.000000Z', 'acct-1', 5.25::decimal(38,2)), "
                        + "('2026-01-01T09:01:00.000000Z', 'acct-2', 7.50::decimal(38,2))");
                golden.commit(job, "acct-1", "('2026-01-01T09:10:00.000000Z', 'acct-1', 1.01::decimal(38,2))");
                golden.commit(job, "acct-2 next day", "('2026-01-02T09:00:00.000000Z', 'acct-2', -2.75::decimal(38,2))");
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    data/d.0000000000000000 192 2c8303ef6c05545135282cf19f9e9fc57d2bff44a30e782b218179ebfc38d976
                    data/d.0000000000000007 96 e408d4d6f4d641844375f32864e8fe77896987202667b73becd417a6eda9cdb6
                    data/d.0000000000000014 96 985e70d077441dab6b45a3d0dedbead8cd76a27948d510a45f5d76d1f3b6ffc2
                    meta/m.0000000000000001 279 a782d5d8470df6fcfaa8efd972960ea6117c7e3c0ce63423649f72d5d2d64764
                    meta/m.0000000000000002 387 34c22614eb7d6269020d88883e52ebb8030ec8d1d9ba907b1f0a27a7d0908bcb
                    meta/m.0000000000000003 416 a9f3f52baae9e6b71aa53a7286006f85e89de0b9a9983d0c7efbb3c7bd4f2a77
                    meta/m.0000000000000004 425 1126997852cfe8409c0fb375752bcb6e1ccfa6e1030ce167f541d247b1052f2d
                    meta/m.0000000000000008 203 d72217642180ba94f2c554090c72a0786ce200a425c3e5de3b84ad1efb61f6c4
                    meta/m.0000000000000009 387 a316cb7c0ac83a26271d1ff9c51fcfe526f84d5bc62405a2dd78446ac2452627
                    meta/m.0000000000000010 432 3ddcb2d87e3ba59c7e72655b40de737a6c1d25391beb1413cd30a2f987ae4623
                    meta/m.0000000000000011 441 88acbc298b2d26162219c60f5d6299fb401fbec560f13c3c14a34dc9520f165e
                    meta/m.0000000000000012 160 001363952f9c61cc0a96db06263823553679671c3e0c1c3867cc2d610774c158
                    meta/m.0000000000000013 560 825805341c9fb4812fffe20579729a30a66c403989bb6f0a9a621ec0b8e37dbd
                    meta/m.0000000000000015 279 f872fc782113f7e318986d5c2f8d6650be78adbab5ddc9d9e54d45e64b349fa2
                    meta/m.0000000000000016 387 ad41a67bf0af787fb9cd207bbd8690232cd77f45707babd1dd10798408f17675
                    meta/m.0000000000000017 432 6c42089868517591dca77fb4faf3918a280f935062900f1a41d935604b867b71
                    meta/m.0000000000000018 433 b3d8e68c491504e5bedfef1bdcabfdad4d4c4a2abe206869fd641a02c76ef52a
                    meta/m.0000000000000019 220 5e7aa09ea87ebc9a8546c95034294c526dffd2c9717594c01dc8622fc154b0f4
                    meta/m.0000000000000020 840 f66953a750ad8a9649311dcaf8b5517aaa281536250eb2199d2a9feddf35a9da
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258060000000 root=4/301/124
                      window 1/116/163 identity=55:a792d92f68f6dfa8 anchorType=8 inline=8 keySchema=8:09da8010bedba854 manifest=24:9fa9ba9430dac90e map=1/24/92 entries=2
                        + 0600000061006300630074002d003100 -> 0040204648470600 refs=0
                        + 0600000061006300630074002d003200 -> 0040204648470600 refs=0
                      function 0 2/166/221 identity=105:d9b1ffbafd79650f version=1 keySchema=8:09da8010bedba854 map=2/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 0000000000000000000000000000000000000000000000000d0200000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 000000000000000000000000000000000000000000000000ee0200000000000000 refs=0
                      function 1 3/180/236 identity=104:a0aea06fe11f292c version=1 keySchema=8:09da8010bedba854 map=3/24/156 entries=2
                        + 0600000061006300630074002d003100 ->  refs=1 0/0/96/96/k65/c0/r1/f0
                        + 0600000061006300630074002d003200 ->  refs=1 0/96/96/96/k65/c0/r1/f0
                    boundary checkpoint=1 maxTs=1767258600000000 root=11/301/140
                      window 8/24/179 identity=55:a792d92f68f6dfa8 anchorType=8 inline=8 keySchema=8:09da8010bedba854 manifest=24:9fa9ba9430dac90e map=1/24/92 entries=2
                      function 0 9/166/221 identity=105:d9b1ffbafd79650f version=1 keySchema=8:09da8010bedba854 map=9/24/142 entries=2
                        + 0600000061006300630074002d003100 -> 000000000000000000000000000000000000000000000000720200000000000000 refs=0
                      function 1 10/180/252 identity=104:a0aea06fe11f292c version=1 keySchema=8:09da8010bedba854 map=10/24/156 entries=2
                        + 0600000061006300630074002d003100 ->  refs=1 7/0/96/96/k65/c0/r1/f0
                    boundary checkpoint=2 maxTs=1767344400000000 root=18/301/132
                      window 15/116/163 identity=55:a792d92f68f6dfa8 anchorType=8 inline=8 keySchema=8:09da8010bedba854 manifest=24:9fa9ba9430dac90e map=15/24/92 entries=2
                        + 0600000061006300630074002d003200 -> 00a0f7635c470600 refs=0
                      function 0 16/166/221 identity=105:d9b1ffbafd79650f version=1 keySchema=8:09da8010bedba854 map=16/24/142 entries=2
                        + 0600000061006300630074002d003200 -> ffffffffffffffffffffffffffffffffffffffffffffffffedfeffffffffffff00 refs=0
                      function 1 17/180/252 identity=104:a0aea06fe11f292c version=1 keySchema=8:09da8010bedba854 map=17/24/156 entries=2
                        + 0600000061006300630074002d003200 ->  refs=1 14/0/96/96/k65/c0/r1/f0
                    """,
                    """
                    complete: puts=6 window roots=1 incremental=0 visited=2 imaged=2 removed=0 probes=0 | function roots=2 incremental=0 visited=4 imaged=4
                    acct-1: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=2 incremental=1 visited=3 imaged=3
                    acct-2 next day: puts=3 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=2 incremental=1 visited=3 imaged=3
                    """
            );
        });
    }

    @Test
    public void testFusedWindowState() throws Exception {
        // (c) Seal 1 is complete. Seal 2 is incremental over two keys, one of which takes a
        // NULL amount: its fused payload comes out byte-equal and must be elided. Seal 3 moves
        // a key's anchor, which rules the elision out without a probe.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute(FUSED_VIEW);
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitFusedFixture(job, golden);
                Assert.assertTrue(window().isWindowStateFused());
                // (l) The overlay image of the same runtime, which the fallback repair stages.
                golden.overlay = overlayHex(window());
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    meta/m.0000000000000001 475 d23fbfeb6032a8cbd9192b2501de925be08852dfacbec06a41b1b03437f5ee30
                    meta/m.0000000000000002 144 705c71f38219329083f055a882b466408e72823a10565656c7e5cc59efb7085b
                    meta/m.0000000000000006 475 49be63b0136c85448e98239a05fbb0f08824f5ece2071fdfd5d4b0e3f75f75f7
                    meta/m.0000000000000007 144 7bd0a0f7eca79d768b4a69190bf24ed5dd9471e216025d23b5c48c840835ed9e
                    meta/m.0000000000000008 160 e07377704447275f452884a687ed9f48c3fa5551cfa26e37e2d26c39a531401c
                    meta/m.0000000000000009 320 ba8b7a9d6eab8fc56179746b602be2725a8a0e2197f97ef2070718abd0034e8e
                    meta/m.0000000000000011 475 b06719f3daa3d337879acc1e3fae34db24bb105ec81cea1239e5126620f42f84
                    meta/m.0000000000000012 144 d7cd836cf14e686e2407020baa632e369e8ed3573d2534bb3d94d24b774ba9b5
                    meta/m.0000000000000013 220 d7037d6f07775de50fc00c7f74cdbccc36fcaab686b9915bb8034cae16a5614f
                    meta/m.0000000000000014 480 37cace6a33bc294d3fae9487a27178f75f2eb4cd6e93e5aea60a1e0db6f045fb
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258120000000 root=2/44/100
                      window 1/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=1/24/200 entries=3
                        + 0600000061006300630074002d003100 -> 0040204648470600000000000000144001000000000000000000000000001440 refs=0
                        + 0600000061006300630074002d003200 -> 00402046484706000000000000001c4001000000000000000000000000001c40 refs=0
                        + 0600000061006300630074002d003300 -> 0040204648470600000000000000f03f0100000000000000000000000000f03f refs=0
                    boundary checkpoint=1 maxTs=1767258660000000 root=7/44/100
                      window 6/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=6/24/200 entries=3
                        + 0600000061006300630074002d003200 -> 0040204648470600000000000000244002000000000000000000000000001c40 refs=0
                    boundary checkpoint=2 maxTs=1767344400000000 root=12/44/100
                      window 11/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=11/24/200 entries=3
                        + 0600000061006300630074002d003300 -> 00a0f7635c470600000000000000004001000000000000000000000000000040 refs=0
                    """,
                    """
                    complete: puts=3 window roots=1 incremental=0 visited=3 imaged=3 removed=0 probes=0 | function roots=0 incremental=0 visited=0 imaged=0
                    incremental, acct-1 unchanged: puts=1 window roots=1 incremental=1 visited=2 imaged=2 removed=0 probes=2 | function roots=0 incremental=0 visited=0 imaged=0
                    anchor moved: puts=1 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=0 incremental=0 visited=0 imaged=0
                    """
            );
            assertGolden(
                    "overlay snapshot",
                    "010000007700010000000b000000080000001800000003000000000000000600000061006300630074002d0031000040"
                            + "2046484706000000000000001440010000000000000000000000000014400600000061006300630074002d0032000040"
                            + "204648470600000000000000244002000000000000000000000000001c400600000061006300630074002d00330000a0"
                            + "f7635c470600000000000000004001000000000000000000000000000040",
                    golden.overlay
            );
        });
    }

    @Test
    public void testGroupedMembersPastTheLeafBudget() throws Exception {
        // (e) Sixteen sums overflow the leaf, so the last sum, count(*) and the guarded
        // count(account_id) are runtime-only members, each on a function root of its own.
        // (j) The NULL-key partition's guarded count image is zero, so its second seal
        // elides it, while the NULL-amount row leaves acct-1's leaf and sum member unchanged.
        assertMemoryLeak(() -> {
            final StringBuilder ddl = new StringBuilder();
            final StringBuilder projections = new StringBuilder();
            final StringBuilder values = new StringBuilder();
            final StringBuilder nulls = new StringBuilder();
            for (int i = 1; i <= GROUPED_SUMS; i++) {
                ddl.append(", q").append(i).append(" DOUBLE");
                projections.append(", sum(q").append(i).append(") OVER w AS s").append(i);
                values.append(", ").append(i).append(".5");
                nulls.append(", NULL");
            }
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL" + ddl + ") "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT created_at, account_id"
                    + projections + ", count(*) OVER w AS n, count(account_id) OVER w AS c FROM tx" + DAILY_WINDOW);
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                golden.commit(job, "complete", "('2026-01-01T09:00:00.000000Z', 'acct-1'" + values
                        + "), ('2026-01-01T09:01:00.000000Z', NULL" + values + ")");
                final LiveViewWindowStatePlan plan = window().getCheckpointWindowStatePlan();
                Assert.assertNotNull("the group must be fused", plan);
                int runtimeOnly = 0;
                int guarded = 0;
                for (int i = 0, n = plan.getProjectionCount(); i < n; i++) {
                    if (!plan.isDurableProjection(i)) {
                        runtimeOnly++;
                        if (plan.getProjection(i).isPartitionKeyGuarded()) {
                            guarded++;
                        }
                    }
                }
                Assert.assertEquals("runtime-only members", 3, runtimeOnly);
                Assert.assertEquals("guarded runtime-only members", 1, guarded);
                golden.commit(job, "incremental", "('2026-01-01T09:10:00.000000Z', NULL" + values
                        + "), ('2026-01-01T09:11:00.000000Z', 'acct-1'" + nulls + ")");
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    meta/m.0000000000000001 1407 caa89d4bc82bce7eb30b9152fe8536a92eeabcd7c15fd5ff474c448f516ec9cf
                    meta/m.0000000000000002 339 bd06eb81c1b5e1aa4657b371b35928ca5116bff1de350e33d11db7764817ccb4
                    meta/m.0000000000000003 325 5a278eb9d0cf64217db31a047643fdcfbc34ec54eb0cdf67825c5bc944e0c134
                    meta/m.0000000000000004 327 327687f46ed224af52788ae2fcbc707cb12ccc1d6e1015c053e7415fd6253918
                    meta/m.0000000000000005 555 bd851cb79c1d03077a584fc51be715e7af76b429bb4b503050554a4c89240058
                    meta/m.0000000000000006 100 71be3417e5ecda2b1039ddb01fbfca648e778961b4365c9be82d32e42573ff10
                    meta/m.0000000000000007 280 26d36cc023ef08477d73af271456f1c1a299c02cf740963cefc7dc75bdf28c33
                    meta/m.0000000000000009 1407 2db0f888614bf60a024b6d974cda3b85a396c97be1fbe1f24a13f32050177753
                    meta/m.0000000000000010 339 25c052490500ec6e84df62591b78acc62a5eee5ac9f2f2da43ab455d4bd64fd7
                    meta/m.0000000000000011 325 897a0655cd5b9602ef15e3ae79924b7110b1f3e24cdd4f0f7884f40051dfbf0e
                    meta/m.0000000000000012 327 63d8ccb6e3970df35438cbbfcd56cc72606a2a24f20754c2bc1718adb6ef5140
                    meta/m.0000000000000013 555 285adab3befd4e6799a7433423fbb33720390b80f68fd05fa2fccb45a7e9983e
                    meta/m.0000000000000014 160 e92310aef601765fc6e86d90aca6ad39d552196cbe80872ba366e76075d483c3
                    meta/m.0000000000000015 560 d68bcf7e9fc41e40530fe3c5956a3974bf5339edd6706f28a49b3421f72cb37e
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258060000000 root=5/431/124
                      window 1/584/823 identity=55:a792d92f68f6dfa8 anchorType=8 inline=248 keySchema=8:09da8010bedba854 manifest=684:b2db8ef440df5e26 map=1/24/560 entries=2
                        + 0600000061006300630074002d003100 -> 0040204648470600000000000000f83f0100000000000000000000000000044001000000000000000000000000000c4001000000000000000000000000001240
                              0100000000000000000000000000164001000000000000000000000000001a4001000000000000000000000000001e4001000000000000000000000000002140
                              01000000000000000000000000002340010000000000000000000000000025400100000000000000000000000000274001000000000000000000000000002940
                              01000000000000000000000000002b4001000000000000000000000000002d4001000000000000000000000000002f400100000000000000 refs=0
                        + ffffffff -> 0040204648470600000000000000f83f0100000000000000000000000000044001000000000000000000000000000c4001000000000000000000000000001240
                              0100000000000000000000000000164001000000000000000000000000001a4001000000000000000000000000001e4001000000000000000000000000002140
                              01000000000000000000000000002340010000000000000000000000000025400100000000000000000000000000274001000000000000000000000000002940
                              01000000000000000000000000002b4001000000000000000000000000002d4001000000000000000000000000002f400100000000000000 refs=0
                      function 0 2/120/219 identity=103:060df75a1c635f12 version=1 keySchema=8:09da8010bedba854 map=2/24/96 entries=2
                        + 0600000061006300630074002d003100 -> 00000000008030400100000000000000 refs=0
                        + ffffffff -> 00000000008030400100000000000000 refs=0
                      function 1 3/104/221 identity=105:602dabb73b94b2f8 version=1 keySchema=8:09da8010bedba854 map=3/24/80 entries=2
                        + 0600000061006300630074002d003100 -> 0100000000000000 refs=0
                        + ffffffff -> 0100000000000000 refs=0
                      function 2 4/104/223 identity=107:e2a2dce5247dd6ed version=1 keySchema=8:09da8010bedba854 map=4/24/80 entries=2
                        + 0600000061006300630074002d003100 -> 0100000000000000 refs=0
                        + ffffffff -> 0000000000000000 refs=0
                    boundary checkpoint=1 maxTs=1767258660000000 root=13/431/124
                      window 9/584/823 identity=55:a792d92f68f6dfa8 anchorType=8 inline=248 keySchema=8:09da8010bedba854 manifest=684:b2db8ef440df5e26 map=9/24/560 entries=2
                        + ffffffff -> 004020464847060000000000000008400200000000000000000000000000144002000000000000000000000000001c4002000000000000000000000000002240
                              0200000000000000000000000000264002000000000000000000000000002a4002000000000000000000000000002e4002000000000000000000000000003140
                              02000000000000000000000000003340020000000000000000000000000035400200000000000000000000000000374002000000000000000000000000003940
                              02000000000000000000000000003b4002000000000000000000000000003d4002000000000000000000000000003f400200000000000000 refs=0
                      function 0 10/120/219 identity=103:060df75a1c635f12 version=1 keySchema=8:09da8010bedba854 map=10/24/96 entries=2
                        + ffffffff -> 00000000008040400200000000000000 refs=0
                      function 1 11/104/221 identity=105:602dabb73b94b2f8 version=1 keySchema=8:09da8010bedba854 map=11/24/80 entries=2
                        + 0600000061006300630074002d003100 -> 0200000000000000 refs=0
                        + ffffffff -> 0200000000000000 refs=0
                      function 2 12/104/223 identity=107:e2a2dce5247dd6ed version=1 keySchema=8:09da8010bedba854 map=12/24/80 entries=2
                        + 0600000061006300630074002d003100 -> 0200000000000000 refs=0
                    """,
                    """
                    complete: puts=8 window roots=1 incremental=0 visited=2 imaged=2 removed=0 probes=0 | function roots=3 incremental=0 visited=2 imaged=6
                    incremental: puts=5 window roots=1 incremental=1 visited=2 imaged=2 removed=0 probes=2 | function roots=3 incremental=3 visited=2 imaged=6
                    """
            );
        });
    }

    @Test
    public void testInlineFunctionState() throws Exception {
        // (a) An eight-byte whole-state image inlined into the function root's leaf.
        assertStubSeals(
                false,
                """
                meta/m.0000000000000001 324 99ce57e6bb1ada1df5bf2d40151488f6aeb8dbcb29ab165d44a4c09adeae972e
                meta/m.0000000000000002 252 b74ff710b4f5ce607e53224b8036d08dd6bed04c11333c514dce14892df200aa
                meta/m.0000000000000003 100 72fa1be2af2731223f7936e82a840e0713f22f63a15659b9ccd8415595395b8d
                meta/m.0000000000000004 160 913f65505c561e9ce5b02d5e892eae6657b42c2ae7e3a8b8e1793e9df0a50f3f
                meta/m.0000000000000006 352 36ac340018fd5289d7e0fd96c5f0967335a45d8be81fe80aa070d8ed10962d0d
                meta/m.0000000000000007 252 95bfcd9dcfa85c1ddaff93fadfef9f2f0fe92acfc74619b6fd7f0842deda30e2
                meta/m.0000000000000008 160 a681a0b2a079cf92fbcb922c49e21ea408ae83abd58cf82e4049ca703ff1fcbf
                meta/m.0000000000000009 320 75b0bc5d4e50b7f98beb501eed770091eafa4f265ad8d7c0bb2638c0d224a5dd
                meta/m.0000000000000011 236 ada6a77212a51c924a944b30c79f85d8a7f67220f9b480c072c3d83811a93c44
                meta/m.0000000000000012 260 0cf48e580abd455e49bf24de64c6b364c30005560b0318720f802dec9575c827
                meta/m.0000000000000013 220 08d5992553ff7e1a2e499646bef8ca4b5b123d3b6e87e3b3c8d2c2330609ae67
                meta/m.0000000000000014 480 8d6f1c1846500eb11ebfe856066c7b91625b0c8738b4d74a110033b160ced4ec
                """,
                """
                boundary checkpoint=0 maxTs=1000000 root=2/152/100
                  function 0 1/128/196 identity=84:942acc1112ec06fb version=1 keySchema=4:df3f619804a92fdb map=1/24/104 entries=3
                    + 0100000000000000 -> 0807060504030201 refs=0
                    + 0200000000000000 -> 1817161514131211 refs=0
                    + 0300000000000000 -> fdffffffffffffff refs=0
                boundary checkpoint=1 maxTs=2000000 root=7/152/100
                  function 0 6/156/196 identity=84:942acc1112ec06fb version=1 keySchema=4:df3f619804a92fdb map=6/24/132 entries=4
                    + 0200000000000000 -> 2827262524232221 refs=0
                    + 0400000000000000 -> 0000000000000080 refs=0
                boundary checkpoint=2 maxTs=3000000 root=12/152/108
                  function 0 11/24/212 identity=84:942acc1112ec06fb version=1 keySchema=4:df3f619804a92fdb map=6/24/132 entries=4
                """,
                """
                complete: puts=3 function roots=1 incremental=0 visited=3 imaged=3
                one moved, one new: puts=2 function roots=1 incremental=0 visited=4 imaged=4
                unmoved: puts=0 function roots=1 incremental=0 visited=4 imaged=4
                """
        );
    }

    @Test
    public void testKeyDomainLimitedLocalizedRebuild() throws Exception {
        // (h) A ROWS frame beside a minute-anchored fused window. A late row makes a
        // localized rebuild, which is not chained, and a ROWS replay does not reconstruct
        // every key, so the capture freezes the window state limited to Q, the keys with a
        // row in the re-emitted interval. The cold keys the primary runtime still walks sit
        // outside Q: their payload is NO_PAYLOAD and the old root's entry stands.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT created_at, account_id, "
                    + "sum(amount) OVER w AS a, sum(amount) OVER (PARTITION BY account_id ORDER BY created_at "
                    + "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r FROM tx "
                    + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR EXPRESSION timestamp_floor('1m', created_at))");
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                golden.commit(job, "every key", "('2026-01-01T00:00:10.000000Z', 'acct-0', 1.0), "
                        + "('2026-01-01T00:00:10.000000Z', 'acct-1', 2.0), ('2026-01-01T00:00:10.000000Z', 'acct-2', 3.0)");
                for (int second = 20; second <= 150; second += 10) {
                    golden.commit(job, "hot " + second, "('2026-01-01T00:" + String.format("%02d:%02d", second / 60, second % 60)
                            + ".000000Z', 'acct-0', " + second + ".0)");
                }
                golden.commit(job, "late hot", "('2026-01-01T00:00:55.000000Z', 'acct-0', 9000.0)");
                golden.recordRoute();
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    data/d.0000000000000000 144 046fdc8408b79ce9e7c626e85f8e1e973d3c0e1a76fda32c57930ac219dcbeea
                    data/d.0000000000000006 48 c840f18ae9b12d808b3495d30e15515aa64c3d9e123985cb44a0f2a5a01ff6b9
                    data/d.0000000000000012 48 a6fa3ef1284d120c741a71b4ce6f146fb45e1b68efe0f79a53c7b592953a231b
                    data/d.0000000000000018 48 e4972237a633c230d752657aae0531f5841a3aec7ea61c4dabdd152b8a4c6aa9
                    data/d.0000000000000024 48 3736ab889ae56c1c33e8529e766972685fca01b28dabe335515c4c740e14d567
                    data/d.0000000000000030 48 01d3dacf4ffc9b9174d51fdb99ebdec94f498947ae7ec8b7727aa96aebd3f07f
                    data/d.0000000000000036 48 70d0b614c1cdcabdfb3fe6a3c5a52d4c15021cde6c7fdc359e1cab05541b5230
                    data/d.0000000000000042 48 3f375bdd94a9af224c59702608dfa6af01b89b18db96e8424852515eaa6d62f3
                    data/d.0000000000000048 48 89b76f8d943e9b8451754ac087a42f5dd06f933ef1e3972a3bc7fda74a7250da
                    data/d.0000000000000054 48 46e1d7bff71cdf2c690da0106fdf74dbd3e2b8a4a7cfbd86a1ae27c94a7e5c0b
                    data/d.0000000000000060 48 543dd11eaaaaf0bf2af4cb4318d07473b91ab0174091e9aaeb2bd254a2d2933d
                    data/d.0000000000000066 48 9bd541f4f43b27301d8b4532452509c67d561869d0e084d8c8889616633f8606
                    data/d.0000000000000072 48 a3bd286746d662f362fb1098b4213b140ab8619380b62d35e225ac4424cb1b08
                    data/d.0000000000000078 48 16561d682a04799f9119f0302c3b2ad062ade1d2c11000ab597cb62f2a3af7d6
                    data/d.0000000000000084 48 30a9e6dc6ac70deb3a62f6ec094c9bb0cbdf6e177ab4768f10755dff8bcfc1a2
                    data/d.0000000000000090 144 6ab8ca4e9e235ba36b1f79933be07a5c26398935bfc977b21b248a6ed0f103c7
                    meta/m.0000000000000001 407 a1a3ed2b94bcc127ec06a45af868dd6905bc56f4c84c2d5b7cba2739b491dbdc
                    meta/m.0000000000000002 482 d48c965728647b186b2421a4e064faace5612b60c6e8d712d064977acb7f4129
                    meta/m.0000000000000003 286 d6156f265db45ac32e506a70f413f4d35728b4ba41a2eba730a2cb3a571542d1
                    meta/m.0000000000000007 407 0d93935a2c62b3a32e4fc193a8b00d41331e5608088ef562b923b41dc506ebd3
                    meta/m.0000000000000008 498 c7f1a745c750b554fe7f466874f7c99d9511787b1572d01465cd98f066dbf6fd
                    meta/m.0000000000000009 294 51bfc1abb50031758374324005cd6b3ef200d4ac0bd5a7def07008d6b206726f
                    meta/m.0000000000000013 407 7534a69b716c0ae14aea10e765dcd48f66998e4ce58adce83c6cf32010f153d8
                    meta/m.0000000000000014 498 26e48ec8f14496f4cb2408d3de06b0f5a339553b72c79d7e4c3f4d5ef3a3a67f
                    meta/m.0000000000000015 294 e79b9f12a4fe7d6bf8418652821790c77b966dab978c2191e091a851ad75eddb
                    meta/m.0000000000000019 407 fb045ac38e1e4496b73ab54e01e304c01d769f5844771fddba07fd5d93bf0890
                    meta/m.0000000000000020 498 cfe22a10af92c32299c23afc27f74408c854a3eb8434cd507b72c2a1821f4d78
                    meta/m.0000000000000021 294 5a5d74830f86f205208a818734a8d6e948ead683af88de1a6f553da31b4d9898
                    meta/m.0000000000000025 407 ae576fcdcb1ca77fc4fdd3c4896062df49033f147c9323a62e8ad2825950a110
                    meta/m.0000000000000026 498 d0ef28e7fda9fabb43003aac147ad25b167d55fe4e11cd89bc55ec144008cc2a
                    meta/m.0000000000000027 294 cc1bf43989024768e68a2af78002f7c5ec4ac3463592b8c0188aee340deddb17
                    meta/m.0000000000000031 407 264d2cafac2f6fcc5cc675a91431c8445f4920fb29f29bcec14bad8499d5b86b
                    meta/m.0000000000000032 498 a2380ff823fb583caa4d35fac4aad8d667c90e6d2aaa1ee0cc9b40adf85c2b48
                    meta/m.0000000000000033 294 208aade958f46693f43dd76e2e9773d80bb7adaddd3efb1209ea25a54ef716f7
                    meta/m.0000000000000037 407 e438a4d7966c7ac4b32727398368bdb4d4d55bd93679bb3688c19de695a79c20
                    meta/m.0000000000000038 498 ba51571157b52fcfdc865c355600449b579eb8e0499dc5f71bb521c836ac5d6a
                    meta/m.0000000000000039 294 98544122f1eac2383fc02cde389ae7a80e79ef44c9e80b5c7cc8316fe66dfb60
                    meta/m.0000000000000043 407 4bd807b392280e0be7a5ad2a2a2523632793397fcfb3483aab3d817ba9dadf5b
                    meta/m.0000000000000044 498 65a930279c0f8cea8c05a28dede6a4074dcdc2f3f698a900655ccb195d41e8fe
                    meta/m.0000000000000045 294 211923c5f7bee123096f2aa4aff6813b1d7881a793374429f86cd620ef6adecd
                    meta/m.0000000000000049 407 d629224e603ed24bca8e851e89c577857c8af53287cd9cb31f5c6c9a1e0d5227
                    meta/m.0000000000000050 498 6bb0fbfbc629b21565927674cdd950c2f99192f8085857443ce4f0929f5cd94b
                    meta/m.0000000000000051 294 33ce7dde661e4084e626a63082eaacaed55d1f013d6dcd0e0cae0802605c2a5d
                    meta/m.0000000000000055 407 b3bfab9149dffe9a2d256d6dc9eeb34694d8e72f93e6f7281570e6a712f3ebbf
                    meta/m.0000000000000056 498 11eaf8208b9275237a175adb9e3f4b259a2bc0010f46cae15e35544dea2c384e
                    meta/m.0000000000000057 294 877f69821c19a02bb86f24be5e9c93fc36334738ce5566984a846e0ebf596a44
                    meta/m.0000000000000061 407 b8e76a16edc9b3d159b8f57ea9d5200c47ecfdf6c17605287112e6b16f688e44
                    meta/m.0000000000000062 498 a1fc575f050e0d65b0bab658e0ad3b6928297c0dc8752ea111788f854e5fbb69
                    meta/m.0000000000000063 294 fa7d63d9d0587f42e592cf503248e4235f16a135e61f50b4dc9939e07c215be1
                    meta/m.0000000000000067 407 d3227d4cde38c9ca08972ffbde74f0d700e7741ac7dcf29126b29b55fd55153a
                    meta/m.0000000000000068 498 8ec7c39c2204357bbc7135798324aa26b735f100388bc536d2f989a6ec78c369
                    meta/m.0000000000000069 294 3af48dbd32a84a5a1501fc72dd001aeaa92d1511789ab9683961e308dfe93a3c
                    meta/m.0000000000000073 407 08ad3075d2a07cb7a311f892fa9aa5280ace5deeacb8417636f1fb992578f217
                    meta/m.0000000000000074 498 1a513e1192f6b01dc7971a840f02b3fd0178243ef276281911a24af68ef290e9
                    meta/m.0000000000000075 294 155502717f31eb3dcd4d1a7a6b37a9b0000ce7036481ed36756b648b12edfcab
                    meta/m.0000000000000079 407 e54256376b3c9ff971413800957aaeba3ac61fa6dffaef93c16e31f319629c07
                    meta/m.0000000000000080 498 5eeed3a64af4f3f471e478581546a1db0f6cdb0f8c0898913231e36b1eff4ddf
                    meta/m.0000000000000081 294 d2eae0150944788440b3399d5e3022f1c062cad1a90b37f936fe03fc45f2b4d4
                    meta/m.0000000000000085 407 affbb365738fcea5cf7eb2f9b39c035620daf6c71fa69bb5220d9de6052c1fde
                    meta/m.0000000000000086 498 5ce71f4772c376599b828791802bd4612e19d1744782d50cff67396d41a93463
                    meta/m.0000000000000087 294 da4faee7b9ed6fcc2d0e6339155895ef081ea94f7a24d3f360a17f083ae25bd7
                    meta/m.0000000000000088 940 7a55e94adf837cd6a9e53b539eb30a58f55e35c10656a217144f935865668e77
                    meta/m.0000000000000089 2728 8ca1be621f89a029db2773abd50dab7623d00ff43866ea28d58b6aef4aa397ef
                    meta/m.0000000000000091 2115 9404a1852e515bdef496eeda3431210421b1fabdaf50798eb8f8edd202079246
                    meta/m.0000000000000092 834 45dab6eea5d8c72218ccc6e1e622cb41d469d2b06cbbade2d0cb8bca990c2933
                    meta/m.0000000000000093 940 a9506d120e14f9b526c57f7c2256e1a32f06751b17bb486764cadd59294b6638
                    meta/m.0000000000000094 64 f4ee6adc8ca90e9c1ede4c7507882ebedeca1cf5554c5677f7fdcb6baebe23e1
                    meta/m.0000000000000095 2968 1da2dac3cc77b0553f5be721763c1524a9e64505c67ce11de74038736b2dbc3e
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767225610000000 root=3/170/116
                      window 1/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=1/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 0040204648470600000000000000f03f0100000000000000 refs=0
                        + 0600000061006300630074002d003100 -> 004020464847060000000000000000400100000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 004020464847060000000000000008400100000000000000 refs=0
                      function 0 2/248/234 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=2/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 0/0/48/48/k65/c0/r1/f0
                        + 0600000061006300630074002d003100 ->  refs=1 0/48/48/48/k65/c0/r1/f0
                        + 0600000061006300630074002d003200 ->  refs=1 0/96/48/48/k65/c0/r1/f0
                    boundary checkpoint=1 maxTs=1767225620000000 root=9/170/124
                      window 7/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=7/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 004020464847060000000000000035400200000000000000 refs=0
                      function 0 8/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=8/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 6/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=2 maxTs=1767225630000000 root=15/170/124
                      window 13/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=13/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 004020464847060000000000008049400300000000000000 refs=0
                      function 0 14/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=14/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 12/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=3 maxTs=1767225640000000 root=21/170/124
                      window 19/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=19/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00402046484706000000000000c056400400000000000000 refs=0
                      function 0 20/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=20/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 18/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=4 maxTs=1767225650000000 root=27/170/124
                      window 25/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=25/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00402046484706000000000000a061400500000000000000 refs=0
                      function 0 26/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=26/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 24/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=5 maxTs=1767225660000000 root=92/170/124
                      window 91/24/223 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=31/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00c7b349484706000000000000004e400100000000000000 refs=0
                      function 0 91/471/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=91/247/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 90/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=6 maxTs=1767225670000000 root=92/440/124
                      window 91/721/223 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=37/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00c7b3494847060000000000004060400200000000000000 refs=0
                      function 0 91/1168/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=91/944/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 90/48/48/48/k65/c0/r1/f0
                    boundary checkpoint=7 maxTs=1767225680000000 root=92/710/124
                      window 91/1418/223 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=43/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00c7b349484706000000000000406a400300000000000000 refs=0
                      function 0 91/1865/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=91/1641/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 90/96/48/48/k65/c0/r1/f0
                    boundary checkpoint=8 maxTs=1767225690000000 root=51/170/124
                      window 49/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=49/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00c7b349484706000000000000c072400400000000000000 refs=0
                      function 0 50/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=50/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 48/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=9 maxTs=1767225700000000 root=57/170/124
                      window 55/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=55/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00c7b3494847060000000000000079400500000000000000 refs=0
                      function 0 56/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=56/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 54/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=10 maxTs=1767225710000000 root=63/170/124
                      window 61/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=61/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 00c7b349484706000000000000e07f400600000000000000 refs=0
                      function 0 62/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=62/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 60/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=11 maxTs=1767225720000000 root=69/170/124
                      window 67/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=67/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 004e474d484706000000000000005e400100000000000000 refs=0
                      function 0 68/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=68/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 66/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=12 maxTs=1767225730000000 root=75/170/124
                      window 73/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=73/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 004e474d484706000000000000406f400200000000000000 refs=0
                      function 0 74/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=74/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 72/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=13 maxTs=1767225740000000 root=81/170/124
                      window 79/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=79/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 004e474d4847060000000000006078400300000000000000 refs=0
                      function 0 80/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=80/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 78/0/48/48/k65/c0/r1/f0
                    boundary checkpoint=14 maxTs=1767225750000000 root=87/170/124
                      window 85/200/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=85/24/176 entries=3
                        + 0600000061006300630074002d003000 -> 004e474d484706000000000000e080400400000000000000 refs=0
                      function 0 86/248/250 identity=102:161f4e7b86bb4715 version=1 keySchema=8:09da8010bedba854 map=86/24/224 entries=3
                        + 0600000061006300630074002d003000 ->  refs=1 84/0/48/48/k65/c0/r1/f0
                    """,
                    """
                    every key: puts=6 window roots=1 incremental=0 visited=3 imaged=3 removed=0 probes=0 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 20: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 30: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 40: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 50: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 60: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 70: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 80: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 90: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 100: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 110: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 120: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 130: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 140: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    hot 150: puts=2 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=1 | function roots=1 incremental=0 visited=3 imaged=3
                    late hot: puts=2 window roots=3 incremental=0 visited=9 imaged=9 removed=0 probes=0 | function roots=3 incremental=0 visited=9 imaged=3
                    route: checkpoint_repair_plan,checkpoint_repair_last_disposition,checkpoint_repair_last_denial|rows+anchor,localized rebuild,|
                    """
            );
        });
    }

    @Test
    public void testKeyedOpenSegmentResumeAndTransplant() throws Exception {
        // (k) A keyed open-segment resume: a non-chained repair limited to Q = {acct-1},
        // replayed through an isolated runtime that holds acct-1 alone. Its result is
        // transplanted back into the primary runtime, and the head seal after it images the
        // transplanted key beside one the transplant did not touch.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL NOCACHE INDEX CAPACITY 4, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
            final StringBuilder seed = new StringBuilder();
            for (int day = 2; day <= 3; day++) {
                for (int hour = 0; hour < 10; hour++) {
                    for (int account = 1; account <= 4; account++) {
                        if (seed.length() > 0) {
                            seed.append(", ");
                        }
                        seed.append(keyedRow(day, hour, account * 10, account));
                    }
                }
            }
            execute("INSERT INTO tx VALUES " + seed);
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                    + "SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum FROM tx" + DAILY_WINDOW);
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                golden.record(job, "seed");
                for (int hour = 0; hour < 10; hour++) {
                    final StringBuilder rows = new StringBuilder();
                    for (int account = 1; account <= 4; account++) {
                        if (rows.length() > 0) {
                            rows.append(", ");
                        }
                        rows.append(keyedRow(4, hour, account * 10, account));
                    }
                    golden.commit(job, "d4 h" + hour, rows.toString());
                }
                golden.commit(job, "keyed resume", keyedRow(4, 2, 35, 1));
                Assert.assertEquals("the resume must follow the correction's keys", 1, job.openSegmentKeyedResumeCountForTest());
                Assert.assertTrue("the corrected keys must be transplanted", job.transplantedKeyCountForTest() > 0);
                golden.recordRoute();
                golden.commit(job, "head seal", keyedRow(4, 10, 10, 1) + ", " + keyedRow(4, 10, 20, 2));
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    meta/m.0000000000000001 459 4fb7ad27c8909f0b70ee1319127b9d49ed3f53b7912f0ac105b5fbe880ba7655
                    meta/m.0000000000000002 144 60afb41f4a42e700208390306d15666132d9e183d754f6aad5bed7ef7eb82e76
                    meta/m.0000000000000006 459 ae096aadcc57fe3b51e07584a2b37d8e648148cdde23a8a387ecef00f81deacf
                    meta/m.0000000000000007 144 b3138c61756c333d11b367e29bafb6a06476794b0b9ffae0282e828f95fc3b19
                    meta/m.0000000000000011 459 b5f6d82954e648acf9e7261de7d1b206b546b10a5930eb424980ef2860dd7210
                    meta/m.0000000000000012 144 21fd11a375d4b673e0ae7f7cff7c62c975bbc1ad965375d709144c09f35afa09
                    meta/m.0000000000000056 3504 39c826c14bfae532ed7e7a0fcc2ff78bc66ddd451eb60e08a40e15cbfe990237
                    meta/m.0000000000000057 984 4a481d80a2c0a6ed857191dbd845c451517021f90b8d1e47487fd730c7458bd1
                    meta/m.0000000000000058 700 a22b9f25396385d78cf81c7db992b43eae7ecef30e00610b5dbd084b02a68f9d
                    meta/m.0000000000000059 1280 3797253665aca44bb64ba8c3d6f121f32b06625a13723e50050200dde183f39c
                    meta/m.0000000000000061 459 9309006687ceb290705782b3ef6321353ec27aebe07f56b3e4de676a078379c6
                    meta/m.0000000000000062 144 8fb77e3068cd0439f93c290d9f4d1a28b25180114190f7dbd27668bbcece47df
                    meta/m.0000000000000063 760 30b2086c91a771eecd10a7fd899992ea58c2f529069a78aa4b7b99638e0c4ac3
                    meta/m.0000000000000064 1280 2644569be8cf372a81899439b5d8fa69c2b96a2b6be317c03c1e91e84efa5ed3
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767433200000000 root=2/44/100
                      window 1/252/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=1/24/228 entries=4
                        + 0600000061006300630074002d003100 -> 0000cf817047060000000000000024400a00000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0000cf817047060000000000000024400a00000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0000cf817047060000000000000024400a00000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0000cf817047060000000000000024400a00000000000000 refs=0
                    boundary checkpoint=1 maxTs=1767487200000000 root=7/44/100
                      window 6/252/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=6/24/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f84470600000000000000f03f0100000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f84470600000000000000f03f0100000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f84470600000000000000f03f0100000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f84470600000000000000f03f0100000000000000 refs=0
                    boundary checkpoint=2 maxTs=1767490800000000 root=12/44/100
                      window 11/252/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=11/24/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000000400200000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000000400200000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000000400200000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000000400200000000000000 refs=0
                    boundary checkpoint=3 maxTs=1767494400000000 root=57/44/100
                      window 56/252/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/24/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000010400400000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000008400300000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000008400300000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000008400300000000000000 refs=0
                    boundary checkpoint=4 maxTs=1767498000000000 root=57/164/100
                      window 56/687/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/459/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000014400500000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000010400400000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000010400400000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000010400400000000000000 refs=0
                    boundary checkpoint=5 maxTs=1767501600000000 root=57/284/100
                      window 56/1122/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/894/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000018400600000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000014400500000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000014400500000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000014400500000000000000 refs=0
                    boundary checkpoint=6 maxTs=1767505200000000 root=57/404/100
                      window 56/1557/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/1329/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f844706000000000000001c400700000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000018400600000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000018400600000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000018400600000000000000 refs=0
                    boundary checkpoint=7 maxTs=1767508800000000 root=57/524/100
                      window 56/1992/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/1764/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000020400800000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f844706000000000000001c400700000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f844706000000000000001c400700000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f844706000000000000001c400700000000000000 refs=0
                    boundary checkpoint=8 maxTs=1767512400000000 root=57/644/100
                      window 56/2427/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/2199/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000022400900000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000020400800000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000020400800000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000020400800000000000000 refs=0
                    boundary checkpoint=9 maxTs=1767516000000000 root=57/764/100
                      window 56/2862/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/2634/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000024400a00000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000022400900000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000022400900000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000022400900000000000000 refs=0
                    boundary checkpoint=10 maxTs=1767519600000000 root=57/884/100
                      window 56/3297/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=56/3069/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000026400b00000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000024400a00000000000000 refs=0
                        + 0600000061006300630074002d003300 -> 0060a69f8447060000000000000024400a00000000000000 refs=0
                        + 0600000061006300630074002d003400 -> 0060a69f8447060000000000000024400a00000000000000 refs=0
                    boundary checkpoint=11 maxTs=1767522000000000 root=62/44/100
                      window 61/252/207 identity=55:a792d92f68f6dfa8 anchorType=8 inline=24 keySchema=8:09da8010bedba854 manifest=68:e558fbaae13540a6 map=61/24/228 entries=4
                        + 0600000061006300630074002d003100 -> 0060a69f8447060000000000000028400c00000000000000 refs=0
                        + 0600000061006300630074002d003200 -> 0060a69f8447060000000000000026400b00000000000000 refs=0
                    """,
                    """
                    seed: puts=4 window roots=81 incremental=79 visited=84 imaged=84 removed=0 probes=72 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h0: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=0 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h1: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h2: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h3: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h4: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h5: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h6: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h7: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h8: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    d4 h9: puts=4 window roots=1 incremental=1 visited=4 imaged=4 removed=0 probes=4 | function roots=0 incremental=0 visited=0 imaged=0
                    keyed resume: puts=1 window roots=8 incremental=0 visited=8 imaged=8 removed=0 probes=0 | function roots=0 incremental=0 visited=0 imaged=0
                    route: checkpoint_repair_plan,checkpoint_repair_last_disposition,checkpoint_repair_last_denial|anchor,resume from anchor,resume cheaper|
                    head seal: puts=2 window roots=1 incremental=1 visited=2 imaged=2 removed=0 probes=2 | function roots=0 incremental=0 visited=0 imaged=0
                    """
            );
        });
    }

    @Test
    public void testPageBackedFunctionState() throws Exception {
        // (b) The same stub with an undeclared width, so each image is a data page the leaf
        // names by reference.
        assertStubSeals(
                true,
                """
                data/d.0000000000000000 24 07b1aaaae701d108472fb5f37fc8309a3a79da10ef0aad1aaff354b6963b08bb
                data/d.0000000000000005 16 8811b1b388df9a7c0a5a8e50780a2d97d4e6f2c4323f83c9be0319e876e8d110
                meta/m.0000000000000001 436 12d27eb0c17659510abe04c6eb25e395af2107fa4213390c58b236c996b33d41
                meta/m.0000000000000002 260 d4e2fe9c386f3eb78f9252203f14473745bd9f1e398a4d2576a817c25ea94f30
                meta/m.0000000000000003 100 a22e9476c5df5214ddc7be6b388887baf5a8f7e532e01b8d1fda37a8c34ad770
                meta/m.0000000000000004 200 dd75e98206ea8312c950eac00f225fda7322e6a4c2c26cc57339a4a303b8dfa3
                meta/m.0000000000000006 512 0ac91f0c095c4178c770acd6a12b790af269806ce4f1017a5d8db5e92aca7cab
                meta/m.0000000000000007 268 c59b3438e82cbf4d94d3f5abce7b4e0c86878cec2c118915ba81ff72aac8a1de
                meta/m.0000000000000008 160 9afbd58ab78cdc32dda3e4f3cfc872b6bcbd79ad5dc91ce4549f498cf35eca37
                meta/m.0000000000000009 400 d45da57880fdae1172b379d06314a914fad83136099ee273defa8a7fcedc2aa9
                meta/m.0000000000000011 268 428e70831cf97a2aecbb4c6fa257ddcd43c4f5b11398be1012d3c39f42e3461e
                meta/m.0000000000000012 276 1d7da0f26e9b30662f2897195f864d6e5aed0d1ff6069146e31809fd03a30560
                meta/m.0000000000000013 220 09ac6fc305edbc1cc4db54ed85272f108d5bd47268ec13998b9c5b55d5df99bf
                meta/m.0000000000000014 560 e751d4828078938ec5a94e3bfbf73dceb7d22bb597c86c80d07da528097846a3
                """,
                """
                boundary checkpoint=0 maxTs=1000000 root=2/152/108
                  function 0 1/224/212 identity=84:942acc1112ec06fb version=1 keySchema=4:df3f619804a92fdb map=1/24/200 entries=3
                    + 0100000000000000 ->  refs=1 0/0/8/8/k65/c0/r1/f0
                    + 0200000000000000 ->  refs=1 0/8/8/8/k65/c0/r1/f0
                    + 0300000000000000 ->  refs=1 0/16/8/8/k65/c0/r1/f0
                boundary checkpoint=1 maxTs=2000000 root=7/152/116
                  function 0 6/284/228 identity=84:942acc1112ec06fb version=1 keySchema=4:df3f619804a92fdb map=6/24/260 entries=4
                    + 0200000000000000 ->  refs=1 5/0/8/8/k65/c0/r1/f0
                    + 0400000000000000 ->  refs=1 5/8/8/8/k65/c0/r1/f0
                boundary checkpoint=2 maxTs=3000000 root=12/152/124
                  function 0 11/24/244 identity=84:942acc1112ec06fb version=1 keySchema=4:df3f619804a92fdb map=6/24/260 entries=4
                """,
                """
                complete: puts=3 function roots=1 incremental=0 visited=3 imaged=3
                one moved, one new: puts=2 function roots=1 incremental=0 visited=4 imaged=4
                unmoved: puts=0 function roots=1 incremental=0 visited=4 imaged=4
                """
        );
    }

    @Test
    public void testRingFunctionState() throws Exception {
        // (f) A RANGE first_value, whose state is a ring of chunk pages plus a scalar. Seal 2
        // carries both keys' earlier chunks forward by reference; seal 3 carries the key it
        // does not touch forward whole.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT created_at, account_id, "
                    + "first_value(amount) OVER (PARTITION BY account_id ORDER BY created_at "
                    + "RANGE BETWEEN '200' SECOND PRECEDING AND CURRENT ROW) AS f FROM tx");
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                golden.commit(job, "complete", ringRows(0, 130, 2));
                golden.commit(job, "carry forward", ringRows(130, 135, 2));
                golden.commit(job, "one key", ringRows(135, 140, 1));
                assertNoRefreshFaults("lv");
            }
            golden.assertView(
                    """
                    data/d.0000000000000000 454 ecb9192786d0d4e3aeb44c4a38f2cc4c6c1bac2f4f47afa5663cf79630f32e41
                    data/d.0000000000000005 100 f6ca6cb0bff219fb919617276e546be93eba5578dbb24a41cacce589b7739305
                    data/d.0000000000000010 475 fdbbd7cbc8ad614bbaab7c88f13955c9e88022ca37863b2467bbb864ee55ba09
                    meta/m.0000000000000001 570 8c413dc359925ca38fe8a018a088d2806ac57c839d3dbe25e7668dc1e7e3dc7e
                    meta/m.0000000000000002 294 9c48fb03f3d4a3574cd486cad712a5a347107029e895800ddaff8d2f89dd04dc
                    meta/m.0000000000000006 746 6f73d0610d4d05f7f34fbbeb8b3fb0f67cd78e532ec11029364d92b0b8451865
                    meta/m.0000000000000007 302 9456bffc9f39a29d1c4892e1cf37c6664270025138f4fb296c6b3808a57d6189
                    meta/m.0000000000000008 160 7de06d71ad621eb6bb1c6240527c5d3daef809c3eb8f0071542042ab013c249a
                    meta/m.0000000000000009 400 0da881b8b21eeea8f91a904efd1af582e85510a75849fc963bd5984b4dd5e9fa
                    meta/m.0000000000000011 570 efaaef09ac8e23cff4645c6de164c12201a6d0c47f4e5f2e48d0543dc0821d8d
                    meta/m.0000000000000012 294 d1f76a62ef1bc82225c580ab775ffdee4387e093f69aed7eb69fd8517dd738bb
                    meta/m.0000000000000013 220 3a497f8a12daedfa8dc9702004dd540edfa3cc601cd466bd0b5dc6a71ebc61a9
                    meta/m.0000000000000014 600 e6010263987524fa973a2cd4a9d1edade03c88fc11e6eb8023cbc7eb311bd156
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258129000000 root=2/186/108
                      function 0 1/320/250 identity=118:a97d19a577029b99 version=1 keySchema=8:09da8010bedba854 map=1/24/296 entries=2
                        + 010000006100 -> 01000001000000008200000000000000000000000000f87f8200000000000000406601d94f470600 refs=2 0/0/29/1040/k33/c1/r130/f0 0/29/198/1040/k34/c2/r130/f0
                        + 010000006200 -> 01000001000000008200000000000000000000000000f87f8200000000000000406601d94f470600 refs=2 0/227/29/1040/k33/c1/r130/f0 0/256/198/1040/k34/c2/r130/f0
                    boundary checkpoint=1 maxTs=1767258134000000 root=7/186/116
                      function 0 6/480/266 identity=118:a97d19a577029b99 version=1 keySchema=8:09da8010bedba854 map=6/24/456 entries=2
                        + 010000006100 -> 01000001000000008700000000000000000000000000f87f870000000000000080b14dd94f470600 refs=4 0/0/29/1040/k33/c1/r130/f0 0/29/198/1040/k34/c2/r130/f0 5/0/27/40/k33/c1/r5/f0 5/27/23/40/k34/c2/r5/f0
                        + 010000006200 -> 01000001000000008700000000000000000000000000f87f870000000000000080b14dd94f470600 refs=4 0/227/29/1040/k33/c1/r130/f0 0/256/198/1040/k34/c2/r130/f0 5/50/27/40/k33/c1/r5/f0 5/77/23/40/k34/c2/r5/f0
                    boundary checkpoint=2 maxTs=1767258139000000 root=12/186/108
                      function 0 11/320/250 identity=118:a97d19a577029b99 version=1 keySchema=8:09da8010bedba854 map=11/24/296 entries=2
                        + 010000006100 -> 01000001000000008c00000000000000000000000000f87f8c00000000000000c0fc99d94f470600 refs=2 10/0/29/1120/k33/c1/r140/f0 10/29/212/1120/k34/c2/r140/f0
                        + 010000006200 -> 01000001000000008700000000000000000000000000f87f870000000000000080b14dd94f470600 refs=2 10/241/29/1080/k33/c1/r135/f0 10/270/205/1080/k34/c2/r135/f0
                    """,
                    """
                    complete: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    carry forward: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    one key: puts=2 window roots=0 incremental=0 visited=0 imaged=0 removed=0 probes=0 | function roots=1 incremental=0 visited=2 imaged=2
                    """
            );
        });
    }

    @Test
    public void testUnfusedWindowState() throws Exception {
        // (d) The fused fixture with fusion off: the seal reads each component out of its
        // contributor's private map. The last seal finds two anchor keys the max contributor
        // does not hold, which writes that component's identity image (resetStateInto). No
        // SQL path this test found leaves a contributor without a key the anchor map holds,
        // so the case empties the contributor's map and puts the window on a complete
        // freeze, which is exactly the state that arm is written for.
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_MAP_FUSION_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL");
            execute(FUSED_VIEW);
            final Golden golden = new Golden();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                commitFusedFixture(job, golden);
                final LiveViewWindow window = window();
                Assert.assertFalse(window.isWindowStateFused());
                final LiveViewWindowStatePlan plan = window.getCheckpointStoragePlan();
                Assert.assertNotNull(plan);
                Map maxMap = null;
                for (int c = 0, n = plan.getComponentCount(); c < n; c++) {
                    if ("max".equals(plan.getContributor(c).getName())) {
                        maxMap = plan.getContributor(c).getPartitionMap();
                    }
                }
                Assert.assertNotNull("the max contributor must keep a private map", maxMap);
                Assert.assertEquals(3, maxMap.size());
                maxMap.clear();
                try (LiveViewCheckpointSealState state = new LiveViewCheckpointSealState()) {
                    window.detachCheckpointSealState(state);
                    Assert.assertTrue("the window must now owe a complete freeze", window.isCheckpointFullScanRequired());
                }
                golden.commit(job, "absent contributor", "('2026-01-02T09:05:00.000000Z', 'acct-1', 6.0)");
                Assert.assertEquals("acct-1 is the one key the max contributor holds again", 1, maxMap.size());
            }
            golden.assertView(
                    """
                    meta/m.0000000000000001 475 d23fbfeb6032a8cbd9192b2501de925be08852dfacbec06a41b1b03437f5ee30
                    meta/m.0000000000000002 144 705c71f38219329083f055a882b466408e72823a10565656c7e5cc59efb7085b
                    meta/m.0000000000000006 475 49be63b0136c85448e98239a05fbb0f08824f5ece2071fdfd5d4b0e3f75f75f7
                    meta/m.0000000000000007 144 7bd0a0f7eca79d768b4a69190bf24ed5dd9471e216025d23b5c48c840835ed9e
                    meta/m.0000000000000011 475 b06719f3daa3d337879acc1e3fae34db24bb105ec81cea1239e5126620f42f84
                    meta/m.0000000000000012 144 d7cd836cf14e686e2407020baa632e369e8ed3573d2534bb3d94d24b774ba9b5
                    meta/m.0000000000000013 220 d7037d6f07775de50fc00c7f74cdbccc36fcaab686b9915bb8034cae16a5614f
                    meta/m.0000000000000014 480 37cace6a33bc294d3fae9487a27178f75f2eb4cd6e93e5aea60a1e0db6f045fb
                    meta/m.0000000000000016 475 3b66cf2bbb375aa52d811f301d05c9b15f88847eb3f837e664daccb75c94797f
                    meta/m.0000000000000017 144 abf983152516a26dbfd5f93964fcdbe3045bbd7dc838b6881279ae079c2a11cf
                    meta/m.0000000000000018 280 edef846a2362bada2a1376d52ca6d241bfdc20ee1195cad538d6584235816d1e
                    meta/m.0000000000000019 560 99af50015af206bc3d9ac61ed97b7ac894a7135284e19fca79f0e0e1767a0506
                    """,
                    """
                    boundary checkpoint=0 maxTs=1767258120000000 root=2/44/100
                      window 1/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=1/24/200 entries=3
                        + 0600000061006300630074002d003100 -> 0040204648470600000000000000144001000000000000000000000000001440 refs=0
                        + 0600000061006300630074002d003200 -> 00402046484706000000000000001c4001000000000000000000000000001c40 refs=0
                        + 0600000061006300630074002d003300 -> 0040204648470600000000000000f03f0100000000000000000000000000f03f refs=0
                    boundary checkpoint=1 maxTs=1767258660000000 root=7/44/100
                      window 6/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=6/24/200 entries=3
                        + 0600000061006300630074002d003200 -> 0040204648470600000000000000244002000000000000000000000000001c40 refs=0
                    boundary checkpoint=2 maxTs=1767344400000000 root=12/44/100
                      window 11/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=11/24/200 entries=3
                        + 0600000061006300630074002d003300 -> 00a0f7635c470600000000000000004001000000000000000000000000000040 refs=0
                    boundary checkpoint=3 maxTs=1767344700000000 root=17/44/100
                      window 16/224/251 identity=55:a792d92f68f6dfa8 anchorType=8 inline=32 keySchema=8:09da8010bedba854 manifest=112:7646c21f743cb604 map=16/24/200 entries=3
                        + 0600000061006300630074002d003100 -> 00a0f7635c470600000000000000184001000000000000000000000000001840 refs=0
                        + 0600000061006300630074002d003200 -> 004020464847060000000000000024400200000000000000000000000000f87f refs=0
                        + 0600000061006300630074002d003300 -> 00a0f7635c47060000000000000000400100000000000000000000000000f87f refs=0
                    """,
                    """
                    complete: puts=3 window roots=1 incremental=0 visited=3 imaged=3 removed=0 probes=0 | function roots=0 incremental=0 visited=0 imaged=0
                    incremental, acct-1 unchanged: puts=1 window roots=1 incremental=1 visited=2 imaged=2 removed=0 probes=2 | function roots=0 incremental=0 visited=0 imaged=0
                    anchor moved: puts=1 window roots=1 incremental=1 visited=1 imaged=1 removed=0 probes=0 | function roots=0 incremental=0 visited=0 imaged=0
                    absent contributor: puts=3 window roots=1 incremental=0 visited=3 imaged=3 removed=0 probes=3 | function roots=0 incremental=0 visited=0 imaged=0
                    """
            );
        });
    }

    private static void assertGolden(String what, String expected, String actual) {
        if (!expected.equals(actual)) {
            System.out.println("GOLDEN " + what.replace(' ', '_') + " BEGIN\n" + actual + "\nGOLDEN " + what.replace(' ', '_') + " END");
        }
        Assert.assertEquals(what, expected, actual);
    }

    /**
     * Compares all three renderings, printing every one that differs before the first
     * assertion fails, so a regression shows its whole footprint in one run.
     */
    private static void assertGoldens(
            String expectedFiles,
            String actualFiles,
            String expectedDump,
            String actualDump,
            String expectedElision,
            CharSequence actualElision
    ) {
        final String elision = actualElision.toString();
        if (!expectedFiles.equals(actualFiles)) {
            System.out.println("GOLDEN files BEGIN\n" + actualFiles + "GOLDEN files END");
        }
        if (!expectedDump.equals(actualDump)) {
            System.out.println("GOLDEN dump BEGIN\n" + actualDump + "GOLDEN dump END");
        }
        if (!expectedElision.equals(elision)) {
            System.out.println("GOLDEN elision BEGIN\n" + elision + "GOLDEN elision END");
        }
        Assert.assertEquals("elision decisions", expectedElision, elision);
        Assert.assertEquals("decoded roots", expectedDump, actualDump);
        Assert.assertEquals("checkpoint file digests", expectedFiles, actualFiles);
    }

    private static Path checkpointsDir(Path sink, LiveViewInstance instance) {
        return sink.of(configuration.getDbRoot()).concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static String decimalRow(String minute, String account, String amount) {
        return "('" + minute + ":00.000000Z', '" + account + "', " + amount + ", " + amount + "::decimal(38,2))";
    }

    private static String digest(byte[] bytes) throws NoSuchAlgorithmException {
        return bytes.length + ":" + sha256(bytes).substring(0, 16);
    }

    /**
     * One line per file under {@code meta} and {@code data}, sorted by name: the file name,
     * its length and the SHA-256 of its bytes.
     */
    private static String digestCheckpointFiles(Path checkpointsDir) throws IOException, NoSuchAlgorithmException {
        final List<String> lines = new ArrayList<>();
        final String root = checkpointsDir.toString();
        for (String dirName : new String[]{LiveViewCheckpointLayout.META_DIR_NAME, LiveViewCheckpointLayout.DATA_DIR_NAME}) {
            final java.nio.file.Path dir = Paths.get(root, dirName);
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (Stream<java.nio.file.Path> files = Files.list(dir)) {
                for (java.nio.file.Path file : (Iterable<java.nio.file.Path>) files::iterator) {
                    final byte[] bytes = Files.readAllBytes(file);
                    lines.add(dirName + "/" + file.getFileName() + " " + bytes.length + " " + sha256(bytes));
                }
            }
        }
        Collections.sort(lines);
        return String.join("\n", lines) + "\n";
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    /**
     * Names one root of the current boundary, and answers whether the boundary rewrote it:
     * false, having marked it unchanged, when the boundary below named the same page.
     */
    private static boolean isRootRewritten(
            String slot,
            LiveViewCheckpointPageRef rootRef,
            HashMap<String, String> previousRefs,
            StringBuilder out
    ) {
        final String rendered = ref(rootRef);
        out.append("  ").append(slot).append(' ').append(rendered);
        if (rendered.equals(previousRefs.put(slot, rendered))) {
            out.append(" unchanged\n");
            return false;
        }
        return true;
    }

    private static String keyedRow(int day, int hour, int minute, int account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', 'acct-" + account + "', 1.0)";
    }

    private static String overlayHex(LiveViewWindow window) {
        try (MemoryCARW sink = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            window.snapshot(sink);
            final byte[] bytes = new byte[(int) sink.getAppendOffset()];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = sink.getByte(i);
            }
            return hex(bytes);
        }
    }

    private static String ref(LiveViewCheckpointPageRef ref) {
        return ref.getSegmentId() + "/" + ref.getOffset() + "/" + ref.getLength();
    }

    private static String ringRows(int fromSecond, int toSecond, int keyCount) {
        final StringBuilder rows = new StringBuilder();
        for (int second = fromSecond; second < toSecond; second++) {
            for (int k = 0; k < keyCount; k++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append("('2026-01-01T09:").append(String.format("%02d:%02d", second / 60, second % 60))
                        .append(".000000Z', '").append((char) ('a' + k)).append("', ")
                        .append(second + 0.5 + 1000 * k).append(')');
            }
        }
        return rows.toString();
    }

    /**
     * The one place a test reads a partition entry's scalar payload. The payload conversion
     * may change this accessor, and nothing else in this class.
     */
    private static String scalarHex(LiveViewCheckpointPartitionMapEntry entry) {
        return hex(entry.copyScalarStateForTest());
    }

    private static String sha256(byte[] bytes) throws NoSuchAlgorithmException {
        return hex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static String statePageRef(LiveViewCheckpointStatePageRef ref) {
        return ref.getSegmentId() + "/" + ref.getOffset() + "/" + ref.getStoredLength() + "/" + ref.getDecodedLength()
                + "/k" + ref.getPageKind() + "/c" + ref.getCodec() + "/r" + ref.getRowCount() + "/f" + ref.getFlags();
    }

    private static Path stubCheckpointsDir(Path sink) {
        return sink.of(configuration.getDbRoot()).concat(STUB_DIR).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private void assertStubSeals(
            boolean isPageBacked,
            String expectedFiles,
            String expectedDump,
            String expectedElision
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (Path dir = new Path(); Path path = new Path()) {
                final FilesFacade ff = configuration.getFilesFacade();
                stubCheckpointsDir(dir);
                ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
                ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
            }
            final StringBuilder elision = new StringBuilder();
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(isPageBacked);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.putState(1, 0x0102_0304_0506_0708L);
                stub.putState(2, 0x1112_1314_1516_1718L);
                stub.putState(3, -3);
                stubSeal(writer, stub, 1, "complete", elision);
                stub.putState(2, 0x2122_2324_2526_2728L);
                stub.putState(4, Long.MIN_VALUE);
                stubSeal(writer, stub, 2, "one moved, one new", elision);
                stubSeal(writer, stub, 3, "unmoved", elision);
            }
            try (Path dir = new Path()) {
                stubCheckpointsDir(dir);
                assertGoldens(expectedFiles, digestCheckpointFiles(dir), expectedDump, dumpTimeline(dir), expectedElision, elision);
            }
        });
    }

    private void commitFusedFixture(LiveViewRefreshJob job, Golden golden) throws Exception {
        golden.commit(job, "complete", "('2026-01-01T09:00:00.000000Z', 'acct-1', 5.0), "
                + "('2026-01-01T09:01:00.000000Z', 'acct-2', 7.0), ('2026-01-01T09:02:00.000000Z', 'acct-3', 1.0)");
        golden.commit(job, "incremental, acct-1 unchanged", "('2026-01-01T09:10:00.000000Z', 'acct-1', NULL), "
                + "('2026-01-01T09:11:00.000000Z', 'acct-2', 3.0)");
        golden.commit(job, "anchor moved", "('2026-01-02T09:00:00.000000Z', 'acct-3', 2.0)");
    }

    /**
     * Renders one partition map as the difference from what the same root slot named one
     * boundary below, and records it for the boundary above.
     */
    private void dumpPartitions(
            LiveViewCheckpointPartitionMapReader partitions,
            LiveViewCheckpointPageRef mapRootRef,
            HashMap<String, TreeMap<String, String>> previousEntries,
            String slot,
            StringBuilder out
    ) {
        final TreeMap<String, String> entries = new TreeMap<>();
        partitions.iterateAll(mapRootRef, entry -> {
            final StringBuilder rendering = new StringBuilder();
            final String scalar = scalarHex(entry);
            // A wide fused payload wraps at 64 bytes, so a line stays readable.
            for (int at = 0; at < scalar.length(); at += 128) {
                rendering.append(at == 0 ? "" : "\n          ").append(scalar, at, Math.min(scalar.length(), at + 128));
            }
            rendering.append(" refs=").append(entry.getStatePageCount());
            for (int p = 0, n = entry.getStatePageCount(); p < n; p++) {
                rendering.append(' ').append(statePageRef(entry.getStatePageRef(p)));
            }
            entries.put(hex(entry.copyKeyForTest()), rendering.toString());
        });
        out.append(" entries=").append(entries.size()).append('\n');
        final TreeMap<String, String> previous = previousEntries.get(slot);
        if (previous != null) {
            for (String key : previous.keySet()) {
                if (!entries.containsKey(key)) {
                    out.append("    - ").append(key).append('\n');
                }
            }
        }
        for (java.util.Map.Entry<String, String> entry : entries.entrySet()) {
            if (previous == null || !entry.getValue().equals(previous.get(entry.getKey()))) {
                out.append("    + ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append('\n');
            }
        }
        previousEntries.put(slot, entries);
    }

    /**
     * Every logical boundary, ascending, with the roots its current version names. A root
     * the boundary shares with the one below it is marked unchanged. A root it rewrote lists
     * the entries that differ from the same root one boundary below: {@code +} for an entry
     * added or changed, {@code -} for one removed. The first boundary lists every entry, so
     * the dump still describes each boundary whole.
     */
    private String dumpTimeline(Path dir) throws NoSuchAlgorithmException {
        final StringBuilder out = new StringBuilder();
        try (LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)) {
            metaStore.of(dir);
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointWindowRoot windowRoot = new LiveViewCheckpointWindowRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                // Per root slot ("window", "function <i>"): the ref and the entries the
                // boundary below named, keyed by the entry's key hex.
                final HashMap<String, String> previousRefs = new HashMap<>();
                final HashMap<String, TreeMap<String, String>> previousEntries = new HashMap<>();
                final LiveViewCheckpointPageRef stateRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef mapRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
                final NoSuchAlgorithmException[] failure = new NoSuchAlgorithmException[1];
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    try {
                        out.append("boundary checkpoint=").append(entry.checkpointId)
                                .append(" maxTs=").append(entry.maxTimestamp)
                                .append(" root=").append(ref(entry.rootRef)).append('\n');
                        root.of(dir, entry.rootRef);
                        root.getStateRootRef(stateRootRef);
                        if (!stateRootRef.isNull()) {
                            if (isRootRewritten("window", stateRootRef, previousRefs, out)) {
                                Assert.assertTrue("the state root must be a window root", windowRoot.ofIfWindowRoot(dir, stateRootRef));
                                windowRoot.getPartitionMapRootRef(mapRootRef);
                                out.append(" identity=").append(digest(windowRoot.getWindowIdentity()))
                                        .append(" anchorType=").append(windowRoot.getAnchorValueType())
                                        .append(" inline=").append(windowRoot.getTotalInlineStateBytes())
                                        .append(" keySchema=").append(digest(windowRoot.getKeySchema()))
                                        .append(" manifest=").append(digest(windowRoot.getManifest()))
                                        .append(" map=").append(ref(mapRootRef));
                                dumpPartitions(partitions, mapRootRef, previousEntries, "window", out);
                            }
                        }
                        root.getFunctionDirectoryRef(functionDirectoryRef);
                        functions.of(dir, functionDirectoryRef);
                        for (int i = 0, n = functions.size(); i < n; i++) {
                            functions.getRootRef(i, functionRootRef);
                            final String slot = "function " + i;
                            if (isRootRewritten(slot, functionRootRef, previousRefs, out)) {
                                functionRoot.of(dir, functionRootRef);
                                functionRoot.getPartitionMapRootRef(mapRootRef);
                                functionRoot.getScalarStateRef(scalarRef);
                                out.append(" identity=").append(digest(functionRoot.getFunctionIdentity()))
                                        .append(" version=").append(functionRoot.getStateFormatVersion())
                                        .append(" keySchema=").append(digest(functionRoot.getKeySchema()))
                                        .append(" map=").append(ref(mapRootRef));
                                if (!scalarRef.isNull()) {
                                    out.append(" scalar=").append(statePageRef(scalarRef));
                                }
                                if (mapRootRef.isNull()) {
                                    out.append('\n');
                                } else {
                                    dumpPartitions(partitions, mapRootRef, previousEntries, slot, out);
                                }
                            }
                        }
                    } catch (NoSuchAlgorithmException e) {
                        failure[0] = e;
                    }
                });
                if (failure[0] != null) {
                    throw failure[0];
                }
            }
        }
        return out.toString();
    }

    private void stubSeal(
            LiveViewCheckpointTimelineStoreWriter writer,
            PartitionedStateStub stub,
            long seq,
            String label,
            StringBuilder elision
    ) {
        try (Path dir = new Path()) {
            stubCheckpointsDir(dir);
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            writer.append(
                    dir,
                    functions,
                    null,
                    DEFINITION_TXN,
                    0,
                    seq,
                    seq,
                    0,
                    LIFECYCLE_IDENTITY,
                    true,
                    seq * 1_000_000L,
                    seq,
                    seq * 1_000_000L,
                    Numbers.LONG_NULL,
                    null
            );
        }
        final LiveViewCheckpointCaptureLedger ledger = writer.getCaptureLedger();
        elision.append(label).append(": puts=").append(writer.getLastBoundaryPartitionPuts())
                .append(" function roots=").append(ledger.getFunctionCaptures())
                .append(" incremental=").append(ledger.getFunctionIncrementalCaptures())
                .append(" visited=").append(ledger.getFunctionKeysVisited())
                .append(" imaged=").append(ledger.getFunctionKeysImaged())
                .append('\n');
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private LiveViewWindow window() {
        final LiveViewWindow window = viewInstance().getAnchorWindow();
        Assert.assertNotNull("the view must run an anchored window", window);
        return window;
    }

    /**
     * Drives one view's commits and collects what each of them sealed.
     */
    private final class Golden {
        private final StringBuilder elision = new StringBuilder();
        private long functionCaptures;
        private long functionIncrementalCaptures;
        private long functionKeysImaged;
        private long functionKeysVisited;
        private String overlay;
        private long windowCaptures;
        private long windowElisionProbes;
        private long windowIncrementalCaptures;
        private long windowKeysImaged;
        private long windowKeysRemoved;
        private long windowKeysVisited;

        private void assertView(String expectedFiles, String expectedDump, String expectedElision) throws Exception {
            try (Path dir = new Path()) {
                checkpointsDir(dir, viewInstance());
                assertGoldens(expectedFiles, digestCheckpointFiles(dir), expectedDump, dumpTimeline(dir), expectedElision, elision);
            }
        }

        private void commit(LiveViewRefreshJob job, String label, String values) throws Exception {
            execute("INSERT INTO tx VALUES " + values);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            record(job, label);
        }

        /**
         * Appends what the refresh since the previous record sealed: the last boundary's
         * builder puts and the capture ledger's deltas.
         */
        private void record(LiveViewRefreshJob job, String label) throws Exception {
            final LiveViewCheckpointTimelineStoreWriter writer = checkpointTimelineStoreWriter(job);
            final LiveViewInstance instance = viewInstance();
            elision.append(label).append(": puts=").append(writer.getLastBoundaryPartitionPuts())
                    .append(" window roots=").append(instance.getCheckpointCaptureWindowRoots() - windowCaptures)
                    .append(" incremental=").append(instance.getCheckpointCaptureWindowRootsIncremental() - windowIncrementalCaptures)
                    .append(" visited=").append(instance.getCheckpointCaptureWindowKeysVisited() - windowKeysVisited)
                    .append(" imaged=").append(instance.getCheckpointCaptureWindowKeysImaged() - windowKeysImaged)
                    .append(" removed=").append(instance.getCheckpointCaptureWindowKeysRemoved() - windowKeysRemoved)
                    .append(" probes=").append(instance.getCheckpointCaptureWindowElisionProbes() - windowElisionProbes)
                    .append(" | function roots=").append(instance.getCheckpointCaptureFunctionRoots() - functionCaptures)
                    .append(" incremental=").append(instance.getCheckpointCaptureFunctionRootsIncremental() - functionIncrementalCaptures)
                    .append(" visited=").append(instance.getCheckpointCaptureFunctionKeysVisited() - functionKeysVisited)
                    .append(" imaged=").append(instance.getCheckpointCaptureFunctionKeysImaged() - functionKeysImaged)
                    .append('\n');
            windowCaptures = instance.getCheckpointCaptureWindowRoots();
            windowIncrementalCaptures = instance.getCheckpointCaptureWindowRootsIncremental();
            windowKeysVisited = instance.getCheckpointCaptureWindowKeysVisited();
            windowKeysImaged = instance.getCheckpointCaptureWindowKeysImaged();
            windowKeysRemoved = instance.getCheckpointCaptureWindowKeysRemoved();
            windowElisionProbes = instance.getCheckpointCaptureWindowElisionProbes();
            functionCaptures = instance.getCheckpointCaptureFunctionRoots();
            functionIncrementalCaptures = instance.getCheckpointCaptureFunctionRootsIncremental();
            functionKeysVisited = instance.getCheckpointCaptureFunctionKeysVisited();
            functionKeysImaged = instance.getCheckpointCaptureFunctionKeysImaged();
        }

        /**
         * Appends what a compaction pass published: how many roots it rewrote, the segment
         * it moved their live pages into and the generation it published.
         */
        private void recordCompaction(LiveViewCheckpointCompaction.Result result) {
            elision.append("compaction: roots rewritten=").append(result.getRootsRewritten())
                    .append(" target=").append(result.getTargetSegmentId())
                    .append(" generation=").append(result.getGeneration())
                    .append('\n');
        }

        /**
         * Appends the repair route the view last took, which is what says the case reached
         * the shape it is named for.
         */
        private void recordRoute() throws Exception {
            printSql("SELECT checkpoint_repair_plan, checkpoint_repair_last_disposition, "
                    + "checkpoint_repair_last_denial FROM live_views()");
            elision.append("route: ").append(sink.toString().replace('\n', '|').replace('\t', ',')).append('\n');
        }
    }

    /**
     * A partitioned function with LONG keys and one LONG of state per key, frozen as eight
     * bytes inline or, undeclared, as a data page.
     */
    private static final class PartitionedStateStub extends BaseWindowFunction {
        private static final ColumnTypes KEY_TYPES = new SingleColumnType(ColumnType.LONG);
        private final boolean isPageBacked;
        private final Map map;

        private PartitionedStateStub(boolean isPageBacked) {
            super(null);
            this.isPageBacked = isPageBacked;
            this.map = new OrderedMap(1024, KEY_TYPES, new SingleColumnType(ColumnType.LONG), 16, 0.7, 8);
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "payload_golden_stub()",
                            0,
                            "k",
                            "ts asc",
                            "payload-golden-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.FIXED_ANCHOR_SEGMENT,
                            "k",
                            "ts asc",
                            0,
                            0,
                            0,
                            ColumnType.TIMESTAMP,
                            false,
                            false,
                            false,
                            LiveViewCheckpointDependency.StructuralConvergence.EXACT,
                            LiveViewCheckpointDependency.NumericConvergence.EXACT
                    )
            );
        }

        @Override
        public int checkpointStateFixedLength() {
            return isPageBacked ? -1 : Long.BYTES;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            sink.putLong(value.getLong(0));
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return KEY_TYPES;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            return 1;
        }

        @Override
        public String getName() {
            return "payload_golden_stub";
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            map.clear();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            value.putLong(0, source.getLong(offset));
            return Long.BYTES;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }

        private void putState(long key, long state) {
            final MapKey mapKey = map.withKey();
            mapKey.putLong(key);
            mapKey.createValue().putLong(0, state);
        }
    }
}
