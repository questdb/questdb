/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ParallelGroupByTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;

    private final boolean enableParallelGroupBy;

    public ParallelGroupByTest(boolean enableParallelGroupBy) {
        this.enableParallelGroupBy = enableParallelGroupBy;
    }

    @Parameterized.Parameters(name = "parallel={0} threshold={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Override
    @Before
    public void setUp() {
        pageFrameMaxRows = PAGE_FRAME_MAX_ROWS;
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        pageFrameReduceShardCount = 2;
        pageFrameReduceQueueCapacity = PAGE_FRAME_COUNT;
        // Set the sharding threshold to a small value to test sharding.
        groupByShardingThreshold = 2;
        super.setUp();
        configOverrideParallelGroupByEnabled(enableParallelGroupBy);
    }

    @Test
    public void testGroupByOverJoin() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            insert("INSERT INTO t VALUES ('2023-09-21T11:00:00.000000Z', 1, 1);");

            assertQuery(
                    "count\n" +
                            "2\n",
                    "SELECT count(1)\n" +
                            "FROM t as T1 JOIN t as T2 ON T1.created = T2.created\n" +
                            "WHERE T1.event = 1.0",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByOverLatestBy() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t (\n" +
                            "  created timestamp,\n" +
                            "  event symbol,\n" +
                            "  origin symbol\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 'a', 'c');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:01.000000Z', 'a', 'c');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:02.000000Z', 'a', 'd');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 'b', 'c');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:01.000000Z', 'b', 'c');");

            assertQuery(
                    "count\n" +
                            "2\n",
                    "SELECT count()\n" +
                            "FROM t\n" +
                            "WHERE origin = 'c'\n" +
                            "LATEST ON created PARTITION BY event",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByOverUnion() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t1 (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t1 VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            insert("INSERT INTO t1 VALUES ('2023-09-21T10:00:01.000000Z', 2, 2);");

            ddl(
                    "CREATE TABLE t2 (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t2 VALUES ('2023-09-21T10:00:02.000000Z', 3, 1);");
            insert("INSERT INTO t2 VALUES ('2023-09-21T10:00:00.000000Z', 4, 2);");

            assertQuery(
                    "event\tcount\n" +
                            "1\t1\n" +
                            "3\t1\n",
                    "SELECT event, count()\n" +
                            "FROM (t1 UNION t2) WHERE origin = 1",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNonKeyedGroupByEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        assertQueries(
                                engine,
                                sqlExecutionContext,
                                "select vwap(price, quantity) from tab",
                                "vwap\n" +
                                        "NaN\n"
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelCountOverMultiKeyGroupBy() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT count(*) FROM (SELECT key1, key2 FROM tab GROUP BY key1, key2 ORDER BY key1, key2)",
                "count\n" +
                        "5\n"
        );
    }

    @Test
    public void testParallelCountOverSingleKeyGroupBy() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT count(*) FROM (SELECT key FROM tab WHERE key IS NOT NULL GROUP BY key ORDER BY key)",
                "count\n" +
                        "5\n"
        );
    }

    @Test
    public void testParallelFunctionKeyExplicitGroupBy() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab GROUP BY day, key ORDER BY day, key",
                "day\tkey\tvwap\tsum\n" +
                        "1\tk0\t2848.23852863102\t263700.0\n" +
                        "1\tk1\t2848.94253657797\t263820.0\n" +
                        "1\tk2\t2849.6468136697736\t263940.0\n" +
                        "1\tk3\t2850.3513595394984\t264060.0\n" +
                        "1\tk4\t2851.05617382088\t264180.0\n" +
                        "2\tk0\t2624.4694763291645\t239025.0\n" +
                        "2\tk1\t2598.96097084443\t235085.0\n" +
                        "2\tk2\t2599.691650489951\t235195.0\n" +
                        "2\tk3\t2600.4225929755667\t235305.0\n" +
                        "2\tk4\t2601.153797916691\t235415.0\n" +
                        "3\tk0\t2526.5384615384614\t204750.0\n" +
                        "3\tk1\t2527.3046131315596\t204850.0\n" +
                        "3\tk2\t2528.070992925104\t204950.0\n" +
                        "3\tk3\t2528.8376005852233\t205050.0\n" +
                        "3\tk4\t2529.6044357786986\t205150.0\n" +
                        "4\tk0\t2594.679907219484\t215425.0\n" +
                        "4\tk1\t2595.0011126435716\t215585.0\n" +
                        "4\tk2\t2595.617813662006\t215695.0\n" +
                        "4\tk3\t2596.234922950459\t215805.0\n" +
                        "4\tk4\t2596.8524398569757\t215915.0\n" +
                        "5\tk0\t2651.1220904699167\t227700.0\n" +
                        "5\tk1\t2651.7251338776227\t227820.0\n" +
                        "5\tk2\t2652.3285952443625\t227940.0\n" +
                        "5\tk3\t2652.9324739103745\t228060.0\n" +
                        "5\tk4\t2653.5367692172845\t228180.0\n" +
                        "6\tk0\t2713.3938256153524\t239700.0\n" +
                        "6\tk1\t2714.035610040864\t239820.0\n" +
                        "6\tk2\t2714.6777527715262\t239940.0\n" +
                        "6\tk3\t2715.3202532700157\t240060.0\n" +
                        "6\tk4\t2715.9631110000832\t240180.0\n" +
                        "7\tk0\t2779.263011521653\t251700.0\n" +
                        "7\tk1\t2779.938130410611\t251820.0\n" +
                        "7\tk2\t2780.6135587838376\t251940.0\n" +
                        "7\tk3\t2781.2892961993175\t252060.0\n" +
                        "7\tk4\t2781.9653422158776\t252180.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByMultipleKeys1() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), day_of_week(ts) day, hour(ts) hour, sum(colTop) " +
                        "FROM tab ORDER BY day, hour",
                "vwap\tday\thour\tsum\n" +
                        "2816.111833952912\t1\t0\t64560.0\n" +
                        "2819.2256743179537\t1\t1\t51756.0\n" +
                        "2821.9986885751755\t1\t2\t51852.0\n" +
                        "2824.776237776238\t1\t3\t51948.0\n" +
                        "2827.5582968257627\t1\t4\t52044.0\n" +
                        "2830.3448408131953\t1\t5\t52140.0\n" +
                        "2833.485377430715\t1\t6\t65310.0\n" +
                        "2836.630835052334\t1\t7\t52356.0\n" +
                        "2839.4317852512772\t1\t8\t52452.0\n" +
                        "2842.2371165410673\t1\t9\t52548.0\n" +
                        "2845.046804954031\t1\t10\t52644.0\n" +
                        "2847.860826697004\t1\t11\t52740.0\n" +
                        "2851.0320920375416\t1\t12\t66060.0\n" +
                        "2854.208097288315\t1\t13\t52956.0\n" +
                        "2857.0360401115886\t1\t14\t53052.0\n" +
                        "2859.8682170542634\t1\t15\t53148.0\n" +
                        "2862.704605213733\t1\t16\t53244.0\n" +
                        "2865.5451818522683\t1\t17\t53340.0\n" +
                        "2868.746145786559\t1\t18\t66810.0\n" +
                        "2871.9516767495707\t1\t19\t53556.0\n" +
                        "2874.805710877507\t1\t20\t53652.0\n" +
                        "2877.6638386544614\t1\t21\t53748.0\n" +
                        "2880.5260381843846\t1\t22\t53844.0\n" +
                        "2883.3922877271043\t1\t23\t53940.0\n" +
                        "2736.632776425153\t2\t0\t67560.0\n" +
                        "2695.944281906248\t2\t1\t54156.0\n" +
                        "2698.804979342865\t2\t2\t54252.0\n" +
                        "2701.6700058291412\t2\t3\t54348.0\n" +
                        "2704.539336737992\t2\t4\t54444.0\n" +
                        "2707.412947628777\t2\t5\t54540.0\n" +
                        "2710.6511997252865\t2\t6\t68310.0\n" +
                        "2713.8940954746963\t2\t7\t54756.0\n" +
                        "2716.7814497338663\t2\t8\t54852.0\n" +
                        "2719.6729821417143\t2\t9\t54948.0\n" +
                        "2722.568669207999\t2\t10\t55044.0\n" +
                        "2725.4684876182378\t2\t11\t55140.0\n" +
                        "2517.6369896704377\t2\t12\t52850.0\n" +
                        "2457.3950932788143\t2\t13\t39130.0\n" +
                        "2460.37311910227\t2\t14\t39210.0\n" +
                        "2463.3553066938152\t2\t15\t39290.0\n" +
                        "2466.3416306832614\t2\t16\t39370.0\n" +
                        "2469.3320659062106\t2\t17\t39450.0\n" +
                        "2472.7015680323725\t2\t18\t49425.0\n" +
                        "2476.0754478930103\t2\t19\t39630.0\n" +
                        "2479.0790732812893\t2\t20\t39710.0\n" +
                        "2482.0867052023123\t2\t21\t39790.0\n" +
                        "2485.0983195385\t2\t22\t39870.0\n" +
                        "2488.1138923654566\t2\t23\t39950.0\n" +
                        "2491.5114885114886\t3\t0\t50050.0\n" +
                        "2494.9132818340395\t3\t1\t40130.0\n" +
                        "2497.94155682666\t3\t2\t40210.0\n" +
                        "2500.9736907421197\t3\t3\t40290.0\n" +
                        "2504.0096606390885\t3\t4\t40370.0\n" +
                        "2507.0494437577254\t3\t5\t40450.0\n" +
                        "2510.474099654662\t3\t6\t50675.0\n" +
                        "2513.9027811961605\t3\t7\t40630.0\n" +
                        "2516.954802259887\t3\t8\t40710.0\n" +
                        "2520.0105417994605\t3\t9\t40790.0\n" +
                        "2523.0699779789575\t3\t10\t40870.0\n" +
                        "2526.133089133089\t3\t11\t40950.0\n" +
                        "2529.583820662768\t3\t12\t51300.0\n" +
                        "2533.0384147823975\t3\t13\t41130.0\n" +
                        "2536.113322009221\t3\t14\t41210.0\n" +
                        "2539.191813998547\t3\t15\t41290.0\n" +
                        "2542.273869954073\t3\t16\t41370.0\n" +
                        "2545.3594692400484\t3\t17\t41450.0\n" +
                        "2548.835339431873\t3\t18\t51925.0\n" +
                        "2552.314917127072\t3\t19\t41630.0\n" +
                        "2555.411891632702\t3\t20\t41710.0\n" +
                        "2558.5123235223737\t3\t21\t41790.0\n" +
                        "2561.616192978266\t3\t22\t41870.0\n" +
                        "2564.7234803337305\t3\t23\t41950.0\n" +
                        "2567.979545238322\t4\t0\t52550.0\n" +
                        "2570.9360273355005\t4\t1\t42130.0\n" +
                        "2573.570434041344\t4\t2\t42210.0\n" +
                        "2576.2105200973556\t4\t3\t42290.0\n" +
                        "2578.856250147381\t4\t4\t42370.0\n" +
                        "2581.5075891281326\t4\t5\t42450.0\n" +
                        "2584.4973939991546\t4\t6\t53175.0\n" +
                        "2587.4934298362728\t4\t7\t42630.0\n" +
                        "2590.1627591687898\t4\t8\t42710.0\n" +
                        "2592.837551610721\t4\t9\t42790.0\n" +
                        "2595.517773587541\t4\t10\t42870.0\n" +
                        "2598.20339179928\t4\t11\t42950.0\n" +
                        "2596.278893309892\t4\t12\t54010.0\n" +
                        "2597.6253344404467\t4\t13\t43356.0\n" +
                        "2599.8774739942924\t4\t14\t43452.0\n" +
                        "2602.1373197391385\t4\t15\t43548.0\n" +
                        "2604.4048208230224\t4\t16\t43644.0\n" +
                        "2606.6799268404206\t4\t17\t43740.0\n" +
                        "2609.2488596971357\t4\t18\t54810.0\n" +
                        "2611.826462826463\t4\t19\t43956.0\n" +
                        "2614.1259420684646\t4\t20\t44052.0\n" +
                        "2616.4328168886473\t4\t21\t44148.0\n" +
                        "2618.747039146551\t4\t22\t44244.0\n" +
                        "2621.0685611186286\t4\t23\t44340.0\n" +
                        "2623.689344852412\t5\t0\t55560.0\n" +
                        "2626.3184307388456\t5\t1\t44556.0\n" +
                        "2628.6633521454805\t5\t2\t44652.0\n" +
                        "2631.0153749888264\t5\t3\t44748.0\n" +
                        "2633.3744536615823\t5\t4\t44844.0\n" +
                        "2635.7405429461505\t5\t5\t44940.0\n" +
                        "2638.411117030723\t5\t6\t56310.0\n" +
                        "2641.089644786961\t5\t7\t45156.0\n" +
                        "2643.478210907805\t5\t8\t45252.0\n" +
                        "2645.873599717738\t5\t9\t45348.0\n" +
                        "2648.2757679781707\t5\t10\t45444.0\n" +
                        "2650.6846728151077\t5\t11\t45540.0\n" +
                        "2653.4030844724853\t5\t12\t57060.0\n" +
                        "2656.1291196782936\t5\t13\t45756.0\n" +
                        "2658.5596266247926\t5\t14\t45852.0\n" +
                        "2660.996691912597\t5\t15\t45948.0\n" +
                        "2663.440274520024\t5\t16\t46044.0\n" +
                        "2665.8903337667966\t5\t17\t46140.0\n" +
                        "2668.654731015395\t5\t18\t57810.0\n" +
                        "2671.4264388644406\t5\t19\t46356.0\n" +
                        "2673.897270300525\t5\t20\t46452.0\n" +
                        "2676.3744092119964\t5\t21\t46548.0\n" +
                        "2678.857816653803\t5\t22\t46644.0\n" +
                        "2681.3474540008556\t5\t23\t46740.0\n" +
                        "2684.156079234973\t6\t0\t58560.0\n" +
                        "2686.9717182042764\t6\t1\t46956.0\n" +
                        "2689.4813397942703\t6\t2\t47052.0\n" +
                        "2691.997030626962\t6\t3\t47148.0\n" +
                        "2694.5187537041743\t6\t4\t47244.0\n" +
                        "2697.046472327841\t6\t5\t47340.0\n" +
                        "2699.897656381723\t6\t6\t59310.0\n" +
                        "2702.7555723778282\t6\t7\t47556.0\n" +
                        "2705.302526651557\t6\t8\t47652.0\n" +
                        "2707.855323783195\t6\t9\t47748.0\n" +
                        "2710.4139286012874\t6\t10\t47844.0\n" +
                        "2712.9783062161036\t6\t11\t47940.0\n" +
                        "2715.8704628704627\t6\t12\t60060.0\n" +
                        "2718.769083810948\t6\t13\t48156.0\n" +
                        "2721.3519854099313\t6\t14\t48252.0\n" +
                        "2723.9405146024656\t6\t15\t48348.0\n" +
                        "2726.5346379324583\t6\t16\t48444.0\n" +
                        "2729.134322208488\t6\t17\t48540.0\n" +
                        "2732.0659431014637\t6\t18\t60810.0\n" +
                        "2735.003773894495\t6\t19\t48756.0\n" +
                        "2737.621305166626\t6\t20\t48852.0\n" +
                        "2740.2442592138596\t6\t21\t48948.0\n" +
                        "2742.872604192154\t6\t22\t49044.0\n" +
                        "2745.5063085063084\t6\t23\t49140.0\n" +
                        "2748.475958414555\t7\t0\t61560.0\n" +
                        "2751.45157630278\t7\t1\t49356.0\n" +
                        "2754.102483216048\t7\t2\t49452.0\n" +
                        "2756.758617905869\t7\t3\t49548.0\n" +
                        "2759.4199500443156\t7\t4\t49644.0\n" +
                        "2762.0864495375954\t7\t5\t49740.0\n" +
                        "2765.092761996469\t7\t6\t62310.0\n" +
                        "2768.1048122347665\t7\t7\t49956.0\n" +
                        "2770.7879005833934\t7\t8\t50052.0\n" +
                        "2773.4760309483927\t7\t9\t50148.0\n" +
                        "2776.1691744287873\t7\t10\t50244.0\n" +
                        "2778.8673023440606\t7\t11\t50340.0\n" +
                        "2781.9089755788136\t7\t12\t63060.0\n" +
                        "2784.9561674183083\t7\t13\t50556.0\n" +
                        "2787.670299297165\t7\t14\t50652.0\n" +
                        "2790.3892961298966\t7\t15\t50748.0\n" +
                        "2793.113130359531\t7\t16\t50844.0\n" +
                        "2795.8417746368277\t7\t17\t50940.0\n" +
                        "2798.917567779345\t7\t18\t63810.0\n" +
                        "2801.9986707326607\t7\t19\t51156.0\n" +
                        "2804.7427612580973\t7\t20\t51252.0\n" +
                        "2807.4915478694397\t7\t21\t51348.0\n" +
                        "2810.245004276495\t7\t22\t51444.0\n" +
                        "2813.0031043849435\t7\t23\t51540.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByMultipleKeys2() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), day_of_week(ts) day, sum(colTop), regexp_replace(key, 'k0', 'k42') key " +
                        "FROM tab ORDER BY day, key",
                "vwap\tday\tsum\tkey\n" +
                        "2848.94253657797\t1\t263820.0\tk1\n" +
                        "2849.6468136697736\t1\t263940.0\tk2\n" +
                        "2850.3513595394984\t1\t264060.0\tk3\n" +
                        "2851.05617382088\t1\t264180.0\tk4\n" +
                        "2848.23852863102\t1\t263700.0\tk42\n" +
                        "2598.96097084443\t2\t235085.0\tk1\n" +
                        "2599.691650489951\t2\t235195.0\tk2\n" +
                        "2600.4225929755667\t2\t235305.0\tk3\n" +
                        "2601.153797916691\t2\t235415.0\tk4\n" +
                        "2624.4694763291645\t2\t239025.0\tk42\n" +
                        "2527.3046131315596\t3\t204850.0\tk1\n" +
                        "2528.070992925104\t3\t204950.0\tk2\n" +
                        "2528.8376005852233\t3\t205050.0\tk3\n" +
                        "2529.6044357786986\t3\t205150.0\tk4\n" +
                        "2526.5384615384614\t3\t204750.0\tk42\n" +
                        "2595.0011126435716\t4\t215585.0\tk1\n" +
                        "2595.617813662006\t4\t215695.0\tk2\n" +
                        "2596.234922950459\t4\t215805.0\tk3\n" +
                        "2596.8524398569757\t4\t215915.0\tk4\n" +
                        "2594.679907219484\t4\t215425.0\tk42\n" +
                        "2651.7251338776227\t5\t227820.0\tk1\n" +
                        "2652.3285952443625\t5\t227940.0\tk2\n" +
                        "2652.9324739103745\t5\t228060.0\tk3\n" +
                        "2653.5367692172845\t5\t228180.0\tk4\n" +
                        "2651.1220904699167\t5\t227700.0\tk42\n" +
                        "2714.035610040864\t6\t239820.0\tk1\n" +
                        "2714.6777527715262\t6\t239940.0\tk2\n" +
                        "2715.3202532700157\t6\t240060.0\tk3\n" +
                        "2715.9631110000832\t6\t240180.0\tk4\n" +
                        "2713.3938256153524\t6\t239700.0\tk42\n" +
                        "2779.938130410611\t7\t251820.0\tk1\n" +
                        "2780.6135587838376\t7\t251940.0\tk2\n" +
                        "2781.2892961993175\t7\t252060.0\tk3\n" +
                        "2781.9653422158776\t7\t252180.0\tk4\n" +
                        "2779.263011521653\t7\t251700.0\tk42\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadSafe() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY day, key",
                "day\tkey\tvwap\tsum\n" +
                        "1\tk0\t2848.23852863102\t263700.0\n" +
                        "1\tk1\t2848.94253657797\t263820.0\n" +
                        "1\tk2\t2849.6468136697736\t263940.0\n" +
                        "1\tk3\t2850.3513595394984\t264060.0\n" +
                        "1\tk4\t2851.05617382088\t264180.0\n" +
                        "2\tk0\t2624.4694763291645\t239025.0\n" +
                        "2\tk1\t2598.96097084443\t235085.0\n" +
                        "2\tk2\t2599.691650489951\t235195.0\n" +
                        "2\tk3\t2600.4225929755667\t235305.0\n" +
                        "2\tk4\t2601.153797916691\t235415.0\n" +
                        "3\tk0\t2526.5384615384614\t204750.0\n" +
                        "3\tk1\t2527.3046131315596\t204850.0\n" +
                        "3\tk2\t2528.070992925104\t204950.0\n" +
                        "3\tk3\t2528.8376005852233\t205050.0\n" +
                        "3\tk4\t2529.6044357786986\t205150.0\n" +
                        "4\tk0\t2594.679907219484\t215425.0\n" +
                        "4\tk1\t2595.0011126435716\t215585.0\n" +
                        "4\tk2\t2595.617813662006\t215695.0\n" +
                        "4\tk3\t2596.234922950459\t215805.0\n" +
                        "4\tk4\t2596.8524398569757\t215915.0\n" +
                        "5\tk0\t2651.1220904699167\t227700.0\n" +
                        "5\tk1\t2651.7251338776227\t227820.0\n" +
                        "5\tk2\t2652.3285952443625\t227940.0\n" +
                        "5\tk3\t2652.9324739103745\t228060.0\n" +
                        "5\tk4\t2653.5367692172845\t228180.0\n" +
                        "6\tk0\t2713.3938256153524\t239700.0\n" +
                        "6\tk1\t2714.035610040864\t239820.0\n" +
                        "6\tk2\t2714.6777527715262\t239940.0\n" +
                        "6\tk3\t2715.3202532700157\t240060.0\n" +
                        "6\tk4\t2715.9631110000832\t240180.0\n" +
                        "7\tk0\t2779.263011521653\t251700.0\n" +
                        "7\tk1\t2779.938130410611\t251820.0\n" +
                        "7\tk2\t2780.6135587838376\t251940.0\n" +
                        "7\tk3\t2781.2892961993175\t252060.0\n" +
                        "7\tk4\t2781.9653422158776\t252180.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadUnsafe() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT regexp_replace(key, 'k0', 'k42') key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n" +
                        "k4\t2684.756229953121\t1641200.0\n" +
                        "k42\t2685.431565967941\t1642000.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadUnsafe2() throws Exception {
        // This query shouldn't be executed in parallel,
        // so this test verifies that nothing breaks.
        testParallelSymbolKeyGroupBy(
                "SELECT concat(key, 'abc')::symbol key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0abc\t2685.431565967941\t1642000.0\n" +
                        "k1abc\t2682.7321472695826\t1638800.0\n" +
                        "k2abc\t2683.4065201284266\t1639600.0\n" +
                        "k3abc\t2684.081214514935\t1640400.0\n" +
                        "k4abc\t2684.756229953121\t1641200.0\n"
        );
    }

    @Test
    public void testParallelGroupByCastToSymbol() throws Exception {
        // This query shouldn't be executed in parallel,
        // so this test verifies that nothing breaks.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "create table x as (select * from (select rnd_symbol('a','b','c') a, 'x' || x b from long_sequence(" + ROW_COUNT + ")))",
                                sqlExecutionContext
                        );
                        assertQueries(
                                engine,
                                sqlExecutionContext,
                                "select a, ct, c\n" +
                                        "from \n" +
                                        "(\n" +
                                        "  select a, cast(b as SYMBOL) as ct, count(*) c\n" +
                                        "  from x\n" +
                                        ") order by a, ct limit 10",
                                "a\tct\tc\n" +
                                        "a\tx1\t1\n" +
                                        "a\tx100\t1\n" +
                                        "a\tx1001\t1\n" +
                                        "a\tx1005\t1\n" +
                                        "a\tx1007\t1\n" +
                                        "a\tx1008\t1\n" +
                                        "a\tx1009\t1\n" +
                                        "a\tx101\t1\n" +
                                        "a\tx1010\t1\n" +
                                        "a\tx1011\t1\n"
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelMultiKeyGroupBy() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n" +
                        "k0\tk0\t2027.5\t1642000.0\n" +
                        "k1\tk1\t2023.5\t1638800.0\n" +
                        "k2\tk2\t2024.5\t1639600.0\n" +
                        "k3\tk3\t2025.5\t1640400.0\n" +
                        "k4\tk4\t2026.5\t1641200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupBySubQuery() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg + sum from (" +
                        "  SELECT key1, key2, avg(value), sum(colTop) FROM tab" +
                        ") ORDER BY key1, key2",
                "key1\tkey2\tcolumn\n" +
                        "k0\tk0\t1644027.5\n" +
                        "k1\tk1\t1640823.5\n" +
                        "k2\tk2\t1641624.5\n" +
                        "k3\tk3\t1642425.5\n" +
                        "k4\tk4\t1643226.5\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 80 ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n" +
                        "k0\tk0\t46.25\t325.0\n" +
                        "k1\tk1\t45.31818181818182\t381.0\n" +
                        "k2\tk2\t46.31818181818182\t387.0\n" +
                        "k3\tk3\t47.31818181818182\t393.0\n" +
                        "k4\tk4\t48.31818181818182\t399.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithLimit() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2 LIMIT 3",
                "key1\tkey2\tavg\tsum\n" +
                        "k0\tk0\t2027.5\t1642000.0\n" +
                        "k1\tk1\t2023.5\t1638800.0\n" +
                        "k2\tk2\t2024.5\t1639600.0\n",
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2 LIMIT -3",
                "key1\tkey2\tavg\tsum\n" +
                        "k2\tk2\t2024.5\t1639600.0\n" +
                        "k3\tk3\t2025.5\t1640400.0\n" +
                        "k4\tk4\t2026.5\t1641200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNestedFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT avg(v), sum(ct), k1, k2 " +
                        "FROM (SELECT value v, colTop ct, key2 k2, key1 k1 FROM tab WHERE value < 80) ORDER BY k1, k2",
                "avg\tsum\tk1\tk2\n" +
                        "46.25\t325.0\tk0\tk0\n" +
                        "45.31818181818182\t381.0\tk1\tk1\n" +
                        "46.31818181818182\t387.0\tk2\tk2\n" +
                        "47.31818181818182\t393.0\tk3\tk3\n" +
                        "48.31818181818182\t399.0\tk4\tk4\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctions() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2 FROM tab GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n" +
                        "k0\tk0\n" +
                        "k1\tk1\n" +
                        "k2\tk2\n" +
                        "k3\tk3\n" +
                        "k4\tk4\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctionsAndFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2 FROM tab WHERE key1 != 'k1' and key2 != 'k2' GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n" +
                        "k0\tk0\n" +
                        "k3\tk3\n" +
                        "k4\tk4\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctionsAndTooStrictFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2 FROM tab WHERE value < 0 GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 0 ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupBy() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab",
                "vwap\tsum\n" +
                        "2684.615238095238\t8202000.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByConcurrent() throws Exception {
        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT avg(value), sum(colTop) FROM tab";
        final String expected = "avg\tsum\n" +
                "2025.5\t8202000.0\n";

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(
                            compiler,
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    insert(
                            compiler,
                            "insert into tab select (x * 864000000)::timestamp, x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    insert(
                            compiler,
                            "insert into tab " +
                                    "select ((50 + x) * 864000000)::timestamp, 50 + x, 50 + x " +
                                    "from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

                    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(numOfThreads);

                    for (int i = 0; i < numOfThreads; i++) {
                        final int threadId = i;
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            TestUtils.await(barrier);
                            try {
                                for (int j = 0; j < numOfIterations; j++) {
                                    assertQueries(engine, sqlExecutionContext, sink, query, expected);
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.put(threadId, e);
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }
                    haltLatch.await();
                },
                configuration,
                LOG
        );

        if (!errors.isEmpty()) {
            for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
            }
            fail("Error in threads");
        }
    }

    @Test
    public void testParallelNonKeyedGroupByConstant() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT count(*) FROM tab GROUP BY 1+2",
                "count\n" +
                        "8000\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByFaultTolerance() throws Exception {
        testParallelGroupByFaultTolerance("select vwap(price, quantity) from tab where npe();");
    }

    @Test
    public void testParallelNonKeyedGroupBySubQueryWithReadThreadSafeTimestampFilter() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ip) FROM " +
                        "(SELECT * FROM tab WHERE ts in '1970-01-13' and a > 0 LIMIT 10)",
                "count_distinct\n" +
                        "10\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupBySubQueryWithReadThreadUnsafeTimestampFilter() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ip) FROM " +
                        "(SELECT * FROM tab WHERE ts in '1970-01-13' and a > 0 and i in (select i from tab where length(i) = 4) LIMIT 10)",
                "count_distinct\n" +
                        "10\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByThrowsOnTimeout() throws Exception {
        testParallelGroupByThrowsOnTimeout("select vwap(price, quantity) from tab");
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctIPv4Function() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ip2) FROM tab",
                "count_distinct\n" +
                        "3265\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctIntFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(a2) FROM tab",
                "count_distinct\n" +
                        "3308\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctLongFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(j2) FROM tab",
                "count_distinct\n" +
                        "3342\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 100",
                "vwap\tsum\n" +
                        "1981.006198090988\t3675.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMinMaxStrFunction() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT min(key), max(key) FROM tab",
                "min\tmax\n" +
                        "k0\tk4\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMinMaxSymbolFunction() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT min(key), max(key) FROM tab",
                "min\tmax\n" +
                        "k0\tk4\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMultipleCountDistinctIntFunctions() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(a), count_distinct(a2), count_distinct(ip), count_distinct(ip2) FROM tab",
                "count_distinct\tcount_distinct1\tcount_distinct2\tcount_distinct3\n" +
                        "4000\t3308\t4000\t3265\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithNestedFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(p, q), sum(ct) " +
                        "FROM (SELECT colTop ct, quantity q, price p FROM tab WHERE quantity < 80)",
                "vwap\tsum\n" +
                        "1974.5391511088592\t1885.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80",
                "vwap\tsum\n" +
                        "1974.5391511088592\t1885.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeTimestampFilter1() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ip) FROM tab WHERE ts in '1970-01-13' and a > 0",
                "count_distinct\n" +
                        "84\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeTimestampFilter2() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT min(key), max(key) FROM tab " +
                        "WHERE ts in '1970-01-13' and a > 0",
                "min\tmax\n" +
                        "k0\tk4\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE key = 'k1'",
                "vwap\tsum\n" +
                        "2682.7321472695826\t1638800.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeTimestampFilter1() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ip) FROM tab " +
                        "WHERE ts in '1970-01-13' and a > 0 and i in (select i from tab where length(i) = 4)",
                "count_distinct\n" +
                        "14\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeTimestampFilter2() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT min(i), max(i) FROM tab " +
                        "WHERE ts in '1970-01-13' and a > 0 and i in (select i from tab where length(i) >= 4 and length(i) <= 6)",
                "min\tmax\n" +
                        "NRXGZ\tXUXIBB\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTooStrictFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 0",
                "vwap\tsum\n" +
                        "NaN\tNaN\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTwoCountDistinctLongFunctions() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(j), count_distinct(j2) FROM tab",
                "count_distinct\tcount_distinct1\n" +
                        "4000\t3342\n"
        );
    }

    @Test
    public void testParallelOperationKeyGroupBy() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT ((key is not null) and (colTop is not null)) key, sum(colTop) FROM tab ORDER BY key",
                "key\tsum\n" +
                        "false\tNaN\n" +
                        "true\t8202000.0\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByConcurrent() throws Exception {
        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT key, avg + sum from (" +
                "  SELECT key, avg(value), sum(colTop) FROM tab" +
                ") ORDER BY key";
        final String expected = "key\tcolumn\n" +
                "k0\t1644027.5\n" +
                "k1\t1640823.5\n" +
                "k2\t1641624.5\n" +
                "k3\t1642425.5\n" +
                "k4\t1643226.5\n";

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(
                            compiler,
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  key STRING," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    insert(
                            compiler,
                            "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    insert(
                            compiler,
                            "insert into tab " +
                                    "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                    "from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

                    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(numOfThreads);

                    for (int i = 0; i < numOfThreads; i++) {
                        final int threadId = i;
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            TestUtils.await(barrier);
                            try {
                                for (int j = 0; j < numOfIterations; j++) {
                                    assertQueries(engine, sqlExecutionContext, sink, query, expected);
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.put(threadId, e);
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }
                    haltLatch.await();
                },
                configuration,
                LOG
        );

        if (!errors.isEmpty()) {
            for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
            }
            fail("Error in threads");
        }
    }

    @Test
    public void testParallelSingleKeyGroupByFaultTolerance() throws Exception {
        testParallelGroupByFaultTolerance(
                "select case when quantity > 100 then 'a lot' else 'a few' end, vwap(price, quantity) " +
                        "from tab " +
                        "where npe();"
        );
    }

    @Test
    public void testParallelSingleKeyGroupBySubQuery() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg + sum from (" +
                        "SELECT key, avg(value), sum(colTop) FROM tab" +
                        ") ORDER BY key",
                "key\tcolumn\n" +
                        "k0\t1644027.5\n" +
                        "k1\t1640823.5\n" +
                        "k2\t1641624.5\n" +
                        "k3\t1642425.5\n" +
                        "k4\t1643226.5\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByThrowsOnTimeout() throws Exception {
        testParallelGroupByThrowsOnTimeout("select quantity % 100, vwap(price, quantity) from tab");
    }

    @Test
    public void testParallelSingleKeyGroupByWithCountDistinctIntFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(a), count_distinct(ip) FROM tab ORDER BY key",
                "key\tcount_distinct\tcount_distinct1\n" +
                        "k0\t800\t800\n" +
                        "k1\t800\t800\n" +
                        "k2\t800\t800\n" +
                        "k3\t800\t800\n" +
                        "k4\t800\t800\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithCountDistinctLongFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(j) FROM tab ORDER BY key",
                "key\tcount_distinct\n" +
                        "k0\t800\n" +
                        "k1\t800\n" +
                        "k2\t800\n" +
                        "k3\t800\n" +
                        "k4\t800\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithFilter() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 80 ORDER BY key",
                "key\tavg\tsum\tcount\n" +
                        "k0\t46.25\t325.0\t20\n" +
                        "k1\t45.31818181818182\t381.0\t22\n" +
                        "k2\t46.31818181818182\t387.0\t22\n" +
                        "k3\t47.31818181818182\t393.0\t22\n" +
                        "k4\t48.31818181818182\t399.0\t22\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithNoFunctions() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab GROUP BY key ORDER BY key",
                "key\n" +
                        "k0\n" +
                        "k1\n" +
                        "k2\n" +
                        "k3\n" +
                        "k4\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithNoFunctionsAndFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab WHERE key != 'k1' GROUP BY key ORDER BY key",
                "key\n" +
                        "k0\n" +
                        "k2\n" +
                        "k3\n" +
                        "k4\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithNoFunctionsAndTooStrictFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab WHERE quantity < 0 GROUP BY key ORDER BY key",
                "key\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithReadThreadSafeTimestampFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(ip) FROM tab " +
                        "WHERE ts in '1970-01-13' and d < 1000 ORDER BY key DESC",
                "key\tcount_distinct\n" +
                        "k4\t28\n" +
                        "k3\t29\n" +
                        "k2\t27\n" +
                        "k1\t31\n" +
                        "k0\t27\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithReadThreadUnsafeTimestampFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(ip) FROM tab " +
                        "WHERE ts in '1970-01-13' and d < 1000 and key in ('k1', 'k2') ORDER BY key DESC",
                "key\tcount_distinct\n" +
                        "k2\t27\n" +
                        "k1\t31\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 0 ORDER BY key",
                "key\tavg\tsum\tcount\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithTwoCountDistinctLongFunctions() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(j), count_distinct(j2) FROM tab ORDER BY key",
                "key\tcount_distinct\tcount_distinct1\n" +
                        "k0\t800\t680\n" +
                        "k1\t800\t654\n" +
                        "k2\t800\t671\n" +
                        "k3\t800\t671\n" +
                        "k4\t800\t671\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupBy() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab ORDER BY key",
                "key\tavg\tsum\tcount\n" +
                        "k0\t2027.5\t1642000.0\t1600\n" +
                        "k1\t2023.5\t1638800.0\t1600\n" +
                        "k2\t2024.5\t1639600.0\t1600\n" +
                        "k3\t2025.5\t1640400.0\t1600\n" +
                        "k4\t2026.5\t1641200.0\t1600\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithLimit() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab ORDER BY key LIMIT 3",
                "key\tavg\tsum\n" +
                        "k0\t2027.5\t1642000.0\n" +
                        "k1\t2023.5\t1638800.0\n" +
                        "k2\t2024.5\t1639600.0\n",
                "SELECT key, avg(value), sum(colTop) FROM tab ORDER BY key LIMIT -3",
                "key\tavg\tsum\n" +
                        "k2\t2024.5\t1639600.0\n" +
                        "k3\t2025.5\t1640400.0\n" +
                        "k4\t2026.5\t1641200.0\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithMinMaxStrFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, min(n), max(n) FROM tab ORDER BY key",
                "key\tmin\tmax\n" +
                        "k0\tBBESDHRQLODO\tZZBIRKYMQIJSSYVB\n" +
                        "k1\tBBDTOUBOOV\tZZXERHVNQRHEMY\n" +
                        "k2\tBBCMFKVGYNNEND\tZZSVHMXRTDYBN\n" +
                        "k3\tBBXMKDIWWF\tZYUPDRBHF\n" +
                        "k4\tBBFVKZOVCXTXY\tZZKLNTGZNIDJXOX\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithMinMaxSymbolFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, min(i), max(i) FROM tab ORDER BY key",
                "key\tmin\tmax\n" +
                        "k0\tNRXGZ\tXUXIBB\n" +
                        "k1\tNRXGZ\tXUXIBB\n" +
                        "k2\tNRXGZ\tXUXIBB\n" +
                        "k3\tNRXGZ\tXUXIBB\n" +
                        "k4\tNRXGZ\tXUXIBB\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithNestedFilter() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT avg(v), k, sum(ct) " +
                        "FROM (SELECT colTop ct, value v, key k FROM tab WHERE value < 80) ORDER BY k",
                "avg\tk\tsum\n" +
                        "46.25\tk0\t325.0\n" +
                        "45.31818181818182\tk1\t381.0\n" +
                        "46.31818181818182\tk2\t387.0\n" +
                        "47.31818181818182\tk3\t393.0\n" +
                        "48.31818181818182\tk4\t399.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupBy() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0\t2685.431565967941\t1642000.0\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n" +
                        "k4\t2684.756229953121\t1641200.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByFilterWithSubQuery() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab " +
                        "where key in (select key from tab where key in ('k1','k3')) ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupBySubQuery() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap + sum from (" +
                        "SELECT key, vwap(price, quantity), sum(colTop) FROM tab" +
                        ") ORDER BY key",
                "key\tcolumn\n" +
                        "k0\t1644685.4315659679\n" +
                        "k1\t1641482.7321472696\n" +
                        "k2\t1642283.4065201285\n" +
                        "k3\t1643084.081214515\n" +
                        "k4\t1643884.7562299531\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithLimit() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key LIMIT 3",
                "key\tvwap\tsum\n" +
                        "k0\t2685.431565967941\t1642000.0\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n",
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key LIMIT -3",
                "key\tvwap\tsum\n" +
                        "k2\t2683.4065201284266\t1639600.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n" +
                        "k4\t2684.756229953121\t1641200.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithMinMaxStrFunction() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, min(key2), max(key2) FROM tab ORDER BY key1",
                "key1\tmin\tmax\n" +
                        "k0\tk0\tk0\n" +
                        "k1\tk1\tk1\n" +
                        "k2\tk2\tk2\n" +
                        "k3\tk3\tk3\n" +
                        "k4\tk4\tk4\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithNestedFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(p, q), k, sum(ct) " +
                        "FROM (SELECT colTop ct, price p, quantity q, key k FROM tab WHERE quantity < 80) ORDER BY k",
                "vwap\tk\tsum\n" +
                        "56.62162162162162\tk0\t325.0\n" +
                        "57.01805416248746\tk1\t381.0\n" +
                        "57.76545632973504\tk2\t387.0\n" +
                        "58.52353506243996\tk3\t393.0\n" +
                        "59.29162746942615\tk4\t399.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithWithReadThreadSafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80 ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0\t56.62162162162162\t325.0\n" +
                        "k1\t57.01805416248746\t381.0\n" +
                        "k2\t57.76545632973504\t387.0\n" +
                        "k3\t58.52353506243996\t393.0\n" +
                        "k4\t59.29162746942615\t399.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE key in ('k1','k2') ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n"
        );
    }

    @Test
    public void testStringKeyGroupByEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key STRING," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        assertQueries(
                                engine,
                                sqlExecutionContext,
                                "select key, sum(value) from tab ORDER BY key",
                                "key\tsum\n"
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String... queriesAndExpectedResults) throws SqlException {
        assertQueries(engine, sqlExecutionContext, sink, queriesAndExpectedResults);
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, StringSink sink, String... queriesAndExpectedResults) throws SqlException {
        for (int i = 0, n = queriesAndExpectedResults.length; i < n; i += 2) {
            final String query = queriesAndExpectedResults[i];
            final String expected = queriesAndExpectedResults[i + 1];
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    query,
                    sink,
                    expected
            );
        }
    }

    private void testParallelGroupByAllTypes(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "create table tab as (select" +
                                        " 'k' || ((50 + x) % 5) key," +
                                        " cast(x as int) kk," +
                                        " rnd_int() a," +
                                        " rnd_int(0, 1000000, 2) a2," +
                                        " rnd_ipv4() ip," +
                                        " rnd_ipv4('10.5/16', 2) ip2," +
                                        " rnd_boolean() b," +
                                        " rnd_str(1,1,2) c," +
                                        " rnd_double(2) d," +
                                        " rnd_float(2) e," +
                                        " rnd_short(10,1024) f," +
                                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                        " rnd_symbol(4,4,8,2) i," +
                                        " rnd_long() j," +
                                        " rnd_long(0, 1000000, 2) j2," +
                                        " timestamp_sequence(400000000000, 500000000) ts," +
                                        " rnd_byte(2,50) l," +
                                        " rnd_bin(10, 20, 2) m," +
                                        " rnd_str(5,16,2) n," +
                                        " rnd_char() cc," +
                                        " rnd_long256() l2," +
                                        " rnd_geohash(1) hash1b," +
                                        " rnd_geohash(2) hash2b," +
                                        " rnd_geohash(3) hash3b," +
                                        " rnd_geohash(5) hash1c," +
                                        " rnd_geohash(10) hash2c," +
                                        " rnd_geohash(20) hash4c," +
                                        " rnd_geohash(40) hash8c" +
                                        " from long_sequence(" + ROW_COUNT + ")) timestamp(ts) partition by day",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelGroupByFaultTolerance(String query) throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );

                        try {
                            try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                                try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    //noinspection StatementWithEmptyBody
                                    while (cursor.hasNext()) {
                                    } // drain cursor until exception
                                    Assert.fail();
                                }
                            }
                        } catch (Throwable e) {
                            TestUtils.assertContains(e.getMessage(), "unexpected filter error");
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelGroupByThrowsOnTimeout(String query) throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            currentMicros = 0;
            NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        @NotNull
                        public MillisecondClock getClock() {
                            return () -> Long.MAX_VALUE;
                        }

                        @Override
                        public long getQueryTimeout() {
                            return 1;
                        }
                    },
                    MemoryTag.NATIVE_DEFAULT
            );

            ddl(
                    "CREATE TABLE tab (" +
                            "  ts TIMESTAMP," +
                            "  price DOUBLE," +
                            "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY"
            );
            insert("insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")");

            context.with(
                    context.getSecurityContext(),
                    context.getBindVariableService(),
                    context.getRandom(),
                    context.getRequestFd(),
                    circuitBreaker
            );

            try {
                assertSql("", query);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
            } finally {
                context.with(
                        context.getSecurityContext(),
                        context.getBindVariableService(),
                        context.getRandom(),
                        context.getRequestFd(),
                        null
                );
                Misc.free(circuitBreaker);
            }
        });
    }

    private void testParallelMultiSymbolKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (\n" +
                                        "  ts TIMESTAMP," +
                                        "  key1 SYMBOL," +
                                        "  key2 SYMBOL," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelNonKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelStringKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool,
                    (engine) -> pool.assign(new GroupByMergeShardJob(engine.getMessageBus())),
                    (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key STRING," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelSymbolKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key SYMBOL," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, " +
                                        "  'k' || ((50 + x) % 5), 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }
}
