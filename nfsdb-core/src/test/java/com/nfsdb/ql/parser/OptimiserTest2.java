/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class OptimiserTest2 extends AbstractOptimiserTest {

    @Test
    public void testAddDoubleAndIntConst() throws Exception {
        createTabWithNaNs();
        final String expected = "KIWIHBROKZKUTIQ\t10.000003940208\t0.000003940208\n" +
                "KIWIHBROKZKUTIQ\t10.000241351052\t0.000241351052\n" +
                "KIWIHBROKZKUTIQ\t10.000000436641\t0.000000436641\n" +
                "KIWIHBROKZKUTIQ\t10.000000004652\t0.000000004652\n" +
                "KIWIHBROKZKUTIQ\t10.000000923917\t0.000000923917\n" +
                "KIWIHBROKZKUTIQ\t10.000029283142\t0.000029283142\n" +
                "KIWIHBROKZKUTIQ\t10.000274364982\t0.000274364982\n" +
                "KIWIHBROKZKUTIQ\t10.001289066189\t0.001289066189\n" +
                "KIWIHBROKZKUTIQ\t10.000000006491\t0.000000006491\n" +
                "KIWIHBROKZKUTIQ\t10.000000005004\t0.000000005004\n" +
                "KIWIHBROKZKUTIQ\t10.002742654004\t0.002742654004\n" +
                "KIWIHBROKZKUTIQ\t10.000000019003\t0.000000019003\n" +
                "KIWIHBROKZKUTIQ\t10.000001821820\t0.000001821820\n" +
                "KIWIHBROKZKUTIQ\t10.002185028803\t0.002185028803\n" +
                "KIWIHBROKZKUTIQ\t10.000000106593\t0.000000106593\n" +
                "KIWIHBROKZKUTIQ\t10.004314885940\t0.004314885940\n" +
                "KIWIHBROKZKUTIQ\t10.000000902759\t0.000000902759\n" +
                "KIWIHBROKZKUTIQ\t10.758562207222\t0.758562207222\n" +
                "KIWIHBROKZKUTIQ\t10.000022764745\t0.000022764745\n" +
                "KIWIHBROKZKUTIQ\t10.000000003658\t0.000000003658\n" +
                "KIWIHBROKZKUTIQ\t10.000409294655\t0.000409294655\n" +
                "KIWIHBROKZKUTIQ\t10.000000000000\t0.000000000000\n" +
                "KIWIHBROKZKUTIQ\t10.000000028044\t0.000000028044\n" +
                "KIWIHBROKZKUTIQ\t10.000000017631\t0.000000017631\n" +
                "KIWIHBROKZKUTIQ\t10.001603563025\t0.001603563025\n" +
                "KIWIHBROKZKUTIQ\t10.149615820497\t0.149615820497\n" +
                "KIWIHBROKZKUTIQ\t10.000000073341\t0.000000073341\n" +
                "KIWIHBROKZKUTIQ\t10.141415018588\t0.141415018588\n" +
                "KIWIHBROKZKUTIQ\t10.000002008458\t0.000002008458\n" +
                "KIWIHBROKZKUTIQ\t10.000013604858\t0.000013604858\n" +
                "KIWIHBROKZKUTIQ\t10.000000313020\t0.000000313020\n" +
                "KIWIHBROKZKUTIQ\t10.098757246509\t0.098757246509\n" +
                "KIWIHBROKZKUTIQ\t10.220758058131\t0.220758058131\n";

        assertThat(expected, "select id, x + 10, x from tab where x >= 0 and x <= 1 and id ~ 'HBRO'");

    }

    @Test
    public void testAddInt() throws Exception {
        createTabWithNaNs();
        final String expected = "CLLERSMKRZUDJGN\t-416\t-436\n" +
                "CLLERSMKRZUDJGN\t-434\t-454\n" +
                "CLLERSMKRZUDJGN\t-183\t-203\n" +
                "CLLERSMKRZUDJGN\t-280\t-300\n" +
                "CLLERSMKRZUDJGN\t-194\t-214\n" +
                "CLLERSMKRZUDJGN\t-419\t-439\n" +
                "CLLERSMKRZUDJGN\t-410\t-430\n" +
                "CLLERSMKRZUDJGN\t-247\t-267\n" +
                "CLLERSMKRZUDJGN\t-406\t-426\n" +
                "CLLERSMKRZUDJGN\t-389\t-409\n" +
                "CLLERSMKRZUDJGN\t-371\t-391\n" +
                "CLLERSMKRZUDJGN\t-188\t-208\n" +
                "CLLERSMKRZUDJGN\t-316\t-336\n" +
                "CLLERSMKRZUDJGN\t-291\t-311\n" +
                "CLLERSMKRZUDJGN\t-290\t-310\n" +
                "CLLERSMKRZUDJGN\t-478\t-498\n" +
                "CLLERSMKRZUDJGN\t-369\t-389\n" +
                "CLLERSMKRZUDJGN\t-335\t-355\n" +
                "CLLERSMKRZUDJGN\t-407\t-427\n" +
                "CLLERSMKRZUDJGN\t-339\t-359\n" +
                "CLLERSMKRZUDJGN\t-254\t-274\n" +
                "CLLERSMKRZUDJGN\t-464\t-484\n" +
                "CLLERSMKRZUDJGN\t-196\t-216\n" +
                "CLLERSMKRZUDJGN\t-282\t-302\n" +
                "CLLERSMKRZUDJGN\t-357\t-377\n" +
                "CLLERSMKRZUDJGN\t-337\t-357\n" +
                "CLLERSMKRZUDJGN\t-352\t-372\n" +
                "CLLERSMKRZUDJGN\t-436\t-456\n";

        assertThat(expected, "select id, w + 20, w from tab where id ~ 'LLER' and w < -200");

        final String expected2 = "CLLERSMKRZUDJGN\t17.552673339844\t-436\t433.552673339844\n" +
                "CLLERSMKRZUDJGN\t261.817749023438\t-454\t695.817749023438\n" +
                "CLLERSMKRZUDJGN\t219.212882995605\t-203\t402.212882995606\n" +
                "CLLERSMKRZUDJGN\t230.619750976563\t-300\t510.619750976563\n" +
                "CLLERSMKRZUDJGN\t-193.999999997492\t-214\t0.000000002508\n" +
                "CLLERSMKRZUDJGN\t93.000000000000\t-439\t512.000000000000\n" +
                "CLLERSMKRZUDJGN\t-409.025098264217\t-430\t0.974901735783\n" +
                "CLLERSMKRZUDJGN\t-435.000000000000\t-267\t-188.000000000000\n" +
                "CLLERSMKRZUDJGN\t-405.874753475189\t-426\t0.125246524811\n" +
                "CLLERSMKRZUDJGN\t379.000000000000\t-409\t768.000000000000\n" +
                "CLLERSMKRZUDJGN\t-370.985364873894\t-391\t0.014635126106\n" +
                "CLLERSMKRZUDJGN\t-187.999999994615\t-208\t0.000000005385\n" +
                "CLLERSMKRZUDJGN\t-246.611997604370\t-336\t69.388002395630\n" +
                "CLLERSMKRZUDJGN\t-843.457031250000\t-311\t-552.457031250000\n" +
                "CLLERSMKRZUDJGN\t-964.750000000000\t-310\t-674.750000000000\n" +
                "CLLERSMKRZUDJGN\t-477.999999974712\t-498\t0.000000025288\n" +
                "CLLERSMKRZUDJGN\t-368.993329117890\t-389\t0.006670882110\n" +
                "CLLERSMKRZUDJGN\t-290.455463409424\t-355\t44.544536590576\n" +
                "CLLERSMKRZUDJGN\t-384.206546306610\t-427\t22.793453693390\n" +
                "CLLERSMKRZUDJGN\t-338.969897582196\t-359\t0.030102417804\n" +
                "CLLERSMKRZUDJGN\t-116.773674011230\t-274\t137.226325988770\n" +
                "CLLERSMKRZUDJGN\t-976.000000000000\t-484\t-512.000000000000\n" +
                "CLLERSMKRZUDJGN\t-189.609647154808\t-216\t6.390352845192\n" +
                "CLLERSMKRZUDJGN\t-881.211730957031\t-302\t-599.211730957031\n" +
                "CLLERSMKRZUDJGN\t-335.414469718933\t-377\t21.585530281067\n" +
                "CLLERSMKRZUDJGN\t-336.999599984134\t-357\t0.000400015866\n" +
                "CLLERSMKRZUDJGN\t-351.999999861198\t-372\t0.000000138802\n" +
                "CLLERSMKRZUDJGN\t255.250000000000\t-456\t691.250000000000\n";

        assertThat(expected2, "select id, x+(w + 20), w, x from tab where id ~ 'LLER' and w < -200");
        final String expected3 = "CLLERSMKRZUDJGN\t-343\t-436\t73\n" +
                "CLLERSMKRZUDJGN\t-267\t-454\t167\n" +
                "CLLERSMKRZUDJGN\t112\t-203\t295\n" +
                "CLLERSMKRZUDJGN\t-6\t-300\t274\n" +
                "CLLERSMKRZUDJGN\t-164\t-214\t30\n" +
                "CLLERSMKRZUDJGN\t-709\t-439\t-290\n" +
                "CLLERSMKRZUDJGN\t-408\t-430\t2\n" +
                "CLLERSMKRZUDJGN\t-716\t-267\t-469\n" +
                "CLLERSMKRZUDJGN\t-636\t-426\t-230\n" +
                "CLLERSMKRZUDJGN\tNaN\t-409\tNaN\n" +
                "CLLERSMKRZUDJGN\t-614\t-391\t-243\n" +
                "CLLERSMKRZUDJGN\tNaN\t-208\tNaN\n" +
                "CLLERSMKRZUDJGN\t-369\t-336\t-53\n" +
                "CLLERSMKRZUDJGN\t-593\t-311\t-302\n" +
                "CLLERSMKRZUDJGN\t-227\t-310\t63\n" +
                "CLLERSMKRZUDJGN\t-51\t-498\t427\n" +
                "CLLERSMKRZUDJGN\t-253\t-389\t116\n" +
                "CLLERSMKRZUDJGN\t-557\t-355\t-222\n" +
                "CLLERSMKRZUDJGN\t-83\t-427\t324\n" +
                "CLLERSMKRZUDJGN\tNaN\t-359\tNaN\n" +
                "CLLERSMKRZUDJGN\t-189\t-274\t65\n" +
                "CLLERSMKRZUDJGN\tNaN\t-484\tNaN\n" +
                "CLLERSMKRZUDJGN\t273\t-216\t469\n" +
                "CLLERSMKRZUDJGN\t-92\t-302\t190\n" +
                "CLLERSMKRZUDJGN\tNaN\t-377\tNaN\n" +
                "CLLERSMKRZUDJGN\t-614\t-357\t-277\n" +
                "CLLERSMKRZUDJGN\t121\t-372\t473\n" +
                "CLLERSMKRZUDJGN\t-340\t-456\t96\n";

        assertThat(expected3, "select id, z+(w + 20), w, z from tab where id ~ 'LLER' and w < -200");
    }

    @Test
    public void testAddLong() throws Exception {
        createTabWithNaNs();
        final String expected = "BROMNXKUIZULIGY\t506\t256\n" +
                "BROMNXKUIZULIGY\t652\t402\n" +
                "BROMNXKUIZULIGY\t668\t418\n" +
                "BROMNXKUIZULIGY\t688\t438\n" +
                "BROMNXKUIZULIGY\t706\t456\n" +
                "BROMNXKUIZULIGY\t650\t400\n" +
                "BROMNXKUIZULIGY\t531\t281\n" +
                "BROMNXKUIZULIGY\t591\t341\n" +
                "BROMNXKUIZULIGY\t514\t264\n" +
                "BROMNXKUIZULIGY\t672\t422\n" +
                "BROMNXKUIZULIGY\t590\t340\n" +
                "BROMNXKUIZULIGY\t738\t488\n" +
                "BROMNXKUIZULIGY\t627\t377\n" +
                "BROMNXKUIZULIGY\t655\t405\n" +
                "BROMNXKUIZULIGY\t550\t300\n" +
                "BROMNXKUIZULIGY\t723\t473\n" +
                "BROMNXKUIZULIGY\t737\t487\n" +
                "BROMNXKUIZULIGY\t734\t484\n";

        assertThat(expected, "select id, z + 250, z from tab where z >= 250 and id ~ 'ULIGY'");

        final String expected2 = "BROMNXKUIZULIGY\t-59.255371093750\t221\t-290.255371093750\n" +
                "BROMNXKUIZULIGY\t267.535586595535\t256\t1.535586595535\n" +
                "BROMNXKUIZULIGY\t412.000018740170\t402\t0.000018740170\n" +
                "BROMNXKUIZULIGY\t1211.500000000000\t418\t783.500000000000\n" +
                "BROMNXKUIZULIGY\t-958.000000000000\t56\t-1024.000000000000\n" +
                "BROMNXKUIZULIGY\t660.000000000000\t438\t212.000000000000\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t0.000006116291\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t0.000010187643\n" +
                "BROMNXKUIZULIGY\t466.000153154480\t456\t0.000153154480\n" +
                "BROMNXKUIZULIGY\t195.474104791880\t185\t0.474104791880\n" +
                "BROMNXKUIZULIGY\t410.001604365563\t400\t0.001604365563\n" +
                "BROMNXKUIZULIGY\t418.070312500000\t281\t127.070312500000\n" +
                "BROMNXKUIZULIGY\t-677.000000000000\t-455\t-232.000000000000\n" +
                "BROMNXKUIZULIGY\t-479.999999985485\t-490\t0.000000014515\n" +
                "BROMNXKUIZULIGY\t768.000000000000\t-50\t808.000000000000\n" +
                "BROMNXKUIZULIGY\t-117.999999720194\t-128\t0.000000279806\n" +
                "BROMNXKUIZULIGY\t-277.361108899117\t-289\t1.638891100883\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t0.000251280326\n" +
                "BROMNXKUIZULIGY\t-308.999952732278\t-319\t0.000047267722\n" +
                "BROMNXKUIZULIGY\t26.000149987023\t16\t0.000149987023\n" +
                "BROMNXKUIZULIGY\t-423.956409015693\t-434\t0.043590984307\n" +
                "BROMNXKUIZULIGY\t-781.000000000000\t-279\t-512.000000000000\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t76.327049255371\n" +
                "BROMNXKUIZULIGY\t607.000000000000\t341\t256.000000000000\n" +
                "BROMNXKUIZULIGY\t138.000000000000\t128\t0.000000000000\n" +
                "BROMNXKUIZULIGY\t398.000000000000\t-92\t480.000000000000\n" +
                "BROMNXKUIZULIGY\t626.569580078125\t17\t599.569580078125\n" +
                "BROMNXKUIZULIGY\t116.143798828125\t-50\t156.143798828125\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t0.000720751763\n" +
                "BROMNXKUIZULIGY\t274.000144880152\t264\t0.000144880152\n" +
                "BROMNXKUIZULIGY\t-34.999607196361\t-45\t0.000392803639\n" +
                "BROMNXKUIZULIGY\t226.648437500000\t-299\t515.648437500000\n" +
                "BROMNXKUIZULIGY\t432.003766982234\t422\t0.003766982234\n" +
                "BROMNXKUIZULIGY\t487.326309204102\t60\t417.326309204102\n" +
                "BROMNXKUIZULIGY\t516.000000000000\t-134\t640.000000000000\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t-664.000000000000\n" +
                "BROMNXKUIZULIGY\t194.359175443649\t179\t5.359175443649\n" +
                "BROMNXKUIZULIGY\t66.252536773682\t48\t8.252536773682\n" +
                "BROMNXKUIZULIGY\t3.425781250000\t53\t-59.574218750000\n" +
                "BROMNXKUIZULIGY\t-452.999999995122\t-463\t0.000000004878\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t0.010744831059\n" +
                "BROMNXKUIZULIGY\t-40.999999992162\t-51\t0.000000007838\n" +
                "BROMNXKUIZULIGY\t-848.431396484375\t29\t-887.431396484375\n" +
                "BROMNXKUIZULIGY\t-290.999999830891\t-301\t0.000000169109\n" +
                "BROMNXKUIZULIGY\t122.000168943778\t112\t0.000168943778\n" +
                "BROMNXKUIZULIGY\t339.000000000000\t-183\t512.000000000000\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t7.454445600510\n" +
                "BROMNXKUIZULIGY\t240.001445442904\t230\t0.001445442904\n" +
                "BROMNXKUIZULIGY\t249.375976562500\t-261\t500.375976562500\n" +
                "BROMNXKUIZULIGY\t-819.500000000000\t0\t-829.500000000000\n" +
                "BROMNXKUIZULIGY\t-364.999998135639\t-375\t0.000001864361\n" +
                "BROMNXKUIZULIGY\t92.284413337708\t81\t1.284413337708\n" +
                "BROMNXKUIZULIGY\t-908.812500000000\t-272\t-646.812500000000\n" +
                "BROMNXKUIZULIGY\t-218.879174321890\t-229\t0.120825678110\n" +
                "BROMNXKUIZULIGY\t350.000097934379\t340\t0.000097934379\n" +
                "BROMNXKUIZULIGY\t8.000360143276\t-2\t0.000360143276\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t736.000000000000\n" +
                "BROMNXKUIZULIGY\t498.000020276546\t488\t0.000020276546\n" +
                "BROMNXKUIZULIGY\t-24.999999978179\t-35\t0.000000021821\n" +
                "BROMNXKUIZULIGY\t486.000000000000\t220\t256.000000000000\n" +
                "BROMNXKUIZULIGY\t-188.975300276652\t-199\t0.024699723348\n" +
                "BROMNXKUIZULIGY\t-135.999999362280\t-146\t0.000000637720\n" +
                "BROMNXKUIZULIGY\t65.700256347656\t-393\t448.700256347656\n" +
                "BROMNXKUIZULIGY\t-1011.250000000000\t-499\t-522.250000000000\n" +
                "BROMNXKUIZULIGY\t-384.984375000000\t45\t-439.984375000000\n" +
                "BROMNXKUIZULIGY\t-147.000000000000\t-249\t92.000000000000\n" +
                "BROMNXKUIZULIGY\t-330.999999834806\t-341\t0.000000165194\n" +
                "BROMNXKUIZULIGY\t-960.312500000000\t-241\t-729.312500000000\n" +
                "BROMNXKUIZULIGY\tNaN\tNaN\t384.000000000000\n" +
                "BROMNXKUIZULIGY\t-208.853453457356\t-220\t1.146546542645\n" +
                "BROMNXKUIZULIGY\t398.924531459808\t377\t11.924531459808\n" +
                "BROMNXKUIZULIGY\t415.000000109825\t405\t0.000000109825\n" +
                "BROMNXKUIZULIGY\t-69.875890344381\t-80\t0.124109655619\n" +
                "BROMNXKUIZULIGY\t232.000052321986\t222\t0.000052321986\n" +
                "BROMNXKUIZULIGY\t-202.054351806641\t300\t-512.054351806641\n" +
                "BROMNXKUIZULIGY\t483.000926071953\t473\t0.000926071953\n" +
                "BROMNXKUIZULIGY\t497.681155398488\t487\t0.681155398488\n" +
                "BROMNXKUIZULIGY\t84.000002542528\t74\t0.000002542528\n" +
                "BROMNXKUIZULIGY\t226.000292068784\t216\t0.000292068784\n" +
                "BROMNXKUIZULIGY\t99.032178618945\t89\t0.032178618945\n" +
                "BROMNXKUIZULIGY\t256.000000000000\t-10\t256.000000000000\n" +
                "BROMNXKUIZULIGY\t494.000000013356\t484\t0.000000013356\n" +
                "BROMNXKUIZULIGY\t587.446777343750\t-58\t635.446777343750\n" +
                "BROMNXKUIZULIGY\t43.634443283081\t-23\t56.634443283081\n";

        assertThat(expected2, "select id, (z + 10) + x, z, x from tab where id ~ 'ULIGY'");
    }

    @Test
    public void testDoubleEquals() throws Exception {
        createTabWithNaNs2();
        final String expected = "512.000000000000\t512.000000000000\n" +
                "-512.000000000000\t-512.000000000000\n" +
                "-512.000000000000\t-512.000000000000\n" +
                "0.000000000000\t0.000000000000\n";

        assertThat(expected, "select x,y from tab where x=y");

    }

    @Test
    public void testDoubleEqualsNaN() throws Exception {
        createTabWithNaNs();
        final String expected4 = "SCJOUOUIGENFELW\tNaN\tNaN\t67\n" +
                "LLEYMIWTCWLFORG\tNaN\tNaN\t19\n" +
                "NSXHHDILELRUMMZ\tNaN\tNaN\t35\n" +
                "MQMUDDCIHCNPUGJ\tNaN\tNaN\t55\n" +
                "PDHHGGIWHPZRHHM\tNaN\tNaN\t19\n" +
                "UWZOOVPPLIPRMDB\tNaN\tNaN\t24\n";

        assertThat(expected4, "select id, y, z, w from tab where y = NaN and z = NaN and w > 0 and w < 100");
    }

    @Test
    public void testDoubleGreaterThanLong() throws Exception {
        createTabNoNaNs();
        final String expected2 = "ELLKKHTWNWIFFLR\t297.791748046875\t-3667808512242035601\n" +
                "ELLKKHTWNWIFFLR\t-128.000000000000\t-2480561350807009780\n" +
                "ELLKKHTWNWIFFLR\t0.000000039690\t-1353669656981228147\n" +
                "ELLKKHTWNWIFFLR\t574.872116088867\t-3672802971292736086\n" +
                "ELLKKHTWNWIFFLR\t0.042308801785\t-8211394673630614539\n" +
                "ELLKKHTWNWIFFLR\t559.121917724609\t-8480545405650604184\n" +
                "ELLKKHTWNWIFFLR\t238.864746093750\t-8379531572046504130\n" +
                "ELLKKHTWNWIFFLR\t54.359863281250\t-3428995636807101867\n" +
                "ELLKKHTWNWIFFLR\t0.882504612207\t-6849499820684489569\n" +
                "ELLKKHTWNWIFFLR\t-763.598632812500\t-8452913288971477984\n" +
                "ELLKKHTWNWIFFLR\t0.000000210756\t-6068564420724880784\n" +
                "ELLKKHTWNWIFFLR\t0.000016662992\t-7248871158761080358\n" +
                "ELLKKHTWNWIFFLR\t15.427758693695\t-8228471787113332608\n" +
                "ELLKKHTWNWIFFLR\t257.875000000000\t-9035530510236235549\n" +
                "ELLKKHTWNWIFFLR\t0.000000017807\t-7942345008044734492\n" +
                "ELLKKHTWNWIFFLR\t0.000000004254\t-6954754049368371082\n" +
                "ELLKKHTWNWIFFLR\t166.464355468750\t-7951399573747427375\n" +
                "ELLKKHTWNWIFFLR\t-592.000000000000\t-6347971497010040712\n" +
                "ELLKKHTWNWIFFLR\t0.553697407246\t-6066174730230152205\n" +
                "ELLKKHTWNWIFFLR\t-523.500000000000\t-8348983847397851938\n" +
                "ELLKKHTWNWIFFLR\t4.989529609680\t-8069522251723029044\n" +
                "ELLKKHTWNWIFFLR\t0.000054070339\t-8854203718260377704\n" +
                "ELLKKHTWNWIFFLR\t0.000041724121\t-7422008773883138925\n" +
                "ELLKKHTWNWIFFLR\t15.229828834534\t-9067189034165407096\n" +
                "ELLKKHTWNWIFFLR\t1.147946476936\t-6173082243678285767\n" +
                "ELLKKHTWNWIFFLR\t0.002029277734\t-6870496635654489557\n" +
                "ELLKKHTWNWIFFLR\t0.962346389890\t-7210334357949030384\n" +
                "ELLKKHTWNWIFFLR\t-1003.000000000000\t-8466817036372362690\n" +
                "ELLKKHTWNWIFFLR\t769.317382812500\t-4163745463633062001\n" +
                "ELLKKHTWNWIFFLR\t5.615920782089\t-5893035837620611936\n" +
                "ELLKKHTWNWIFFLR\t0.000002686209\t-6412588441789765608\n" +
                "ELLKKHTWNWIFFLR\t886.468750000000\t-8988986703954297491\n" +
                "ELLKKHTWNWIFFLR\t480.000000000000\t-3204352031405525466\n" +
                "ELLKKHTWNWIFFLR\t0.000000062803\t-7378155554311656758\n" +
                "ELLKKHTWNWIFFLR\t-1005.500000000000\t-6124707786689761772\n" +
                "ELLKKHTWNWIFFLR\t0.440555199981\t-1389821736495367122\n" +
                "ELLKKHTWNWIFFLR\t266.759155273438\t-5786884395180365909\n" +
                "ELLKKHTWNWIFFLR\t-477.012908935547\t-8007008358829053694\n" +
                "ELLKKHTWNWIFFLR\t0.000251047953\t-1192005720958947897\n" +
                "ELLKKHTWNWIFFLR\t0.000171783307\t-3215580874046433001\n" +
                "ELLKKHTWNWIFFLR\t351.105468750000\t-7851460397851196709\n" +
                "ELLKKHTWNWIFFLR\t740.000000000000\t-9134741907814147811\n" +
                "ELLKKHTWNWIFFLR\t-880.000000000000\t-8365335649241205979\n" +
                "ELLKKHTWNWIFFLR\t-912.937500000000\t-8833078844097231375\n" +
                "ELLKKHTWNWIFFLR\t-442.691406250000\t-7785098153707841074\n" +
                "ELLKKHTWNWIFFLR\t0.001473797718\t-9127159587821404289\n" +
                "ELLKKHTWNWIFFLR\t0.083041120321\t-8675132201476021699\n" +
                "ELLKKHTWNWIFFLR\t3.126584589481\t-7476947810350851317\n" +
                "ELLKKHTWNWIFFLR\t229.976379394531\t-7315094745979069439\n" +
                "ELLKKHTWNWIFFLR\t255.432174682617\t-3735628027815901806\n";

        assertThat(expected2, "select id,x,z from tab where x > z and id ~ 'LLK'");
    }

    @Test
    public void testDoubleLessOrEqual() throws Exception {
        createTabWithNaNs();

        final String expected = "ZKDMPVRHWUVMBPS\t0.000000782469\t-0.000000782469\t0.000001559133\n" +
                "ZKDMPVRHWUVMBPS\t992.000000000000\t-992.000000000000\t-964.568237304688\n" +
                "ZKDMPVRHWUVMBPS\t1.501749962568\t-1.501749962568\t0.000000363599\n" +
                "ZKDMPVRHWUVMBPS\t0.000000029045\t-0.000000029045\t672.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t705.327758789063\t-705.327758789063\t940.560058593750\n" +
                "ZKDMPVRHWUVMBPS\t776.500000000000\t-776.500000000000\t0.477167338133\n" +
                "ZKDMPVRHWUVMBPS\t0.000000011066\t-0.000000011066\t2.694594800472\n" +
                "ZKDMPVRHWUVMBPS\t238.082031250000\t-238.082031250000\t690.500000000000\n" +
                "ZKDMPVRHWUVMBPS\t0.056706264615\t-0.056706264615\t149.757812500000\n" +
                "ZKDMPVRHWUVMBPS\t0.000010041330\t-0.000010041330\t0.226926967502\n" +
                "ZKDMPVRHWUVMBPS\t810.505126953125\t-810.505126953125\t0.000000073849\n" +
                "ZKDMPVRHWUVMBPS\t0.000000100292\t-0.000000100292\t492.164550781250\n" +
                "ZKDMPVRHWUVMBPS\t0.000000005093\t-0.000000005093\t0.140176177025\n" +
                "ZKDMPVRHWUVMBPS\t0.000000009899\t-0.000000009899\t0.002363841981\n" +
                "ZKDMPVRHWUVMBPS\t0.163436520845\t-0.163436520845\t0.000000003471\n" +
                "ZKDMPVRHWUVMBPS\t-362.312500000000\t362.312500000000\t695.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t440.015625000000\t-440.015625000000\t96.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t244.125000000000\t-244.125000000000\t592.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t-653.000000000000\t653.000000000000\t899.097412109375\n" +
                "ZKDMPVRHWUVMBPS\t0.000029481607\t-0.000029481607\t531.110839843750\n" +
                "ZKDMPVRHWUVMBPS\t3.174971699715\t-3.174971699715\t742.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t0.001115875493\t-0.001115875493\t0.000040224068\n" +
                "ZKDMPVRHWUVMBPS\t0.236538611352\t-0.236538611352\t606.500000000000\n" +
                "ZKDMPVRHWUVMBPS\t188.476562500000\t-188.476562500000\t0.202657476068\n" +
                "ZKDMPVRHWUVMBPS\t1.538488507271\t-1.538488507271\t0.000005232169\n" +
                "ZKDMPVRHWUVMBPS\t0.000000342807\t-0.000000342807\t375.093750000000\n" +
                "ZKDMPVRHWUVMBPS\t0.000031970392\t-0.000031970392\t1.120753705502\n" +
                "ZKDMPVRHWUVMBPS\t177.562500000000\t-177.562500000000\t0.606957197189\n" +
                "ZKDMPVRHWUVMBPS\t0.000002141734\t-0.000002141734\t0.434345155954\n" +
                "ZKDMPVRHWUVMBPS\t50.848601341248\t-50.848601341248\t0.075392298400\n" +
                "ZKDMPVRHWUVMBPS\t0.929583758116\t-0.929583758116\t0.000000009447\n" +
                "ZKDMPVRHWUVMBPS\t57.289062500000\t-57.289062500000\t0.000001145164\n" +
                "ZKDMPVRHWUVMBPS\t0.000000030220\t-0.000000030220\t0.000000004784\n" +
                "ZKDMPVRHWUVMBPS\t0.461020886898\t-0.461020886898\t0.001043495402\n" +
                "ZKDMPVRHWUVMBPS\t0.000082285467\t-0.000082285467\t0.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t748.596679687500\t-748.596679687500\t0.066894590855\n" +
                "ZKDMPVRHWUVMBPS\t24.985045433044\t-24.985045433044\t0.000000010769\n" +
                "ZKDMPVRHWUVMBPS\t0.000000164400\t-0.000000164400\t0.000000440414\n" +
                "ZKDMPVRHWUVMBPS\t0.000245498741\t-0.000245498741\t32.000000000000\n" +
                "ZKDMPVRHWUVMBPS\t2.650694251060\t-2.650694251060\t0.000000044308\n" +
                "ZKDMPVRHWUVMBPS\t0.000123091359\t-0.000123091359\t0.000007164070\n" +
                "ZKDMPVRHWUVMBPS\t0.000326511123\t-0.000326511123\t0.000377249235\n";

        assertThat(expected, "select id, x, -x, y from tab where -x <= y and id ~ 'MBP'");
    }

    @Test
    public void testIntGreaterThanLong() throws Exception {
        createTabNoNaNs();
        final String expected1 = "YYPDVRGRQGKNPHK\t1732259734\t-3021521195481153751\n" +
                "YYVSYYEQBORDTQH\t1074418358\t-1250013196955974337\n" +
                "YYVSYYEQBORDTQH\t1869846049\t-3293939683000520022\n" +
                "YYVSYYEQBORDTQH\t2107101718\t-4059279568806753897\n" +
                "YYPDVRGRQGKNPHK\t43895530\t-8215185008367459611\n" +
                "YYVSYYEQBORDTQH\t1807433486\t-6695197968174233668\n" +
                "YYVSYYEQBORDTQH\t1682781018\t-8961571799857858169\n" +
                "YYVSYYEQBORDTQH\t1389327737\t-8150537647016473904\n" +
                "YYVSYYEQBORDTQH\t232536155\t-2062686406452665888\n" +
                "YYVSYYEQBORDTQH\t655260350\t-6611381597279030524\n" +
                "YYPDVRGRQGKNPHK\t1450685521\t-6755470093605033649\n" +
                "YYPDVRGRQGKNPHK\t775128500\t-1603384165602419979\n" +
                "YYVSYYEQBORDTQH\t302641038\t-6659218620013274534\n" +
                "YYPDVRGRQGKNPHK\t42770157\t-7725045074040640547\n" +
                "YYVSYYEQBORDTQH\t930570545\t-5165705652147500083\n" +
                "YYPDVRGRQGKNPHK\t686867046\t-7995900445378478368\n" +
                "YYVSYYEQBORDTQH\t942293136\t-4354832645014387548\n" +
                "YYVSYYEQBORDTQH\t469224578\t-1713177576758902743\n" +
                "YYPDVRGRQGKNPHK\t760033546\t-3061237516373607267\n" +
                "YYVSYYEQBORDTQH\t638901687\t-6964333218227647733\n" +
                "YYPDVRGRQGKNPHK\t2011914222\t-8564422627802162493\n" +
                "YYPDVRGRQGKNPHK\t477899560\t-6145013344845734540\n" +
                "YYPDVRGRQGKNPHK\t730358123\t-8028766349854463734\n" +
                "YYPDVRGRQGKNPHK\t2138967707\t-5910759393125456205\n" +
                "YYVSYYEQBORDTQH\t1775722971\t-559701791304616111\n" +
                "YYVSYYEQBORDTQH\t786289628\t-8886911689567580315\n" +
                "YYVSYYEQBORDTQH\t1620460059\t-3884478673753129704\n" +
                "YYPDVRGRQGKNPHK\t410967417\t-7207880573114798977\n" +
                "YYPDVRGRQGKNPHK\t113885025\t-8070027230069846195\n" +
                "YYPDVRGRQGKNPHK\t64230586\t-3840280077638956674\n" +
                "YYVSYYEQBORDTQH\t1692887379\t-8643677238336803273\n";

        assertThat(expected1, "select id, w, z from tab where w > z and w > 0 and id ~ '^YY'");
    }

    @Test
    public void testIntLessOrEqual() throws Exception {
        createTabWithNaNs2();

        final String expected = "GWFFYUDEYYQEHBH\t99\ttrue\n" +
                "ZVRLPTYXYGYFUXC\t100\ttrue\n" +
                "BROMNXKUIZULIGY\t98\ttrue\n" +
                "QZVKHTLQZSLQVFG\t96\ttrue\n" +
                "NWDSWLUVDRHFBCZ\t100\ttrue\n" +
                "NKGQVZWEVQTQOZK\t96\ttrue\n" +
                "RQLGYDONNLITWGL\t94\ttrue\n" +
                "KFIJZZYNPPBXBHV\t94\ttrue\n" +
                "HYBTVZNCLNXFSUW\t100\ttrue\n" +
                "WSWSRGOONFCLTJC\t98\ttrue\n" +
                "KUNRDCWNPQYTEWH\t93\ttrue\n" +
                "VCVUYGMBMKSCPWL\t94\ttrue\n" +
                "KIWIHBROKZKUTIQ\t90\ttrue\n" +
                "SJOJIPHZEPIHVLT\t93\ttrue\n" +
                "ZSFXUNYQXTGNJJI\t95\ttrue\n" +
                "YUHNBCCPMOOUHWU\t93\ttrue\n" +
                "UGGLNYRZLCBDMIG\t94\ttrue\n" +
                "NZVDJIGSYLXGYTE\t93\ttrue\n" +
                "PDHHGGIWHPZRHHM\t99\ttrue\n" +
                "FUUTOMFUIOXLQLU\t100\ttrue\n" +
                "BHLNEJRMDIKDISG\t100\ttrue\n" +
                "IOLYLPGZHITQJLK\t95\ttrue\n" +
                "WCCNGTNLEGPUHHI\t91\ttrue\n" +
                "PPRGSXBHYSBQYMI\t90\ttrue\n" +
                "CMONRCXNUZFNWHF\t97\ttrue\n" +
                "KBBQFNPOYNNCTFS\t92\ttrue\n" +
                "KUNRDCWNPQYTEWH\t92\ttrue\n" +
                "NMUREIJUHCLQCMZ\t94\ttrue\n" +
                "THMHZNVZHCNXZEQ\t100\ttrue\n" +
                "SNGIZRPFMDVVGSV\t93\ttrue\n" +
                "UXBWYWRLHUHJECI\t96\ttrue\n" +
                "BHLNEJRMDIKDISG\t94\ttrue\n" +
                "CIWXCYXGDHUWEPV\t97\ttrue\n" +
                "KFMQNTOGMXUKLGM\t100\ttrue\n" +
                "ZJSVTNPIWZNFKPE\t97\ttrue\n" +
                "YUHNBCCPMOOUHWU\t96\ttrue\n" +
                "UMKUBKXPMSXQSTV\t95\ttrue\n" +
                "PXMKJSMKIXEYVTU\t96\ttrue\n" +
                "KOJSOLDYRODIPUN\t90\ttrue\n" +
                "FOWLPDXYSBEOUOJ\t91\ttrue\n" +
                "BHLNEJRMDIKDISG\t93\ttrue\n" +
                "PNXHQUTZODWKOCP\t99\ttrue\n" +
                "NZVDJIGSYLXGYTE\t91\ttrue\n" +
                "RZUPVQFULMERTPI\t100\ttrue\n" +
                "PNXHQUTZODWKOCP\t95\ttrue\n" +
                "HOLNVTIQBZXIOVI\t100\ttrue\n" +
                "XSLUQDYOPHNIMYF\t98\ttrue\n" +
                "VTJWCPSWHYRXPEH\t95\ttrue\n" +
                "STYSWHLSWPFHXDB\t100\ttrue\n" +
                "NWDSWLUVDRHFBCZ\t90\ttrue\n" +
                "DLRBIDSTDTFBYHS\t98\ttrue\n" +
                "JUEBWVLOMPBETTT\t97\ttrue\n" +
                "CCYVBDMQEHDHQHK\t95\ttrue\n" +
                "HBXOWVYUVVRDPCH\t96\ttrue\n" +
                "NWDSWLUVDRHFBCZ\t97\ttrue\n" +
                "VFZFKWZLUOGXHFV\t100\ttrue\n" +
                "SCJOUOUIGENFELW\t93\ttrue\n" +
                "CNGZTOYTOXRSFPV\t93\ttrue\n" +
                "GZJYYFLSVIHDWWL\t100\ttrue\n" +
                "IWEODDBHEVGXYHJ\t97\ttrue\n" +
                "PDHHGGIWHPZRHHM\t96\ttrue\n" +
                "WSWSRGOONFCLTJC\t100\ttrue\n" +
                "YSSMPGLUOHNZHZS\t96\ttrue\n" +
                "BHLNEJRMDIKDISG\t97\ttrue\n" +
                "NRXGZSXUXIBBTGP\t100\ttrue\n" +
                "FIEVMKPYVGPYKKB\t96\ttrue\n" +
                "KOJSOLDYRODIPUN\t91\ttrue\n" +
                "XWCKYLSUWDSWUGS\t92\ttrue\n" +
                "YYVSYYEQBORDTQH\t97\ttrue\n" +
                "JWIMGPLRQUJJFGQ\t96\ttrue\n" +
                "XZOUICWEKGHVUVS\t97\ttrue\n" +
                "KBBQFNPOYNNCTFS\t95\ttrue\n" +
                "IFGZUFEVTEROCBP\t92\ttrue\n" +
                "VFZFKWZLUOGXHFV\t95\ttrue\n" +
                "KJSMSSUQSRLTKVV\t90\ttrue\n" +
                "VMCGFNWGRMDGGIJ\t95\ttrue\n" +
                "YSSMPGLUOHNZHZS\t98\ttrue\n" +
                "KIWIHBROKZKUTIQ\t92\ttrue\n" +
                "YYPDVRGRQGKNPHK\t99\ttrue\n" +
                "SHRUEDRQQULOFJG\t96\ttrue\n" +
                "VFZFKWZLUOGXHFV\t94\ttrue\n" +
                "NMUREIJUHCLQCMZ\t96\ttrue\n" +
                "PPRGSXBHYSBQYMI\t98\ttrue\n" +
                "KOJSOLDYRODIPUN\t95\ttrue\n" +
                "QFNIZOSBOSEPGIU\t94\ttrue\n" +
                "PNXHQUTZODWKOCP\t92\ttrue\n" +
                "PNXHQUTZODWKOCP\t93\ttrue\n" +
                "PXMKJSMKIXEYVTU\t96\ttrue\n" +
                "NZVDJIGSYLXGYTE\t94\ttrue\n" +
                "JCTIZKYFLUHZQSN\t99\ttrue\n" +
                "EEHRUGPBMBTKVSB\t95\ttrue\n" +
                "TRDLVSYLMSRHGKR\t97\ttrue\n" +
                "MQMUDDCIHCNPUGJ\t100\ttrue\n" +
                "KIWIHBROKZKUTIQ\t90\ttrue\n" +
                "EDNKRCGKSQDCMUM\t95\ttrue\n" +
                "NZVDJIGSYLXGYTE\t96\ttrue\n" +
                "SNGIZRPFMDVVGSV\t97\ttrue\n" +
                "NZVDJIGSYLXGYTE\t90\ttrue\n" +
                "WSWSRGOONFCLTJC\t99\ttrue\n" +
                "BSQCNSFFLTRYZUZ\t94\ttrue\n" +
                "ETJRSZSRYRFBVTM\t100\ttrue\n" +
                "QZVKHTLQZSLQVFG\t91\ttrue\n" +
                "DVIKRPCFECGPVRB\t93\ttrue\n";

        assertThat(expected, "select id, w, w <= 100 from tab where w <= 100 and w >= 90");

        final String expected2 = "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n" +
                "QBUYZVQQHSQSPZP\tNaN\tfalse\n";

        assertThat(expected2, "select id, w, w <= 100 from tab where w = NaN and id ~ 'SQS'");
    }

    @Test
    public void testLongConstant() throws Exception {
        createTabWithNaNs2();
        final String expected = "YPHRIPZIMNZZRMF\t-99547.129409790032\t452.870590209961\n" +
                "YPHRIPZIMNZZRMF\t-99208.047485351568\t791.952514648438\n" +
                "YPHRIPZIMNZZRMF\t-99562.298080444336\t437.701919555664\n" +
                "YPHRIPZIMNZZRMF\t-99115.172851562496\t884.827148437500\n" +
                "YPHRIPZIMNZZRMF\t-99069.638671875008\t930.361328125000\n" +
                "YPHRIPZIMNZZRMF\t-99116.000000000000\t884.000000000000\n" +
                "YPHRIPZIMNZZRMF\t-99488.000000000000\t512.000000000000\n" +
                "YPHRIPZIMNZZRMF\t-99575.083465576176\t424.916534423828\n" +
                "YPHRIPZIMNZZRMF\t-99455.000000000000\t545.000000000000\n" +
                "YPHRIPZIMNZZRMF\t-99385.928710937504\t614.071289062500\n" +
                "YPHRIPZIMNZZRMF\t-99200.000000000000\t800.000000000000\n" +
                "YPHRIPZIMNZZRMF\t-99463.482421875008\t536.517578125000\n" +
                "YPHRIPZIMNZZRMF\t-99590.147300720208\t409.852699279785\n" +
                "YPHRIPZIMNZZRMF\t-99592.000000000000\t408.000000000000\n" +
                "YPHRIPZIMNZZRMF\t-99104.000000000000\t896.000000000000\n";

        assertThat(expected, "select id, x - (100000000000/1000000),x from tab where x > 400 and id~'MNZ'");

        final String expected2 = "YPHRIPZIMNZZRMF\tNaN\t0.000065929848\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.000014064731\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.826377153397\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.000185413708\n" +
                "YPHRIPZIMNZZRMF\tNaN\t884.827148437500\n" +
                "YPHRIPZIMNZZRMF\tNaN\t930.361328125000\n" +
                "YPHRIPZIMNZZRMF\tNaN\t884.000000000000\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.205070361495\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.003745394410\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.000000001150\n" +
                "YPHRIPZIMNZZRMF\tNaN\t614.071289062500\n" +
                "YPHRIPZIMNZZRMF\tNaN\t-355.753906250000\n" +
                "YPHRIPZIMNZZRMF\tNaN\t0.000002353352\n" +
                "YPHRIPZIMNZZRMF\tNaN\t-290.875000000000\n" +
                "YPHRIPZIMNZZRMF\tNaN\t256.000000000000\n";

        assertThat(expected2, "select id, y - (100000000000/1000000),x from tab where y = NaN and id~'MNZ'");


        final String expected3 = "YPHRIPZIMNZZRMF\t100000000307\t307\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000135\t135\n" +
                "YPHRIPZIMNZZRMF\t100000000334\t334\n" +
                "YPHRIPZIMNZZRMF\t99999999962\t-38\n" +
                "YPHRIPZIMNZZRMF\t99999999636\t-364\n" +
                "YPHRIPZIMNZZRMF\t100000000459\t459\n" +
                "YPHRIPZIMNZZRMF\t99999999925\t-75\n" +
                "YPHRIPZIMNZZRMF\t99999999780\t-220\n" +
                "YPHRIPZIMNZZRMF\t100000000428\t428\n" +
                "YPHRIPZIMNZZRMF\t99999999793\t-207\n" +
                "YPHRIPZIMNZZRMF\t99999999640\t-360\n" +
                "YPHRIPZIMNZZRMF\t99999999782\t-218\n" +
                "YPHRIPZIMNZZRMF\t100000000358\t358\n" +
                "YPHRIPZIMNZZRMF\t99999999878\t-122\n" +
                "YPHRIPZIMNZZRMF\t99999999703\t-297\n" +
                "YPHRIPZIMNZZRMF\t100000000418\t418\n" +
                "YPHRIPZIMNZZRMF\t100000000099\t99\n" +
                "YPHRIPZIMNZZRMF\t100000000246\t246\n" +
                "YPHRIPZIMNZZRMF\t100000000453\t453\n" +
                "YPHRIPZIMNZZRMF\t99999999769\t-231\n" +
                "YPHRIPZIMNZZRMF\t99999999815\t-185\n" +
                "YPHRIPZIMNZZRMF\t100000000180\t180\n" +
                "YPHRIPZIMNZZRMF\t100000000427\t427\n" +
                "YPHRIPZIMNZZRMF\t99999999905\t-95\n" +
                "YPHRIPZIMNZZRMF\t100000000078\t78\n" +
                "YPHRIPZIMNZZRMF\t99999999909\t-91\n" +
                "YPHRIPZIMNZZRMF\t100000000379\t379\n" +
                "YPHRIPZIMNZZRMF\t99999999975\t-25\n" +
                "YPHRIPZIMNZZRMF\t99999999626\t-374\n" +
                "YPHRIPZIMNZZRMF\t100000000117\t117\n" +
                "YPHRIPZIMNZZRMF\t100000000447\t447\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000255\t255\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000022\t22\n" +
                "YPHRIPZIMNZZRMF\t99999999640\t-360\n" +
                "YPHRIPZIMNZZRMF\t100000000025\t25\n" +
                "YPHRIPZIMNZZRMF\t100000000252\t252\n" +
                "YPHRIPZIMNZZRMF\t100000000105\t105\n" +
                "YPHRIPZIMNZZRMF\t100000000290\t290\n" +
                "YPHRIPZIMNZZRMF\t100000000346\t346\n" +
                "YPHRIPZIMNZZRMF\t100000000203\t203\n" +
                "YPHRIPZIMNZZRMF\t100000000446\t446\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000236\t236\n" +
                "YPHRIPZIMNZZRMF\t99999999805\t-195\n" +
                "YPHRIPZIMNZZRMF\t99999999552\t-448\n" +
                "YPHRIPZIMNZZRMF\t100000000397\t397\n" +
                "YPHRIPZIMNZZRMF\t100000000399\t399\n" +
                "YPHRIPZIMNZZRMF\t99999999566\t-434\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000270\t270\n" +
                "YPHRIPZIMNZZRMF\t100000000137\t137\n" +
                "YPHRIPZIMNZZRMF\t100000000119\t119\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000246\t246\n" +
                "YPHRIPZIMNZZRMF\t99999999969\t-31\n" +
                "YPHRIPZIMNZZRMF\t99999999966\t-34\n" +
                "YPHRIPZIMNZZRMF\t99999999656\t-344\n" +
                "YPHRIPZIMNZZRMF\t99999999654\t-346\n" +
                "YPHRIPZIMNZZRMF\t100000000055\t55\n" +
                "YPHRIPZIMNZZRMF\t100000000487\t487\n" +
                "YPHRIPZIMNZZRMF\t100000000217\t217\n" +
                "YPHRIPZIMNZZRMF\t100000000242\t242\n" +
                "YPHRIPZIMNZZRMF\t100000000468\t468\n" +
                "YPHRIPZIMNZZRMF\t99999999656\t-344\n" +
                "YPHRIPZIMNZZRMF\t100000000242\t242\n" +
                "YPHRIPZIMNZZRMF\t99999999829\t-171\n" +
                "YPHRIPZIMNZZRMF\t100000000407\t407\n" +
                "YPHRIPZIMNZZRMF\t100000000262\t262\n" +
                "YPHRIPZIMNZZRMF\t99999999836\t-164\n" +
                "YPHRIPZIMNZZRMF\t100000000179\t179\n" +
                "YPHRIPZIMNZZRMF\t100000000306\t306\n" +
                "YPHRIPZIMNZZRMF\t99999999831\t-169\n" +
                "YPHRIPZIMNZZRMF\t99999999884\t-116\n" +
                "YPHRIPZIMNZZRMF\t100000000379\t379\n" +
                "YPHRIPZIMNZZRMF\t99999999613\t-387\n" +
                "YPHRIPZIMNZZRMF\t99999999822\t-178\n" +
                "YPHRIPZIMNZZRMF\t99999999928\t-72\n" +
                "YPHRIPZIMNZZRMF\t100000000428\t428\n" +
                "YPHRIPZIMNZZRMF\t99999999713\t-287\n" +
                "YPHRIPZIMNZZRMF\t99999999725\t-275\n" +
                "YPHRIPZIMNZZRMF\tNaN\tNaN\n" +
                "YPHRIPZIMNZZRMF\t100000000161\t161\n" +
                "YPHRIPZIMNZZRMF\t100000000233\t233\n";

        assertThat(expected3, "select id, z + 100000000000, z from tab where id~'MNZ'");
    }

    @Test
    public void testLongEqualsInt() throws Exception {
        createTabWithNaNs();
        final String expected2 = "XWCKYLSUWDSWUGS\t-392\t-392\n" +
                "BHLNEJRMDIKDISG\t-168\t-168\n" +
                "NZVDJIGSYLXGYTE\t345\t345\n" +
                "NWDSWLUVDRHFBCZ\t316\t316\n" +
                "XWCKYLSUWDSWUGS\t-276\t-276\n" +
                "GZJYYFLSVIHDWWL\t-366\t-366\n" +
                "DKDWOMDXCBJFRPX\t262\t262\n" +
                "ZSFXUNYQXTGNJJI\t-270\t-270\n" +
                "KJSMSSUQSRLTKVV\t161\t161\n" +
                "WRSLBMQHGJBFQBB\t316\t316\n";

        assertThat(expected2, "select id,z,w from tab where z = w");
    }

    @Test
    public void testLongEqualsNaN() throws Exception {
        createTabWithNaNs();
        final String expected = "SNGIZRPFMDVVGSV\tNaN\t928.000000000000\n" +
                "QCHNDCWOJHGBBSR\tNaN\t901.750000000000\n" +
                "QZVKHTLQZSLQVFG\tNaN\t1016.000000000000\n" +
                "VEZDYHDHRFEVHKK\tNaN\t907.000000000000\n" +
                "VEZDYHDHRFEVHKK\tNaN\t1023.510437011719\n" +
                "FIEVMKPYVGPYKKB\tNaN\t1012.000000000000\n" +
                "UMKUBKXPMSXQSTV\tNaN\t960.000000000000\n" +
                "WRSLBMQHGJBFQBB\tNaN\t1008.655456542969\n" +
                "OPJEUKWMDNZZBBU\tNaN\t969.453125000000\n" +
                "UXBWYWRLHUHJECI\tNaN\t1010.221679687500\n" +
                "GZJYYFLSVIHDWWL\tNaN\t1010.000000000000\n" +
                "IFGZUFEVTEROCBP\tNaN\t942.821411132813\n" +
                "UWZOOVPPLIPRMDB\tNaN\t1020.000000000000\n" +
                "VQEBNDCQCEHNOMV\tNaN\t960.000000000000\n" +
                "SNGIZRPFMDVVGSV\tNaN\t960.000000000000\n";

        assertThat(expected, "select id, z, x from tab where z = NaN and x > 900.0");
    }

    @Test
    public void testLongGreaterThanDouble() throws Exception {
        createTabNoNaNs();
        final String expected3 = "KKUSIMYDXUUSKCX\t669.826049804688\t3875444123502462003\n" +
                "IZKMDCXYTRVYQNF\t824.000000000000\t4688034729252126306\n" +
                "CMONRCXNUZFNWHF\t615.089355468750\t7085454771022142397\n" +
                "NDESHYUMEUKVZIE\t640.000000000000\t2356466624879708775\n" +
                "KFMQNTOGMXUKLGM\t864.392089843750\t3177032506931624144\n" +
                "IZKMDCXYTRVYQNF\t671.903076171875\t6407332390825116324\n" +
                "KFMQNTOGMXUKLGM\t1016.000000000000\t4608254594945758594\n" +
                "CMONRCXNUZFNWHF\t849.250000000000\t3877410618142302179\n" +
                "IZKMDCXYTRVYQNF\t846.882812500000\t7942734211642082207\n" +
                "IZKMDCXYTRVYQNF\t676.031250000000\t6484041992063172141\n" +
                "CMONRCXNUZFNWHF\t819.046875000000\t6452582387942072011\n" +
                "NDESHYUMEUKVZIE\t997.767562866211\t8812402360346018824\n" +
                "NDESHYUMEUKVZIE\t736.000000000000\t4675380837596017832\n" +
                "NDESHYUMEUKVZIE\t688.000000000000\t5659765607113344347\n" +
                "IZKMDCXYTRVYQNF\t694.064636230469\t6468084771677377095\n" +
                "KFMQNTOGMXUKLGM\t848.000000000000\t9090383113270985873\n" +
                "IZKMDCXYTRVYQNF\t608.000000000000\t5070419589855070673\n" +
                "KFMQNTOGMXUKLGM\t720.000000000000\t9038465588369926344\n" +
                "IZKMDCXYTRVYQNF\t699.865692138672\t6213877979563893210\n" +
                "KKUSIMYDXUUSKCX\t766.381103515625\t3188305102325147634\n" +
                "KKUSIMYDXUUSKCX\t861.750000000000\t8337682751207954574\n";

        assertThat(expected3, "select id,x,z from tab where z > x and id ~ 'UK|CX' and x > 600");
    }

    @Test
    public void testLongLessOrEqual() throws Exception {
        createTabWithNaNs2();
        final String expected = "FYXPVKNCBWLNLRH\ttrue\t-417\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-285\n" +
                "FYXPVKNCBWLNLRH\tfalse\t425\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-130\n" +
                "FYXPVKNCBWLNLRH\ttrue\t36\n" +
                "FYXPVKNCBWLNLRH\tfalse\t242\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-201\n" +
                "FYXPVKNCBWLNLRH\ttrue\t58\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-181\n" +
                "FYXPVKNCBWLNLRH\tfalse\t282\n" +
                "FYXPVKNCBWLNLRH\tfalse\t180\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\tfalse\t129\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-106\n" +
                "FYXPVKNCBWLNLRH\tfalse\t214\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-432\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-82\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\tfalse\t156\n" +
                "FYXPVKNCBWLNLRH\tfalse\t188\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-297\n" +
                "FYXPVKNCBWLNLRH\tfalse\t152\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-408\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-427\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-319\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-139\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-198\n" +
                "FYXPVKNCBWLNLRH\tfalse\t456\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-371\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-385\n" +
                "FYXPVKNCBWLNLRH\tfalse\t452\n" +
                "FYXPVKNCBWLNLRH\tfalse\t433\n" +
                "FYXPVKNCBWLNLRH\ttrue\t75\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-57\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-71\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-56\n" +
                "FYXPVKNCBWLNLRH\tfalse\t381\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\tfalse\t270\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-143\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-78\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-352\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-494\n" +
                "FYXPVKNCBWLNLRH\tfalse\t308\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-287\n" +
                "FYXPVKNCBWLNLRH\ttrue\t79\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-47\n" +
                "FYXPVKNCBWLNLRH\tfalse\t234\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-207\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-431\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-415\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-279\n" +
                "FYXPVKNCBWLNLRH\tfalse\t129\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-470\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-36\n" +
                "FYXPVKNCBWLNLRH\tfalse\t113\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-351\n" +
                "FYXPVKNCBWLNLRH\tfalse\t258\n" +
                "FYXPVKNCBWLNLRH\tfalse\t250\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-457\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-332\n" +
                "FYXPVKNCBWLNLRH\tfalse\t278\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-32\n" +
                "FYXPVKNCBWLNLRH\tfalse\t328\n" +
                "FYXPVKNCBWLNLRH\tfalse\t173\n" +
                "FYXPVKNCBWLNLRH\tfalse\t266\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-419\n" +
                "FYXPVKNCBWLNLRH\ttrue\t79\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-416\n" +
                "FYXPVKNCBWLNLRH\ttrue\t71\n" +
                "FYXPVKNCBWLNLRH\tfalse\t269\n" +
                "FYXPVKNCBWLNLRH\tfalse\t489\n" +
                "FYXPVKNCBWLNLRH\ttrue\t91\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-387\n" +
                "FYXPVKNCBWLNLRH\tfalse\t249\n" +
                "FYXPVKNCBWLNLRH\ttrue\t1\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t61\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-265\n" +
                "FYXPVKNCBWLNLRH\ttrue\t52\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-125\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-29\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-178\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-349\n" +
                "FYXPVKNCBWLNLRH\ttrue\t74\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-319\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-166\n" +
                "FYXPVKNCBWLNLRH\tfalse\tNaN\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-187\n" +
                "FYXPVKNCBWLNLRH\ttrue\t-163\n" +
                "FYXPVKNCBWLNLRH\ttrue\t95\n";

        assertThat(expected, "select id, z <= 100, z from tab where id = 'FYXPVKNCBWLNLRH'");
    }

    @Test
    public void testLongLessThanInt() throws Exception {
        createTabWithNaNs();
        final String expected3 = "OWBVDEGHLXGZMDJ\t391\t454\n" +
                "OWBVDEGHLXGZMDJ\t98\t303\n" +
                "OWBVDEGHLXGZMDJ\t-47\t326\n" +
                "OWBVDEGHLXGZMDJ\t-232\t-37\n" +
                "OWBVDEGHLXGZMDJ\t-312\t172\n" +
                "OWBVDEGHLXGZMDJ\t253\t423\n" +
                "OWBVDEGHLXGZMDJ\t40\t408\n" +
                "OWBVDEGHLXGZMDJ\t-268\t230\n" +
                "OWBVDEGHLXGZMDJ\t48\t335\n" +
                "OWBVDEGHLXGZMDJ\t-283\t153\n" +
                "OWBVDEGHLXGZMDJ\t183\t470\n" +
                "OWBVDEGHLXGZMDJ\t-62\t359\n" +
                "OWBVDEGHLXGZMDJ\t-133\t-22\n" +
                "OWBVDEGHLXGZMDJ\t-166\t439\n" +
                "OWBVDEGHLXGZMDJ\t339\t467\n" +
                "OWBVDEGHLXGZMDJ\t-80\t367\n" +
                "OWBVDEGHLXGZMDJ\t414\t491\n" +
                "OWBVDEGHLXGZMDJ\t-171\t303\n" +
                "OWBVDEGHLXGZMDJ\t-292\t270\n" +
                "OWBVDEGHLXGZMDJ\t-323\t-105\n" +
                "OWBVDEGHLXGZMDJ\t-480\t142\n" +
                "OWBVDEGHLXGZMDJ\t-346\t460\n" +
                "OWBVDEGHLXGZMDJ\t318\t447\n" +
                "OWBVDEGHLXGZMDJ\t-257\t-93\n" +
                "OWBVDEGHLXGZMDJ\t268\t326\n" +
                "OWBVDEGHLXGZMDJ\t226\t388\n" +
                "OWBVDEGHLXGZMDJ\t142\t256\n" +
                "OWBVDEGHLXGZMDJ\t-140\t-11\n" +
                "OWBVDEGHLXGZMDJ\t-199\t-31\n" +
                "OWBVDEGHLXGZMDJ\t-229\t401\n" +
                "OWBVDEGHLXGZMDJ\t315\t420\n" +
                "OWBVDEGHLXGZMDJ\t13\t449\n" +
                "OWBVDEGHLXGZMDJ\t-417\t-77\n" +
                "OWBVDEGHLXGZMDJ\t-100\t97\n" +
                "OWBVDEGHLXGZMDJ\t-194\t386\n";

        assertThat(expected3, "select id, z, w from tab where z < w and id = 'OWBVDEGHLXGZMDJ'");
    }

    @Test
    public void testLongNegative() throws Exception {
        createTabWithNaNs2();

        final String expected = "KKUSIMYDXUUSKCX\tNaN\tNaN\t-338.665039062500\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-11\t11\t-481.765014648438\t-470.765014648438\n" +
                "KKUSIMYDXUUSKCX\tNaN\tNaN\t0.000183005621\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-79\t79\t5.404115438461\t84.404115438461\n" +
                "KKUSIMYDXUUSKCX\t-27\t27\t-436.000000000000\t-409.000000000000\n" +
                "KKUSIMYDXUUSKCX\tNaN\tNaN\t-807.692016601563\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-57\t57\t0.000013659448\t57.000013659448\n" +
                "KKUSIMYDXUUSKCX\tNaN\tNaN\t384.000000000000\tNaN\n" +
                "KKUSIMYDXUUSKCX\t2\t-2\t0.003575030481\t-1.996424969519\n" +
                "KKUSIMYDXUUSKCX\t39\t-39\t35.019264221191\t-3.980735778809\n" +
                "KKUSIMYDXUUSKCX\t-51\t51\t-512.000000000000\t-461.000000000000\n" +
                "KKUSIMYDXUUSKCX\t57\t-57\t-256.000000000000\t-313.000000000000\n" +
                "KKUSIMYDXUUSKCX\tNaN\tNaN\t240.000000000000\tNaN\n" +
                "KKUSIMYDXUUSKCX\t72\t-72\t-612.000000000000\t-684.000000000000\n" +
                "KKUSIMYDXUUSKCX\t12\t-12\t0.000000343896\t-11.999999656104\n" +
                "KKUSIMYDXUUSKCX\tNaN\tNaN\t0.059096898884\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\tNaN\t0.036795516498\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-73\t73\t0.000000136839\t73.000000136839\n" +
                "KKUSIMYDXUUSKCX\t53\t-53\t0.000001200607\t-52.999998799393\n" +
                "KKUSIMYDXUUSKCX\t-14\t14\t5.540870904922\t19.540870904922\n";

        assertThat(expected, "select id, z, -z, x, x+-z from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN)");
    }

    @Test
    public void testMinusInt() throws Exception {
        createTabWithNaNs2();

        final String expected = "KKUSIMYDXUUSKCX\t2\t-338.665039062500\tNaN\t-320.665039062500\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-485\t-481.765014648438\t-11\t23.234985351563\t-526\n" +
                "KKUSIMYDXUUSKCX\t17\t0.000183005621\tNaN\t3.000183005621\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-229\t5.404115438461\t-79\t254.404115438461\t-338\n" +
                "KKUSIMYDXUUSKCX\t237\t-436.000000000000\t-27\t-653.000000000000\t180\n" +
                "KKUSIMYDXUUSKCX\t-71\t-807.692016601563\tNaN\t-716.692016601563\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-189\t0.000013659448\t-57\t209.000013659448\t-276\n" +
                "KKUSIMYDXUUSKCX\t-397\t384.000000000000\tNaN\t801.000000000000\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\t0.003575030481\t2\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-276\t35.019264221191\t39\t331.019264221191\t-267\n" +
                "KKUSIMYDXUUSKCX\t262\t-512.000000000000\t-51\t-754.000000000000\t181\n" +
                "KKUSIMYDXUUSKCX\t258\t-256.000000000000\t57\t-494.000000000000\t285\n" +
                "KKUSIMYDXUUSKCX\t-379\t240.000000000000\tNaN\t639.000000000000\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\t-612.000000000000\t72\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-111\t0.000000343896\t12\t131.000000343896\t-129\n" +
                "KKUSIMYDXUUSKCX\t-16\t0.059096898884\tNaN\t36.059096898884\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-313\t0.036795516498\tNaN\t333.036795516498\tNaN\n" +
                "KKUSIMYDXUUSKCX\t141\t0.000000136839\t-73\t-120.999999863161\t38\n" +
                "KKUSIMYDXUUSKCX\t149\t0.000001200607\t53\t-128.999998799393\t172\n" +
                "KKUSIMYDXUUSKCX\t-126\t5.540870904922\t-14\t151.540870904923\t-170\n";

        assertThat(expected, "select id, w, x, z, x - (w-20), z + (w-30) from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN)");
    }

    @Test
    public void testMinusLong() throws Exception {
        createTabWithNaNs2();
        final String expected = "KKUSIMYDXUUSKCX\t-338.665039062500\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-481.765014648438\t-11\t-111\t-512.765014648438\n" +
                "KKUSIMYDXUUSKCX\t0.000183005621\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t5.404115438461\t-79\t-179\t-93.595884561539\n" +
                "KKUSIMYDXUUSKCX\t-436.000000000000\t-27\t-127\t-483.000000000000\n" +
                "KKUSIMYDXUUSKCX\t-807.692016601563\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t0.000013659448\t-57\t-157\t-76.999986340552\n" +
                "KKUSIMYDXUUSKCX\t384.000000000000\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t0.003575030481\t2\t-98\t-17.996424969519\n" +
                "KKUSIMYDXUUSKCX\t35.019264221191\t39\t-61\t54.019264221191\n" +
                "KKUSIMYDXUUSKCX\t-512.000000000000\t-51\t-151\t-583.000000000000\n" +
                "KKUSIMYDXUUSKCX\t-256.000000000000\t57\t-43\t-219.000000000000\n" +
                "KKUSIMYDXUUSKCX\t240.000000000000\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t-612.000000000000\t72\t-28\t-560.000000000000\n" +
                "KKUSIMYDXUUSKCX\t0.000000343896\t12\t-88\t-7.999999656104\n" +
                "KKUSIMYDXUUSKCX\t0.059096898884\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t0.036795516498\tNaN\tNaN\tNaN\n" +
                "KKUSIMYDXUUSKCX\t0.000000136839\t-73\t-173\t-92.999999863161\n" +
                "KKUSIMYDXUUSKCX\t0.000001200607\t53\t-47\t33.000001200607\n" +
                "KKUSIMYDXUUSKCX\t5.540870904922\t-14\t-114\t-28.459129095078\n";

        assertThat(expected, "select id, x, z, z-100, x + (z-20) from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN)");
    }

    @Test
    public void testMultDouble() throws Exception {
        createTabWithNaNs2();

        final String expected = "-338.665039062500\t9.986581325531\t-3382.105954711791\n" +
                "-481.765014648438\t0.000194547960\t-0.093726400720\n" +
                "0.000183005621\t0.216939434409\t0.000039701136\n" +
                "5.404115438461\t8.854092121124\t47.848535925326\n" +
                "-436.000000000000\t-811.000000000000\t353596.000000000000\n" +
                "-807.692016601563\t1.505146384239\t-1215.694718366707\n" +
                "0.000013659448\t0.000006695827\t0.000000000091\n" +
                "384.000000000000\t638.000000000000\t244992.000000000000\n" +
                "0.003575030481\t0.000002332791\t0.000000008340\n" +
                "35.019264221191\t89.257812500000\t3125.742919743061\n" +
                "-512.000000000000\t-278.166625976563\t142421.312500000000\n" +
                "-256.000000000000\t0.000000168164\t-0.000043050055\n" +
                "240.000000000000\t0.000415830291\t0.099799269810\n" +
                "-612.000000000000\t0.000000004888\t-0.000002991690\n" +
                "0.000000343896\tNaN\tNaN\n" +
                "0.059096898884\t0.000015207836\t0.000000898736\n" +
                "0.036795516498\tNaN\tNaN\n" +
                "0.000000136839\t-560.000000000000\t-0.000076629905\n" +
                "0.000001200607\tNaN\tNaN\n" +
                "5.540870904922\t0.000076783974\t0.000425450086\n";

        assertThat(expected, "select x, y, x*y from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN)");
    }

    @Test
    public void testMultInt() throws Exception {
        createTabWithNaNs2();

        final String expected = "-338.665039062500\t2\tNaN\t-330.665039062500\tNaN\n" +
                "-481.765014648438\t-485\t-11\t-2421.765014648438\t959\n" +
                "0.000183005621\t17\tNaN\t68.000183005621\tNaN\n" +
                "5.404115438461\t-229\t-79\t-910.595884561539\t379\n" +
                "-436.000000000000\t237\t-27\t512.000000000000\t-501\n" +
                "-807.692016601563\t-71\tNaN\t-1091.692016601563\tNaN\n" +
                "0.000013659448\t-189\t-57\t-755.999986340552\t321\n" +
                "384.000000000000\t-397\tNaN\t-1204.000000000000\tNaN\n" +
                "0.003575030481\tNaN\t2\tNaN\tNaN\n" +
                "35.019264221191\t-276\t39\t-1068.980735778809\t591\n" +
                "-512.000000000000\t262\t-51\t536.000000000000\t-575\n" +
                "-256.000000000000\t258\t57\t776.000000000000\t-459\n" +
                "240.000000000000\t-379\tNaN\t-1276.000000000000\tNaN\n" +
                "-612.000000000000\tNaN\t72\tNaN\tNaN\n" +
                "0.000000343896\t-111\t12\t-443.999999656104\t234\n" +
                "0.059096898884\t-16\tNaN\t-63.940903101116\tNaN\n" +
                "0.036795516498\t-313\tNaN\t-1251.963204483502\tNaN\n" +
                "0.000000136839\t141\t-73\t564.000000136839\t-355\n" +
                "0.000001200607\t149\t53\t596.000001200607\t-245\n" +
                "5.540870904922\t-126\t-14\t-498.459129095078\t238\n";

        assertThat(expected, "select x,w,z, x + (w * 4), z - (w*2) from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN)");

    }

    @Test
    public void testMultLong() throws Exception {
        createTabWithNaNs2();

        final String expected = "-338.665039062500\tNaN\tNaN\tNaN\n" +
                "-481.765014648438\t-11\t-591.765014648438\t-55\n" +
                "0.000183005621\tNaN\tNaN\tNaN\n" +
                "5.404115438461\t-79\t-784.595884561539\t-395\n" +
                "-436.000000000000\t-27\t-706.000000000000\t-135\n" +
                "-807.692016601563\tNaN\tNaN\tNaN\n" +
                "0.000013659448\t-57\t-569.999986340552\t-285\n" +
                "384.000000000000\tNaN\tNaN\tNaN\n" +
                "0.003575030481\t2\t20.003575030481\t10\n" +
                "35.019264221191\t39\t425.019264221191\t195\n" +
                "-512.000000000000\t-51\t-1022.000000000000\t-255\n" +
                "-256.000000000000\t57\t314.000000000000\t285\n" +
                "240.000000000000\tNaN\tNaN\tNaN\n" +
                "-612.000000000000\t72\t108.000000000000\t360\n" +
                "0.000000343896\t12\t120.000000343896\t60\n" +
                "0.059096898884\tNaN\tNaN\tNaN\n" +
                "0.036795516498\tNaN\tNaN\tNaN\n" +
                "0.000000136839\t-73\t-729.999999863161\t-365\n" +
                "0.000001200607\t53\t530.000001200607\t265\n" +
                "5.540870904922\t-14\t-134.459129095078\t-70\n";

        assertThat(expected, "select x,z,x+(z*10),z*5 from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN)");
    }

    @Test
    public void testNegativeInt() throws Exception {
        createTabWithNaNs2();

        final String expected = "FYXPVKNCBWLNLRH\t118\t0.001198394399\t-417\t-117.998801605601\t-535\n" +
                "FYXPVKNCBWLNLRH\t-344\t859.375000000000\t-285\t1203.375000000000\t59\n" +
                "FYXPVKNCBWLNLRH\t31\t-664.590087890625\t425\t-695.590087890625\t394\n" +
                "FYXPVKNCBWLNLRH\t-56\t0.000000020402\tNaN\t56.000000020402\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-293\t-864.000000000000\t-130\t-571.000000000000\t163\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000007512215\t36\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t165\t0.000000697772\t242\t-164.999999302228\t77\n" +
                "FYXPVKNCBWLNLRH\t133\t-1024.000000000000\t-201\t-1157.000000000000\t-334\n" +
                "FYXPVKNCBWLNLRH\t-399\t47.056144714355\t58\t446.056144714356\t457\n" +
                "FYXPVKNCBWLNLRH\tNaN\t928.000000000000\t-181\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-329\t-591.000000000000\t282\t-262.000000000000\t611\n" +
                "FYXPVKNCBWLNLRH\t246\t0.000000042973\t180\t-245.999999957027\t-66\n" +
                "FYXPVKNCBWLNLRH\t338\t689.625000000000\tNaN\t351.625000000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t185\t74.610691070557\t129\t-110.389308929443\t-56\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000001833725\t-106\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-492\t0.213398203254\t214\t492.213398203254\t706\n" +
                "FYXPVKNCBWLNLRH\t196\t0.000000005970\tNaN\t-195.999999994030\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-268\t-872.000000000000\t-432\t-604.000000000000\t-164\n" +
                "FYXPVKNCBWLNLRH\tNaN\t-761.250000000000\t-82\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t407\t0.000000101970\tNaN\t-406.999999898030\tNaN\n" +
                "FYXPVKNCBWLNLRH\t445\t0.006385272485\t156\t-444.993614727515\t-289\n" +
                "FYXPVKNCBWLNLRH\t161\t-228.232543945313\t188\t-389.232543945313\t27\n" +
                "FYXPVKNCBWLNLRH\t219\t304.000000000000\tNaN\t85.000000000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t112\t0.000007125924\t-297\t-111.999992874077\t-409\n" +
                "FYXPVKNCBWLNLRH\t22\t410.000000000000\t152\t388.000000000000\t130\n" +
                "FYXPVKNCBWLNLRH\t-27\t-719.250000000000\t-408\t-692.250000000000\t-381\n" +
                "FYXPVKNCBWLNLRH\t-263\t-1024.000000000000\tNaN\t-761.000000000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-223\t0.035682333633\t-427\t223.035682333633\t-204\n" +
                "FYXPVKNCBWLNLRH\t310\t-310.000000000000\tNaN\t-620.000000000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t261\t-185.000000000000\tNaN\t-446.000000000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-484\t69.323999404907\t-319\t553.323999404907\t165\n" +
                "FYXPVKNCBWLNLRH\t330\t0.000002720652\t-139\t-329.999997279348\t-469\n" +
                "FYXPVKNCBWLNLRH\t488\t737.581054687500\t-198\t249.581054687500\t-686\n" +
                "FYXPVKNCBWLNLRH\t-445\t189.171875000000\t456\t634.171875000000\t901\n" +
                "FYXPVKNCBWLNLRH\t201\t-34.500000000000\t-371\t-235.500000000000\t-572\n" +
                "FYXPVKNCBWLNLRH\t-22\t0.007598446216\t-385\t22.007598446216\t-363\n" +
                "FYXPVKNCBWLNLRH\t196\t0.000004923836\t452\t-195.999995076164\t256\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000064058751\t433\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000673567469\t75\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t402\t0.000097944039\t-57\t-401.999902055961\t-459\n" +
                "FYXPVKNCBWLNLRH\t-130\t-651.024291992188\t-71\t-521.024291992188\t59\n" +
                "FYXPVKNCBWLNLRH\t-311\t858.582031250000\t-56\t1169.582031250000\t255\n" +
                "FYXPVKNCBWLNLRH\t306\t0.000003057669\t381\t-305.999996942331\t75\n" +
                "FYXPVKNCBWLNLRH\t-85\t0.000110899655\tNaN\t85.000110899655\tNaN\n" +
                "FYXPVKNCBWLNLRH\tNaN\t103.183797836304\t270\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t493\t0.000000000000\t-143\t-493.000000000000\t-636\n" +
                "FYXPVKNCBWLNLRH\t114\t-825.000000000000\t-78\t-939.000000000000\t-192\n" +
                "FYXPVKNCBWLNLRH\t74\t718.767578125000\t-352\t644.767578125000\t-426\n" +
                "FYXPVKNCBWLNLRH\t-387\t0.000007173486\tNaN\t387.000007173486\tNaN\n" +
                "FYXPVKNCBWLNLRH\t161\t0.097649306059\t-494\t-160.902350693941\t-655\n" +
                "FYXPVKNCBWLNLRH\t289\t507.500000000000\t308\t218.500000000000\t19\n" +
                "FYXPVKNCBWLNLRH\t280\t0.082590892911\t-287\t-279.917409107089\t-567\n" +
                "FYXPVKNCBWLNLRH\t-228\t0.000081144792\t79\t228.000081144792\t307\n" +
                "FYXPVKNCBWLNLRH\t190\t73.875000000000\t-47\t-116.125000000000\t-237\n" +
                "FYXPVKNCBWLNLRH\t-8\t0.000000007061\t234\t8.000000007061\t242\n" +
                "FYXPVKNCBWLNLRH\t-315\t0.000074375428\tNaN\t315.000074375428\tNaN\n" +
                "FYXPVKNCBWLNLRH\t447\t0.030170871876\t-207\t-446.969829128124\t-654\n" +
                "FYXPVKNCBWLNLRH\t335\t338.981475830078\t-431\t3.981475830078\t-766\n" +
                "FYXPVKNCBWLNLRH\t325\t11.184396266937\t-415\t-313.815603733063\t-740\n" +
                "FYXPVKNCBWLNLRH\t336\t-534.500000000000\t-279\t-870.500000000000\t-615\n" +
                "FYXPVKNCBWLNLRH\t-211\t254.718750000000\t129\t465.718750000000\t340\n" +
                "FYXPVKNCBWLNLRH\t159\t0.000000478142\t-470\t-158.999999521858\t-629\n" +
                "FYXPVKNCBWLNLRH\tNaN\t-489.500000000000\t-36\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t409\t-832.000000000000\t113\t-1241.000000000000\t-296\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000095694279\t-351\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-440\t0.006677458994\t258\t440.006677458994\t698\n" +
                "FYXPVKNCBWLNLRH\t490\t-796.328125000000\t250\t-1286.328125000000\t-240\n" +
                "FYXPVKNCBWLNLRH\t-87\t311.858428955078\t-457\t398.858428955078\t-370\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.003459116502\t-332\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000000003561\t278\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t245\t441.697952270508\t-32\t196.697952270508\t-277\n" +
                "FYXPVKNCBWLNLRH\t-187\t0.041321944445\t328\t187.041321944445\t515\n" +
                "FYXPVKNCBWLNLRH\t-329\t-977.315917968750\t173\t-648.315917968750\t502\n" +
                "FYXPVKNCBWLNLRH\t-403\t-128.000000000000\t266\t275.000000000000\t669\n" +
                "FYXPVKNCBWLNLRH\t-43\t0.000015625721\t-419\t43.000015625721\t-376\n" +
                "FYXPVKNCBWLNLRH\t-276\t0.000000734143\t79\t276.000000734143\t355\n" +
                "FYXPVKNCBWLNLRH\t456\t0.002172315610\tNaN\t-455.997827684390\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-110\t768.000000000000\t-416\t878.000000000000\t-306\n" +
                "FYXPVKNCBWLNLRH\t-79\t0.000031115842\t71\t79.000031115842\t150\n" +
                "FYXPVKNCBWLNLRH\t-347\t-854.062500000000\t269\t-507.062500000000\t616\n" +
                "FYXPVKNCBWLNLRH\t309\t0.000000414023\t489\t-308.999999585977\t180\n" +
                "FYXPVKNCBWLNLRH\t239\t-107.854492187500\t91\t-346.854492187500\t-148\n" +
                "FYXPVKNCBWLNLRH\t-75\t95.421875000000\t-387\t170.421875000000\t-312\n" +
                "FYXPVKNCBWLNLRH\t356\t983.109375000000\t249\t627.109375000000\t-107\n" +
                "FYXPVKNCBWLNLRH\t340\t0.000000013920\t1\t-339.999999986080\t-339\n" +
                "FYXPVKNCBWLNLRH\t-132\t-512.421875000000\tNaN\t-380.421875000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-318\t939.000000000000\t61\t1257.000000000000\t379\n" +
                "FYXPVKNCBWLNLRH\tNaN\t65.913402557373\t-265\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t-270\t240.000000000000\t52\t510.000000000000\t322\n" +
                "FYXPVKNCBWLNLRH\t227\t768.000000000000\t-125\t541.000000000000\t-352\n" +
                "FYXPVKNCBWLNLRH\tNaN\t0.000042232014\tNaN\tNaN\tNaN\n" +
                "FYXPVKNCBWLNLRH\t190\t0.000000044001\t-29\t-189.999999955999\t-219\n" +
                "FYXPVKNCBWLNLRH\t-52\t-663.138671875000\t-178\t-611.138671875000\t-126\n" +
                "FYXPVKNCBWLNLRH\t71\t0.013041995466\t-349\t-70.986958004534\t-420\n" +
                "FYXPVKNCBWLNLRH\t-333\t0.000000001181\t74\t333.000000001181\t407\n" +
                "FYXPVKNCBWLNLRH\t-102\t-767.203125000000\t-319\t-665.203125000000\t-217\n" +
                "FYXPVKNCBWLNLRH\t-4\t-296.000000000000\t-166\t-292.000000000000\t-162\n" +
                "FYXPVKNCBWLNLRH\t-227\t240.000000000000\tNaN\t467.000000000000\tNaN\n" +
                "FYXPVKNCBWLNLRH\t222\t-980.000000000000\t-187\t-1202.000000000000\t-409\n" +
                "FYXPVKNCBWLNLRH\t309\t13.880827903748\t-163\t-295.119172096252\t-472\n" +
                "FYXPVKNCBWLNLRH\t-308\t161.783554077148\t95\t469.783554077148\t403\n";

        assertThat(expected, "select id,w,x,z,x + -w, z+-w from tab where and id = 'FYXPVKNCBWLNLRH'");
    }

    @Test
    public void testNoColumns() throws Exception {
        createTabWithNaNs2();

        final String expected = "KKUSIMYDXUUSKCX\t0.000000001306\t-524.334808349609\t-163\t-214\t2015-03-14T23:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-338.665039062500\t9.986581325531\tNaN\t2\t2015-03-21T01:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-481.765014648438\t0.000194547960\t-11\t-485\t2015-05-17T03:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000010163\t334.607933044434\t425\t487\t2015-06-03T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-1024.000000000000\t0.281865596771\t421\t407\t2015-06-08T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000183005621\t0.216939434409\tNaN\t17\t2015-06-15T09:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-397.171875000000\t303.623046875000\t-102\t-310\t2015-06-16T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000025275158\t0.000012965906\t-320\tNaN\t2015-06-19T18:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000005719630\t56.000000000000\t262\t-230\t2015-06-19T19:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t245.200160980225\t-720.000000000000\t393\tNaN\t2015-06-26T18:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t314.000000000000\t0.006269838428\t204\t332\t2015-07-07T21:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-975.423828125000\t0.000010835186\t370\t-436\t2015-07-11T15:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t512.000000000000\t-352.000000000000\t-384\t306\t2015-07-13T10:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t5.404115438461\t8.854092121124\t-79\t-229\t2015-07-14T11:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-436.000000000000\t-811.000000000000\t-27\t237\t2015-07-14T16:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-687.623046875000\t-794.699401855469\t488\t162\t2015-07-19T12:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-288.000000000000\t69.312500000000\t341\t-290\t2015-07-25T20:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-807.692016601563\t1.505146384239\tNaN\t-71\t2015-08-10T06:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.944997668266\t-719.970214843750\t-116\t-82\t2015-08-10T18:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000013659448\t0.000006695827\t-57\t-189\t2015-08-12T13:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000845499017\t-576.000000000000\t-466\t179\t2015-08-12T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t238.849365234375\t0.000000004954\t202\t-22\t2015-08-13T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-1024.000000000000\t0.000000000000\t329\t-18\t2015-08-14T01:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-1024.000000000000\t-496.000000000000\t273\tNaN\t2015-08-16T18:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t384.000000000000\t638.000000000000\tNaN\t-397\t2015-08-17T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t320.248397827148\t0.099960185587\t169\tNaN\t2015-08-21T06:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000065215048\t-781.699218750000\t-484\t136\t2015-08-25T17:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t1.377335935831\t-512.000000000000\t-391\t340\t2015-09-07T00:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000158194605\t95.681182861328\t-447\t250\t2015-09-14T11:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-768.000000000000\t0.656211644411\t190\t-174\t2015-09-19T17:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t107.280467987061\t-830.501251220703\t-352\t-15\t2015-09-28T17:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t17.522465705872\t0.001122028742\t367\t26\t2015-09-30T11:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t712.062500000000\t-722.000000000000\t-188\t-474\t2015-10-07T07:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.003575030481\t0.000002332791\t2\tNaN\t2015-10-09T03:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000012474\t764.072387695313\t453\t-401\t2015-10-09T12:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t35.019264221191\t89.257812500000\t39\t-276\t2015-10-11T05:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000369906\t0.000000267083\t432\t-20\t2015-10-13T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000007178878\t0.007157569751\t-412\t-33\t2015-10-23T20:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.025878265500\t0.063682073727\t150\t145\t2015-10-29T02:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t84.000000000000\t0.000084220943\t-346\t213\t2015-11-12T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t9.068818807602\t21.524306297302\t494\t191\t2015-12-01T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-327.500000000000\tNaN\t130\t-92\t2015-12-02T20:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-512.000000000000\t-278.166625976563\t-51\t262\t2015-12-09T23:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-256.000000000000\t0.000000168164\t57\t258\t2015-12-10T05:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-267.936401367188\tNaN\t-346\tNaN\t2015-12-11T08:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t978.937500000000\t0.001185453089\t-326\t-325\t2015-12-18T19:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t2.489773750305\t104.570877075195\t108\t467\t2015-12-23T02:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t240.000000000000\t0.000415830291\tNaN\t-379\t2015-12-25T13:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000005017\tNaN\t-138\tNaN\t2015-12-25T17:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t896.000000000000\t4.382300496101\t-103\tNaN\t2015-12-30T01:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-612.000000000000\t0.000000004888\t72\tNaN\t2016-01-06T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000188779795\t0.000268302101\t-275\t117\t2016-01-07T00:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t1005.438659667969\t-286.437500000000\t-145\t364\t2016-01-09T15:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000343896\tNaN\t12\t-111\t2016-01-11T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000002748\t0.000000013812\t-171\t-358\t2016-01-13T09:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t358.500000000000\t0.000074812062\t-334\t379\t2016-01-22T03:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.059096898884\t0.000015207836\tNaN\t-16\t2016-01-24T07:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-442.187500000000\t704.000000000000\t466\t206\t2016-01-26T06:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000192925\tNaN\t-424\t-9\t2016-02-08T16:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t748.446655273438\t571.218750000000\t-354\t177\t2016-02-10T10:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t11.142787933350\t0.000000077990\t-485\t357\t2016-02-19T06:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.036795516498\tNaN\tNaN\t-313\t2016-02-23T01:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t642.199157714844\t0.000001014604\t-310\tNaN\t2016-02-26T18:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.035430195741\tNaN\t424\t266\t2016-03-11T00:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000136839\t-560.000000000000\t-73\t141\t2016-03-13T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000546713796\t131.358078002930\t-447\t183\t2016-03-23T07:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-209.717956542969\t10.917424678802\t-451\t312\t2016-03-26T11:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000001200607\tNaN\t53\t149\t2016-04-01T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000606306\t560.000000000000\t295\t115\t2016-04-01T08:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.010378313251\t225.566406250000\t341\t-82\t2016-04-03T09:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-894.284301757813\t0.271541014314\t-371\t189\t2016-04-15T22:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t77.797851562500\t683.051757812500\t-206\t389\t2016-04-22T23:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t176.000000000000\t0.038123233244\t-247\t334\t2016-04-28T04:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t5.540870904922\t0.000076783974\t-14\t-126\t2016-04-29T10:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000000193413\t405.375000000000\t441\t345\t2016-04-30T13:00:00.000Z\n";

        assertThat(expected, "tab where id = 'KKUSIMYDXUUSKCX'");
    }

    @Test
    public void testSigLookupError() throws Exception {
        createTabWithNaNs2();
        try {
            compile("select x,y from tab where x~0");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(28, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("No such function"));
        }
    }

    @Test
    public void testStrConcat2() throws Exception {
        createTabWithNaNs2();

        final String expected = "KKUSIMYDXUUSKCX\t-338.665039062500\n" +
                "KKUSIMYDXUUSKCX\t-481.765014648438\n" +
                "KKUSIMYDXUUSKCX\t0.000183005621\n" +
                "KKUSIMYDXUUSKCX\t5.404115438461\n" +
                "KKUSIMYDXUUSKCX\t-436.000000000000\n" +
                "KKUSIMYDXUUSKCX\t-807.692016601563\n" +
                "KKUSIMYDXUUSKCX\t0.000013659448\n" +
                "KKUSIMYDXUUSKCX\t384.000000000000\n" +
                "KKUSIMYDXUUSKCX\t0.003575030481\n" +
                "KKUSIMYDXUUSKCX\t35.019264221191\n" +
                "KKUSIMYDXUUSKCX\t-512.000000000000\n" +
                "KKUSIMYDXUUSKCX\t-256.000000000000\n" +
                "KKUSIMYDXUUSKCX\t240.000000000000\n" +
                "KKUSIMYDXUUSKCX\t-612.000000000000\n" +
                "KKUSIMYDXUUSKCX\t0.000000343896\n" +
                "KKUSIMYDXUUSKCX\t0.059096898884\n" +
                "KKUSIMYDXUUSKCX\t0.036795516498\n" +
                "KKUSIMYDXUUSKCX\t0.000000136839\n" +
                "KKUSIMYDXUUSKCX\t0.000001200607\n" +
                "KKUSIMYDXUUSKCX\t5.540870904922\n";

        assertThat(expected, "select id, x from tab where id+'-BLAH'='KKUSIMYDXUUSKCX-BLAH' and ((z > -100 and z < 100) or z = NaN)");
    }

    @Test
    public void testSubQuery1() throws Exception {
        createTabWithNaNs2();

        final String expected = "KKUSIMYDXUUSKCX\t-338.665039062500\t9.986581325531\tNaN\t2\t2015-03-21T01:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.000183005621\t0.216939434409\tNaN\t17\t2015-06-15T09:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t-807.692016601563\t1.505146384239\tNaN\t-71\t2015-08-10T06:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t384.000000000000\t638.000000000000\tNaN\t-397\t2015-08-17T14:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t240.000000000000\t0.000415830291\tNaN\t-379\t2015-12-25T13:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.059096898884\t0.000015207836\tNaN\t-16\t2016-01-24T07:00:00.000Z\n" +
                "KKUSIMYDXUUSKCX\t0.036795516498\tNaN\tNaN\t-313\t2016-02-23T01:00:00.000Z\n";

        assertThat(expected, "(tab where z = NaN) where id = 'KKUSIMYDXUUSKCX'");
    }

    @Test
    public void testSubQuery2() throws Exception {
        createTabWithNaNs2();

        final String expected = "KKUSIMYDXUUSKCX\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\n" +
                "KKUSIMYDXUUSKCX\tNaN\n";

        assertThat(expected, "select id, z from (tab where z = NaN) where id = 'KKUSIMYDXUUSKCX'");
    }

    @Test
    public void testSubQuery3() throws Exception {
        createTabWithNaNs2();
        try {
            compile("select id, z from (select id from tab where z = NaN) where id = 'KKUSIMYDXUUSKCX'");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(11, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("Invalid column"));
        }
    }


    @Test
    public void testSubQuery4() throws Exception {
        createTabWithNaNs2();

        final String expected = "ZSFXUNYQXTGNJJI\t-162\n" +
                "BHLNEJRMDIKDISG\t-458\n" +
                "YYVSYYEQBORDTQH\t-227\n";

        assertThat(expected, "select id, z from (tab where !(id in 'GMPLUCFTLNKYTSZ')) where timestamp = '2015-03-12T10:00:00;5m;30m;10'");
    }

    @Test
    public void testSubQuery5() throws Exception {
        createTabWithNaNs2();

        final String expected = "ZSFXUNYQXTGNJJI\t-162\n" +
                "BHLNEJRMDIKDISG\t-458\n" +
                "YYVSYYEQBORDTQH\t-227\n";

        assertThat(expected, "select id, z from (tab where !(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10')");
    }

    @Test
    public void testSubQuery7() throws Exception {
        createTabWithNaNs2();
        assertThat("", "select id, z from (tab where !(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10') where 10 < 3");
    }

    @Test
    public void testSubQueryConstantTrueWhere() throws Exception {
        createTabWithNaNs2();
        final String expected = "ZSFXUNYQXTGNJJI\t-162\n" +
                "BHLNEJRMDIKDISG\t-458\n" +
                "YYVSYYEQBORDTQH\t-227\n";

        assertThat(expected, "select id, z from (tab where !(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10') where 10 > 3");
    }

    @Test
    public void testSubQueryFalseModel() throws Exception {
        createTabWithNaNs2();
        assertThat("", "select id, z from (tab where !(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10') where timestamp = '2015-03-12T10:00:00' and timestamp = '2015-03-12T14:00:00'");
    }

    private void createTabNoNaNs() throws JournalException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        );

        Rnd rnd = new Rnd();
        int n = 128;
        ObjHashSet<String> names = getNames(rnd, n);

        int mask = n - 1;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putLong(3, rnd.nextLong());
            ew.putInt(4, rnd.nextInt());
            ew.putDate(5, t += 10);
            ew.append();
        }
        w.commit();
    }

    private void createTabWithNaNs() throws JournalException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        );

        Rnd rnd = new Rnd();
        int n = 128;
        ObjHashSet<String> names = getNames(rnd, n);

        int mask = n - 1;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(2);
            } else {
                ew.putDouble(2, rnd.nextDouble());
            }
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(3);
            } else {
                ew.putLong(3, rnd.nextLong() % 500);
            }
            ew.putInt(4, rnd.nextInt() % 500);
            ew.putDate(5, t += 10);
            ew.append();
        }
        w.commit();
    }

    private void createTabWithNaNs2() throws JournalException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        );

        Rnd rnd = new Rnd();
        int n = 128;
        ObjHashSet<String> names = getNames(rnd, n);

        int mask = n - 1;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(2);
            } else {
                ew.putDouble(2, rnd.nextDouble());
            }
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(3);
            } else {
                ew.putLong(3, rnd.nextLong() % 500);
            }
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(4);
            } else {
                ew.putInt(4, rnd.nextInt() % 500);
            }
            ew.putDate(5, t += (60 * 60 * 1000));
            ew.append();
        }
        w.commit();
    }
}
