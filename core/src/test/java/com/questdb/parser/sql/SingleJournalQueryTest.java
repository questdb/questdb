/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.parser.sql;

import com.questdb.common.JournalRuntimeException;
import com.questdb.ex.ParserException;
import com.questdb.ex.UndefinedParameterException;
import com.questdb.model.Quote;
import com.questdb.ql.RecordSource;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Dates;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SingleJournalQueryTest extends AbstractTest {

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
    public void testAddMillis() throws Exception {
        createTabWithNaNs2();
        assertThat("2015-03-18T09:00:00.000Z\t2015-03-18T10:00:00.000Z\n" +
                        "2015-03-25T20:00:00.000Z\t2015-03-25T21:00:00.000Z\n" +
                        "2015-04-13T07:00:00.000Z\t2015-04-13T08:00:00.000Z\n" +
                        "2015-04-15T18:00:00.000Z\t2015-04-15T19:00:00.000Z\n" +
                        "2015-04-16T14:00:00.000Z\t2015-04-16T15:00:00.000Z\n",
                "select timestamp, timestamp + 60*60*1000L incr from tab where id ~ 'BHE' limit 5");

    }

    @Test
    public void testAliasInWhere() throws Exception {
        createTabWithNaNs();
        final String expected = "BROMNXKUIZULIGY\t506\t256\n" +
                "BROMNXKUIZULIGY\t531\t281\n" +
                "BROMNXKUIZULIGY\t591\t341\n" +
                "BROMNXKUIZULIGY\t514\t264\n" +
                "BROMNXKUIZULIGY\t590\t340\n" +
                "BROMNXKUIZULIGY\t550\t300\n";

        assertThat(expected, "(select id k, z + 250 u, z from tab where z >= 250 and k ~ 'BROM') where u < 600 ");
    }

    @Test
    public void testAliasInWhere2() throws Exception {
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

        assertThat(expected, "select id k, z + 250 u, z from tab where z >= 250 and k ~ 'BROM'");
    }

    @Test
    public void testAndArgCheck() {
        // missing left arg on 'and'
        try {
            expectFailure("select id,w,x,z,x + -w, z+-w from tab where and id = 'FYXPVKNCBWLNLRH'");
        } catch (ParserException e) {
            Assert.assertEquals(44, QueryError.getPosition());
        }
    }

    @Test
    public void testAndArgCheck2() {
        // missing left arg on 'and'
        try {
            expectFailure("select id,w,x,z,x + -w, z+-w from tab where id = 'FYXPVKNCBWLNLRH' and ");
        } catch (ParserException e) {
            Assert.assertEquals(67, QueryError.getPosition());
        }
    }

    @Test
    public void testAtoI() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").$str("intC").$()
        )) {

            StringSink sink = new StringSink();
            Rnd rnd = new Rnd();
            for (int i = 0; i < 30; i++) {
                sink.clear();
                Numbers.append(sink, rnd.nextInt());
                CharSequence cs = rnd.nextChars(4);
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, rnd.nextBoolean() ? cs : sink);
                ew.append();
            }
            w.commit();


            final String expected = "-1148479920\t-1148479920\n" +
                    "1326447242\t1326447242\n" +
                    "-1436881714\t-1436881714\n" +
                    "-409854405\t-409854405\n" +
                    "1125579207\t1125579207\n" +
                    "-1844391305\t-1844391305\n" +
                    "-1125169127\t-1125169127\n" +
                    "-2119387831\t-2119387831\n" +
                    "-422941535\t-422941535\n" +
                    "-2132716300\t-2132716300\n" +
                    "-483853667\t-483853667\n" +
                    "NaN\tQQUL\n" +
                    "-1418341054\t-1418341054\n" +
                    "936627841\t936627841\n" +
                    "NaN\tFBVT\n" +
                    "-1515787781\t-1515787781\n" +
                    "-1538602195\t-1538602195\n" +
                    "-10505757\t-10505757\n" +
                    "NaN\tICWE\n" +
                    "1876812930\t1876812930\n" +
                    "NaN\tOTSE\n" +
                    "-916132123\t-916132123\n" +
                    "NaN\tLYXW\n" +
                    "NaN\tYLSU\n" +
                    "-1768335227\t-1768335227\n" +
                    "-876466531\t-876466531\n" +
                    "NaN\tQBZX\n" +
                    "NaN\tVIKJ\n" +
                    "-2088317486\t-2088317486\n" +
                    "614536941\t614536941\n";
            assertThat(expected, "select atoi(intC), intC from tab");


            assertThat("-1.14847992E9\t-1148479920\n" +
                            "1.326447242E9\t1326447242\n" +
                            "-1.436881714E9\t-1436881714\n" +
                            "-4.09854405E8\t-409854405\n" +
                            "1.125579207E9\t1125579207\n" +
                            "-1.844391305E9\t-1844391305\n" +
                            "-1.125169127E9\t-1125169127\n" +
                            "-2.119387831E9\t-2119387831\n" +
                            "-4.22941535E8\t-422941535\n" +
                            "-2.1327163E9\t-2132716300\n" +
                            "-4.83853667E8\t-483853667\n" +
                            "NaN\tQQUL\n" +
                            "-1.418341054E9\t-1418341054\n" +
                            "9.36627841E8\t936627841\n" +
                            "NaN\tFBVT\n" +
                            "-1.515787781E9\t-1515787781\n" +
                            "-1.538602195E9\t-1538602195\n" +
                            "-1.0505757E7\t-10505757\n" +
                            "NaN\tICWE\n" +
                            "1.87681293E9\t1876812930\n" +
                            "NaN\tOTSE\n" +
                            "-9.16132123E8\t-916132123\n" +
                            "NaN\tLYXW\n" +
                            "NaN\tYLSU\n" +
                            "-1.768335227E9\t-1768335227\n" +
                            "-8.76466531E8\t-876466531\n" +
                            "NaN\tQBZX\n" +
                            "NaN\tVIKJ\n" +
                            "-2.088317486E9\t-2088317486\n" +
                            "6.14536941E8\t614536941\n",
                    "select atod(intC), intC from tab");
        }
    }

    @Test
    public void testColumnAliases() throws Exception {
        createTabWithNaNs2();

        assertThat("a\tb\n" +
                        "1014.000000000000\tNaN\n" +
                        "1008.553894042969\tNaN\n" +
                        "1017.000000000000\tNaN\n",
                "select x a, y b from tab where y = NaN and x > 1000", true);
    }

    @Test
    public void testConstantCondition1() throws Exception {
        createTab();
        assertPlan("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"NoOpJournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}",
                "select id, x, y from tab where x > 0 and 1 > 1");
    }

    @Test
    public void testConstantCondition2() throws Exception {
        createTab();
        assertPlan("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}",
                "select id, x, y from tab where x > 0 or 1 = 1");
    }

    @Test
    public void testConstantCondition3() throws Exception {
        createTab();
        assertPlan("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"NoOpJournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}",
                "select id, x, y from tab where 1 > 1 or 2 > 2");
    }

    @Test
    public void testConstants() throws Exception {
        createTabWithNaNs2();
        assertThat("FLOAT\tDOUBLE\tINT\tLONG\n", "select typeOf(123.34f), typeOf(123.34), typeOf(1234), typeOf(1234L) from tab limit 1");
        assertSymbol("select typeOf(123.34f), typeOf(123.34), typeOf(1234), typeOf(1234L) from tab limit 1");
    }

    @Test
    public void testDateEquals() throws Exception {
        createTabWithSymbol();
        final String expected = "FLRJLIUCZXHEPGK\tPZDYLPYJJP\t802.000000000000\t0.000000003518\t2015-03-12T00:01:39.630Z\n";
        assertThat(expected, "tab where timestamp = toDate('2015-03-12T00:01:39.630Z')");
        assertThat(expected, "tab where timestamp = '2015-03-12T00:01:39.630Z'");
    }

    @Test
    public void testDateGreater() throws Exception {
        createTabWithSymbol();
        final String expected = "OUZBRTJQZFHPYNJ\tEUIMIKBLKD\t16.000000000000\t936.000000000000\t2015-03-12T00:01:39.640Z\n" +
                "YCFIBRUTIIPOFZO\tLGUBNTLOBY\t0.000000204810\t11.203086614609\t2015-03-12T00:01:39.650Z\n" +
                "WMNVMFFLSQTWPRT\tSRSQCLKJVK\t0.000000218690\t384.000000000000\t2015-03-12T00:01:39.660Z\n" +
                "EOHBGLBYXFGYIBH\tFHCHIFIPHZ\t5.323504686356\t0.000191266379\t2015-03-12T00:01:39.670Z\n" +
                "ZMEOUEPFSZEYDNM\tCWOFRMJORR\t0.000001766383\t-82.781250000000\t2015-03-12T00:01:39.680Z\n" +
                "FHZNVQJXWNXHJWR\tDLQJBVVTTV\t640.000000000000\t0.000000007695\t2015-03-12T00:01:39.690Z\n" +
                "QLDGLOGIFOUSZMZ\tCHJNLZOOYL\t110.375000000000\t987.000000000000\t2015-03-12T00:01:39.700Z\n" +
                "DLRBIDSTDTFBYHS\tSXXBMBBOGG\t-957.750000000000\t0.110677853227\t2015-03-12T00:01:39.710Z\n" +
                "UZIZKMFCKVHMRTG\tTETQNSQTYO\t800.000000000000\t0.523283928633\t2015-03-12T00:01:39.720Z\n" +
                "VFZFKWZLUOGXHFV\tIBNOKWHVKP\t0.000003309520\t-845.750000000000\t2015-03-12T00:01:39.730Z\n" +
                "IUNJPYZVOJBTYGL\tGLONQBVNJQ\t406.359375000000\t-889.875000000000\t2015-03-12T00:01:39.740Z\n" +
                "PFZPXRHMUJOGWMJ\tUSGCNKKKGI\t0.000885035086\t918.488769531250\t2015-03-12T00:01:39.750Z\n" +
                "ZHVJFPVRMUBLILX\tPLUQPOKPIW\t-320.000000000000\t709.773437500000\t2015-03-12T00:01:39.760Z\n" +
                "GEUBDFBTXXGWQFT\tNFDSBMBZFB\t11.990052223206\t560.000000000000\t2015-03-12T00:01:39.770Z\n" +
                "BELCSCIUZMCFUWY\tSRSQCLKJVK\t0.007322372403\t276.781250000000\t2015-03-12T00:01:39.780Z\n" +
                "TEHIOFKMQPUNEUD\tSXXBMBBOGG\t290.000000000000\t0.000000062047\t2015-03-12T00:01:39.790Z\n" +
                "YNLERUGEWULQWLN\tCJCNMPYSJF\t439.851211547852\t0.000417203366\t2015-03-12T00:01:39.800Z\n" +
                "OIBUGYXIMDIDNRO\tMRFPKLNWQL\t0.000245904943\t0.049118923023\t2015-03-12T00:01:39.810Z\n" +
                "XXTPBXNCJEDGPLH\tDLQJBVVTTV\t58.250000000000\t0.191959321499\t2015-03-12T00:01:39.820Z\n" +
                "ZNIFDRPHNGTNJJP\tKIWPCXCIGW\t666.264648437500\t0.000283217822\t2015-03-12T00:01:39.830Z\n" +
                "ONIJMVFQFDBOMQB\tEDBVUVMLSX\t0.000000120621\t-694.250000000000\t2015-03-12T00:01:39.840Z\n" +
                "UCSOZSLOQGPOHIM\tOJPOHDOWSK\t29.429825782776\t-13.079589843750\t2015-03-12T00:01:39.850Z\n" +
                "VVOFHXMMEJXJNRD\tVZQSGLLZQG\t-384.000000000000\t0.006880680798\t2015-03-12T00:01:39.860Z\n" +
                "WDDZKIFCBRMKKRH\tMERDPIDQGJ\t387.875000000000\t0.000000007496\t2015-03-12T00:01:39.870Z\n" +
                "UPJFSREKEUNMKWO\tCWOFRMJORR\t642.000000000000\t1.016614258289\t2015-03-12T00:01:39.880Z\n" +
                "TMIWHRQISSCIGXL\tRULHMZROYY\t-736.000000000000\t0.000000238132\t2015-03-12T00:01:39.890Z\n" +
                "WJWZBSMWXQOFNRO\tDZDUVNCYYV\t9.389577388763\t8.453002452850\t2015-03-12T00:01:39.900Z\n" +
                "IFBEPWLUTGRGEIW\tMUTSHRDWCI\t681.700683593750\t0.000765276010\t2015-03-12T00:01:39.910Z\n" +
                "MBJEIRSSGXDDXKD\tUSGCNKKKGI\t0.000001004210\t-1024.000000000000\t2015-03-12T00:01:39.920Z\n" +
                "LXGRVRKVTIFXRLE\tVGYEPKGPSJ\t-860.750000000000\t-210.259765625000\t2015-03-12T00:01:39.930Z\n" +
                "DEPQDCICIZUYOGK\tWVJFWXNHTG\t-227.859375000000\t-236.156250000000\t2015-03-12T00:01:39.940Z\n" +
                "XCHCUBYPMDFJZYJ\tWYCLIXTWVG\t0.000409339424\t-175.941406250000\t2015-03-12T00:01:39.950Z\n" +
                "TPCOITVXSGCPELN\tHTIWQZBCZV\t-420.375000000000\t0.005818145582\t2015-03-12T00:01:39.960Z\n" +
                "QJMRPDXTXOMBKZY\tSMCKFERVGM\t10.221145391464\t512.000000000000\t2015-03-12T00:01:39.970Z\n" +
                "UHHRIRJGLQNMETN\tELYYIFISZU\t375.230987548828\t0.121945030987\t2015-03-12T00:01:39.980Z\n" +
                "SDRLMSHKFZJJECT\tCJCNMPYSJF\t384.000000000000\t0.000000000000\t2015-03-12T00:01:39.990Z\n" +
                "MTLSJOWYLNEDVLL\tIPVOKGQWDX\t2.619887888432\t-477.522460937500\t2015-03-12T00:01:40.000Z\n";

        assertThat(expected, "tab where timestamp > toDate('2015-03-12T00:01:39.630Z')");
        assertThat(expected, "tab where timestamp > '2015-03-12T00:01:39.630Z'");
    }

    @Test
    public void testDateGreaterOrEqual() throws Exception {
        createTabWithSymbol();
        final String expected = "FLRJLIUCZXHEPGK\tPZDYLPYJJP\t802.000000000000\t0.000000003518\t2015-03-12T00:01:39.630Z\n" +
                "OUZBRTJQZFHPYNJ\tEUIMIKBLKD\t16.000000000000\t936.000000000000\t2015-03-12T00:01:39.640Z\n" +
                "YCFIBRUTIIPOFZO\tLGUBNTLOBY\t0.000000204810\t11.203086614609\t2015-03-12T00:01:39.650Z\n" +
                "WMNVMFFLSQTWPRT\tSRSQCLKJVK\t0.000000218690\t384.000000000000\t2015-03-12T00:01:39.660Z\n" +
                "EOHBGLBYXFGYIBH\tFHCHIFIPHZ\t5.323504686356\t0.000191266379\t2015-03-12T00:01:39.670Z\n" +
                "ZMEOUEPFSZEYDNM\tCWOFRMJORR\t0.000001766383\t-82.781250000000\t2015-03-12T00:01:39.680Z\n" +
                "FHZNVQJXWNXHJWR\tDLQJBVVTTV\t640.000000000000\t0.000000007695\t2015-03-12T00:01:39.690Z\n" +
                "QLDGLOGIFOUSZMZ\tCHJNLZOOYL\t110.375000000000\t987.000000000000\t2015-03-12T00:01:39.700Z\n" +
                "DLRBIDSTDTFBYHS\tSXXBMBBOGG\t-957.750000000000\t0.110677853227\t2015-03-12T00:01:39.710Z\n" +
                "UZIZKMFCKVHMRTG\tTETQNSQTYO\t800.000000000000\t0.523283928633\t2015-03-12T00:01:39.720Z\n" +
                "VFZFKWZLUOGXHFV\tIBNOKWHVKP\t0.000003309520\t-845.750000000000\t2015-03-12T00:01:39.730Z\n" +
                "IUNJPYZVOJBTYGL\tGLONQBVNJQ\t406.359375000000\t-889.875000000000\t2015-03-12T00:01:39.740Z\n" +
                "PFZPXRHMUJOGWMJ\tUSGCNKKKGI\t0.000885035086\t918.488769531250\t2015-03-12T00:01:39.750Z\n" +
                "ZHVJFPVRMUBLILX\tPLUQPOKPIW\t-320.000000000000\t709.773437500000\t2015-03-12T00:01:39.760Z\n" +
                "GEUBDFBTXXGWQFT\tNFDSBMBZFB\t11.990052223206\t560.000000000000\t2015-03-12T00:01:39.770Z\n" +
                "BELCSCIUZMCFUWY\tSRSQCLKJVK\t0.007322372403\t276.781250000000\t2015-03-12T00:01:39.780Z\n" +
                "TEHIOFKMQPUNEUD\tSXXBMBBOGG\t290.000000000000\t0.000000062047\t2015-03-12T00:01:39.790Z\n" +
                "YNLERUGEWULQWLN\tCJCNMPYSJF\t439.851211547852\t0.000417203366\t2015-03-12T00:01:39.800Z\n" +
                "OIBUGYXIMDIDNRO\tMRFPKLNWQL\t0.000245904943\t0.049118923023\t2015-03-12T00:01:39.810Z\n" +
                "XXTPBXNCJEDGPLH\tDLQJBVVTTV\t58.250000000000\t0.191959321499\t2015-03-12T00:01:39.820Z\n" +
                "ZNIFDRPHNGTNJJP\tKIWPCXCIGW\t666.264648437500\t0.000283217822\t2015-03-12T00:01:39.830Z\n" +
                "ONIJMVFQFDBOMQB\tEDBVUVMLSX\t0.000000120621\t-694.250000000000\t2015-03-12T00:01:39.840Z\n" +
                "UCSOZSLOQGPOHIM\tOJPOHDOWSK\t29.429825782776\t-13.079589843750\t2015-03-12T00:01:39.850Z\n" +
                "VVOFHXMMEJXJNRD\tVZQSGLLZQG\t-384.000000000000\t0.006880680798\t2015-03-12T00:01:39.860Z\n" +
                "WDDZKIFCBRMKKRH\tMERDPIDQGJ\t387.875000000000\t0.000000007496\t2015-03-12T00:01:39.870Z\n" +
                "UPJFSREKEUNMKWO\tCWOFRMJORR\t642.000000000000\t1.016614258289\t2015-03-12T00:01:39.880Z\n" +
                "TMIWHRQISSCIGXL\tRULHMZROYY\t-736.000000000000\t0.000000238132\t2015-03-12T00:01:39.890Z\n" +
                "WJWZBSMWXQOFNRO\tDZDUVNCYYV\t9.389577388763\t8.453002452850\t2015-03-12T00:01:39.900Z\n" +
                "IFBEPWLUTGRGEIW\tMUTSHRDWCI\t681.700683593750\t0.000765276010\t2015-03-12T00:01:39.910Z\n" +
                "MBJEIRSSGXDDXKD\tUSGCNKKKGI\t0.000001004210\t-1024.000000000000\t2015-03-12T00:01:39.920Z\n" +
                "LXGRVRKVTIFXRLE\tVGYEPKGPSJ\t-860.750000000000\t-210.259765625000\t2015-03-12T00:01:39.930Z\n" +
                "DEPQDCICIZUYOGK\tWVJFWXNHTG\t-227.859375000000\t-236.156250000000\t2015-03-12T00:01:39.940Z\n" +
                "XCHCUBYPMDFJZYJ\tWYCLIXTWVG\t0.000409339424\t-175.941406250000\t2015-03-12T00:01:39.950Z\n" +
                "TPCOITVXSGCPELN\tHTIWQZBCZV\t-420.375000000000\t0.005818145582\t2015-03-12T00:01:39.960Z\n" +
                "QJMRPDXTXOMBKZY\tSMCKFERVGM\t10.221145391464\t512.000000000000\t2015-03-12T00:01:39.970Z\n" +
                "UHHRIRJGLQNMETN\tELYYIFISZU\t375.230987548828\t0.121945030987\t2015-03-12T00:01:39.980Z\n" +
                "SDRLMSHKFZJJECT\tCJCNMPYSJF\t384.000000000000\t0.000000000000\t2015-03-12T00:01:39.990Z\n" +
                "MTLSJOWYLNEDVLL\tIPVOKGQWDX\t2.619887888432\t-477.522460937500\t2015-03-12T00:01:40.000Z\n";

        assertThat(expected, "tab where timestamp >= toDate('2015-03-12T00:01:39.630Z')");
        assertThat(expected, "tab where timestamp >= '2015-03-12T00:01:39.630Z'");
    }

    @Test
    public void testDateLess() throws Exception {
        createTabWithSymbol();
        final String expected = "LTISIRJQLWPWDVI\tQJLOQQWWRQ\t0.001847213891\t782.780761718750\t2015-03-12T00:00:00.010Z\n" +
                "KKHXEFMRDTNMZXQ\tOJPOHDOWSK\t280.588867187500\t26.110730648041\t2015-03-12T00:00:00.020Z\n" +
                "IFELSVFCLLTKNRI\tIPVOKGQWDX\t730.000000000000\t0.000000215277\t2015-03-12T00:00:00.030Z\n" +
                "BVBDTCLGEJBYBSJ\tTETQNSQTYO\t-1024.000000000000\t0.906509011984\t2015-03-12T00:00:00.040Z\n" +
                "UWOUBDHXHUWGRJY\tTKMQLRBSEN\t265.834960937500\t389.984191894531\t2015-03-12T00:00:00.050Z\n" +
                "XPNKGQELQDWQGMZ\tKIWPCXCIGW\t0.000000175463\t0.085401995108\t2015-03-12T00:00:00.060Z\n" +
                "WRWFCYOPVEICZCC\tNYSEHQIFHZ\t0.000025559940\t152.464843750000\t2015-03-12T00:00:00.070Z\n" +
                "WJGLDUTMUGDVOVL\tQUIXCJQBRM\t-927.687500000000\t-202.000000000000\t2015-03-12T00:00:00.080Z\n" +
                "GOJNELWTYCFNLUF\tMRFPKLNWQL\t512.000000000000\t0.000000709136\t2015-03-12T00:00:00.090Z\n" +
                "PXQEPFERZKBFFKK\tOJPOHDOWSK\t23.054984092712\t2.820306181908\t2015-03-12T00:00:00.100Z\n" +
                "OPWOLFQBXZICTJN\tPLUQPOKPIW\t-498.000000000000\t0.000131426888\t2015-03-12T00:00:00.110Z\n";

        assertThat(expected, "tab where timestamp < toDate('2015-03-12T00:00:00.120Z')");
        assertThat(expected, "tab where timestamp < '2015-03-12T00:00:00.120Z'");
    }

    @Test
    public void testDateLessOrEqual() throws Exception {
        createTabWithSymbol();
        final String expected = "LTISIRJQLWPWDVI\tQJLOQQWWRQ\t0.001847213891\t782.780761718750\t2015-03-12T00:00:00.010Z\n" +
                "KKHXEFMRDTNMZXQ\tOJPOHDOWSK\t280.588867187500\t26.110730648041\t2015-03-12T00:00:00.020Z\n" +
                "IFELSVFCLLTKNRI\tIPVOKGQWDX\t730.000000000000\t0.000000215277\t2015-03-12T00:00:00.030Z\n" +
                "BVBDTCLGEJBYBSJ\tTETQNSQTYO\t-1024.000000000000\t0.906509011984\t2015-03-12T00:00:00.040Z\n" +
                "UWOUBDHXHUWGRJY\tTKMQLRBSEN\t265.834960937500\t389.984191894531\t2015-03-12T00:00:00.050Z\n" +
                "XPNKGQELQDWQGMZ\tKIWPCXCIGW\t0.000000175463\t0.085401995108\t2015-03-12T00:00:00.060Z\n" +
                "WRWFCYOPVEICZCC\tNYSEHQIFHZ\t0.000025559940\t152.464843750000\t2015-03-12T00:00:00.070Z\n" +
                "WJGLDUTMUGDVOVL\tQUIXCJQBRM\t-927.687500000000\t-202.000000000000\t2015-03-12T00:00:00.080Z\n" +
                "GOJNELWTYCFNLUF\tMRFPKLNWQL\t512.000000000000\t0.000000709136\t2015-03-12T00:00:00.090Z\n" +
                "PXQEPFERZKBFFKK\tOJPOHDOWSK\t23.054984092712\t2.820306181908\t2015-03-12T00:00:00.100Z\n" +
                "OPWOLFQBXZICTJN\tPLUQPOKPIW\t-498.000000000000\t0.000131426888\t2015-03-12T00:00:00.110Z\n" +
                "PFZGUJBKNTPYXUB\tKIWPCXCIGW\t0.000006574179\t222.422851562500\t2015-03-12T00:00:00.120Z\n";

        assertThat(expected, "tab where timestamp <= toDate('2015-03-12T00:00:00.120Z')");
        assertThat(expected, "tab where timestamp <= '2015-03-12T00:00:00.120Z'");
    }

    @Test
    public void testDateMultInterval() throws Exception {
        createTabWithSymbol();
        final String expected = "QRZCGENVSOFKRKE\tOJPOHDOWSK\t0.026915318333\t108.585937500000\t2015-03-12T00:01:11.120Z\n" +
                "YSLYCGDGJVDWMTP\tFHCHIFIPHZ\t-168.000000000000\t-1024.000000000000\t2015-03-12T00:01:11.230Z\n" +
                "QRZCGENVSOFKRKE\tPLUQPOKPIW\t0.002588719828\t1012.000000000000\t2015-03-12T00:01:11.740Z\n" +
                "OQDHZTECGRBEFYC\tTETQNSQTYO\t0.000003892420\t0.002290764009\t2015-03-12T00:01:30.300Z\n" +
                "JODBFRSHPSQECGJ\tIBNOKWHVKP\t0.000000000000\t720.000000000000\t2015-03-12T00:01:30.880Z\n";

        assertThat(expected, "tab where id ~ 'CG' and timestamp = '2015' and (timestamp = '2015-03-12T00:01:11' or timestamp = '2015-03-12T00:01:30')");
    }

    @Test
    public void testDateMultInterval2() throws Exception {
        createTabWithSymbol();
        final String expected = "BVBDTCLGEJBYBSJ\tTETQNSQTYO\t-1024.000000000000\t0.906509011984\t2015-03-12T00:00:00.040Z\n" +
                "BVBDTCLGEJBYBSJ\tPZDYLPYJJP\t0.000007882405\t0.064732506871\t2015-03-12T00:00:00.540Z\n" +
                "WQTBDTSJOVQJYLZ\tIPVOKGQWDX\t598.000000000000\t0.000005879177\t2015-03-12T00:00:12.340Z\n" +
                "BVBDTCLGEJBYBSJ\tJMCPJCLCWV\t86.466068267822\t768.000000000000\t2015-03-12T00:00:21.910Z\n" +
                "BVBDTCLGEJBYBSJ\tWWLCPSOEVR\t-827.000000000000\t0.376058250666\t2015-03-12T00:00:25.520Z\n" +
                "BVBDTCLGEJBYBSJ\tNYSEHQIFHZ\t0.000139812866\t0.657814309001\t2015-03-12T00:00:32.540Z\n" +
                "WQTBDTSJOVQJYLZ\tQJLOQQWWRQ\t-764.964843750000\t0.000750026142\t2015-03-12T00:00:39.670Z\n" +
                "BVBDTCLGEJBYBSJ\tVEEIXOZZFN\t0.012393638026\t20.759119987488\t2015-03-12T00:00:40.170Z\n" +
                "BVBDTCLGEJBYBSJ\tMRFPKLNWQL\t928.000000000000\t0.024308424909\t2015-03-12T00:00:40.770Z\n" +
                "WQTBDTSJOVQJYLZ\tCWOFRMJORR\t0.000000017971\t0.000013092605\t2015-03-12T00:00:41.090Z\n" +
                "BVBDTCLGEJBYBSJ\tWOIJWXOIOT\t0.000000007575\t0.001671805512\t2015-03-12T00:00:44.060Z\n" +
                "WQTBDTSJOVQJYLZ\tEUIMIKBLKD\t0.028749017045\t0.000000224847\t2015-03-12T00:00:47.200Z\n" +
                "WQTBDTSJOVQJYLZ\tSXXBMBBOGG\t71.453750610352\t0.000343156316\t2015-03-12T00:00:48.080Z\n" +
                "BVBDTCLGEJBYBSJ\tKJNBLQHSHH\t0.000000000000\t-384.000000000000\t2015-03-12T00:00:52.550Z\n" +
                "WQTBDTSJOVQJYLZ\tWYCLIXTWVG\t2.525636672974\t-370.000000000000\t2015-03-12T00:00:59.050Z\n" +
                "WQTBDTSJOVQJYLZ\tBVMFXSKVMS\t0.017297931015\t0.011041654274\t2015-03-12T00:01:00.940Z\n" +
                "BVBDTCLGEJBYBSJ\tDBEHZQPMTN\t196.375000000000\t755.625000000000\t2015-03-12T00:01:07.790Z\n" +
                "WQTBDTSJOVQJYLZ\tWVJFWXNHTG\t25.559150695801\t-512.000000000000\t2015-03-12T00:01:15.940Z\n" +
                "WQTBDTSJOVQJYLZ\tRULHMZROYY\t-140.000000000000\t0.000005934012\t2015-03-12T00:01:18.040Z\n" +
                "WQTBDTSJOVQJYLZ\tKPOZXJTBMM\t0.000000011937\t-704.000000000000\t2015-03-12T00:01:35.440Z\n" +
                "BVBDTCLGEJBYBSJ\tJMCPJCLCWV\t-696.050415039063\t512.000000000000\t2015-03-12T00:01:36.900Z\n" +
                "WQTBDTSJOVQJYLZ\tIPCBXJGYRN\t5.286757946014\t210.500000000000\t2015-03-12T00:01:37.510Z\n";

        assertThat(expected, "tab where timestamp = '2015;1d;1y;2' and timestamp = '2015-03-11;1d;1d;2' and id ~ 'BDT'");
    }

    @Test
    public void testDateNotEqual() throws Exception {
        createTabWithSymbol();
        final String expected = "LTISIRJQLWPWDVI\tQJLOQQWWRQ\t0.001847213891\t782.780761718750\t2015-03-12T00:00:00.010Z\n" +
                "KKHXEFMRDTNMZXQ\tOJPOHDOWSK\t280.588867187500\t26.110730648041\t2015-03-12T00:00:00.020Z\n" +
                "IFELSVFCLLTKNRI\tIPVOKGQWDX\t730.000000000000\t0.000000215277\t2015-03-12T00:00:00.030Z\n" +
                "BVBDTCLGEJBYBSJ\tTETQNSQTYO\t-1024.000000000000\t0.906509011984\t2015-03-12T00:00:00.040Z\n" +
                "UWOUBDHXHUWGRJY\tTKMQLRBSEN\t265.834960937500\t389.984191894531\t2015-03-12T00:00:00.050Z\n" +
                "WRWFCYOPVEICZCC\tNYSEHQIFHZ\t0.000025559940\t152.464843750000\t2015-03-12T00:00:00.070Z\n" +
                "WJGLDUTMUGDVOVL\tQUIXCJQBRM\t-927.687500000000\t-202.000000000000\t2015-03-12T00:00:00.080Z\n" +
                "GOJNELWTYCFNLUF\tMRFPKLNWQL\t512.000000000000\t0.000000709136\t2015-03-12T00:00:00.090Z\n" +
                "PXQEPFERZKBFFKK\tOJPOHDOWSK\t23.054984092712\t2.820306181908\t2015-03-12T00:00:00.100Z\n" +
                "OPWOLFQBXZICTJN\tPLUQPOKPIW\t-498.000000000000\t0.000131426888\t2015-03-12T00:00:00.110Z\n" +
                "PFZGUJBKNTPYXUB\tKIWPCXCIGW\t0.000006574179\t222.422851562500\t2015-03-12T00:00:00.120Z\n";

        assertThat(expected, "tab where timestamp <= toDate('2015-03-12T00:00:00.120Z') and timestamp != toDate('2015-03-12T00:00:00.060Z')");
        assertThat(expected, "tab where timestamp <= '2015-03-12T00:00:00.120Z' and timestamp != '2015-03-12T00:00:00.060Z'");
    }

    @Test
    public void testDateNotIn() throws Exception {
        createTabWithSymbol();
        final String expected = "OUZBRTJQZFHPYNJ\tEUIMIKBLKD\t16.000000000000\t936.000000000000\t2015-03-12T00:01:39.640Z\n" +
                "YCFIBRUTIIPOFZO\tLGUBNTLOBY\t0.000000204810\t11.203086614609\t2015-03-12T00:01:39.650Z\n" +
                "WMNVMFFLSQTWPRT\tSRSQCLKJVK\t0.000000218690\t384.000000000000\t2015-03-12T00:01:39.660Z\n" +
                "EOHBGLBYXFGYIBH\tFHCHIFIPHZ\t5.323504686356\t0.000191266379\t2015-03-12T00:01:39.670Z\n" +
                "ZMEOUEPFSZEYDNM\tCWOFRMJORR\t0.000001766383\t-82.781250000000\t2015-03-12T00:01:39.680Z\n" +
                "FHZNVQJXWNXHJWR\tDLQJBVVTTV\t640.000000000000\t0.000000007695\t2015-03-12T00:01:39.690Z\n" +
                "QLDGLOGIFOUSZMZ\tCHJNLZOOYL\t110.375000000000\t987.000000000000\t2015-03-12T00:01:39.700Z\n" +
                "DLRBIDSTDTFBYHS\tSXXBMBBOGG\t-957.750000000000\t0.110677853227\t2015-03-12T00:01:39.710Z\n" +
                "UZIZKMFCKVHMRTG\tTETQNSQTYO\t800.000000000000\t0.523283928633\t2015-03-12T00:01:39.720Z\n" +
                "VFZFKWZLUOGXHFV\tIBNOKWHVKP\t0.000003309520\t-845.750000000000\t2015-03-12T00:01:39.730Z\n" +
                "IUNJPYZVOJBTYGL\tGLONQBVNJQ\t406.359375000000\t-889.875000000000\t2015-03-12T00:01:39.740Z\n" +
                "PFZPXRHMUJOGWMJ\tUSGCNKKKGI\t0.000885035086\t918.488769531250\t2015-03-12T00:01:39.750Z\n" +
                "ZHVJFPVRMUBLILX\tPLUQPOKPIW\t-320.000000000000\t709.773437500000\t2015-03-12T00:01:39.760Z\n" +
                "GEUBDFBTXXGWQFT\tNFDSBMBZFB\t11.990052223206\t560.000000000000\t2015-03-12T00:01:39.770Z\n" +
                "BELCSCIUZMCFUWY\tSRSQCLKJVK\t0.007322372403\t276.781250000000\t2015-03-12T00:01:39.780Z\n" +
                "TEHIOFKMQPUNEUD\tSXXBMBBOGG\t290.000000000000\t0.000000062047\t2015-03-12T00:01:39.790Z\n" +
                "IFBEPWLUTGRGEIW\tMUTSHRDWCI\t681.700683593750\t0.000765276010\t2015-03-12T00:01:39.910Z\n" +
                "MBJEIRSSGXDDXKD\tUSGCNKKKGI\t0.000001004210\t-1024.000000000000\t2015-03-12T00:01:39.920Z\n" +
                "LXGRVRKVTIFXRLE\tVGYEPKGPSJ\t-860.750000000000\t-210.259765625000\t2015-03-12T00:01:39.930Z\n" +
                "DEPQDCICIZUYOGK\tWVJFWXNHTG\t-227.859375000000\t-236.156250000000\t2015-03-12T00:01:39.940Z\n" +
                "XCHCUBYPMDFJZYJ\tWYCLIXTWVG\t0.000409339424\t-175.941406250000\t2015-03-12T00:01:39.950Z\n" +
                "TPCOITVXSGCPELN\tHTIWQZBCZV\t-420.375000000000\t0.005818145582\t2015-03-12T00:01:39.960Z\n" +
                "QJMRPDXTXOMBKZY\tSMCKFERVGM\t10.221145391464\t512.000000000000\t2015-03-12T00:01:39.970Z\n" +
                "UHHRIRJGLQNMETN\tELYYIFISZU\t375.230987548828\t0.121945030987\t2015-03-12T00:01:39.980Z\n" +
                "SDRLMSHKFZJJECT\tCJCNMPYSJF\t384.000000000000\t0.000000000000\t2015-03-12T00:01:39.990Z\n" +
                "MTLSJOWYLNEDVLL\tIPVOKGQWDX\t2.619887888432\t-477.522460937500\t2015-03-12T00:01:40.000Z\n";

        assertThat(expected, "tab where timestamp > '2015-03-12T00:01:39.630Z' and not(timestamp in ('2015-03-12T00:01:39.800Z','2015-03-12T00:01:39.900Z'))");
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
    public void testDoubleOrderBy() throws Exception {
        createTabWithNaNs2();

        final String expected = "-807.692016601563\tNaN\tNaN\tNaN\n" +
                "-612.000000000000\t72\t108.000000000000\t360\n" +
                "-512.000000000000\t-51\t-1022.000000000000\t-255\n" +
                "-481.765014648438\t-11\t-591.765014648438\t-55\n" +
                "-436.000000000000\t-27\t-706.000000000000\t-135\n" +
                "-338.665039062500\tNaN\tNaN\tNaN\n" +
                "-256.000000000000\t57\t314.000000000000\t285\n" +
                "0.000000136839\t-73\t-729.999999863161\t-365\n" +
                "0.000000343896\t12\t120.000000343896\t60\n" +
                "0.000001200607\t53\t530.000001200607\t265\n" +
                "0.000013659448\t-57\t-569.999986340552\t-285\n" +
                "0.000183005621\tNaN\tNaN\tNaN\n" +
                "0.003575030481\t2\t20.003575030481\t10\n" +
                "0.036795516498\tNaN\tNaN\tNaN\n" +
                "0.059096898884\tNaN\tNaN\tNaN\n" +
                "5.404115438461\t-79\t-784.595884561539\t-395\n" +
                "5.540870904922\t-14\t-134.459129095078\t-70\n" +
                "35.019264221191\t39\t425.019264221191\t195\n" +
                "240.000000000000\tNaN\tNaN\tNaN\n" +
                "384.000000000000\tNaN\tNaN\tNaN\n";

        assertThat(expected, "select x,z,x+(z*10),z*5 from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN) order by x");
    }

    @Test
    public void testDoubleOrderByNaN() throws Exception {
        createTabWithNaNs2();

        final String expected = "-338.665039062500\tNaN\tNaN\tNaN\n" +
                "0.000183005621\tNaN\tNaN\tNaN\n" +
                "-807.692016601563\tNaN\tNaN\tNaN\n" +
                "384.000000000000\tNaN\tNaN\tNaN\n" +
                "240.000000000000\tNaN\tNaN\tNaN\n" +
                "0.059096898884\tNaN\tNaN\tNaN\n" +
                "0.036795516498\tNaN\tNaN\tNaN\n" +
                "5.404115438461\t-79\t-784.595884561539\t-395\n" +
                "0.000000136839\t-73\t-729.999999863161\t-365\n" +
                "0.000013659448\t-57\t-569.999986340552\t-285\n" +
                "-512.000000000000\t-51\t-1022.000000000000\t-255\n" +
                "-436.000000000000\t-27\t-706.000000000000\t-135\n" +
                "5.540870904922\t-14\t-134.459129095078\t-70\n" +
                "-481.765014648438\t-11\t-591.765014648438\t-55\n" +
                "0.003575030481\t2\t20.003575030481\t10\n" +
                "0.000000343896\t12\t120.000000343896\t60\n" +
                "35.019264221191\t39\t425.019264221191\t195\n" +
                "0.000001200607\t53\t530.000001200607\t265\n" +
                "-256.000000000000\t57\t314.000000000000\t285\n" +
                "-612.000000000000\t72\t108.000000000000\t360\n";

        assertThat(expected, "select x,z,x+(z*10),z*5 from tab where id~'KKUSI' and ((z > -100 and z < 100) or z = NaN) order by z");
    }

    @Test
    public void testInAsColumn() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
        }

        final String expected = "BP.L\t0.000000253226\t1022.955993652344\t2015-02-13T02:59:34.000Z\ttrue\n" +
                "GKN.L\t688.000000000000\t256.000000000000\t2015-02-13T02:59:50.000Z\tfalse\n";

        assertThat(expected, "select sym, bid, ask, timestamp, sym in ('BP.L', 'TLW.L', 'ABF.L') from q latest by sym where sym in ('GKN.L', 'BP.L') and ask > 100");
    }

    @Test
    public void testInAsColumnAliased() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
        }

        final String expected = "BP.L\t0.000000253226\t1022.955993652344\t2015-02-13T02:59:34.000Z\ttrue\n" +
                "GKN.L\t688.000000000000\t256.000000000000\t2015-02-13T02:59:50.000Z\tfalse\n";

        assertThat(expected, "select sym, bid, ask, timestamp, sym in ('BP.L', 'TLW.L', 'ABF.L') from q a latest by sym where a.sym in ('GKN.L', 'BP.L') and a.ask > 100");
    }

    @Test
    public void testIntComparison() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $sym("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $int("i1").
                        $int("i2").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 128);

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putSym(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putInt(3, rnd.nextInt() & 63);
                ew.putInt(4, rnd.nextInt() & 63);
                ew.putDate(5, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected1 = "XSPEPTTKIBWFCKD\t290.742401123047\t0.000001862155\t2\t1\n" +
                "XSPEPTTKIBWFCKD\t294.856933593750\t-539.875854492188\t55\t42\n" +
                "XSPEPTTKIBWFCKD\t422.000000000000\t0.000001386965\t58\t54\n";

        assertThat(expected1, "select id,x,y,i1,i2 from tab where i1 >= i2 and x>=y  and x>=i1 and id = 'XSPEPTTKIBWFCKD'");

        final String expected2 = "XSPEPTTKIBWFCKD\t0.007556580706\t-444.759765625000\t14\t20\n" +
                "XSPEPTTKIBWFCKD\t0.002191273379\t-587.421875000000\t1\t33\n" +
                "XSPEPTTKIBWFCKD\t0.000000050401\t-873.569183349609\t34\t41\n" +
                "XSPEPTTKIBWFCKD\t0.000000002468\t-1009.435546875000\t7\t35\n" +
                "XSPEPTTKIBWFCKD\t0.000000134022\t0.000000010189\t38\t44\n" +
                "XSPEPTTKIBWFCKD\t0.053807163611\t0.000000005584\t18\t40\n" +
                "XSPEPTTKIBWFCKD\t307.605468750000\t0.000000032526\t5\t49\n" +
                "XSPEPTTKIBWFCKD\t588.000000000000\t0.000029410815\t42\t62\n" +
                "XSPEPTTKIBWFCKD\t102.474868774414\t-704.000000000000\t61\t63\n" +
                "XSPEPTTKIBWFCKD\t0.000166006441\t-400.250000000000\t23\t49\n" +
                "XSPEPTTKIBWFCKD\t0.006018321496\t0.000000163838\t21\t55\n" +
                "XSPEPTTKIBWFCKD\t82.432384490967\t0.000043923766\t36\t45\n" +
                "XSPEPTTKIBWFCKD\t256.000000000000\t-648.000000000000\t12\t46\n" +
                "XSPEPTTKIBWFCKD\t384.875000000000\t0.000049836333\t23\t46\n" +
                "XSPEPTTKIBWFCKD\t0.000000883287\t-844.890625000000\t4\t24\n" +
                "XSPEPTTKIBWFCKD\t0.103299163282\t0.000076360289\t16\t54\n" +
                "XSPEPTTKIBWFCKD\t0.003647925099\t0.000000019679\t15\t38\n" +
                "XSPEPTTKIBWFCKD\t0.000589750460\t0.000000023659\t4\t39\n";

        assertThat(expected2, "select id,x,y,i1,i2 from tab where i1 < i2 and x>=y  and y<i1 and id = 'XSPEPTTKIBWFCKD'");
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
    public void testIntMultiplication() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $int("x").
                        $int("y").
                        $str("z").
                        $ts()

        )) {

            Rnd rnd = new Rnd();

            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 1000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putInt(0, rnd.nextInt() & 255);
                ew.putInt(1, rnd.nextInt() & 255);
                ew.putStr(2, rnd.nextString(4));
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected1 = "29512\t-129\t119\t248\tCBJF\t2015-03-12T00:00:01.370Z\n" +
                "30906\t49\t202\t153\tCJZJ\t2015-03-12T00:00:07.470Z\n" +
                "2508\t113\t132\t19\tCGCJ\t2015-03-12T00:00:09.070Z\n";

        assertThat(expected1, "select x * y, x - y, x, y, z, timestamp from tab where z ~ '^C.*J+'");
    }

    @Test
    public void testIntervalAndIndexHeapSearch() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24 * 10, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
        }

        final String expected =
                "ADM.L\t837.343750000000\t0.061431560665\t2015-02-12T10:00:04.000Z\n" +
                        "BP.L\t564.425537109375\t0.000000003711\t2015-02-12T10:00:40.000Z\n" +
                        "WTB.L\t646.000000000000\t512.000000000000\t2015-02-12T10:00:55.000Z\n" +
                        "WTB.L\t1024.000000000000\t0.000036626770\t2015-02-12T10:01:02.000Z\n" +
                        "WTB.L\t676.215667724609\t0.000000047206\t2015-02-12T10:01:09.000Z\n" +
                        "BP.L\t768.000000000000\t0.000000011709\t2015-02-12T10:01:18.000Z\n" +
                        "BP.L\t512.000000000000\t74.948242187500\t2015-02-12T10:01:31.000Z\n" +
                        "WTB.L\t784.000000000000\t320.000000000000\t2015-02-12T10:01:36.000Z\n" +
                        "BP.L\t980.000000000000\t133.570312500000\t2015-02-12T10:02:14.000Z\n" +
                        "ADM.L\t768.000000000000\t296.109375000000\t2015-02-12T10:02:35.000Z\n" +
                        "WTB.L\t1024.000000000000\t0.000000055754\t2015-02-12T10:02:38.000Z\n" +
                        "BP.L\t807.750000000000\t705.548904418945\t2015-02-12T10:02:49.000Z\n" +
                        "BP.L\t949.156250000000\t63.068359375000\t2015-02-12T10:02:53.000Z\n" +
                        "WTB.L\t505.468750000000\t1024.000000000000\t2015-02-12T10:03:09.000Z\n" +
                        "ADM.L\t768.000000000000\t0.000047940810\t2015-02-12T10:03:19.000Z\n" +
                        "BP.L\t968.953491210938\t0.029868379235\t2015-02-12T10:03:21.000Z\n" +
                        "ADM.L\t512.000000000000\t0.000000000000\t2015-02-12T10:03:31.000Z\n" +
                        "WTB.L\t1024.000000000000\t0.001693658938\t2015-02-12T10:03:32.000Z\n" +
                        "WTB.L\t642.662750244141\t1008.000000000000\t2015-02-12T10:03:53.000Z\n" +
                        "BP.L\t512.000000000000\t0.000000318310\t2015-02-12T10:03:56.000Z\n" +
                        "BP.L\t788.000000000000\t55.569427490234\t2015-02-12T10:04:02.000Z\n" +
                        "BP.L\t768.000000000000\t924.000000000000\t2015-02-12T10:04:04.000Z\n" +
                        "ADM.L\t718.848632812500\t907.609375000000\t2015-02-12T10:04:13.000Z\n" +
                        "ADM.L\t965.062500000000\t0.000000591804\t2015-02-12T10:04:22.000Z\n" +
                        "ADM.L\t696.000000000000\t9.672361135483\t2015-02-12T10:04:25.000Z\n" +
                        "BP.L\t992.000000000000\t0.750000000000\t2015-02-12T10:04:27.000Z\n" +
                        "WTB.L\t813.869140625000\t0.000338987120\t2015-02-12T10:04:31.000Z\n" +
                        "BP.L\t518.117187500000\t765.889160156250\t2015-02-12T10:04:33.000Z\n" +
                        "WTB.L\t824.867187500000\t350.810546875000\t2015-02-12T10:04:40.000Z\n";

        assertThat(expected, "select sym, bid, ask, timestamp from q where timestamp = '2015-02-12T10:00:00;5m' and sym in ('BP.L','ADM.L', 'WTB.L') and bid > 500");
    }

    @Test
    public void testIntervalAndIndexSearch() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24 * 10, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();

            final String expected = "ADM.L\t837.343750000000\t0.061431560665\t2015-02-12T10:00:04.000Z\n" +
                    "BP.L\t564.425537109375\t0.000000003711\t2015-02-12T10:00:40.000Z\n" +
                    "BP.L\t768.000000000000\t0.000000011709\t2015-02-12T10:01:18.000Z\n" +
                    "BP.L\t512.000000000000\t74.948242187500\t2015-02-12T10:01:31.000Z\n" +
                    "BP.L\t980.000000000000\t133.570312500000\t2015-02-12T10:02:14.000Z\n" +
                    "ADM.L\t768.000000000000\t296.109375000000\t2015-02-12T10:02:35.000Z\n" +
                    "BP.L\t807.750000000000\t705.548904418945\t2015-02-12T10:02:49.000Z\n" +
                    "BP.L\t949.156250000000\t63.068359375000\t2015-02-12T10:02:53.000Z\n" +
                    "ADM.L\t768.000000000000\t0.000047940810\t2015-02-12T10:03:19.000Z\n" +
                    "BP.L\t968.953491210938\t0.029868379235\t2015-02-12T10:03:21.000Z\n" +
                    "ADM.L\t512.000000000000\t0.000000000000\t2015-02-12T10:03:31.000Z\n" +
                    "BP.L\t512.000000000000\t0.000000318310\t2015-02-12T10:03:56.000Z\n" +
                    "BP.L\t788.000000000000\t55.569427490234\t2015-02-12T10:04:02.000Z\n" +
                    "BP.L\t768.000000000000\t924.000000000000\t2015-02-12T10:04:04.000Z\n" +
                    "ADM.L\t718.848632812500\t907.609375000000\t2015-02-12T10:04:13.000Z\n" +
                    "ADM.L\t965.062500000000\t0.000000591804\t2015-02-12T10:04:22.000Z\n" +
                    "ADM.L\t696.000000000000\t9.672361135483\t2015-02-12T10:04:25.000Z\n" +
                    "BP.L\t992.000000000000\t0.750000000000\t2015-02-12T10:04:27.000Z\n" +
                    "BP.L\t518.117187500000\t765.889160156250\t2015-02-12T10:04:33.000Z\n";
            assertThat(expected, "select sym, bid, ask, timestamp from q where timestamp = '2015-02-12T10:00:00;5m' and sym in ('BP.L','ADM.L') and bid > 500");
        }
    }

    @Test
    public void testIntervalIntrinsicFalse() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24 * 10, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
            assertEmpty("select sym, bid, ask, timestamp from q where timestamp = '2015-02-12T10:00:00' and timestamp = '2015-02-12T12:00:00'");
        }
    }

    @Test
    public void testIntrinsicFalseModel() throws Exception {
        createTabWithNaNs2();
        assertEmpty("(select timestamp+1 ts, sum(y) from tab sample by 1d) timestamp(ts) where ts != ts");
    }

    @Test
    public void testIntrinsicGreater() throws Exception {
        createTabWithSymbol();
        final String expected = "YNLERUGEWULQWLN\tCJCNMPYSJF\t439.851211547852\t0.000417203366\t2015-03-12T00:01:39.800Z\n" +
                "OIBUGYXIMDIDNRO\tMRFPKLNWQL\t0.000245904943\t0.049118923023\t2015-03-12T00:01:39.810Z\n" +
                "XXTPBXNCJEDGPLH\tDLQJBVVTTV\t58.250000000000\t0.191959321499\t2015-03-12T00:01:39.820Z\n" +
                "ZNIFDRPHNGTNJJP\tKIWPCXCIGW\t666.264648437500\t0.000283217822\t2015-03-12T00:01:39.830Z\n" +
                "ONIJMVFQFDBOMQB\tEDBVUVMLSX\t0.000000120621\t-694.250000000000\t2015-03-12T00:01:39.840Z\n" +
                "UCSOZSLOQGPOHIM\tOJPOHDOWSK\t29.429825782776\t-13.079589843750\t2015-03-12T00:01:39.850Z\n" +
                "VVOFHXMMEJXJNRD\tVZQSGLLZQG\t-384.000000000000\t0.006880680798\t2015-03-12T00:01:39.860Z\n" +
                "WDDZKIFCBRMKKRH\tMERDPIDQGJ\t387.875000000000\t0.000000007496\t2015-03-12T00:01:39.870Z\n" +
                "UPJFSREKEUNMKWO\tCWOFRMJORR\t642.000000000000\t1.016614258289\t2015-03-12T00:01:39.880Z\n" +
                "TMIWHRQISSCIGXL\tRULHMZROYY\t-736.000000000000\t0.000000238132\t2015-03-12T00:01:39.890Z\n" +
                "WJWZBSMWXQOFNRO\tDZDUVNCYYV\t9.389577388763\t8.453002452850\t2015-03-12T00:01:39.900Z\n";

        assertThat(expected, "tab where timestamp > '2015-03-12T00:01:39.630Z' and timestamp in ('2015-03-12T00:01:39.800Z','2015-03-12T00:01:39.900Z')");
        assertThat(expected, "tab where timestamp >= '2015-03-12T00:01:39.630Z' and timestamp in ('2015-03-12T00:01:39.800Z','2015-03-12T00:01:39.900Z')");
    }

    @Test
    public void testIntrinsicLess() throws Exception {
        createTabWithSymbol();
        final String expected = "YNLERUGEWULQWLN\tCJCNMPYSJF\t439.851211547852\t0.000417203366\t2015-03-12T00:01:39.800Z\n" +
                "OIBUGYXIMDIDNRO\tMRFPKLNWQL\t0.000245904943\t0.049118923023\t2015-03-12T00:01:39.810Z\n" +
                "XXTPBXNCJEDGPLH\tDLQJBVVTTV\t58.250000000000\t0.191959321499\t2015-03-12T00:01:39.820Z\n" +
                "ZNIFDRPHNGTNJJP\tKIWPCXCIGW\t666.264648437500\t0.000283217822\t2015-03-12T00:01:39.830Z\n" +
                "ONIJMVFQFDBOMQB\tEDBVUVMLSX\t0.000000120621\t-694.250000000000\t2015-03-12T00:01:39.840Z\n" +
                "UCSOZSLOQGPOHIM\tOJPOHDOWSK\t29.429825782776\t-13.079589843750\t2015-03-12T00:01:39.850Z\n" +
                "VVOFHXMMEJXJNRD\tVZQSGLLZQG\t-384.000000000000\t0.006880680798\t2015-03-12T00:01:39.860Z\n" +
                "WDDZKIFCBRMKKRH\tMERDPIDQGJ\t387.875000000000\t0.000000007496\t2015-03-12T00:01:39.870Z\n" +
                "UPJFSREKEUNMKWO\tCWOFRMJORR\t642.000000000000\t1.016614258289\t2015-03-12T00:01:39.880Z\n" +
                "TMIWHRQISSCIGXL\tRULHMZROYY\t-736.000000000000\t0.000000238132\t2015-03-12T00:01:39.890Z\n" +
                "WJWZBSMWXQOFNRO\tDZDUVNCYYV\t9.389577388763\t8.453002452850\t2015-03-12T00:01:39.900Z\n";

        assertThat(expected, "tab where timestamp < '2015-03-12T00:01:40.000Z' and timestamp in ('2015-03-12T00:01:39.800Z','2015-03-12T00:01:39.900Z')");
        assertThat(expected, "tab where timestamp <= '2015-03-12T00:01:40.000Z' and timestamp in ('2015-03-12T00:01:39.800Z','2015-03-12T00:01:39.900Z')");
    }

    @Test
    public void testInvalidInterval() throws Exception {
        createTabWithNaNs2();
        try {
            expectFailure("select id, z from (tab where not(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-12T10:00:00;5m;30m;10') where timestamp = '2015-03-12T10:00:00' and timestamp = '2015-03-12T14:00:00'");
        } catch (ParserException e) {
            Assert.assertEquals(74, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidLatestByColumn1() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, bid, ask, timestamp from q latest by symx where sym in ('GKN.L') and ask > 100");
        } catch (ParserException e) {
            Assert.assertEquals(49, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "nvalid column"));
        }
    }

    @Test
    public void testInvalidLatestByColumn2() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, bid, ask, timestamp from q latest by ask where sym in ('GKN.L') and ask > 100");
        } catch (ParserException e) {
            Assert.assertEquals(49, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "symbol, string, int or long column"));
        }
    }

    @Test
    public void testInvalidLatestByColumn3() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, bid, ask, timestamp from q latest by mode where sym in ('GKN.L') and ask > 100");
        } catch (ParserException e) {
            Assert.assertEquals(49, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "not indexed"));
        }
    }

    @Test
    public void testInvalidLiteralColumn() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, bid, ask, timestamp1 from q latest by sym where sym in ('GKN.L') and ask > 100");
        } catch (ParserException e) {
            Assert.assertEquals(22, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "nvalid column"));
        }
    }

    @Test
    public void testInvalidTimestamp() throws Exception {
        createTabWithNaNs2();
        try {
            expectFailure("(select timestamp+1 ts, sum(y) from tab sample by 1d) timestamp(ts1) where ts != ts");
        } catch (ParserException e) {
            Assert.assertEquals(64, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidVirtualColumn() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, (bid+ask2)/2, timestamp from q latest by sym where sym in ('GKN.L') and ask > 100");
        } catch (ParserException e) {
            Assert.assertEquals(17, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidWhereColumn1() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, bid, ask, timestamp from q where sym2 in ('GKN.L') and ask > 100");
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidWhereColumn2() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select sym, bid, ask, timestamp from q where sym in ('GKN.L') and ask2 > 100");
        } catch (ParserException e) {
            Assert.assertEquals(66, QueryError.getPosition());
        }
    }

    @Test
    public void testJournalDoesNotExist() {
        try {
            expectFailure("select id, x, y, timestamp from q where id = ");
        } catch (ParserException e) {
            Assert.assertEquals(32, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "does not exist"));
        }
    }

    @Test
    public void testJournalRefresh() throws Exception {
        createTabWithNaNs();
        assertThat("10000\n", "select count() from tab");
        try (JournalWriter w = getFactory().writer("tab")) {
            w.setSequentialAccess(true);
            appendNaNs(w, DateFormatUtils.parseDateTime("2015-10-12T00:00:00.000Z"));
        }
        assertThat("20000\n", "select count() from tab");
    }

    @Test
    public void testLatestByStr() throws Exception {

        createIndexedTab();

        final String expected = "BVUDTGKDFEPWZYM\t0.000039040189\t-1024.000000000000\t2015-03-12T00:01:04.890Z\n" +
                "COPMLLOUWWZXQEL\t0.000000431389\t0.046957752667\t2015-03-12T00:01:10.010Z\n";

        assertThat(expected, "select id, x, y, timestamp from tab latest by id where id in ('COPMLLOUWWZXQEL', 'BVUDTGKDFEPWZYM')");
    }

    @Test
    public void testLatestByStrFilterOnSym() throws Exception {
        createTabWithSymbol();
        final String expected = "TEHIOFKMQPUNEUD\tMRFPKLNWQL\t0.020352731459\t0.165701057762\t2015-03-12T00:01:22.640Z\n";
        assertThat(expected, "select id, sym, x, y, timestamp from tab latest by id where id = 'TEHIOFKMQPUNEUD' and sym in ('MRFPKLNWQL')");
        assertThat(expected, "select id, sym, x, y, timestamp from tab latest by id where id = 'TEHIOFKMQPUNEUD' and sym = ('MRFPKLNWQL')");
        assertThat(expected, "select id, sym, x, y, timestamp from tab latest by id where id = 'TEHIOFKMQPUNEUD' and 'MRFPKLNWQL' = sym");
    }

    @Test
    public void testLatestByStrIrrelevantFilter() throws Exception {
        createIndexedTab();
        try {
            expectFailure("select id, x, y, timestamp from tab latest by id where x > y");
        } catch (ParserException e) {
            Assert.assertEquals(46, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "column expected"));
        }
    }

    @Test
    public void testLatestByStrNoFilter() throws Exception {
        createIndexedTab();
        try {
            expectFailure("select id, x, y, timestamp from tab latest by id");
        } catch (ParserException e) {
            Assert.assertEquals(46, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Only SYM columns"));
        }
    }

    @Test
    public void testLatestBySym() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
        }

        final String expected = "TLW.L\t0.000000000000\t0.000000048727\t2015-02-13T02:58:41.000Z\n" +
                "ADM.L\t0.000000106175\t0.102090202272\t2015-02-13T02:58:59.000Z\n" +
                "ABF.L\t0.000701488039\t382.432617187500\t2015-02-13T02:59:25.000Z\n" +
                "RRS.L\t161.155059814453\t809.607971191406\t2015-02-13T02:59:26.000Z\n" +
                "BP.L\t0.000003149229\t0.000005004517\t2015-02-13T02:59:40.000Z\n" +
                "GKN.L\t0.101824980229\t1024.000000000000\t2015-02-13T02:59:48.000Z\n" +
                "AGK.L\t0.905496925116\t72.000000000000\t2015-02-13T02:59:53.000Z\n" +
                "WTB.L\t0.006673692260\t348.000000000000\t2015-02-13T02:59:57.000Z\n" +
                "BT-A.L\t0.000000500809\t0.000879329862\t2015-02-13T02:59:58.000Z\n" +
                "LLOY.L\t0.000000328173\t288.000000000000\t2015-02-13T02:59:59.000Z\n";

        assertThat(expected, "select sym, bid, ask, timestamp from q latest by sym where bid < ask");
    }

    @Test
    public void testLatestBySymList() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
        }

        final String expected = "BP.L\t0.000000253226\t1022.955993652344\t2015-02-13T02:59:34.000Z\n" +
                "GKN.L\t688.000000000000\t256.000000000000\t2015-02-13T02:59:50.000Z\n";
        assertThat(expected, "select sym, bid, ask, timestamp from q latest by sym where sym in ('GKN.L', 'BP.L') and ask > 100");
    }

    @Test
    public void testLatestBySymNoFilter() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 3600 * 24, DateFormatUtils.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
            w.commit();
        }

        final String expected = "RRS.L\t946.798400878906\t0.012493257411\t2015-02-13T02:59:43.000Z\n" +
                "ABF.L\t800.000000000000\t0.000625604152\t2015-02-13T02:59:46.000Z\n" +
                "GKN.L\t688.000000000000\t256.000000000000\t2015-02-13T02:59:50.000Z\n" +
                "AGK.L\t0.905496925116\t72.000000000000\t2015-02-13T02:59:53.000Z\n" +
                "ADM.L\t62.812500000000\t1.050346195698\t2015-02-13T02:59:54.000Z\n" +
                "TLW.L\t584.000000000000\t0.004887015559\t2015-02-13T02:59:55.000Z\n" +
                "BP.L\t512.000000000000\t0.000000007648\t2015-02-13T02:59:56.000Z\n" +
                "WTB.L\t0.006673692260\t348.000000000000\t2015-02-13T02:59:57.000Z\n" +
                "BT-A.L\t0.000000500809\t0.000879329862\t2015-02-13T02:59:58.000Z\n" +
                "LLOY.L\t0.000000328173\t288.000000000000\t2015-02-13T02:59:59.000Z\n";

        assertThat(expected, "select sym, bid, ask, timestamp from q latest by sym");
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
    public void testLongIndexSearch() throws Exception {
        createTabWithNaNs3();

        assertThat("z\tw\ta\tb\n" +
                        "414\t-436\t0.000080571304\t-565.593750000000\n" +
                        "414\t-393\t352.000000000000\t-800.000000000000\n" +
                        "414\t136\t0.000316714002\t-742.543395996094\n" +
                        "414\t486\t873.480468750000\t0.000000064994\n" +
                        "414\t454\t-760.938842773438\t736.375000000000\n" +
                        "414\t-335\t0.000000009145\tNaN\n" +
                        "414\t126\t8.567796945572\t864.000000000000\n" +
                        "414\tNaN\t905.000000000000\t578.125000000000\n",
                "select z, w, x a, y b from tab where z = 414", true);

        assertPlan("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"KvIndexIntLookupRowSource\",\"column\":\"z\"\"value\":414}}}",
                "select z, w, x a, y b from tab where z = 414");
    }

    @Test
    public void testLongIndexSearch2() throws Exception {
        createTabWithNaNs3();

        assertThat("z\tw\ta\tb\n" +
                        "414\t-436\t0.000080571304\t-565.593750000000\n" +
                        "414\t-393\t352.000000000000\t-800.000000000000\n" +
                        "414\t136\t0.000316714002\t-742.543395996094\n" +
                        "414\t486\t873.480468750000\t0.000000064994\n" +
                        "212\t257\t0.000120815996\t-789.000000000000\n" +
                        "212\tNaN\t896.000000000000\t0.000287486248\n" +
                        "212\t-422\t0.000008381868\t928.000000000000\n" +
                        "212\tNaN\t0.408159479499\t-332.593750000000\n" +
                        "212\t-97\t0.000004313318\t1.753653943539\n" +
                        "212\t-112\t59.650172233582\t12.106574058533\n" +
                        "212\t-42\t798.500000000000\t-972.920440673828\n" +
                        "414\t454\t-760.938842773438\t736.375000000000\n" +
                        "212\t-495\t251.654296875000\t384.000000000000\n" +
                        "414\t-335\t0.000000009145\tNaN\n" +
                        "212\t252\t-520.000000000000\t182.464950561523\n" +
                        "414\t126\t8.567796945572\t864.000000000000\n" +
                        "212\t111\t0.000000037987\t256.000000000000\n" +
                        "212\t-428\t0.357973538339\t0.000000019578\n" +
                        "414\tNaN\t905.000000000000\t578.125000000000\n",
                "select z, w, x a, y b from tab where z in (414,212) ", true);

        assertPlan("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"KvIndexIntLookupRowSource\",\"column\":\"z\"\"value\":414}}}",
                "select z, w, x a, y b from tab where z = 414");
    }

    @Test
    public void testLongIndexSearch3() throws Exception {
        createTabWithNaNs3();
        assertThat("z\tw\ta\tb\n" +
                        "212\t-428\t0.357973538339\t0.000000019578\n" +
                        "414\tNaN\t905.000000000000\t578.125000000000\n",
                "select z, w, x a, y b from tab latest by z where z in (414,212) ", true);
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
    public void testMissingColumnInSelect() throws Exception {
        createTabWithNaNs2();
        try {
            expectFailure("select id, , z from (tab where z = NaN) where id = 'KKUSIMYDXUUSKCX'");
        } catch (ParserException e) {
            Assert.assertEquals(11, QueryError.getPosition());
        }
    }

    @Test
    public void testMissingEqualsArgument() throws Exception {
        getFactory().writer(Quote.class, "q").close();
        try {
            expectFailure("select id, x, y, timestamp from q where id = ");
        } catch (ParserException e) {
            Assert.assertEquals(43, QueryError.getPosition());
        }
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
    public void testMultipleStrIdSearch() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(32).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 1024);

            int mask = 1023;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");


            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "UHUTMTRRNGCIPFZ\t0.000006506322\t-261.000000000000\t2015-03-12T00:00:00.220Z\n" +
                "FZICFOQEVPXJYQR\t0.000000166602\t367.625000000000\t2015-03-12T00:00:00.260Z\n" +
                "FZICFOQEVPXJYQR\t57.308933258057\t28.255742073059\t2015-03-12T00:00:09.750Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000005319798\t-727.000000000000\t2015-03-12T00:00:10.060Z\n" +
                "FZICFOQEVPXJYQR\t-432.500000000000\t0.013725134078\t2015-03-12T00:00:13.470Z\n" +
                "FZICFOQEVPXJYQR\t-247.761962890625\t768.000000000000\t2015-03-12T00:00:15.170Z\n" +
                "UHUTMTRRNGCIPFZ\t438.929687500000\t0.000031495110\t2015-03-12T00:00:18.300Z\n" +
                "FZICFOQEVPXJYQR\t264.789741516113\t0.033011944033\t2015-03-12T00:00:19.630Z\n" +
                "FZICFOQEVPXJYQR\t6.671853065491\t1.936547994614\t2015-03-12T00:00:20.620Z\n" +
                "UHUTMTRRNGCIPFZ\t864.000000000000\t-1024.000000000000\t2015-03-12T00:00:25.970Z\n" +
                "UHUTMTRRNGCIPFZ\t0.002082723950\t0.000000001586\t2015-03-12T00:00:26.760Z\n" +
                "UHUTMTRRNGCIPFZ\t-976.561523437500\t0.446909941733\t2015-03-12T00:00:29.530Z\n" +
                "UHUTMTRRNGCIPFZ\t0.001273257891\t1.239676237106\t2015-03-12T00:00:31.270Z\n" +
                "UHUTMTRRNGCIPFZ\t-287.234375000000\t236.000000000000\t2015-03-12T00:00:33.720Z\n" +
                "FZICFOQEVPXJYQR\t1.589631736279\t128.217994689941\t2015-03-12T00:00:34.580Z\n" +
                "UHUTMTRRNGCIPFZ\t32.605212211609\t0.000000182797\t2015-03-12T00:00:35.120Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000029479873\t11.629675865173\t2015-03-12T00:00:35.710Z\n" +
                "UHUTMTRRNGCIPFZ\t269.668342590332\t0.000553555525\t2015-03-12T00:00:35.990Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000461809614\t64.250000000000\t2015-03-12T00:00:37.140Z\n" +
                "FZICFOQEVPXJYQR\t-572.296875000000\t0.000020149632\t2015-03-12T00:00:37.190Z\n" +
                "UHUTMTRRNGCIPFZ\t512.000000000000\t49.569551467896\t2015-03-12T00:00:40.250Z\n" +
                "FZICFOQEVPXJYQR\t0.000005206652\t0.272554814816\t2015-03-12T00:00:49.770Z\n" +
                "FZICFOQEVPXJYQR\t0.001125814480\t0.105613868684\t2015-03-12T00:01:06.100Z\n" +
                "UHUTMTRRNGCIPFZ\t704.000000000000\t44.546960830688\t2015-03-12T00:01:06.420Z\n" +
                "UHUTMTRRNGCIPFZ\t258.500000000000\t0.263136833906\t2015-03-12T00:01:07.450Z\n" +
                "FZICFOQEVPXJYQR\t192.000000000000\t-380.804687500000\t2015-03-12T00:01:08.610Z\n" +
                "FZICFOQEVPXJYQR\t56.567952156067\t0.086345635355\t2015-03-12T00:01:13.980Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000097790253\t0.000000006182\t2015-03-12T00:01:17.060Z\n" +
                "FZICFOQEVPXJYQR\t128.000000000000\t469.091918945313\t2015-03-12T00:01:19.730Z\n" +
                "FZICFOQEVPXJYQR\t-592.000000000000\t0.000000797945\t2015-03-12T00:01:20.410Z\n" +
                "FZICFOQEVPXJYQR\t519.500000000000\t0.049629654735\t2015-03-12T00:01:22.360Z\n" +
                "FZICFOQEVPXJYQR\t24.736416816711\t92.901168823242\t2015-03-12T00:01:22.830Z\n" +
                "FZICFOQEVPXJYQR\t336.000000000000\t0.000000089523\t2015-03-12T00:01:26.920Z\n" +
                "FZICFOQEVPXJYQR\t0.044912695885\t64.000000000000\t2015-03-12T00:01:37.820Z\n";

        assertThat(expected, "select id, x, y, timestamp from tab where id in ('FZICFOQEVPXJYQR', 'UHUTMTRRNGCIPFZ')");
    }

    @Test
    public void testMultipleStrIdSearchUsingHeapMerge() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(32).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 1024);

            int mask = 1023;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");


            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected =
                "UHUTMTRRNGCIPFZ\t0.000006506322\t-261.000000000000\t2015-03-12T00:00:00.220Z\n" +
                        "FZICFOQEVPXJYQR\t0.000000166602\t367.625000000000\t2015-03-12T00:00:00.260Z\n" +
                        "KJSMSSUQSRLTKVV\t0.000000000000\t696.000000000000\t2015-03-12T00:00:01.200Z\n" +
                        "FZICFOQEVPXJYQR\t57.308933258057\t28.255742073059\t2015-03-12T00:00:09.750Z\n" +
                        "UHUTMTRRNGCIPFZ\t0.000005319798\t-727.000000000000\t2015-03-12T00:00:10.060Z\n" +
                        "KJSMSSUQSRLTKVV\t-512.000000000000\t12.906219482422\t2015-03-12T00:00:11.190Z\n" +
                        "FZICFOQEVPXJYQR\t-432.500000000000\t0.013725134078\t2015-03-12T00:00:13.470Z\n" +
                        "FZICFOQEVPXJYQR\t-247.761962890625\t768.000000000000\t2015-03-12T00:00:15.170Z\n" +
                        "UHUTMTRRNGCIPFZ\t438.929687500000\t0.000031495110\t2015-03-12T00:00:18.300Z\n" +
                        "FZICFOQEVPXJYQR\t264.789741516113\t0.033011944033\t2015-03-12T00:00:19.630Z\n" +
                        "FZICFOQEVPXJYQR\t6.671853065491\t1.936547994614\t2015-03-12T00:00:20.620Z\n" +
                        "KJSMSSUQSRLTKVV\t664.132812500000\t512.000000000000\t2015-03-12T00:00:25.960Z\n" +
                        "UHUTMTRRNGCIPFZ\t864.000000000000\t-1024.000000000000\t2015-03-12T00:00:25.970Z\n" +
                        "UHUTMTRRNGCIPFZ\t0.002082723950\t0.000000001586\t2015-03-12T00:00:26.760Z\n" +
                        "KJSMSSUQSRLTKVV\t0.000000078358\t-1024.000000000000\t2015-03-12T00:00:27.350Z\n" +
                        "UHUTMTRRNGCIPFZ\t-976.561523437500\t0.446909941733\t2015-03-12T00:00:29.530Z\n" +
                        "KJSMSSUQSRLTKVV\t192.000000000000\t984.000000000000\t2015-03-12T00:00:30.260Z\n" +
                        "UHUTMTRRNGCIPFZ\t0.001273257891\t1.239676237106\t2015-03-12T00:00:31.270Z\n" +
                        "UHUTMTRRNGCIPFZ\t-287.234375000000\t236.000000000000\t2015-03-12T00:00:33.720Z\n" +
                        "FZICFOQEVPXJYQR\t1.589631736279\t128.217994689941\t2015-03-12T00:00:34.580Z\n" +
                        "UHUTMTRRNGCIPFZ\t32.605212211609\t0.000000182797\t2015-03-12T00:00:35.120Z\n" +
                        "UHUTMTRRNGCIPFZ\t0.000029479873\t11.629675865173\t2015-03-12T00:00:35.710Z\n" +
                        "UHUTMTRRNGCIPFZ\t269.668342590332\t0.000553555525\t2015-03-12T00:00:35.990Z\n" +
                        "UHUTMTRRNGCIPFZ\t0.000461809614\t64.250000000000\t2015-03-12T00:00:37.140Z\n" +
                        "FZICFOQEVPXJYQR\t-572.296875000000\t0.000020149632\t2015-03-12T00:00:37.190Z\n" +
                        "UHUTMTRRNGCIPFZ\t512.000000000000\t49.569551467896\t2015-03-12T00:00:40.250Z\n" +
                        "FZICFOQEVPXJYQR\t0.000005206652\t0.272554814816\t2015-03-12T00:00:49.770Z\n" +
                        "FZICFOQEVPXJYQR\t0.001125814480\t0.105613868684\t2015-03-12T00:01:06.100Z\n" +
                        "UHUTMTRRNGCIPFZ\t704.000000000000\t44.546960830688\t2015-03-12T00:01:06.420Z\n" +
                        "UHUTMTRRNGCIPFZ\t258.500000000000\t0.263136833906\t2015-03-12T00:01:07.450Z\n" +
                        "FZICFOQEVPXJYQR\t192.000000000000\t-380.804687500000\t2015-03-12T00:01:08.610Z\n" +
                        "FZICFOQEVPXJYQR\t56.567952156067\t0.086345635355\t2015-03-12T00:01:13.980Z\n" +
                        "KJSMSSUQSRLTKVV\t595.603515625000\t0.000000033307\t2015-03-12T00:01:15.060Z\n" +
                        "UHUTMTRRNGCIPFZ\t0.000097790253\t0.000000006182\t2015-03-12T00:01:17.060Z\n" +
                        "FZICFOQEVPXJYQR\t128.000000000000\t469.091918945313\t2015-03-12T00:01:19.730Z\n" +
                        "FZICFOQEVPXJYQR\t-592.000000000000\t0.000000797945\t2015-03-12T00:01:20.410Z\n" +
                        "FZICFOQEVPXJYQR\t519.500000000000\t0.049629654735\t2015-03-12T00:01:22.360Z\n" +
                        "FZICFOQEVPXJYQR\t24.736416816711\t92.901168823242\t2015-03-12T00:01:22.830Z\n" +
                        "FZICFOQEVPXJYQR\t336.000000000000\t0.000000089523\t2015-03-12T00:01:26.920Z\n" +
                        "KJSMSSUQSRLTKVV\t0.091930281371\t482.941406250000\t2015-03-12T00:01:30.760Z\n" +
                        "KJSMSSUQSRLTKVV\t539.789093017578\t396.667968750000\t2015-03-12T00:01:35.470Z\n" +
                        "FZICFOQEVPXJYQR\t0.044912695885\t64.000000000000\t2015-03-12T00:01:37.820Z\n" +
                        "KJSMSSUQSRLTKVV\t10.140126943588\t0.000004704022\t2015-03-12T00:01:38.600Z\n";

        assertThat(expected, "select id, x, y, timestamp from tab where id in ('FZICFOQEVPXJYQR', 'UHUTMTRRNGCIPFZ', 'KJSMSSUQSRLTKVV')");
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

        assertThat(expected, "select id,w,x,z,x + -w, z+-w from tab where id = 'FYXPVKNCBWLNLRH'");
    }

    @Test
    public void testNewLine() throws Exception {
        createTabWithNaNs2();
        final String expected = "YDVRVNGSTEQODRZ\t-99\n";
        assertThat(expected, "select id, z from (tab where not (id in 'GMPLUCFTLNKYTSZ')) \n limit 1");
    }

    @Test
    public void testNewLine2() throws Exception {
        createTabWithNaNs2();
        final String expected = "YDVRVNGSTEQODRZ\t-513.075195312500\tNaN\t-99\t7\t2015-03-12T01:00:00.000Z\n";
        assertThat(expected, "tab where not(id in 'GMPLUCFTLNKYTSZ') \n limit 1");
    }

    @Test
    public void testNewLine3() throws Exception {
        createTabWithNaNs2();
        final String expected = "1\t2\n";
        assertThat(expected, "select 1 col, 2 col2 from tab\nlimit 1");
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
    public void testOrArgCheck() {
        // missing left arg on 'and'
        try {
            expectFailure("select id,w,x,z,x + -w, z+-w from tab where or id = 'FYXPVKNCBWLNLRH'");
        } catch (ParserException e) {
            Assert.assertEquals(44, QueryError.getPosition());
        }
    }

    @Test
    public void testOrArgCheck2() {
        // missing left arg on 'and'
        try {
            expectFailure("select id,w,x,z,x + -w, z+-w from tab where id = 'FYXPVKNCBWLNLRH' or");
        } catch (ParserException e) {
            Assert.assertEquals(67, QueryError.getPosition());
        }
    }

    @Test
    public void testOrderByReplace() throws Exception {
        tabOfDates();
        assertThat("x\ttimestamp\n" +
                        "2016, Oct 10\t2016-10-10T00:00:00.000Z\n" +
                        "2016, Oct 11\t2016-10-11T00:00:00.000Z\n" +
                        "2016, Oct 12\t2016-10-12T00:00:00.000Z\n" +
                        "2016, Oct 13\t2016-10-13T00:00:00.000Z\n" +
                        "2016, Oct 14\t2016-10-14T00:00:00.000Z\n" +
                        "2016, Oct 15\t2016-10-15T00:00:00.000Z\n" +
                        "2016, Oct 16\t2016-10-16T00:00:00.000Z\n" +
                        "2016, Oct 17\t2016-10-17T00:00:00.000Z\n" +
                        "2016, Oct 18\t2016-10-18T00:00:00.000Z\n" +
                        "2016, Oct 19\t2016-10-19T00:00:00.000Z\n",
                "select replace('(.+) (.+)$', '$2, $1', dtoa4(timestamp)) x, timestamp from tab order by x asc limit 10", true);
    }

    @Test
    public void testParamInLimit() throws Exception {
        createTabWithNaNs2();

        final String expected = "YDVRVNGSTEQODRZ\t-99\n" +
                "RIIYMHOWKCDNZNL\t-397\n" +
                "XZOUICWEKGHVUVS\t367\n" +
                "FDTNPHFLPBNHGZW\t356\n" +
                "MQMUDDCIHCNPUGJ\t304\n" +
                "HYBTVZNCLNXFSUW\t-276\n" +
                "UMKUBKXPMSXQSTV\t-100\n" +
                "KJSMSSUQSRLTKVV\t345\n" +
                "HOLNVTIQBZXIOVI\t112\n" +
                "ZSFXUNYQXTGNJJI\t-162\n";

        sink.clear();
        try (RecordSource src = compile("select id, z from tab limit :xyz")) {
            src.getParam(":xyz").set(10L);
            printer.print(src, getFactory(), false);
            TestUtils.assertEquals(expected, sink);
        }

        // and one more time
        sink.clear();
        try (RecordSource src = compile("select id, z from tab limit :xyz")) {
            src.getParam(":xyz").set(10L);
            printer.print(src, getFactory(), false);
            TestUtils.assertEquals(expected, sink);
        }

        // and now change parameter
        sink.clear();
        try (RecordSource src = compile("select id, z from tab limit :xyz")) {
            src.getParam(":xyz").set(5L);
            printer.print(src, getFactory(), false);

            final String expected2 = "YDVRVNGSTEQODRZ\t-99\n" +
                    "RIIYMHOWKCDNZNL\t-397\n" +
                    "XZOUICWEKGHVUVS\t367\n" +
                    "FDTNPHFLPBNHGZW\t356\n" +
                    "MQMUDDCIHCNPUGJ\t304\n";
            TestUtils.assertEquals(expected2, sink);
        }
    }

    @Test
    public void testParamInQuery() throws Exception {
        createTabWithNaNs2();

        final String expected = "NDESHYUMEUKVZIE\t485\n" +
                "LLEYMIWTCWLFORG\t456\n" +
                "EOCVFFKMEKPFOYM\t481\n" +
                "NZVDJIGSYLXGYTE\t489\n" +
                "KIWIHBROKZKUTIQ\t498\n" +
                "IWEODDBHEVGXYHJ\t463\n" +
                "WCCNGTNLEGPUHHI\t452\n" +
                "EENNEBQQEMXDKXE\t492\n" +
                "BSQCNSFFLTRYZUZ\t494\n" +
                "QBUYZVQQHSQSPZP\t452\n";

        sink.clear();
        try (RecordSource src = compile("select id, z from tab where z > :min limit :lim")) {
            src.getParam(":min").set(450);
            src.getParam(":lim").set(10L);
            printer.print(src, getFactory(), false);
        }

        sink.clear();
        try (RecordSource src = compile("select id, z from tab where :min < z limit :lim")) {
            src.getParam(":min").set(450);
            src.getParam(":lim").set(10L);
            printer.print(src, getFactory(), false);
            TestUtils.assertEquals(expected, sink);
        }
    }

    @Test(expected = JournalRuntimeException.class)
    public void testParamNotFound() throws Exception {
        createTabWithNaNs2();
        try (RecordSource src = compile("select id, z from tab where z > :min limit :lim")) {
            src.getParam(":xyz");
        }
    }

    @Test(expected = UndefinedParameterException.class)
    public void testParamNotSet() throws Exception {
        createTabWithNaNs2();
        sink.clear();
        try (RecordSource src = compile("select id, z from tab where z > :min limit :lim")) {
            printer.print(src, getFactory(), false);
        }
    }

    @Test
    public void testPluck() throws Exception {
        createTabWithNaNs();
        assertThat("BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n" +
                        "BROMNXKUIZULIGY\tM\n",
                "select id, pluck('(?<=^.{3})(.)',id) from tab where z >= 250 and id ~ 'ULIGY'");
    }

    @Test
    public void testRegexNull() throws Exception {
        createTabWithNullsAndTime();
        assertThat("IBBTGPGWFFYUDEYYQEHBHF\t19:36\t2015-03-12T00:00:00.000Z\n", "tab where id ~ 'BT'");
    }

    @Test
    public void testRegexSyntaxError() throws Exception {
        createTabWithNaNs2();
        try {
            expectFailure("select id, w from tab where id ~ 'SQS)'");
        } catch (ParserException e) {
            Assert.assertEquals(37, QueryError.getPosition());
        }
    }

    @Test
    public void testReplace1() throws Exception {
        createTabWithNaNs();
        assertThat("col0\n" +
                        "10 03 2015 x\n" +
                        "10 03 2015 x\n",
                "select replace('(.+)-(.+)-(.+)$', '$3 $2 $1 x', '2015-03-10') from tab limit 2", true);
    }

    @Test
    public void testReplace2() throws Exception {
        createTabWithNaNs();
        assertThat("col0\n" +
                        "10 03 2015\n" +
                        "10 03 2015\n",
                "select replace('(.+)-(.+)-(.+)$', '$3 $2 $1', '2015-03-10') from tab limit 2", true);


    }

    @Test
    public void testReplace3() throws Exception {
        createTabWithNaNs();
        assertThat("col0\n" +
                        "10 03 \n" +
                        "10 03 \n",
                "select replace('(.+)-(.+)-(.+)$', '$3 $2 $10', '2015-03-10') from tab limit 2", true);
    }

    @Test
    public void testReplaceMissingIndexInside() throws Exception {
        createTabWithNaNs();
        try {
            expectFailure("select replace('(.+)-(.+)-(.+)$', '$3 $ $1', '2015-03-10') from tab limit 2");
        } catch (ParserException e) {
            Assert.assertEquals(38, QueryError.getPosition());
        }
    }

    @Test
    public void testReplaceMissingIndexOnEdge() throws Exception {
        createTabWithNaNs();
        try {
            expectFailure("select replace('(.+)-(.+)-(.+)$', '$3 $2 $', '2015-03-10') from tab limit 2");
        } catch (ParserException e) {
            Assert.assertEquals(41, QueryError.getPosition());
        }
    }

    @Test
    public void testReplaceNomatch() throws Exception {
        tabOfDates();
        assertThat("col0\n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n" +
                        ", \n",
                "select replace('(.+)-(.+)$', '$2, $1', dtoa4(timestamp)) from tab limit 10", true);
    }

    @Test
    public void testReplaceNullPattern() throws Exception {
        createTabWithNaNs();
        try {
            expectFailure("select replace('x', null, '2015-03-10') from tab limit 2");
        } catch (ParserException e) {
            Assert.assertEquals(20, QueryError.getPosition());
        }
    }

    @Test
    public void testReplaceNullRegex() throws Exception {
        createTabWithNaNs();
        try {
            expectFailure("select replace(null, '$3 $ $1', '2015-03-10') from tab limit 2");
        } catch (ParserException e) {
            Assert.assertEquals(15, QueryError.getPosition());
        }
    }

    @Test
    public void testReplaceOnlyGroup() throws Exception {
        tabOfDates();
        assertThat("col0\n" +
                        "8\n" +
                        "9\n" +
                        "10\n" +
                        "11\n" +
                        "12\n" +
                        "13\n" +
                        "14\n" +
                        "15\n" +
                        "16\n" +
                        "17\n",
                "select replace(' ([0-9]+) ', '$1', dtoa4(timestamp)) from tab limit 10", true);
    }

    @Test
    public void testReplaceSimple() throws Exception {
        tabOfDates();
        assertThat("col0\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n" +
                        "x2016\n",
                "select replace('(.+) ', 'x', dtoa4(timestamp)) from tab limit 10", true);
    }

    @Test
    public void testReplaceSimpleNoFound() throws Exception {
        tabOfDates();
        assertThat("col0\n" +
                        "Oct 8 2016\n" +
                        "Oct 9 2016\n" +
                        "Oct 10 2016\n" +
                        "Oct 11 2016\n" +
                        "Oct 12 2016\n" +
                        "Oct 13 2016\n" +
                        "Oct 14 2016\n" +
                        "Oct 15 2016\n" +
                        "Oct 16 2016\n" +
                        "Oct 17 2016\n",
                "select replace('2017', 'x', dtoa4(timestamp)) from tab limit 10", true);
    }

    @Test
    public void testReplaceSimpleWithEmpty() throws Exception {
        tabOfDates();
        assertThat("col0\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n" +
                        "2016\n",
                "select replace('(.+) ', '', dtoa4(timestamp)) from tab limit 10", true);
    }

    @Test
    public void testReplaceVar() throws Exception {
        tabOfDates();
        assertThat("col0\n" +
                        "2016, Oct 8\n" +
                        "2016, Oct 9\n" +
                        "2016, Oct 10\n" +
                        "2016, Oct 11\n" +
                        "2016, Oct 12\n" +
                        "2016, Oct 13\n" +
                        "2016, Oct 14\n" +
                        "2016, Oct 15\n" +
                        "2016, Oct 16\n" +
                        "2016, Oct 17\n",
                "select replace('(.+) (.+)$', '$2, $1', dtoa4(timestamp)) from tab limit 10", true);
    }

    @Test
    public void testScaledDoubleComparison() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $sym("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $int("i1").
                        $int("i2").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 128);

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putSym(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putInt(3, rnd.nextInt() & 63);
                ew.putInt(4, rnd.nextInt() & 63);
                ew.putDate(5, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "YYVSYYEQBORDTQH\t0.000000012344\t0.000000017585\n" +
                "OXPKRGIIHYHBOQM\t0.000000042571\t0.000000046094\n" +
                "FUUTOMFUIOXLQLU\t0.000000009395\t0.000000017129\n" +
                "SCJOUOUIGENFELW\t0.000000000000\t0.000000003106\n" +
                "KIWIHBROKZKUTIQ\t0.000000001343\t0.000000006899\n" +
                "KBBQFNPOYNNCTFS\t0.000000001643\t0.000000010727\n" +
                "CIWXCYXGDHUWEPV\t0.000000000000\t0.000000007289\n" +
                "KFIJZZYNPPBXBHV\t0.000000007000\t0.000000009292\n" +
                "HBXOWVYUVVRDPCH\t0.000000000000\t0.000000005734\n" +
                "XWCKYLSUWDSWUGS\t0.000000010381\t0.000000020362\n" +
                "KJSMSSUQSRLTKVV\t0.000000003479\t0.000000006929\n" +
                "CNGZTOYTOXRSFPV\t0.000000000000\t0.000000000000\n" +
                "NMUREIJUHCLQCMZ\t0.000000001398\t0.000000004552\n" +
                "KJSMSSUQSRLTKVV\t0.000000004057\t0.000000009300\n" +
                "YSSMPGLUOHNZHZS\t0.000000003461\t0.000000005232\n" +
                "TRDLVSYLMSRHGKR\t0.000000001688\t0.000000005004\n" +
                "EOCVFFKMEKPFOYM\t0.000000002913\t0.000000000653\n" +
                "JUEBWVLOMPBETTT\t0.000000000000\t0.000000001650\n" +
                "VQEBNDCQCEHNOMV\t0.000000017353\t0.000000020155\n" +
                "JUEBWVLOMPBETTT\t0.000000120419\t0.000000111959\n" +
                "EIWFOQKYHQQUWQO\t0.000000080895\t0.000000081903\n" +
                "EVMLKCJBEVLUHLI\t0.000000005365\t0.000000003773\n" +
                "NZVDJIGSYLXGYTE\t0.000000022596\t0.000000017758\n" +
                "EOCVFFKMEKPFOYM\t0.000000011711\t0.000000006505\n" +
                "STYSWHLSWPFHXDB\t512.000000000000\t512.000000000000\n" +
                "IWEODDBHEVGXYHJ\t0.000000000773\t0.000000009342\n" +
                "KIWIHBROKZKUTIQ\t128.000000000000\t128.000000000000\n" +
                "VQEBNDCQCEHNOMV\t0.000000003251\t0.000000000000\n" +
                "BSQCNSFFLTRYZUZ\t-1024.000000000000\t-1024.000000000000\n" +
                "OPJEUKWMDNZZBBU\t-1024.000000000000\t-1024.000000000000\n" +
                "DOTSEDYYCTGQOLY\t0.000000004748\t0.000000004680\n" +
                "CMONRCXNUZFNWHF\t0.000000000000\t0.000000003728\n" +
                "HYBTVZNCLNXFSUW\t-1024.000000000000\t-1024.000000000000\n" +
                "EGMITINLKFNUHNR\t0.000000017782\t0.000000023362\n" +
                "UXBWYWRLHUHJECI\t0.000000009297\t0.000000009220\n" +
                "HBXOWVYUVVRDPCH\t-512.000000000000\t-512.000000000000\n";

        assertThat(expected, "select id, x, y from tab where eq(x, y, 0.00000001)");
    }

    @Test
    public void testSearchByIntIdUnindexed() throws Exception {

        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $int("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            int[] ids = new int[4096];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = rnd.nextPositiveInt();
            }

            int mask = ids.length - 1;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");


            for (int i = 0; i < 100000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putInt(0, ids[rnd.nextInt() & mask]);
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "1148688404\t19.165769577026\t0.002435457427\t2015-03-12T00:00:10.150Z\n" +
                "1148688404\t0.000000444469\t2.694594800472\t2015-03-12T00:00:28.400Z\n" +
                "1148688404\t-640.000000000000\t-1024.000000000000\t2015-03-12T00:01:06.260Z\n" +
                "1148688404\t0.000000009509\t320.000000000000\t2015-03-12T00:01:26.010Z\n" +
                "1148688404\t0.000107977266\t362.467056274414\t2015-03-12T00:01:57.750Z\n" +
                "1148688404\t0.000000022366\t-451.815429687500\t2015-03-12T00:02:32.560Z\n" +
                "1148688404\t0.000003631694\t152.575393676758\t2015-03-12T00:02:35.210Z\n" +
                "1148688404\t-458.523437500000\t0.003299130592\t2015-03-12T00:03:09.590Z\n" +
                "1148688404\t-576.000000000000\t0.000000005352\t2015-03-12T00:03:15.080Z\n" +
                "1148688404\t0.000000010009\t0.000000336123\t2015-03-12T00:03:33.830Z\n" +
                "1148688404\t0.012756480370\t0.191264979541\t2015-03-12T00:03:56.200Z\n" +
                "1148688404\t-793.000000000000\t6.048339843750\t2015-03-12T00:04:01.350Z\n" +
                "1148688404\t0.000144788552\t10.723476886749\t2015-03-12T00:04:48.380Z\n" +
                "1148688404\t-467.990722656250\t5.262818336487\t2015-03-12T00:04:52.710Z\n" +
                "1148688404\t0.031378546730\t149.346038818359\t2015-03-12T00:05:01.020Z\n" +
                "1148688404\t0.000741893891\t-27.789062500000\t2015-03-12T00:05:19.110Z\n" +
                "1148688404\t0.000000032685\t0.000000002490\t2015-03-12T00:05:26.610Z\n" +
                "1148688404\t0.652305364609\t0.000000029041\t2015-03-12T00:06:56.860Z\n" +
                "1148688404\t-894.000000000000\t51.695074081421\t2015-03-12T00:08:46.620Z\n" +
                "1148688404\t695.000000000000\t0.145211979747\t2015-03-12T00:09:22.390Z\n" +
                "1148688404\t-334.488891601563\t0.000000393977\t2015-03-12T00:09:29.860Z\n" +
                "1148688404\t7.933303117752\t0.000516850792\t2015-03-12T00:10:13.730Z\n" +
                "1148688404\t435.498107910156\t1.287820875645\t2015-03-12T00:10:30.240Z\n" +
                "1148688404\t961.880340576172\t0.000168625862\t2015-03-12T00:10:41.190Z\n" +
                "1148688404\t-84.978515625000\t0.051617769524\t2015-03-12T00:10:46.200Z\n" +
                "1148688404\t0.000000544715\t0.000328194423\t2015-03-12T00:10:52.510Z\n" +
                "1148688404\t512.000000000000\t875.250000000000\t2015-03-12T00:11:46.390Z\n" +
                "1148688404\t0.000000010856\t0.028837248683\t2015-03-12T00:12:15.140Z\n" +
                "1148688404\t0.000027862162\t-896.000000000000\t2015-03-12T00:12:28.700Z\n" +
                "1148688404\t0.000000003071\t0.000025084717\t2015-03-12T00:12:36.370Z\n" +
                "1148688404\t0.000040687404\t0.007985642878\t2015-03-12T00:12:42.940Z\n" +
                "1148688404\t-961.937500000000\t-849.000000000000\t2015-03-12T00:12:49.940Z\n" +
                "1148688404\t0.000384466533\t87.682281494141\t2015-03-12T00:14:11.980Z\n" +
                "1148688404\t0.000000309420\t448.000000000000\t2015-03-12T00:15:38.730Z\n" +
                "1148688404\t29.022820472717\t-123.758422851563\t2015-03-12T00:16:30.770Z\n";
        assertThat(expected, "select id, x, y, timestamp from tab where id = 1148688404");
    }

    @Test
    public void testSearchByStringIdInUnindexed() throws Exception {

        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            int n = 4 * 1024;
            ObjHashSet<String> names = getNames(rnd, n);

            int mask = n - 1;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 100000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "VEGPIGSVMYWRTXV\t0.000015968965\t675.997558593750\n" +
                "VEGPIGSVMYWRTXV\t86.369699478149\t0.000017492367\n" +
                "JKEQQKQWPJVCFKV\t-141.875000000000\t2.248494863510\n" +
                "JKEQQKQWPJVCFKV\t-311.641113281250\t-398.023437500000\n" +
                "VEGPIGSVMYWRTXV\t3.659749031067\t0.000001526956\n" +
                "VEGPIGSVMYWRTXV\t-87.500000000000\t-778.964843750000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000841\t0.000000048359\n" +
                "JKEQQKQWPJVCFKV\t4.028919816017\t576.000000000000\n" +
                "VEGPIGSVMYWRTXV\t896.000000000000\t0.000000293617\n" +
                "VEGPIGSVMYWRTXV\t-1024.000000000000\t0.000001939648\n" +
                "VEGPIGSVMYWRTXV\t0.000019246366\t-1024.000000000000\n" +
                "VEGPIGSVMYWRTXV\t410.933593750000\t0.000000039558\n" +
                "JKEQQKQWPJVCFKV\t0.057562204078\t0.052935207263\n" +
                "VEGPIGSVMYWRTXV\t0.000000001681\t0.000000007821\n" +
                "VEGPIGSVMYWRTXV\t-1024.000000000000\t-921.363525390625\n" +
                "JKEQQKQWPJVCFKV\t0.000003027280\t43.346537590027\n" +
                "VEGPIGSVMYWRTXV\t0.000000009230\t99.335662841797\n" +
                "JKEQQKQWPJVCFKV\t266.000000000000\t0.000033699243\n" +
                "VEGPIGSVMYWRTXV\t5.966133117676\t0.000019340443\n" +
                "VEGPIGSVMYWRTXV\t0.000001273319\t0.000020025251\n" +
                "JKEQQKQWPJVCFKV\t0.007589547429\t0.016206960194\n" +
                "JKEQQKQWPJVCFKV\t-256.000000000000\t213.664222717285\n" +
                "VEGPIGSVMYWRTXV\t5.901823043823\t0.226934209466\n" +
                "VEGPIGSVMYWRTXV\t0.000033694661\t0.036246776581\n" +
                "JKEQQKQWPJVCFKV\t22.610988616943\t0.000000000000\n" +
                "VEGPIGSVMYWRTXV\t0.000000600285\t896.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000030440875\t0.000000002590\n" +
                "VEGPIGSVMYWRTXV\t-612.819580078125\t-768.000000000000\n" +
                "VEGPIGSVMYWRTXV\t652.960937500000\t-163.895019531250\n" +
                "JKEQQKQWPJVCFKV\t0.000001019223\t0.000861373846\n" +
                "VEGPIGSVMYWRTXV\t0.000000237054\t855.149673461914\n" +
                "JKEQQKQWPJVCFKV\t384.625000000000\t-762.664184570313\n" +
                "VEGPIGSVMYWRTXV\t0.000000003865\t269.064453125000\n" +
                "VEGPIGSVMYWRTXV\t1.651362478733\t640.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.772825062275\t701.435363769531\n" +
                "JKEQQKQWPJVCFKV\t191.932769775391\t0.000013081920\n" +
                "JKEQQKQWPJVCFKV\t416.812500000000\t0.000000003177\n" +
                "JKEQQKQWPJVCFKV\t0.000003838093\t810.968750000000\n" +
                "VEGPIGSVMYWRTXV\t0.042331939563\t368.000000000000\n" +
                "VEGPIGSVMYWRTXV\t0.038675817661\t-69.960937500000\n" +
                "VEGPIGSVMYWRTXV\t0.154417395592\t0.000000005908\n" +
                "JKEQQKQWPJVCFKV\t0.041989765130\t728.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000000\t-89.843750000000\n" +
                "VEGPIGSVMYWRTXV\t-224.000000000000\t247.625000000000\n";

        assertThat(expected, "select id, x,y from tab where id in ('JKEQQKQWPJVCFKV', 'VEGPIGSVMYWRTXV')");
    }

    @Test
    public void testSearchByStringIdInUnindexed2() throws Exception {

        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {
            Rnd rnd = new Rnd();
            int n = 4 * 1024;
            ObjHashSet<String> names = getNames(rnd, n);

            int mask = n - 1;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 100000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "JKEQQKQWPJVCFKV\t-141.875000000000\t2.248494863510\n" +
                "JKEQQKQWPJVCFKV\t-311.641113281250\t-398.023437500000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000841\t0.000000048359\n" +
                "JKEQQKQWPJVCFKV\t4.028919816017\t576.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.057562204078\t0.052935207263\n" +
                "JKEQQKQWPJVCFKV\t0.000003027280\t43.346537590027\n" +
                "JKEQQKQWPJVCFKV\t266.000000000000\t0.000033699243\n" +
                "JKEQQKQWPJVCFKV\t0.007589547429\t0.016206960194\n" +
                "JKEQQKQWPJVCFKV\t-256.000000000000\t213.664222717285\n" +
                "JKEQQKQWPJVCFKV\t22.610988616943\t0.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000030440875\t0.000000002590\n" +
                "JKEQQKQWPJVCFKV\t0.000001019223\t0.000861373846\n" +
                "JKEQQKQWPJVCFKV\t384.625000000000\t-762.664184570313\n" +
                "JKEQQKQWPJVCFKV\t0.772825062275\t701.435363769531\n" +
                "JKEQQKQWPJVCFKV\t191.932769775391\t0.000013081920\n" +
                "JKEQQKQWPJVCFKV\t416.812500000000\t0.000000003177\n" +
                "JKEQQKQWPJVCFKV\t0.000003838093\t810.968750000000\n" +
                "JKEQQKQWPJVCFKV\t0.041989765130\t728.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000000\t-89.843750000000\n";

        assertThat(expected, "select id, x,y from tab where id in ('JKEQQKQWPJVCFKV')");
    }

    @Test
    public void testSearchByStringIdIndexed() throws Exception {

        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(32).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 1024);

            int mask = 1023;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");


            for (int i = 0; i < 100000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "XTPNHTDCEBYWXBB\t-292.000000000000\t0.000000006354\t2015-03-12T00:00:02.290Z\n" +
                "XTPNHTDCEBYWXBB\t7.197236061096\t2.818476676941\t2015-03-12T00:00:27.340Z\n" +
                "XTPNHTDCEBYWXBB\t0.000005481412\t1.312383592129\t2015-03-12T00:00:29.610Z\n" +
                "XTPNHTDCEBYWXBB\t446.081878662109\t0.000000051478\t2015-03-12T00:00:31.780Z\n" +
                "XTPNHTDCEBYWXBB\t-809.625000000000\t0.000000104467\t2015-03-12T00:00:33.860Z\n" +
                "XTPNHTDCEBYWXBB\t560.000000000000\t0.526266053319\t2015-03-12T00:00:37.440Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000004619\t0.000028377840\t2015-03-12T00:00:46.630Z\n" +
                "XTPNHTDCEBYWXBB\t-510.983673095703\t-512.000000000000\t2015-03-12T00:00:55.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000527720\t760.000000000000\t2015-03-12T00:01:03.780Z\n" +
                "XTPNHTDCEBYWXBB\t0.012854952831\t-292.297058105469\t2015-03-12T00:01:09.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.082818411291\t2.386151432991\t2015-03-12T00:01:10.310Z\n" +
                "XTPNHTDCEBYWXBB\t330.293212890625\t-268.000000000000\t2015-03-12T00:01:12.810Z\n" +
                "XTPNHTDCEBYWXBB\t278.125000000000\t0.077817678452\t2015-03-12T00:01:16.670Z\n" +
                "XTPNHTDCEBYWXBB\t-448.000000000000\t0.000001829988\t2015-03-12T00:02:02.260Z\n" +
                "XTPNHTDCEBYWXBB\t0.238381892443\t-935.843750000000\t2015-03-12T00:02:33.540Z\n" +
                "XTPNHTDCEBYWXBB\t0.097852131352\t-120.312500000000\t2015-03-12T00:02:41.600Z\n" +
                "XTPNHTDCEBYWXBB\t0.034327778034\t0.000000076055\t2015-03-12T00:02:41.860Z\n" +
                "XTPNHTDCEBYWXBB\t0.016777765006\t1.525665938854\t2015-03-12T00:02:47.630Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000245470\t0.000000012355\t2015-03-12T00:03:21.880Z\n" +
                "XTPNHTDCEBYWXBB\t38.482372283936\t156.359012603760\t2015-03-12T00:03:25.470Z\n" +
                "XTPNHTDCEBYWXBB\t960.000000000000\t-561.500000000000\t2015-03-12T00:03:52.790Z\n" +
                "XTPNHTDCEBYWXBB\t0.000048914401\t0.000535350249\t2015-03-12T00:03:53.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.315786086023\t-544.000000000000\t2015-03-12T00:04:02.560Z\n" +
                "XTPNHTDCEBYWXBB\t-512.000000000000\t512.000000000000\t2015-03-12T00:04:09.410Z\n" +
                "XTPNHTDCEBYWXBB\t0.000469007500\t0.000000003315\t2015-03-12T00:04:29.330Z\n" +
                "XTPNHTDCEBYWXBB\t473.774108886719\t0.005739651737\t2015-03-12T00:04:49.240Z\n" +
                "XTPNHTDCEBYWXBB\t77.079637527466\t-68.750000000000\t2015-03-12T00:04:54.540Z\n" +
                "XTPNHTDCEBYWXBB\t1017.250000000000\t256.000000000000\t2015-03-12T00:04:59.980Z\n" +
                "XTPNHTDCEBYWXBB\t979.558593750000\t0.034476440400\t2015-03-12T00:05:00.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.080838434398\t0.000240437294\t2015-03-12T00:05:16.590Z\n" +
                "XTPNHTDCEBYWXBB\t837.343750000000\t0.000000003163\t2015-03-12T00:05:22.150Z\n" +
                "XTPNHTDCEBYWXBB\t-708.738037109375\t12.065711975098\t2015-03-12T00:05:23.960Z\n" +
                "XTPNHTDCEBYWXBB\t73.905494689941\t968.143554687500\t2015-03-12T00:05:30.160Z\n" +
                "XTPNHTDCEBYWXBB\t858.125000000000\t0.004347450798\t2015-03-12T00:06:06.300Z\n" +
                "XTPNHTDCEBYWXBB\t191.156250000000\t692.151489257813\t2015-03-12T00:06:07.380Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000350446\t0.001085809752\t2015-03-12T00:06:14.550Z\n" +
                "XTPNHTDCEBYWXBB\t877.107116699219\t0.073764367029\t2015-03-12T00:06:26.310Z\n" +
                "XTPNHTDCEBYWXBB\t4.980149984360\t0.000000005301\t2015-03-12T00:06:33.470Z\n" +
                "XTPNHTDCEBYWXBB\t0.000937165081\t-204.000000000000\t2015-03-12T00:06:54.810Z\n" +
                "XTPNHTDCEBYWXBB\t756.876586914063\t-572.703125000000\t2015-03-12T00:06:56.120Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000022885\t0.689865306020\t2015-03-12T00:06:57.920Z\n" +
                "XTPNHTDCEBYWXBB\t723.500000000000\t-592.817382812500\t2015-03-12T00:07:17.570Z\n" +
                "XTPNHTDCEBYWXBB\t-285.125000000000\t-448.250000000000\t2015-03-12T00:07:20.480Z\n" +
                "XTPNHTDCEBYWXBB\t4.877287983894\t-870.000000000000\t2015-03-12T00:07:36.830Z\n" +
                "XTPNHTDCEBYWXBB\t-638.750000000000\t-859.125000000000\t2015-03-12T00:07:38.910Z\n" +
                "XTPNHTDCEBYWXBB\t757.085937500000\t-128.000000000000\t2015-03-12T00:07:45.970Z\n" +
                "XTPNHTDCEBYWXBB\t0.000024196771\t44.254640579224\t2015-03-12T00:07:56.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000002050660\t113.433692932129\t2015-03-12T00:08:25.690Z\n" +
                "XTPNHTDCEBYWXBB\t0.001966100186\t401.331298828125\t2015-03-12T00:08:31.180Z\n" +
                "XTPNHTDCEBYWXBB\t134.605468750000\t0.000778750400\t2015-03-12T00:08:34.070Z\n" +
                "XTPNHTDCEBYWXBB\t304.000000000000\t170.421752929688\t2015-03-12T00:08:36.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000029559\t0.000033108370\t2015-03-12T00:08:42.110Z\n" +
                "XTPNHTDCEBYWXBB\t0.064763752744\t-384.000000000000\t2015-03-12T00:08:49.670Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.016534221359\t2015-03-12T00:09:01.010Z\n" +
                "XTPNHTDCEBYWXBB\t0.060663623735\t0.377497851849\t2015-03-12T00:09:03.830Z\n" +
                "XTPNHTDCEBYWXBB\t0.000001439460\t0.000000291427\t2015-03-12T00:09:05.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000660118021\t0.000000001520\t2015-03-12T00:09:14.030Z\n" +
                "XTPNHTDCEBYWXBB\t394.622238159180\t0.245789200068\t2015-03-12T00:09:35.320Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t0.002625804045\t2015-03-12T00:10:04.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.021761201322\t-805.171875000000\t2015-03-12T00:10:10.920Z\n" +
                "XTPNHTDCEBYWXBB\t18.621844291687\t0.003388853336\t2015-03-12T00:10:24.380Z\n" +
                "XTPNHTDCEBYWXBB\t-514.108642578125\t66.830410003662\t2015-03-12T00:10:30.510Z\n" +
                "XTPNHTDCEBYWXBB\t1.720549345016\t0.000006926386\t2015-03-12T00:10:37.250Z\n" +
                "XTPNHTDCEBYWXBB\t-715.183959960938\t22.427126884460\t2015-03-12T00:10:39.680Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.629212051630\t2015-03-12T00:10:44.310Z\n" +
                "XTPNHTDCEBYWXBB\t257.433593750000\t0.000087903414\t2015-03-12T00:11:03.210Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000070390\t-270.520019531250\t2015-03-12T00:11:18.280Z\n" +
                "XTPNHTDCEBYWXBB\t-439.250000000000\t0.000000093325\t2015-03-12T00:11:25.080Z\n" +
                "XTPNHTDCEBYWXBB\t256.000000000000\t760.565032958984\t2015-03-12T00:11:35.220Z\n" +
                "XTPNHTDCEBYWXBB\t634.375000000000\t0.000000033359\t2015-03-12T00:11:55.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.000031026852\t0.000000000000\t2015-03-12T00:11:58.680Z\n" +
                "XTPNHTDCEBYWXBB\t90.977296829224\t0.000000124888\t2015-03-12T00:12:06.190Z\n" +
                "XTPNHTDCEBYWXBB\t0.845079660416\t0.000001311144\t2015-03-12T00:12:12.980Z\n" +
                "XTPNHTDCEBYWXBB\t-0.500000000000\t216.805793762207\t2015-03-12T00:12:28.700Z\n" +
                "XTPNHTDCEBYWXBB\t0.021825334989\t0.000000003128\t2015-03-12T00:12:29.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.307688817382\t516.472656250000\t2015-03-12T00:12:36.300Z\n" +
                "XTPNHTDCEBYWXBB\t43.792731285095\t0.000372541021\t2015-03-12T00:12:42.040Z\n" +
                "XTPNHTDCEBYWXBB\t-782.687500000000\t252.748397827148\t2015-03-12T00:12:48.780Z\n" +
                "XTPNHTDCEBYWXBB\t137.645996093750\t808.000000000000\t2015-03-12T00:13:09.280Z\n" +
                "XTPNHTDCEBYWXBB\t0.002546578180\t17.097163200378\t2015-03-12T00:13:27.120Z\n" +
                "XTPNHTDCEBYWXBB\t-264.875000000000\t-419.750000000000\t2015-03-12T00:13:40.020Z\n" +
                "XTPNHTDCEBYWXBB\t0.000221305789\t53.479209899902\t2015-03-12T00:13:40.660Z\n" +
                "XTPNHTDCEBYWXBB\t0.030516586266\t-612.226562500000\t2015-03-12T00:13:50.440Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t17.896668434143\t2015-03-12T00:13:53.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000091829\t0.000000000000\t2015-03-12T00:14:06.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000164877347\t0.000000009079\t2015-03-12T00:14:15.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000276606\t512.000000000000\t2015-03-12T00:14:31.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000034906\t-1024.000000000000\t2015-03-12T00:15:19.540Z\n" +
                "XTPNHTDCEBYWXBB\t478.680068969727\t0.000058549787\t2015-03-12T00:15:19.790Z\n" +
                "XTPNHTDCEBYWXBB\t430.000000000000\t639.000000000000\t2015-03-12T00:15:33.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000236331\t-960.000000000000\t2015-03-12T00:15:38.790Z\n" +
                "XTPNHTDCEBYWXBB\t81.210937500000\t0.000056687957\t2015-03-12T00:15:43.330Z\n" +
                "XTPNHTDCEBYWXBB\t648.112548828125\t0.000010239995\t2015-03-12T00:16:30.740Z\n";
        assertThat(expected, "select id, x, y, timestamp from tab where id in ('XTPNHTDCEBYWXBB')");
    }

    @Test
    public void testSearchByStringIdUnindexed() throws Exception {

        createTab();

        final String expected = "XTPNHTDCEBYWXBB\t-292.000000000000\t0.000000006354\t2015-03-12T00:00:02.290Z\n" +
                "XTPNHTDCEBYWXBB\t7.197236061096\t2.818476676941\t2015-03-12T00:00:27.340Z\n" +
                "XTPNHTDCEBYWXBB\t0.000005481412\t1.312383592129\t2015-03-12T00:00:29.610Z\n" +
                "XTPNHTDCEBYWXBB\t446.081878662109\t0.000000051478\t2015-03-12T00:00:31.780Z\n" +
                "XTPNHTDCEBYWXBB\t-809.625000000000\t0.000000104467\t2015-03-12T00:00:33.860Z\n" +
                "XTPNHTDCEBYWXBB\t560.000000000000\t0.526266053319\t2015-03-12T00:00:37.440Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000004619\t0.000028377840\t2015-03-12T00:00:46.630Z\n" +
                "XTPNHTDCEBYWXBB\t-510.983673095703\t-512.000000000000\t2015-03-12T00:00:55.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000527720\t760.000000000000\t2015-03-12T00:01:03.780Z\n" +
                "XTPNHTDCEBYWXBB\t0.012854952831\t-292.297058105469\t2015-03-12T00:01:09.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.082818411291\t2.386151432991\t2015-03-12T00:01:10.310Z\n" +
                "XTPNHTDCEBYWXBB\t330.293212890625\t-268.000000000000\t2015-03-12T00:01:12.810Z\n" +
                "XTPNHTDCEBYWXBB\t278.125000000000\t0.077817678452\t2015-03-12T00:01:16.670Z\n" +
                "XTPNHTDCEBYWXBB\t-448.000000000000\t0.000001829988\t2015-03-12T00:02:02.260Z\n" +
                "XTPNHTDCEBYWXBB\t0.238381892443\t-935.843750000000\t2015-03-12T00:02:33.540Z\n" +
                "XTPNHTDCEBYWXBB\t0.097852131352\t-120.312500000000\t2015-03-12T00:02:41.600Z\n" +
                "XTPNHTDCEBYWXBB\t0.034327778034\t0.000000076055\t2015-03-12T00:02:41.860Z\n" +
                "XTPNHTDCEBYWXBB\t0.016777765006\t1.525665938854\t2015-03-12T00:02:47.630Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000245470\t0.000000012355\t2015-03-12T00:03:21.880Z\n" +
                "XTPNHTDCEBYWXBB\t38.482372283936\t156.359012603760\t2015-03-12T00:03:25.470Z\n" +
                "XTPNHTDCEBYWXBB\t960.000000000000\t-561.500000000000\t2015-03-12T00:03:52.790Z\n" +
                "XTPNHTDCEBYWXBB\t0.000048914401\t0.000535350249\t2015-03-12T00:03:53.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.315786086023\t-544.000000000000\t2015-03-12T00:04:02.560Z\n" +
                "XTPNHTDCEBYWXBB\t-512.000000000000\t512.000000000000\t2015-03-12T00:04:09.410Z\n" +
                "XTPNHTDCEBYWXBB\t0.000469007500\t0.000000003315\t2015-03-12T00:04:29.330Z\n" +
                "XTPNHTDCEBYWXBB\t473.774108886719\t0.005739651737\t2015-03-12T00:04:49.240Z\n" +
                "XTPNHTDCEBYWXBB\t77.079637527466\t-68.750000000000\t2015-03-12T00:04:54.540Z\n" +
                "XTPNHTDCEBYWXBB\t1017.250000000000\t256.000000000000\t2015-03-12T00:04:59.980Z\n" +
                "XTPNHTDCEBYWXBB\t979.558593750000\t0.034476440400\t2015-03-12T00:05:00.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.080838434398\t0.000240437294\t2015-03-12T00:05:16.590Z\n" +
                "XTPNHTDCEBYWXBB\t837.343750000000\t0.000000003163\t2015-03-12T00:05:22.150Z\n" +
                "XTPNHTDCEBYWXBB\t-708.738037109375\t12.065711975098\t2015-03-12T00:05:23.960Z\n" +
                "XTPNHTDCEBYWXBB\t73.905494689941\t968.143554687500\t2015-03-12T00:05:30.160Z\n" +
                "XTPNHTDCEBYWXBB\t858.125000000000\t0.004347450798\t2015-03-12T00:06:06.300Z\n" +
                "XTPNHTDCEBYWXBB\t191.156250000000\t692.151489257813\t2015-03-12T00:06:07.380Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000350446\t0.001085809752\t2015-03-12T00:06:14.550Z\n" +
                "XTPNHTDCEBYWXBB\t877.107116699219\t0.073764367029\t2015-03-12T00:06:26.310Z\n" +
                "XTPNHTDCEBYWXBB\t4.980149984360\t0.000000005301\t2015-03-12T00:06:33.470Z\n" +
                "XTPNHTDCEBYWXBB\t0.000937165081\t-204.000000000000\t2015-03-12T00:06:54.810Z\n" +
                "XTPNHTDCEBYWXBB\t756.876586914063\t-572.703125000000\t2015-03-12T00:06:56.120Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000022885\t0.689865306020\t2015-03-12T00:06:57.920Z\n" +
                "XTPNHTDCEBYWXBB\t723.500000000000\t-592.817382812500\t2015-03-12T00:07:17.570Z\n" +
                "XTPNHTDCEBYWXBB\t-285.125000000000\t-448.250000000000\t2015-03-12T00:07:20.480Z\n" +
                "XTPNHTDCEBYWXBB\t4.877287983894\t-870.000000000000\t2015-03-12T00:07:36.830Z\n" +
                "XTPNHTDCEBYWXBB\t-638.750000000000\t-859.125000000000\t2015-03-12T00:07:38.910Z\n" +
                "XTPNHTDCEBYWXBB\t757.085937500000\t-128.000000000000\t2015-03-12T00:07:45.970Z\n" +
                "XTPNHTDCEBYWXBB\t0.000024196771\t44.254640579224\t2015-03-12T00:07:56.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000002050660\t113.433692932129\t2015-03-12T00:08:25.690Z\n" +
                "XTPNHTDCEBYWXBB\t0.001966100186\t401.331298828125\t2015-03-12T00:08:31.180Z\n" +
                "XTPNHTDCEBYWXBB\t134.605468750000\t0.000778750400\t2015-03-12T00:08:34.070Z\n" +
                "XTPNHTDCEBYWXBB\t304.000000000000\t170.421752929688\t2015-03-12T00:08:36.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000029559\t0.000033108370\t2015-03-12T00:08:42.110Z\n" +
                "XTPNHTDCEBYWXBB\t0.064763752744\t-384.000000000000\t2015-03-12T00:08:49.670Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.016534221359\t2015-03-12T00:09:01.010Z\n" +
                "XTPNHTDCEBYWXBB\t0.060663623735\t0.377497851849\t2015-03-12T00:09:03.830Z\n" +
                "XTPNHTDCEBYWXBB\t0.000001439460\t0.000000291427\t2015-03-12T00:09:05.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000660118021\t0.000000001520\t2015-03-12T00:09:14.030Z\n" +
                "XTPNHTDCEBYWXBB\t394.622238159180\t0.245789200068\t2015-03-12T00:09:35.320Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t0.002625804045\t2015-03-12T00:10:04.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.021761201322\t-805.171875000000\t2015-03-12T00:10:10.920Z\n" +
                "XTPNHTDCEBYWXBB\t18.621844291687\t0.003388853336\t2015-03-12T00:10:24.380Z\n" +
                "XTPNHTDCEBYWXBB\t-514.108642578125\t66.830410003662\t2015-03-12T00:10:30.510Z\n" +
                "XTPNHTDCEBYWXBB\t1.720549345016\t0.000006926386\t2015-03-12T00:10:37.250Z\n" +
                "XTPNHTDCEBYWXBB\t-715.183959960938\t22.427126884460\t2015-03-12T00:10:39.680Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.629212051630\t2015-03-12T00:10:44.310Z\n" +
                "XTPNHTDCEBYWXBB\t257.433593750000\t0.000087903414\t2015-03-12T00:11:03.210Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000070390\t-270.520019531250\t2015-03-12T00:11:18.280Z\n" +
                "XTPNHTDCEBYWXBB\t-439.250000000000\t0.000000093325\t2015-03-12T00:11:25.080Z\n" +
                "XTPNHTDCEBYWXBB\t256.000000000000\t760.565032958984\t2015-03-12T00:11:35.220Z\n" +
                "XTPNHTDCEBYWXBB\t634.375000000000\t0.000000033359\t2015-03-12T00:11:55.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.000031026852\t0.000000000000\t2015-03-12T00:11:58.680Z\n" +
                "XTPNHTDCEBYWXBB\t90.977296829224\t0.000000124888\t2015-03-12T00:12:06.190Z\n" +
                "XTPNHTDCEBYWXBB\t0.845079660416\t0.000001311144\t2015-03-12T00:12:12.980Z\n" +
                "XTPNHTDCEBYWXBB\t-0.500000000000\t216.805793762207\t2015-03-12T00:12:28.700Z\n" +
                "XTPNHTDCEBYWXBB\t0.021825334989\t0.000000003128\t2015-03-12T00:12:29.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.307688817382\t516.472656250000\t2015-03-12T00:12:36.300Z\n" +
                "XTPNHTDCEBYWXBB\t43.792731285095\t0.000372541021\t2015-03-12T00:12:42.040Z\n" +
                "XTPNHTDCEBYWXBB\t-782.687500000000\t252.748397827148\t2015-03-12T00:12:48.780Z\n" +
                "XTPNHTDCEBYWXBB\t137.645996093750\t808.000000000000\t2015-03-12T00:13:09.280Z\n" +
                "XTPNHTDCEBYWXBB\t0.002546578180\t17.097163200378\t2015-03-12T00:13:27.120Z\n" +
                "XTPNHTDCEBYWXBB\t-264.875000000000\t-419.750000000000\t2015-03-12T00:13:40.020Z\n" +
                "XTPNHTDCEBYWXBB\t0.000221305789\t53.479209899902\t2015-03-12T00:13:40.660Z\n" +
                "XTPNHTDCEBYWXBB\t0.030516586266\t-612.226562500000\t2015-03-12T00:13:50.440Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t17.896668434143\t2015-03-12T00:13:53.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000091829\t0.000000000000\t2015-03-12T00:14:06.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000164877347\t0.000000009079\t2015-03-12T00:14:15.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000276606\t512.000000000000\t2015-03-12T00:14:31.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000034906\t-1024.000000000000\t2015-03-12T00:15:19.540Z\n" +
                "XTPNHTDCEBYWXBB\t478.680068969727\t0.000058549787\t2015-03-12T00:15:19.790Z\n" +
                "XTPNHTDCEBYWXBB\t430.000000000000\t639.000000000000\t2015-03-12T00:15:33.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000236331\t-960.000000000000\t2015-03-12T00:15:38.790Z\n" +
                "XTPNHTDCEBYWXBB\t81.210937500000\t0.000056687957\t2015-03-12T00:15:43.330Z\n" +
                "XTPNHTDCEBYWXBB\t648.112548828125\t0.000010239995\t2015-03-12T00:16:30.740Z\n";
        assertThat(expected, "select id, x, y, timestamp from tab where id = 'XTPNHTDCEBYWXBB'");
    }

    @Test
    public void testSearchIndexedStrNull() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = new ObjHashSet<>();
            for (int i = 0; i < 128; i++) {
                names.add(rnd.nextString(15));
            }

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                if ((rnd.nextPositiveInt() % 10) == 0) {
                    ew.putNull(0);
                } else {
                    ew.putStr(0, names.get(rnd.nextInt() & mask));
                }
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }
        assertNullSearch();
    }

    @Test
    public void testSearchIndexedSymNull() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $sym("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 128);

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                if ((rnd.nextPositiveInt() % 10) == 0) {
                    ew.putNull(0);
                } else {
                    ew.putSym(0, names.get(rnd.nextInt() & mask));
                }
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        assertNullSearch();
    }

    @Test
    public void testSearchUnindexedStrNull() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = new ObjHashSet<>();
            for (int i = 0; i < 128; i++) {
                names.add(rnd.nextString(15));
            }

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                if ((rnd.nextPositiveInt() % 10) == 0) {
                    ew.putNull(0);
                } else {
                    ew.putStr(0, names.get(rnd.nextInt() & mask));
                }
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        assertNullSearch();
    }

    @Test
    public void testSearchUnindexedSymNull() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $sym("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 128);

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                if ((rnd.nextPositiveInt() % 10) == 0) {
                    ew.putNull(0);
                } else {
                    ew.putSym(0, names.get(rnd.nextInt() & mask));
                }
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        assertNullSearch();
    }

    @Test
    public void testSigLookupError() throws Exception {
        createTabWithNaNs2();
        try {
            expectFailure("select x,y from tab where x~0");
        } catch (ParserException e) {
            Assert.assertEquals(27, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "No such function"));
        }
    }

    @Test
    public void testStrConcat() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("x").
                        $str("y").
                        $int("z").
                        $ts()

        )) {

            Rnd rnd = new Rnd();

            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 1000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, rnd.nextString(4));
                ew.putStr(1, rnd.nextString(2));
                ew.putInt(2, rnd.nextInt());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "CJOU-OU\n" +
                "CQJS-HG\n" +
                "CEJF-PB\n" +
                "CSJK-PY\n" +
                "CJHU-JY\n";

        assertThat(expected, "select x + '-' + y from tab where x ~ '^C.*J+'");
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
    public void testStrEqualsNull() throws Exception {
        createTabWithNullsAndTime();
        String expected = "RQ\t20:54\t2015-03-12T00:00:00.000Z\n";
        assertThat(expected, "tab where id = 'RQ'");
        assertThat(expected, "tab where 'RQ' = id");
    }

    @Test
    public void testStrNotEqualsNull() throws Exception {
        createTabWithNullsAndTime();
        String expected = "JWCPSWHYRXPEHNRXGZ\t19:27\t2015-03-12T00:00:00.000Z\n" +
                "IBBTGPGWFFYUDEYYQEHBHF\t19:36\t2015-03-12T00:00:00.000Z\n" +
                "\t2:52\t2015-03-12T00:00:00.000Z\n" +
                "\t5:22\t2015-03-12T00:00:00.000Z\n" +
                "OUO\t8:47\t2015-03-12T00:00:00.000Z\n" +
                "\t0:14\t2015-03-12T00:00:00.000Z\n" +
                "\t15:34\t2015-03-12T00:00:00.000Z\n" +
                "ETJRS\t13:27\t2015-03-12T00:00:00.000Z\n" +
                "RFBVTMHGOOZZVDZJMYICCXZ\t12:9\t2015-03-12T00:00:00.000Z\n" +
                "W\t8:19\t2015-03-12T00:00:00.000Z\n" +
                "\t9:5\t2015-03-12T00:00:00.000Z\n" +
                "SDOTSEDYYCTGQOLYXWCK\t9:40\t2015-03-12T00:00:00.000Z\n" +
                "WDSWUGSHOLNVTIQBZXI\t4:10\t2015-03-12T00:00:00.000Z\n" +
                "\t11:43\t2015-03-12T00:00:00.000Z\n" +
                "\t14:7\t2015-03-12T00:00:00.000Z\n" +
                "QSRLTKVVSJOJIPHZEPI\t14:15\t2015-03-12T00:00:00.000Z\n" +
                "OVLJUMLGLHMLLEOYPH\t22:22\t2015-03-12T00:00:00.000Z\n" +
                "\t19:32\t2015-03-12T00:00:00.000Z\n" +
                "\t21:49\t2015-03-12T00:00:00.000Z\n" +
                "\t7:16\t2015-03-12T00:00:00.000Z\n" +
                "BEZGHWVDKFL\t15:19\t2015-03-12T00:00:00.000Z\n" +
                "XPKRGIIHYHBOQ\t22:48\t2015-03-12T00:00:00.000Z\n" +
                "\t16:16\t2015-03-12T00:00:00.000Z\n" +
                "\t14:40\t2015-03-12T00:00:00.000Z\n" +
                "HNZHZSQLDGLOG\t12:44\t2015-03-12T00:00:00.000Z\n" +
                "SZMZVQEBNDCQCEHNOMV\t2:30\t2015-03-12T00:00:00.000Z\n" +
                "\t5:59\t2015-03-12T00:00:00.000Z\n" +
                "WNWIFFLRBROMNXKUIZ\t23:25\t2015-03-12T00:00:00.000Z\n" +
                "YVFZF\t11:46\t2015-03-12T00:00:00.000Z\n" +
                "\t0:14\t2015-03-12T00:00:00.000Z\n" +
                "\t6:47\t2015-03-12T00:00:00.000Z\n" +
                "VWSW\t1:36\t2015-03-12T00:00:00.000Z\n" +
                "ONFCLTJCKFMQN\t21:23\t2015-03-12T00:00:00.000Z\n" +
                "XUKLGMXSLUQ\t15:38\t2015-03-12T00:00:00.000Z\n" +
                "\t20:26\t2015-03-12T00:00:00.000Z\n" +
                "MYFFDTN\t12:56\t2015-03-12T00:00:00.000Z\n" +
                "PBNHGZWWCC\t15:25\t2015-03-12T00:00:00.000Z\n" +
                "LEGPUHHIUGGL\t14:43\t2015-03-12T00:00:00.000Z\n" +
                "LCBDMIGQZVKHTLQZSLQVFGPP\t21:45\t2015-03-12T00:00:00.000Z\n" +
                "BHYSBQYMIZJSVTNPIWZNFK\t13:23\t2015-03-12T00:00:00.000Z\n" +
                "\t2:41\t2015-03-12T00:00:00.000Z\n" +
                "\t8:57\t2015-03-12T00:00:00.000Z\n" +
                "\t12:16\t2015-03-12T00:00:00.000Z\n" +
                "\t14:20\t2015-03-12T00:00:00.000Z\n" +
                "JYDVRVN\t11:57\t2015-03-12T00:00:00.000Z\n" +
                "\t18:45\t2015-03-12T00:00:00.000Z\n" +
                "\t17:31\t2015-03-12T00:00:00.000Z\n" +
                "IWF\t11:20\t2015-03-12T00:00:00.000Z\n" +
                "HQQUWQOEENNEBQQEMXDKXEJ\t22:48\t2015-03-12T00:00:00.000Z\n" +
                "KYFLUHZQSNPXMKJSMKIXEYVT\t19:44\t2015-03-12T00:00:00.000Z\n" +
                "HGGIWH\t16:4\t2015-03-12T00:00:00.000Z\n" +
                "\t15:11\t2015-03-12T00:00:00.000Z\n" +
                "\t6:29\t2015-03-12T00:00:00.000Z\n" +
                "\t18:13\t2015-03-12T00:00:00.000Z\n" +
                "SVIHDWWLEV\t16:40\t2015-03-12T00:00:00.000Z\n" +
                "\t9:38\t2015-03-12T00:00:00.000Z\n" +
                "VLU\t11:45\t2015-03-12T00:00:00.000Z\n" +
                "\t2:33\t2015-03-12T00:00:00.000Z\n" +
                "\t5:25\t2015-03-12T00:00:00.000Z\n" +
                "CLNXFSUWPNXH\t22:39\t2015-03-12T00:00:00.000Z\n" +
                "ODWKOCPFYXPVKNCBWLNLRHWQ\t14:48\t2015-03-12T00:00:00.000Z\n" +
                "VFDBZWNIJEEHR\t19:25\t2015-03-12T00:00:00.000Z\n" +
                "\t17:16\t2015-03-12T00:00:00.000Z\n" +
                "\t2:4\t2015-03-12T00:00:00.000Z\n" +
                "BEGMITINLKFNUHNRJ\t12:13\t2015-03-12T00:00:00.000Z\n" +
                "\t21:50\t2015-03-12T00:00:00.000Z\n" +
                "MPBETTTKRIVOC\t15:50\t2015-03-12T00:00:00.000Z\n" +
                "\t16:22\t2015-03-12T00:00:00.000Z\n" +
                "IVQF\t6:52\t2015-03-12T00:00:00.000Z\n" +
                "SBOSEPGIUQZHE\t1:27\t2015-03-12T00:00:00.000Z\n" +
                "\t12:12\t2015-03-12T00:00:00.000Z\n" +
                "\t16:52\t2015-03-12T00:00:00.000Z\n" +
                "INKG\t18:25\t2015-03-12T00:00:00.000Z\n" +
                "EVQTQOZKXTPNHTDCEBYWX\t3:10\t2015-03-12T00:00:00.000Z\n" +
                "RLPTYXYGYFUXCDKDWOMD\t0:26\t2015-03-12T00:00:00.000Z\n" +
                "FRPXZSFX\t16:37\t2015-03-12T00:00:00.000Z\n" +
                "XTGNJJILLEYMIWT\t15:46\t2015-03-12T00:00:00.000Z\n" +
                "\t14:8\t2015-03-12T00:00:00.000Z\n" +
                "FIEVM\t2:29\t2015-03-12T00:00:00.000Z\n" +
                "GPYKKBMQMUDDCIHCNPUG\t23:38\t2015-03-12T00:00:00.000Z\n" +
                "\t1:58\t2015-03-12T00:00:00.000Z\n" +
                "\t10:56\t2015-03-12T00:00:00.000Z\n" +
                "\t7:57\t2015-03-12T00:00:00.000Z\n" +
                "BBUKOJSOLDYRODIPUNRPSMIF\t21:8\t2015-03-12T00:00:00.000Z\n" +
                "KO\t14:49\t2015-03-12T00:00:00.000Z\n" +
                "QSQJGDIHHNSSTCRZ\t19:39\t2015-03-12T00:00:00.000Z\n" +
                "FULMERTPIQBUYZV\t16:30\t2015-03-12T00:00:00.000Z\n" +
                "QSPZPBHLNEJRMDIKD\t15:22\t2015-03-12T00:00:00.000Z\n" +
                "\t12:49\t2015-03-12T00:00:00.000Z\n" +
                "PZGPZNYVLTPKBBQ\t9:57\t2015-03-12T00:00:00.000Z\n" +
                "YNNCTFSNSXHHD\t0:50\t2015-03-12T00:00:00.000Z\n" +
                "RUMMZSCJOU\t2:14\t2015-03-12T00:00:00.000Z\n" +
                "ENFEL\t4:46\t2015-03-12T00:00:00.000Z\n" +
                "\t11:40\t2015-03-12T00:00:00.000Z\n" +
                "QHGJBFQBBKF\t19:3\t2015-03-12T00:00:00.000Z\n" +
                "\t5:18\t2015-03-12T00:00:00.000Z\n" +
                "\t5:14\t2015-03-12T00:00:00.000Z\n" +
                "BHVRIIYMHOWKCDNZNLCNGZ\t14:48\t2015-03-12T00:00:00.000Z\n" +
                "OXRSFPVRQLGYDONNLI\t20:1\t2015-03-12T00:00:00.000Z\n";
        assertThat(expected, "tab where id != 'RQ'");
        assertThat(expected, "tab where 'RQ' != id");
    }

    @Test
    public void testStrRegex() throws Exception {
        createTab();
        final String expecte = "KEQMMKDFIPNZVZR\t0.000000001530\t2015-03-12T00:00:03.470Z\n" +
                "KEQMMKDFIPNZVZR\t-832.000000000000\t2015-03-12T00:00:04.650Z\n" +
                "KEQMMKDFIPNZVZR\t446.187500000000\t2015-03-12T00:00:05.460Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000005636\t2015-03-12T00:00:24.210Z\n" +
                "KEQMMKDFIPNZVZR\t0.129930097610\t2015-03-12T00:00:52.220Z\n" +
                "KEQMMKDFIPNZVZR\t-677.094238281250\t2015-03-12T00:00:54.020Z\n" +
                "KEQMMKDFIPNZVZR\t-167.187500000000\t2015-03-12T00:00:58.090Z\n" +
                "KEQMMKDFIPNZVZR\t-512.000000000000\t2015-03-12T00:01:11.790Z\n" +
                "KEQMMKDFIPNZVZR\t0.055018983781\t2015-03-12T00:01:19.340Z\n" +
                "KEQMMKDFIPNZVZR\t-862.500000000000\t2015-03-12T00:01:24.430Z\n" +
                "KEQMMKDFIPNZVZR\t883.730468750000\t2015-03-12T00:01:28.870Z\n" +
                "KEQMMKDFIPNZVZR\t193.875000000000\t2015-03-12T00:01:39.320Z\n" +
                "KEQMMKDFIPNZVZR\t-608.000000000000\t2015-03-12T00:01:42.440Z\n" +
                "KEQMMKDFIPNZVZR\t-193.003417968750\t2015-03-12T00:01:47.820Z\n" +
                "KEQMMKDFIPNZVZR\t0.000002046971\t2015-03-12T00:01:55.420Z\n" +
                "KEQMMKDFIPNZVZR\t0.037930097431\t2015-03-12T00:01:55.790Z\n" +
                "KEQMMKDFIPNZVZR\t0.160599559546\t2015-03-12T00:02:08.830Z\n" +
                "KEQMMKDFIPNZVZR\t91.000000000000\t2015-03-12T00:02:19.120Z\n" +
                "KEQMMKDFIPNZVZR\t-1000.000000000000\t2015-03-12T00:02:22.680Z\n" +
                "KEQMMKDFIPNZVZR\t0.000015199104\t2015-03-12T00:02:23.520Z\n" +
                "KEQMMKDFIPNZVZR\t-480.000000000000\t2015-03-12T00:02:29.060Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000224731\t2015-03-12T00:02:31.260Z\n" +
                "KEQMMKDFIPNZVZR\t0.043443457223\t2015-03-12T00:02:40.400Z\n" +
                "KEQMMKDFIPNZVZR\t-554.125000000000\t2015-03-12T00:02:45.230Z\n" +
                "KEQMMKDFIPNZVZR\t0.418899938464\t2015-03-12T00:02:52.550Z\n" +
                "KEQMMKDFIPNZVZR\t0.000002813213\t2015-03-12T00:03:34.680Z\n" +
                "KEQMMKDFIPNZVZR\t-750.970703125000\t2015-03-12T00:03:43.830Z\n" +
                "KEQMMKDFIPNZVZR\t202.477161407471\t2015-03-12T00:03:59.950Z\n" +
                "KEQMMKDFIPNZVZR\t0.000296119222\t2015-03-12T00:04:06.200Z\n" +
                "KEQMMKDFIPNZVZR\t-1001.109375000000\t2015-03-12T00:04:12.750Z\n" +
                "KEQMMKDFIPNZVZR\t-350.539062500000\t2015-03-12T00:04:17.920Z\n" +
                "KEQMMKDFIPNZVZR\t0.270242959261\t2015-03-12T00:04:30.780Z\n" +
                "KEQMMKDFIPNZVZR\t640.000000000000\t2015-03-12T00:04:36.000Z\n" +
                "KEQMMKDFIPNZVZR\t242.000000000000\t2015-03-12T00:04:37.360Z\n" +
                "KEQMMKDFIPNZVZR\t354.109191894531\t2015-03-12T00:04:43.560Z\n" +
                "KEQMMKDFIPNZVZR\t608.000000000000\t2015-03-12T00:05:03.070Z\n" +
                "KEQMMKDFIPNZVZR\t-209.281250000000\t2015-03-12T00:05:18.460Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000009506\t2015-03-12T00:05:39.720Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000018168\t2015-03-12T00:05:40.690Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000002177\t2015-03-12T00:05:41.820Z\n" +
                "KEQMMKDFIPNZVZR\t0.000375485601\t2015-03-12T00:05:49.480Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000039768\t2015-03-12T00:05:55.150Z\n" +
                "KEQMMKDFIPNZVZR\t0.269348338246\t2015-03-12T00:06:09.200Z\n" +
                "KEQMMKDFIPNZVZR\t-671.500000000000\t2015-03-12T00:06:26.130Z\n" +
                "KEQMMKDFIPNZVZR\t0.001258826785\t2015-03-12T00:06:28.910Z\n" +
                "KEQMMKDFIPNZVZR\t0.978091150522\t2015-03-12T00:06:33.330Z\n" +
                "KEQMMKDFIPNZVZR\t44.780708312988\t2015-03-12T00:06:43.700Z\n" +
                "KEQMMKDFIPNZVZR\t-767.601562500000\t2015-03-12T00:06:44.600Z\n" +
                "KEQMMKDFIPNZVZR\t-890.500000000000\t2015-03-12T00:06:59.620Z\n" +
                "KEQMMKDFIPNZVZR\t0.000173775785\t2015-03-12T00:07:01.460Z\n" +
                "KEQMMKDFIPNZVZR\t0.000192599509\t2015-03-12T00:07:04.160Z\n" +
                "KEQMMKDFIPNZVZR\t18.733582496643\t2015-03-12T00:07:23.720Z\n" +
                "KEQMMKDFIPNZVZR\t31.429724693298\t2015-03-12T00:07:38.140Z\n" +
                "KEQMMKDFIPNZVZR\t0.000390803711\t2015-03-12T00:07:39.260Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000044603\t2015-03-12T00:07:49.970Z\n" +
                "KEQMMKDFIPNZVZR\t-881.380859375000\t2015-03-12T00:07:55.910Z\n" +
                "KEQMMKDFIPNZVZR\t-128.000000000000\t2015-03-12T00:08:00.360Z\n" +
                "KEQMMKDFIPNZVZR\t891.539062500000\t2015-03-12T00:08:14.330Z\n" +
                "KEQMMKDFIPNZVZR\t508.000000000000\t2015-03-12T00:08:21.190Z\n" +
                "KEQMMKDFIPNZVZR\t0.002558049746\t2015-03-12T00:08:31.860Z\n" +
                "KEQMMKDFIPNZVZR\t-736.000000000000\t2015-03-12T00:08:57.430Z\n" +
                "KEQMMKDFIPNZVZR\t-968.859375000000\t2015-03-12T00:09:27.030Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000008169\t2015-03-12T00:09:27.200Z\n" +
                "KEQMMKDFIPNZVZR\t399.000000000000\t2015-03-12T00:09:45.580Z\n" +
                "KEQMMKDFIPNZVZR\t0.000239236899\t2015-03-12T00:09:53.250Z\n" +
                "KEQMMKDFIPNZVZR\t-104.871093750000\t2015-03-12T00:10:01.070Z\n" +
                "KEQMMKDFIPNZVZR\t15.412450790405\t2015-03-12T00:10:04.140Z\n" +
                "KEQMMKDFIPNZVZR\t0.185059137642\t2015-03-12T00:10:15.850Z\n" +
                "KEQMMKDFIPNZVZR\t5.659068346024\t2015-03-12T00:10:26.050Z\n" +
                "KEQMMKDFIPNZVZR\t3.807189881802\t2015-03-12T00:10:59.590Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000677441\t2015-03-12T00:11:01.530Z\n" +
                "KEQMMKDFIPNZVZR\t61.405685424805\t2015-03-12T00:11:03.130Z\n" +
                "KEQMMKDFIPNZVZR\t-1024.000000000000\t2015-03-12T00:11:18.220Z\n" +
                "KEQMMKDFIPNZVZR\t641.500000000000\t2015-03-12T00:11:25.040Z\n" +
                "KEQMMKDFIPNZVZR\t0.000001824081\t2015-03-12T00:11:34.740Z\n" +
                "KEQMMKDFIPNZVZR\t0.000056925237\t2015-03-12T00:11:39.260Z\n" +
                "KEQMMKDFIPNZVZR\t128.000000000000\t2015-03-12T00:11:40.030Z\n" +
                "KEQMMKDFIPNZVZR\t0.000168198319\t2015-03-12T00:11:42.890Z\n" +
                "KEQMMKDFIPNZVZR\t0.000002674703\t2015-03-12T00:11:54.620Z\n" +
                "KEQMMKDFIPNZVZR\t0.001482222520\t2015-03-12T00:12:40.320Z\n" +
                "KEQMMKDFIPNZVZR\t56.829874038696\t2015-03-12T00:12:42.760Z\n" +
                "KEQMMKDFIPNZVZR\t41.603179931641\t2015-03-12T00:13:16.840Z\n" +
                "KEQMMKDFIPNZVZR\t164.312500000000\t2015-03-12T00:13:35.470Z\n" +
                "KEQMMKDFIPNZVZR\t-457.061523437500\t2015-03-12T00:13:45.640Z\n" +
                "KEQMMKDFIPNZVZR\t-512.000000000000\t2015-03-12T00:13:46.040Z\n" +
                "KEQMMKDFIPNZVZR\t0.000027407084\t2015-03-12T00:13:51.600Z\n" +
                "KEQMMKDFIPNZVZR\t-473.760742187500\t2015-03-12T00:13:57.560Z\n" +
                "KEQMMKDFIPNZVZR\t-512.000000000000\t2015-03-12T00:13:58.830Z\n" +
                "KEQMMKDFIPNZVZR\t74.750000000000\t2015-03-12T00:14:32.610Z\n" +
                "KEQMMKDFIPNZVZR\t982.715270996094\t2015-03-12T00:14:33.480Z\n" +
                "KEQMMKDFIPNZVZR\t0.235923126340\t2015-03-12T00:14:36.540Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000003422\t2015-03-12T00:14:48.360Z\n" +
                "KEQMMKDFIPNZVZR\t0.000304762289\t2015-03-12T00:15:01.280Z\n" +
                "KEQMMKDFIPNZVZR\t0.000188905338\t2015-03-12T00:15:08.640Z\n" +
                "KEQMMKDFIPNZVZR\t256.000000000000\t2015-03-12T00:15:09.740Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000017417\t2015-03-12T00:15:17.910Z\n" +
                "KEQMMKDFIPNZVZR\t75.859375000000\t2015-03-12T00:15:32.280Z\n" +
                "KEQMMKDFIPNZVZR\t0.091820014641\t2015-03-12T00:15:34.560Z\n" +
                "KEQMMKDFIPNZVZR\t0.000044015695\t2015-03-12T00:15:45.650Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000003026\t2015-03-12T00:15:48.030Z\n" +
                "KEQMMKDFIPNZVZR\t-963.317260742188\t2015-03-12T00:15:49.270Z\n" +
                "KEQMMKDFIPNZVZR\t0.001303359750\t2015-03-12T00:16:08.870Z\n" +
                "KEQMMKDFIPNZVZR\t0.005202150205\t2015-03-12T00:16:14.750Z\n";

        assertThat(expecte, "select id, x, timestamp from tab where id ~ '^KE.*'");
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
            expectFailure("select id, z from (select id from tab where z = NaN) where id = 'KKUSIMYDXUUSKCX'");
        } catch (ParserException e) {
            Assert.assertEquals(11, QueryError.getPosition());
            Assert.assertTrue(Chars.contains(QueryError.getMessage(), "Invalid column"));
        }
    }

    @Test
    public void testSubQuery4() throws Exception {
        createTabWithNaNs2();

        final String expected = "ZSFXUNYQXTGNJJI\t-162\n" +
                "BHLNEJRMDIKDISG\t-458\n" +
                "YYVSYYEQBORDTQH\t-227\n";

        assertThat(expected, "select id, z from (tab where not(id in 'GMPLUCFTLNKYTSZ')) where timestamp = '2015-03-12T10:00:00;5m;30m;10'");
    }

    @Test
    public void testSubQuery5() throws Exception {
        createTabWithNaNs2();

        final String expected = "ZSFXUNYQXTGNJJI\t-162\n" +
                "BHLNEJRMDIKDISG\t-458\n" +
                "YYVSYYEQBORDTQH\t-227\n";

        assertThat(expected, "select id, z from (tab where not(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10')");
    }

    @Test
    public void testSubQuery7() throws Exception {
        createTabWithNaNs2();
        assertEmpty("select id, z from (tab where not(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10') where 10 < 3");
    }

    @Test
    public void testSubQueryConstantTrueWhere() throws Exception {
        createTabWithNaNs2();
        final String expected = "ZSFXUNYQXTGNJJI\t-162\n" +
                "BHLNEJRMDIKDISG\t-458\n" +
                "YYVSYYEQBORDTQH\t-227\n";

        assertThat(expected, "select id, z from (tab where not(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10') where 10 > 3");
    }

    @Test
    public void testSubQueryFalseModel() throws Exception {
        createTabWithNaNs2();
        assertEmpty("select id, z from (tab where not(id in 'GMPLUCFTLNKYTSZ') and timestamp = '2015-03-12T10:00:00;5m;30m;10') where timestamp = '2015-03-12T10:00:00' and timestamp = '2015-03-12T14:00:00'");
    }

    @Test
    public void testSubQueryIntervalSearch() throws Exception {
        createTabWithNaNs2();
        assertThat("2015-03-18T00:00:00.000Z\t662.930252075195\n" +
                        "2015-03-18T01:00:00.000Z\t0.000001096262\n" +
                        "2015-03-18T02:00:00.000Z\t0.000001078617\n" +
                        "2015-03-18T03:00:00.000Z\t-119.000000000000\n" +
                        "2015-03-18T04:00:00.000Z\t-290.255371093750\n" +
                        "2015-03-18T05:00:00.000Z\t-50.148437500000\n" +
                        "2015-03-18T06:00:00.000Z\t0.002493287437\n" +
                        "2015-03-18T07:00:00.000Z\t222.294685363770\n" +
                        "2015-03-18T08:00:00.000Z\tNaN\n" +
                        "2015-03-18T09:00:00.000Z\t58.412302017212\n" +
                        "2015-03-18T10:00:00.000Z\t-948.394042968750\n" +
                        "2015-03-18T11:00:00.000Z\t960.000000000000\n" +
                        "2015-03-18T12:00:00.000Z\t467.771606445313\n" +
                        "2015-03-18T13:00:00.000Z\t-384.000000000000\n" +
                        "2015-03-18T14:00:00.000Z\t0.000034461837\n" +
                        "2015-03-18T15:00:00.000Z\t-1024.000000000000\n" +
                        "2015-03-18T16:00:00.000Z\t61.995271682739\n" +
                        "2015-03-18T17:00:00.000Z\t0.000000780048\n" +
                        "2015-03-18T18:00:00.000Z\t0.000000773129\n" +
                        "2015-03-18T19:00:00.000Z\t-560.250000000000\n" +
                        "2015-03-18T20:00:00.000Z\t0.000000963666\n" +
                        "2015-03-18T21:00:00.000Z\t52.090820312500\n" +
                        "2015-03-18T22:00:00.000Z\t445.687500000000\n" +
                        "2015-03-18T23:00:00.000Z\tNaN\n",
                "(select timestamp+1 ts, sum(y) sum_y from tab sample by 1d) timestamp(ts) where ts = '2015-03-18'");
    }

    @Test
    public void testSubQueryIntervalSearch2() throws Exception {
        createTabWithNaNs2();
        assertThat("2015-03-18T01:00:00.000Z\t0.000001096262\n" +
                        "2015-03-18T02:00:00.000Z\t0.000001078617\n",
                "(select timestamp+1 ts, sum(y) from tab sample by 1d) timestamp(ts) where ts = '2015-03-18T01;1h'");

    }

    @Test
    public void testSubQueryIntervalSearch3() throws Exception {
        createTabWithNaNs2();
        assertThat("2015-03-18T01:00:00.000Z\t0.000001096262\n" +
                        "2015-03-18T02:00:00.000Z\t0.000001078617\n",
                "(select 1+timestamp ts, sum(y) from tab sample by 1d) timestamp(ts) where ts = '2015-03-18T01;1h'");

    }

    @Test
    public void testSymRegex() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $sym("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 128);

            int mask = 127;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putSym(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "EENNEBQQEMXDKXE\t0.005532926181\t2015-03-12T00:01:38.290Z\n" +
                "EDNKRCGKSQDCMUM\t201.500000000000\t2015-03-12T00:01:38.780Z\n" +
                "EVMLKCJBEVLUHLI\t-224.000000000000\t2015-03-12T00:01:39.040Z\n" +
                "EEHRUGPBMBTKVSB\t640.000000000000\t2015-03-12T00:01:39.140Z\n" +
                "EOCVFFKMEKPFOYM\t0.001286557002\t2015-03-12T00:01:39.260Z\n" +
                "ETJRSZSRYRFBVTM\t0.146399393678\t2015-03-12T00:01:39.460Z\n" +
                "ELLKKHTWNWIFFLR\t236.634628295898\t2015-03-12T00:01:39.600Z\n" +
                "EGMITINLKFNUHNR\t53.349147796631\t2015-03-12T00:01:39.850Z\n" +
                "EIWFOQKYHQQUWQO\t-617.734375000000\t2015-03-12T00:01:40.000Z\n";

        assertThat(expected, "select id, y, timestamp from tab latest by id where id ~ '^E.*'");
    }

    @Test
    public void testSymbolInIntervalSource() throws Exception {
        createTabWithSymbol();
        assertSymbol("(select sym, timestamp+1 ts, sum(y) from tab sample by 1d) timestamp(ts) where ts = '2015-03-13T00:01:39'");
    }

    @Test
    public void testTime24() throws Exception {
        createTabWithNullsAndTime();
        assertThat("\t2015-03-12T00:00:00.000Z\t2:52\t2015-03-12T02:52:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t5:22\t2015-03-12T05:22:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t0:14\t2015-03-12T00:14:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t15:34\t2015-03-12T15:34:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t9:5\t2015-03-12T09:05:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t11:43\t2015-03-12T11:43:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t14:7\t2015-03-12T14:07:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t19:32\t2015-03-12T19:32:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t21:49\t2015-03-12T21:49:00.000Z\n" +
                        "\t2015-03-12T00:00:00.000Z\t7:16\t2015-03-12T07:16:00.000Z\n",
                "select id, date, time, date + toDate(time, 'H:m') from tab where id = null limit 10");
    }

    @Test
    public void testTimestampOverride() throws Exception {
        createTabWithNaNs2();

        assertThat("2015-03-18T00:00:00.000Z\t662.930253171457\n" +
                        "2015-03-18T02:00:00.000Z\t-118.999998921383\n" +
                        "2015-03-18T04:00:00.000Z\t-340.403808593750\n" +
                        "2015-03-18T06:00:00.000Z\t222.297178651206\n" +
                        "2015-03-18T08:00:00.000Z\tNaN\n" +
                        "2015-03-18T10:00:00.000Z\t11.605957031250\n" +
                        "2015-03-18T12:00:00.000Z\t83.771606445313\n" +
                        "2015-03-18T14:00:00.000Z\t-1023.999965538164\n" +
                        "2015-03-18T16:00:00.000Z\t61.995272462788\n" +
                        "2015-03-18T18:00:00.000Z\t-560.249999226871\n" +
                        "2015-03-18T20:00:00.000Z\t52.090821276166\n" +
                        "2015-03-18T22:00:00.000Z\tNaN\n",
                "select ts, sum(sum_y) from ((select timestamp+1 ts, sum(y) sum_y from tab sample by 1d) timestamp(ts) where ts = '2015-03-18') sample by 2h");
    }

    @Test
    public void testUnindexedIntNaN() throws Exception {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $int("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");
            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(0);
                } else {
                    ew.putInt(0, rnd.nextInt());
                }
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }

        final String expected = "NaN\t768.000000000000\t-408.000000000000\n" +
                "NaN\t256.000000000000\t-455.750000000000\n" +
                "NaN\t525.287368774414\t-470.171875000000\n" +
                "NaN\t512.000000000000\t-425.962463378906\n" +
                "NaN\t553.796875000000\t-620.062500000000\n" +
                "NaN\t512.000000000000\t-958.144531250000\n" +
                "NaN\t304.062500000000\t-1024.000000000000\n" +
                "NaN\t600.000000000000\t-934.000000000000\n" +
                "NaN\t475.619140625000\t-431.078125000000\n" +
                "NaN\t864.000000000000\t-480.723571777344\n" +
                "NaN\t512.000000000000\t-585.500000000000\n" +
                "NaN\t183.876594543457\t-512.000000000000\n" +
                "NaN\t204.021038055420\t-453.903320312500\n" +
                "NaN\t272.363739013672\t-1024.000000000000\n" +
                "NaN\t973.989135742188\t-444.000000000000\n" +
                "NaN\t768.000000000000\t-1024.000000000000\n" +
                "NaN\t290.253906250000\t-960.000000000000\n" +
                "NaN\t263.580390930176\t-960.000000000000\n" +
                "NaN\t756.500000000000\t-1024.000000000000\n" +
                "NaN\t461.884765625000\t-921.996948242188\n" +
                "NaN\t512.000000000000\t-536.000000000000\n" +
                "NaN\t213.152450561523\t-811.783691406250\n" +
                "NaN\t121.591918945313\t-874.921630859375\n" +
                "NaN\t920.625000000000\t-512.000000000000\n";

        assertThat(expected, "select id, x, y from tab where id = NaN and x > 120 and y < -400");
    }

    @Test
    public void testVirtualColumnQuery() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {
            TestUtils.generateQuoteData(w, 100);
        }

        final String expected = "BT-A.L\t0.474883438625\t0.000001189157\t1.050231933594\n" +
                "ADM.L\t-51.014269662148\t104.021850585938\t0.006688738358\n" +
                "AGK.L\t-686.961853027344\t879.117187500000\t496.806518554688\n" +
                "ABF.L\t-383.000010317080\t768.000000000000\t0.000020634160\n" +
                "ABF.L\t-127.000000017899\t256.000000000000\t0.000000035797\n" +
                "WTB.L\t-459.332875207067\t920.625000000000\t0.040750414133\n" +
                "AGK.L\t-703.000000000000\t512.000000000000\t896.000000000000\n" +
                "RRS.L\t-5.478123126552\t12.923866510391\t0.032379742712\n" +
                "BT-A.L\t0.996734812157\t0.006530375686\t0.000000000000\n" +
                "ABF.L\t-359.000000008662\t0.000000017324\t720.000000000000\n" +
                "AGK.L\t-191.000000009850\t384.000000000000\t0.000000019700\n" +
                "ABF.L\t0.999416386211\t0.001165474765\t0.000001752813\n" +
                "RRS.L\t-347.652348756790\t1.507822513580\t695.796875000000\n" +
                "ADM.L\t-86.378168493509\t172.796875000000\t1.959461987019\n" +
                "RRS.L\t-470.449707034291\t0.000000006081\t942.899414062500\n" +
                "BP.L\t-723.414062500000\t424.828125000000\t1024.000000000000\n" +
                "HSBA.L\t-75.736694544117\t153.473033905029\t0.000355183205\n" +
                "RRS.L\t-489.548828125000\t632.921875000000\t348.175781250000\n" +
                "BT-A.L\t-92.000010057318\t186.000000000000\t0.000020114637\n" +
                "RRS.L\t-334.728804341285\t0.015470010694\t671.442138671875\n" +
                "HSBA.L\t0.969581946437\t0.000000009901\t0.060836097226\n" +
                "GKN.L\t-134.846217133105\t0.003103211522\t271.689331054688\n" +
                "BP.L\t-384.179687507322\t770.359375000000\t0.000000014643\n" +
                "LLOY.L\t-2.041434317827\t1.229880273342\t4.852988362312\n" +
                "TLW.L\t-382.690430340427\t0.000001305853\t767.380859375000\n" +
                "HSBA.L\t0.999757577623\t0.000000776007\t0.000484068747\n" +
                "RRS.L\t-291.082599617541\t583.609375000000\t0.555824235082\n" +
                "BP.L\t-234.659652709961\t296.544433593750\t174.774871826172\n" +
                "WTB.L\t-470.000000000000\t842.000000000000\t100.000000000000\n" +
                "RRS.L\t-181.231244396825\t364.462486267090\t0.000002526560\n" +
                "GKN.L\t0.999684159173\t0.000603844470\t0.000027837185\n" +
                "TLW.L\t-175.000000130841\t0.000000261681\t352.000000000000\n" +
                "GKN.L\t0.999937448983\t0.000125102033\t0.000000000000\n" +
                "AGK.L\t0.999129234003\t0.000000194258\t0.001741337735\n" +
                "ADM.L\t-108.185731784076\t218.371459960938\t0.000003607215\n" +
                "LLOY.L\t-527.821648597717\t1024.000000000000\t33.643297195435\n" +
                "BP.L\t-127.000587929302\t256.000000000000\t0.001175858604\n" +
                "HSBA.L\t-71.210969042524\t144.421875000000\t0.000063085048\n" +
                "BP.L\t-127.000000016025\t256.000000000000\t0.000000032050\n" +
                "GKN.L\t-415.000040076207\t0.000080152415\t832.000000000000\n" +
                "AGK.L\t-289.957031250000\t512.000000000000\t69.914062500000\n" +
                "AGK.L\t-450.494251251221\t768.000000000000\t134.988502502441\n" +
                "LLOY.L\t-293.859375000936\t0.000000001871\t589.718750000000\n" +
                "GKN.L\t-367.000001976696\t736.000000000000\t0.000003953393\n" +
                "AGK.L\t0.999999992240\t0.000000001374\t0.000000014146\n" +
                "LLOY.L\t-1.005833093077\t0.115072973073\t3.896593213081\n" +
                "BT-A.L\t-192.421875002549\t386.843750000000\t0.000000005098\n" +
                "LLOY.L\t-5.999457120895\t5.590153217316\t8.408761024475\n" +
                "GKN.L\t-4.042319541496\t0.000000248992\t10.084638834000\n" +
                "HSBA.L\t-81.109376058324\t0.000002116648\t164.218750000000\n" +
                "WTB.L\t0.999989964510\t0.000005107453\t0.000014963527\n" +
                "BT-A.L\t-468.790763854981\t629.480468750000\t310.101058959961\n" +
                "TLW.L\t0.694377524342\t0.000000049302\t0.611244902015\n" +
                "AGK.L\t-338.896525263786\t672.000000000000\t7.793050527573\n" +
                "TLW.L\t0.260076059727\t0.018715771381\t1.461132109165\n" +
                "ADM.L\t-352.977539062500\t655.625000000000\t52.330078125000\n" +
                "BP.L\t-59.514666617196\t0.000036359392\t121.029296875000\n" +
                "LLOY.L\t-131.905826912553\t265.807006835938\t0.004646989168\n" +
                "GKN.L\t-48.381265968084\t0.971607863903\t97.790924072266\n" +
                "LLOY.L\t-175.841796875000\t0.000000000000\t353.683593750000\n" +
                "LLOY.L\t-7.008397817612\t8.039016723633\t7.977778911591\n" +
                "ABF.L\t-318.007048395928\t638.000000000000\t0.014096791856\n" +
                "HSBA.L\t-409.112306014912\t0.000002654824\t820.224609375000\n" +
                "HSBA.L\t-149.046875020149\t300.093750000000\t0.000000040298\n" +
                "HSBA.L\t0.997081416281\t0.005052038119\t0.000785129319\n" +
                "BT-A.L\t0.936320396314\t0.127358488739\t0.000000718634\n" +
                "ADM.L\t0.999999965448\t0.000000009919\t0.000000059185\n" +
                "GKN.L\t0.979669743518\t0.040659694001\t0.000000818963\n" +
                "TLW.L\t-1.819448314155\t0.000012560774\t5.638884067535\n" +
                "BP.L\t-499.354459762573\t873.000000000000\t127.708919525146\n" +
                "HSBA.L\t-724.575195312500\t939.150390625000\t512.000000000000\n" +
                "ABF.L\t-488.316503390990\t978.632812500000\t0.000194281980\n" +
                "AGK.L\t-444.362694263458\t844.000000000000\t46.725388526917\n" +
                "HSBA.L\t-228.500000000000\t31.000000000000\t428.000000000000\n" +
                "ADM.L\t-36.921404135436\t75.842805862427\t0.000002408446\n" +
                "GKN.L\t-580.579162597656\t283.158325195313\t880.000000000000\n" +
                "ABF.L\t-481.575685286397\t0.000003385293\t965.151367187500\n" +
                "TLW.L\t0.804228177760\t0.031326758675\t0.360216885805\n" +
                "GKN.L\t-637.187500000000\t508.375000000000\t768.000000000000\n" +
                "ADM.L\t-5.150909269229\t12.290055274963\t0.011763263494\n" +
                "GKN.L\t-1.684180170298\t4.111308574677\t1.257051765919\n" +
                "RRS.L\t-113.000794559603\t0.000002205143\t228.001586914063\n" +
                "LLOY.L\t0.994362744171\t0.000000129186\t0.011274382472\n" +
                "ADM.L\t-8.878542166360\t19.756743907928\t0.000340424791\n" +
                "GKN.L\t0.999909967674\t0.000180012023\t0.000000052629\n" +
                "BT-A.L\t-252.331054687500\t400.000000000000\t106.662109375000\n" +
                "RRS.L\t-223.239476203918\t68.043695449829\t380.435256958008\n" +
                "ADM.L\t0.997952489638\t0.004094106262\t0.000000914462\n" +
                "BP.L\t-253.937500000000\t64.000000000000\t445.875000000000\n" +
                "WTB.L\t-2.006443221466\t0.000000157150\t6.012886285782\n" +
                "HSBA.L\t-303.487510681152\t497.000000000000\t111.975021362305\n" +
                "HSBA.L\t-282.980148315430\t549.125503540039\t18.834793090820\n" +
                "TLW.L\t-205.000000075030\t0.000000150060\t412.000000000000\n" +
                "RRS.L\t-19.750000003584\t0.000000007168\t41.500000000000\n" +
                "GKN.L\t-446.143188476563\t354.286376953125\t540.000000000000\n" +
                "GKN.L\t-185.000005207851\t0.000010415702\t372.000000000000\n" +
                "ADM.L\t-370.770515203476\t728.300781250000\t15.240249156952\n" +
                "RRS.L\t-223.348431229591\t448.000000000000\t0.696862459183\n" +
                "AGK.L\t-511.009589326801\t0.019178653602\t1024.000000000000\n" +
                "BP.L\t-705.000000000000\t1021.000000000000\t391.000000000000\n";

        assertThat(expected, "select sym, 1-(bid+ask)/2 mid, bid, ask from q");
    }

    private void appendNaNs(JournalWriter w, long t) throws JournalException {
        Rnd rnd = new Rnd();
        int n = 128;
        ObjHashSet<String> names = getNames(rnd, n);

        int mask = n - 1;

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter(t);
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

    private void assertNullSearch() throws ParserException, IOException {
        final String expected = "\t256.000000000000\t-455.750000000000\n" +
                "\t525.287368774414\t-470.171875000000\n" +
                "\t512.000000000000\t-425.962463378906\n" +
                "\t553.796875000000\t-620.062500000000\n" +
                "\t512.000000000000\t-958.144531250000\n" +
                "\t304.062500000000\t-1024.000000000000\n" +
                "\t600.000000000000\t-934.000000000000\n" +
                "\t475.619140625000\t-431.078125000000\n" +
                "\t864.000000000000\t-480.723571777344\n" +
                "\t512.000000000000\t-585.500000000000\n" +
                "\t183.876594543457\t-512.000000000000\n" +
                "\t204.021038055420\t-453.903320312500\n" +
                "\t272.363739013672\t-1024.000000000000\n" +
                "\t973.989135742188\t-444.000000000000\n" +
                "\t768.000000000000\t-1024.000000000000\n" +
                "\t290.253906250000\t-960.000000000000\n" +
                "\t263.580390930176\t-960.000000000000\n" +
                "\t756.500000000000\t-1024.000000000000\n" +
                "\t461.884765625000\t-921.996948242188\n" +
                "\t512.000000000000\t-536.000000000000\n" +
                "\t213.152450561523\t-811.783691406250\n" +
                "\t121.591918945313\t-874.921630859375\n" +
                "\t920.625000000000\t-512.000000000000\n" +
                "\t256.000000000000\t-488.625000000000\n" +
                "\t361.391540527344\t-1024.000000000000\n";

        assertThat(expected, "select id, x, y from tab where id = null and x > 120 and y < -400");
    }

    private void createIndexedTab() throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(16).
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 1024);

            int mask = 1023;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }
    }

    private void createTab() throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 1024);

            int mask = 1023;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 100000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(1, rnd.nextDouble());
                ew.putDouble(2, rnd.nextDouble());
                ew.putDate(3, t += 10);
                ew.append();
            }
            w.commit();
        }
    }

    private void createTabNoNaNs() throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            int n = 128;
            ObjHashSet<String> names = getNames(rnd, n);

            int mask = n - 1;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

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
    }

    private void createTabWithNaNs() throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        )) {
            appendNaNs(w, DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z"));
        }
    }

    private void createTabWithNaNs2() throws JournalException, NumericException {
        createTabWithNaNs20(new JournalStructure("tab").
                $str("id").
                $double("x").
                $double("y").
                $long("z").
                $int("w").
                $ts()
        );
    }

    private void createTabWithNaNs20(JournalStructure struct) throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(struct)) {

            Rnd rnd = new Rnd();
            int n = 128;
            ObjHashSet<String> names = getNames(rnd, n);

            int mask = n - 1;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

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

    private void createTabWithNaNs3() throws JournalException, NumericException {
        createTabWithNaNs20(new JournalStructure("tab").
                $str("id").index().
                $double("x").
                $double("y").
                $long("z").index().
                $int("w").index().
                $ts()
        );
    }

    private void createTabWithNullsAndTime() throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(new JournalStructure("tab").
                $str("id").index().
                $str("time").
                $date("date"))) {
            Rnd rnd = new Rnd();
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");
            for (int i = 0; i < 100; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, rnd.nextBoolean() ? null : rnd.nextChars(rnd.nextPositiveInt() % 25));
                ew.putStr(1, rnd.nextPositiveInt() % 24 + ":" + rnd.nextPositiveInt() % 60);
                ew.putDate(2, t);
                ew.append();
            }
            w.commit();
        }
    }

    private void createTabWithSymbol() throws JournalException, NumericException {
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(16).
                        $sym("sym").
                        $double("x").
                        $double("y").
                        $ts()

        )) {

            Rnd rnd = new Rnd();
            ObjHashSet<String> names = getNames(rnd, 1024);

            ObjHashSet<String> syms = new ObjHashSet<>();
            for (int i = 0; i < 64; i++) {
                syms.add(rnd.nextString(10));
            }

            int mask = 1023;
            long t = DateFormatUtils.parseDateTime("2015-03-12T00:00:00.000Z");

            for (int i = 0; i < 10000; i++) {
                JournalEntryWriter ew = w.entryWriter();
                ew.putStr(0, names.get(rnd.nextInt() & mask));
                ew.putDouble(2, rnd.nextDouble());
                ew.putDouble(3, rnd.nextDouble());
                ew.putDate(4, t += 10);
                ew.putSym(1, syms.get(rnd.nextInt() & 63));
                ew.append();
            }
            w.commit();
        }
    }

    private ObjHashSet<String> getNames(Rnd r, int n) {
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < n; i++) {
            names.add(r.nextString(15));
        }
        return names;
    }

    private void tabOfDates() throws JournalException, NumericException {
        long t = DateFormatUtils.parseDateTime("2016-10-08T00:00:00.000Z");
        try (JournalWriter w = getFactory().writer(
                new JournalStructure("tab").
                        $ts().
                        recordCountHint(100)

        )) {
            for (int i = 0; i < 100; i++) {
                JournalEntryWriter ew = w.entryWriter(t);
                ew.append();
                t += 1000 * 60 * 60 * 24;
            }
            w.commit();
        }
    }
}
