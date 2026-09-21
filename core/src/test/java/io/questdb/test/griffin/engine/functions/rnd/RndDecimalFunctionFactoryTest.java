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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndDecimalFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RndDecimalFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testDecimal128() throws Exception {
        assertQuery("select rnd_decimal(38,7,0) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        8725303045923578469852476764.3292481
                        479963168928377527998723562175.1662165
                        653371233182669867358662174699.6475386
                        1392543605662946214143744121319.2727316
                        903570602837748323049398862189.8842201
                        1741233326566167063716918015373.1624337
                        753807643559421363643528266676.9522907
                        435882433106153739493605400600.5803803
                        524873085683054542523147333072.7496670
                        1237975357527510049416327667491.9174972
                        """);
    }

    @Test
    public void testDecimal16() throws Exception {
        assertQuery("select rnd_decimal(3,2,0) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        9.48
                        2.24
                        5.51
                        1.24
                        4.27
                        8.60
                        6.58
                        3.12
                        3.34
                        4.45
                        """);
    }

    @Test
    public void testDecimal256() throws Exception {
        assertQuery("select rnd_decimal(76,10,0) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        2969066772569349117273223109439862371820433908432381935048505915.3523027221
                        384060438613458834943006771812872284021098522758339318420391843275.6297348056
                        385877666940838483591060670690670904067800853756113473048073527235.8848594009
                        592510997725554728291931889104595151808011821146765555741165886874.9600972146
                        608179907400961053376159640823802094782969507946226463403226243476.1876109767
                        72818270768502414170564499739275199735009749656441901846677161835.6458033980
                        24847513711522562914811877892933952410876608138534996539509038437.7911536934
                        235928865053261850225595319033343813168431221208368884986342959149.0312153748
                        439416251413952891115029573972828567715600891738933818509042095031.2967545433
                        476831706110695056596053392455956664481693754394837547248662993139.7858420254
                        """);
    }

    @Test
    public void testDecimal32() throws Exception {
        assertQuery("select rnd_decimal(7,2,0) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        55151.49
                        77248.43
                        82634.33
                        28597.30
                        75311.32
                        18451.76
                        53788.60
                        52536.66
                        36622.54
                        96315.07
                        """);
    }

    @Test
    public void testDecimal64() throws Exception {
        assertQuery("select rnd_decimal(15,3,0) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        729996258992.370
                        921502384508.420
                        866532787669.293
                        193255228097.235
                        638984090604.211
                        738589890115.890
                        826605295369.296
                        835130332307.186
                        439391403625.854
                        639942391657.106
                        """);
    }

    @Test
    public void testDecimal8() throws Exception {
        assertQuery("select rnd_decimal(1,1,0) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        0.3
                        0.8
                        0.2
                        0.7
                        0.4
                        0.5
                        0.1
                        0.6
                        0.1
                        0.4
                        """);
    }

    @Test
    public void testNullRateDecimal128() throws Exception {
        assertQuery("select rnd_decimal(30,4,2) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        11546675563415048147184810.8865
                        11176155219453968566048696.8917
                        10631801619883242775074009.6506
                        3958250456696338914523505.7428
                        11149847549044321054269095.9449
                        15546902083994419610538135.8993
                        5409536647041479774120067.9131
                        2000750372904169533651803.6251
                        1322304016142825905995967.1774
                        
                        """);
    }

    @Test
    public void testNullRateDecimal16() throws Exception {
        assertQuery("select rnd_decimal(4,1,2) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        667.2
                        
                        305.9
                        989.9
                        
                        580.9
                        917.6
                        625.6
                        
                        708.0
                        """);
    }

    @Test
    public void testNullRateDecimal256() throws Exception {
        assertQuery("select rnd_decimal(60,4,2) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        54874386060519449216992005684686331938013082679870793309.7237
                        29684571911701513538506123183195375887864575193717484052.1688
                        5905077354671969590332719303701521413575700523498264576.0089
                        27897375365900561140861772657666249012620571681648833608.8434
                        
                        59916154993820845915536596196634136586143552721857130639.7320
                        61182949341377423239059651599552780073607490300425824166.3607
                        48473210670050997964136522259668225842951269233567754920.1327
                        58663454090883328458859437887414441918329436994132749820.4214
                        20375260701440342799102243815934337581274621304078845137.3950
                        """);
    }

    @Test
    public void testNullRateDecimal32() throws Exception {
        assertQuery("select rnd_decimal(9,2,2) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        3155151.18
                        
                        735757.01
                        3264472.43
                        
                        8475310.48
                        418449.74
                        5753787.04
                        
                        5694901.17
                        """);
    }

    @Test
    public void testNullRateDecimal64() throws Exception {
        assertQuery("select rnd_decimal(18,3,2) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        4729996258992.366
                        
                        260188555232587.037
                        611843578141083.005
                        
                        675638984090602.537
                        614738589890112.279
                        489826605295361.814
                        
                        943924477733600.066
                        """);
    }

    @Test
    public void testNullRateDecimal8() throws Exception {
        assertQuery("select rnd_decimal(2,1,2) as testCol from long_sequence(10)")
                .returnsOnce("""
                        testCol
                        3.9
                        
                        8.9
                        9.8
                        
                        6.7
                        6.8
                        1.9
                        
                        5.1
                        """);
    }

    @Test
    public void testRange() {
        assertFailure(0, "invalid precision and scale", 0, 0, 0);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndDecimalFunctionFactory();
    }
}