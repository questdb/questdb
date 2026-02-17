/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class EqStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSimple() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TO\tTO\t0.8645117110218422\n" +
                "JH\tJH\t0.293725743136773\n" +
                "HR\tHR\t0.7052360134891004\n" +
                "GT\tGT\t0.32041851650461484\n" +
                "FF\tFF\t0.7237740605530708\n" +
                "HD\tHD\t0.921018327201997\n" +
                "PG\tPG\t0.5693011480820244\n" +
                "NZ\tNZ\t0.7508124688784439\n" +
                "TK\tTK\t0.6566340574639548\n" +
                "FZ\tFZ\t0.7000560441774076\n" +
                "UW\tUW\t0.015101588517183018\n" +
                "HD\tHD\t0.37936669616421226\n" +
                "TR\tTR\t0.5358533775296352\n" +
                "CO\tCO\t0.00799290812895126\n" +
                "OS\tOS\t0.7693457725433892\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10000)" +
                    ")");

            assertSql(
                    expected, "x where a = b"
            );
        });
    }

    @Test
    public void testStrEquals() throws Exception {
        final String expected = "a\tb\tc\n" +
                "\t\t0.7360057954805926\n" +
                "\t\t0.8884541492501596\n" +
                "\t\t0.5073219750591287\n" +
                "\t\t0.006428156810007857\n" +
                "\t\t0.19504325610690842\n" +
                "\t\t0.8578726994967442\n" +
                "\t\t0.37470601365799705\n" +
                "\t\t0.32341006914413517\n" +
                "\t\t0.05189124141596113\n" +
                "\t\t0.35173746533284167\n" +
                "\t\t0.6106738173115615\n" +
                "\t\t0.56921845403248\n" +
                "\t\t0.6582599909595862\n" +
                "\t\t0.4084523851934708\n" +
                "\t\t0.9907741154585211\n" +
                "\t\t0.1418477571000919\n" +
                "\t\t0.9163281435642537\n" +
                "\t\t0.2591211030658963\n" +
                "\t\t0.2627885219342727\n" +
                "\t\t0.0620294680489023\n" +
                "\t\t0.6440420045442761\n" +
                "\t\t0.5111740946809575\n" +
                "\t\t0.6444756421733581\n" +
                "\t\t0.6895489127267652\n" +
                "\t\t0.41682031088801286\n" +
                "\t\t0.8472422063686806\n" +
                "\t\t0.6714451936914395\n" +
                "\t\t0.36137217390808174\n" +
                "\t\t0.3295206953565475\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(4,4,8) a," +
                    " rnd_str(4,4,8) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10000)" +
                    ")");

            assertSql(
                    expected, "x where a = b"
            );
        });
    }

    @Test
    public void testStrEqualsConstant() throws Exception {
        final String expected = "a\tb\tc\n" +
                "UW\tPV\t0.9236914780318218\n" +
                "UW\tEW\t0.04615858551093466\n" +
                "UW\tLP\t0.8121644564449728\n" +
                "UW\tMD\t0.7430962009725299\n" +
                "UW\tWN\t0.971829797336387\n" +
                "UW\tPQ\t0.9004350461942295\n" +
                "UW\tJS\t0.029348048368739166\n" +
                "UW\tEJ\t0.08510148077734003\n" +
                "UW\tKZ\t0.6661299198232566\n" +
                "UW\tWX\t0.5526435161505604\n" +
                "UW\tHQ\t0.5294238952229465\n" +
                "UW\tUW\t0.015101588517183018\n" +
                "UW\tMK\t0.9642333434663315\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10000)" +
                    ")");

            assertSql(
                    expected, "x where a = 'UW'"
            );
        });
    }

    @Test
    public void testStrEqualsConstant2() throws Exception {
        final String expected = "a\tb\tc\n" +
                "UW\tPV\t0.9236914780318218\n" +
                "UW\tEW\t0.04615858551093466\n" +
                "UW\tLP\t0.8121644564449728\n" +
                "UW\tMD\t0.7430962009725299\n" +
                "UW\tWN\t0.971829797336387\n" +
                "UW\tPQ\t0.9004350461942295\n" +
                "UW\tJS\t0.029348048368739166\n" +
                "UW\tEJ\t0.08510148077734003\n" +
                "UW\tKZ\t0.6661299198232566\n" +
                "UW\tWX\t0.5526435161505604\n" +
                "UW\tHQ\t0.5294238952229465\n" +
                "UW\tUW\t0.015101588517183018\n" +
                "UW\tMK\t0.9642333434663315\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10000)" +
                    ")");

            assertSql(
                    expected, "x where 'UW' = a"
            );
        });
    }

    @Test
    public void testStrEqualsNull() throws Exception {
        final String expected = "a\tb\tc\n" +
                "\tSY\t0.9622838038593742\n" +
                "\tZU\t0.9486852057060722\n" +
                "\tMG\t0.5914536110661014\n" +
                "\tHX\t0.43816506262665933\n" +
                "\tLG\t0.08092288866531683\n" +
                "\tGF\t0.9462291021870604\n" +
                "\tXW\t0.3063931802742671\n" +
                "\tYE\t0.4677236962728637\n" +
                "\tGP\t0.4284036984389522\n" +
                "\tLX\t0.9715862650720033\n" +
                "\tRB\t0.5613422353599637\n" +
                "\tPD\t0.8501026132754606\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,400) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10000)" +
                    ")");

            assertSql(
                    expected, "x where a = null"
            );
        });
    }
}
