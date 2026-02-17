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

public class NotEqStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSimple() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TJ\tCP\t0.0843832076262595\n" +
                "RX\tEH\t0.22452340856088226\n" +
                "ZS\t\t0.4621835429127854\n" +
                "BT\tPG\t0.4224356661645131\n" +
                "UD\tYY\t0.0035983672154330515\n" +
                "HF\t\t0.9771103146051203\n" +
                "XY\tBE\t0.12503042190293423\n" +
                "SH\tUE\t0.8912587536603974\n" +
                "UL\t\t0.810161274171258\n" +
                "TJ\t\t0.8445258177211064\n" +
                "YR\tBV\t0.4900510449885239\n" +
                "OO\tZV\t0.92050039469858\n" +
                "YI\t\t0.2282233596526786\n" +
                "UI\tWE\t0.6821660861001273\n" +
                "UV\tDO\t0.5406709846540508\n" +
                "YY\t\t0.5449155021518948\n" +
                "LY\tWC\t0.7133910271555843\n" +
                "\tWD\t0.2820020716674768\n" +
                "\tHO\t0.2553319339703062\n" +
                "\tQB\t0.9441658975532605\n" +
                "VI\t\t0.5797447096307482\n" +
                "SU\tSR\t0.3045253310626277\n" +
                "\tSJ\t0.49765193229684157\n" +
                "HZ\tPI\t0.8231249461985348\n" +
                "OV\t\t0.5893398488053903\n" +
                "GL\tML\t0.32424562653969957\n" +
                "ZI\tNZ\t0.33747075654972813\n" +
                "MB\tZG\t0.2825582712777682\n" +
                "KF\tOP\t0.6797562990945702\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,1) a," +
                    " rnd_str(2,2,1) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(30)" +
                    ")");

            assertSql(
                    expected, "x where a <> b"
            );
        });
    }


    @Test
    public void testStrEqualsConstant() throws Exception {
        final String expected = "a\tb\tc\n" +
                "RX\tEH\t0.22452340856088226\n" +
                "ZS\tUX\t0.4217768841969397\n" +
                "GP\tWF\t0.6778564558839208\n" +
                "EY\tQE\t0.5249321062686694\n" +
                "OW\tPD\t0.15786635599554755\n" +
                "EO\tOJ\t0.9687423276940171\n" +
                "ED\tQQ\t0.42281342727402726\n" +
                "JG\tTJ\t0.022965637512889825\n" +
                "RY\tFB\t0.0011075361080621349\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10)" +
                    ")");

            assertSql(
                    expected, "x where a <> 'TJ'"
            );
        });
    }

    @Test
    public void testStrEqualsConstant2() throws Exception {
        final String expected = "a\tb\tc\n" +
                "RX\tEH\t0.22452340856088226\n" +
                "ZS\tUX\t0.4217768841969397\n" +
                "GP\tWF\t0.6778564558839208\n" +
                "EY\tQE\t0.5249321062686694\n" +
                "OW\tPD\t0.15786635599554755\n" +
                "EO\tOJ\t0.9687423276940171\n" +
                "ED\tQQ\t0.42281342727402726\n" +
                "JG\tTJ\t0.022965637512889825\n" +
                "RY\tFB\t0.0011075361080621349\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,0) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(10)" +
                    ")");

            assertSql(
                    expected, "x where 'TJ' <> a"
            );
        });
    }

    @Test
    public void testStrEqualsNull() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TJ\tCP\t0.0843832076262595\n" +
                "RX\tEH\t0.22452340856088226\n" +
                "ZS\tUX\t0.4217768841969397\n" +
                "GP\tWF\t0.6778564558839208\n" +
                "EY\tQE\t0.5249321062686694\n" +
                "OW\tPD\t0.15786635599554755\n" +
                "EO\tOJ\t0.9687423276940171\n" +
                "ED\tQQ\t0.42281342727402726\n" +
                "JG\tTJ\t0.022965637512889825\n" +
                "RY\tFB\t0.0011075361080621349\n" +
                "GO\tZZ\t0.18769708157331322\n" +
                "MY\tCC\t0.40455469747939254\n" +
                "SE\tYY\t0.910141759290032\n" +
                "OL\tXW\t0.49428905119584543\n" +
                "SU\tDS\t0.6752509547112409\n" +
                "HO\tNV\t0.8940917126581895\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,1) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(20)" +
                    ")");

            assertSql(
                    expected, "x where a <> null"
            );
        });
    }

    @Test
    public void testStrEqualsNull2() throws Exception {
        final String expected = "a\tb\tc\n" +
                "TJ\tCP\t0.0843832076262595\n" +
                "RX\tEH\t0.22452340856088226\n" +
                "ZS\tUX\t0.4217768841969397\n" +
                "GP\tWF\t0.6778564558839208\n" +
                "EY\tQE\t0.5249321062686694\n" +
                "OW\tPD\t0.15786635599554755\n" +
                "EO\tOJ\t0.9687423276940171\n" +
                "ED\tQQ\t0.42281342727402726\n" +
                "JG\tTJ\t0.022965637512889825\n" +
                "RY\tFB\t0.0011075361080621349\n" +
                "GO\tZZ\t0.18769708157331322\n" +
                "MY\tCC\t0.40455469747939254\n" +
                "SE\tYY\t0.910141759290032\n" +
                "OL\tXW\t0.49428905119584543\n" +
                "SU\tDS\t0.6752509547112409\n" +
                "HO\tNV\t0.8940917126581895\n";

        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    " select" +
                    " rnd_str(2,2,1) a," +
                    " rnd_str(2,2,0) b," +
                    " rnd_double(0) c" +
                    " from long_sequence(20)" +
                    ")");

            assertSql(
                    expected, "x where null <> a"
            );
        });
    }
}
