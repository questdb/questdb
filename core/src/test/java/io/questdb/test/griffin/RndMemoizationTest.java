/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class RndMemoizationTest extends AbstractCairoTest {

    @Test
    public void testIPv4() throws SqlException {
        allowFunctionMemoization();
        assertSql(
                "i\tcast\n" +
                        "187.139.150.80\t187.139.150.80\n" +
                        "18.206.96.238\t18.206.96.238\n" +
                        "92.80.211.65\t92.80.211.65\n" +
                        "212.159.205.29\t212.159.205.29\n" +
                        "4.98.173.21\t4.98.173.21\n" +
                        "199.122.166.85\t199.122.166.85\n" +
                        "79.15.250.138\t79.15.250.138\n" +
                        "35.86.82.23\t35.86.82.23\n" +
                        "111.98.117.250\t111.98.117.250\n" +
                        "205.123.179.216\t205.123.179.216\n",
                "select rnd_ipv4() i, i::string from long_sequence(10)"
        );
    }

    @Test
    public void testMemoizedFunctionsTriggerFullMaterialization() throws Exception {
        allowFunctionMemoization();
        assertSql(
                "x1\tx12\n" +
                        "-1148479919\t-1148479918\n" +
                        "315515119\t315515120\n" +
                        "1548800834\t1548800835\n",
                "select rnd_int()+1 x1, x1+1 x12 from long_sequence(3) order by x12;"
        );
    }

    @Test
    public void testRndByte() throws SqlException {
        allowFunctionMemoization();
        assertSql(
                "b\tk\n" +
                        "76\t86\n" +
                        "102\t112\n" +
                        "27\t37\n" +
                        "87\t97\n" +
                        "79\t89\n" +
                        "79\t89\n" +
                        "122\t132\n" +
                        "83\t93\n" +
                        "90\t100\n" +
                        "76\t86\n",
                "select rnd_byte() b, b + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndChar() throws SqlException {
        allowFunctionMemoization();
        assertSql(
                "c\tk\n" +
                        "V\tv\n" +
                        "T\tt\n" +
                        "J\tj\n" +
                        "W\tw\n" +
                        "C\tc\n" +
                        "P\tp\n" +
                        "S\ts\n" +
                        "W\tw\n" +
                        "H\th\n" +
                        "Y\ty\n",
                "select rnd_char() c, to_lowercase(c::string) k from long_sequence(10)"
        );
    }

    @Test
    public void testRndDate() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "d\tk\n" +
                        "1970-01-01T02:07:23.856Z\t1970-01-01T02:07:23.856001Z\n" +
                        "1970-01-01T02:29:52.366Z\t1970-01-01T02:29:52.366001Z\n" +
                        "1970-01-01T01:45:29.025Z\t1970-01-01T01:45:29.025001Z\n" +
                        "1970-01-01T01:15:01.475Z\t1970-01-01T01:15:01.475001Z\n" +
                        "1970-01-01T00:43:07.029Z\t1970-01-01T00:43:07.029001Z\n" +
                        "1970-01-01T02:07:40.373Z\t1970-01-01T02:07:40.373001Z\n" +
                        "1970-01-01T00:18:02.998Z\t1970-01-01T00:18:02.998001Z\n" +
                        "1970-01-01T02:14:51.881Z\t1970-01-01T02:14:51.881001Z\n" +
                        "1970-01-01T00:14:24.006Z\t1970-01-01T00:14:24.006001Z\n" +
                        "1970-01-01T00:10:02.536Z\t1970-01-01T00:10:02.536001Z\n",
                "select rnd_date() d, d + 1 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndDouble() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "d\tk\n" +
                        "0.6607777894187332\t10.660777789418733\n" +
                        "0.2246301342497259\t10.224630134249725\n" +
                        "0.08486964232560668\t10.084869642325607\n" +
                        "0.299199045961845\t10.299199045961846\n" +
                        "0.20447441837877756\t10.204474418378778\n" +
                        "0.6508594025855301\t10.65085940258553\n" +
                        "0.8423410920883345\t10.842341092088335\n" +
                        "0.9856290845874263\t10.985629084587426\n" +
                        "0.22452340856088226\t10.224523408560882\n" +
                        "0.5093827001617407\t10.50938270016174\n",
                "select rnd_double() d, d + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndDoubleBinary() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "d\tk\n" +
                        "1.3215555788374664\t11.321555578837467\n" +
                        "0.4492602684994518\t10.44926026849945\n" +
                        "0.16973928465121335\t10.169739284651213\n" +
                        "0.59839809192369\t10.59839809192369\n" +
                        "0.4089488367575551\t10.408948836757554\n" +
                        "1.3017188051710602\t11.30171880517106\n" +
                        "1.684682184176669\t11.684682184176669\n" +
                        "1.9712581691748525\t11.971258169174853\n" +
                        "0.44904681712176453\t10.449046817121765\n" +
                        "1.0187654003234814\t11.018765400323481\n",
                "select rnd_double() * 2 d, d + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndDoubleUnary() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "d\tk\n" +
                        "-0.6607777894187332\t9.339222210581267\n" +
                        "-0.2246301342497259\t9.775369865750275\n" +
                        "-0.08486964232560668\t9.915130357674393\n" +
                        "-0.299199045961845\t9.700800954038154\n" +
                        "-0.20447441837877756\t9.795525581621222\n" +
                        "-0.6508594025855301\t9.34914059741447\n" +
                        "-0.8423410920883345\t9.157658907911665\n" +
                        "-0.9856290845874263\t9.014370915412574\n" +
                        "-0.22452340856088226\t9.775476591439118\n" +
                        "-0.5093827001617407\t9.49061729983826\n",
                "select -rnd_double() d, d + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndFloat() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "d\tk\n" +
                        "0.66077775\t10.660778\n" +
                        "0.80432236\t10.804322\n" +
                        "0.22463012\t10.22463\n" +
                        "0.12966657\t10.129666\n" +
                        "0.08486962\t10.084869\n" +
                        "0.28455776\t10.284557\n" +
                        "0.29919904\t10.299199\n" +
                        "0.08438319\t10.084383\n" +
                        "0.20447439\t10.204474\n" +
                        "0.93446046\t10.934461\n",
                "select rnd_float() d, d + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndInt() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "-1148479920\t-1148479910\n" +
                        "315515118\t315515128\n" +
                        "1548800833\t1548800843\n" +
                        "-727724771\t-727724761\n" +
                        "73575701\t73575711\n" +
                        "-948263339\t-948263329\n" +
                        "1326447242\t1326447252\n" +
                        "592859671\t592859681\n" +
                        "1868723706\t1868723716\n" +
                        "-847531048\t-847531038\n",
                "select rnd_int() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndIntBinary() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "1998007456\t1998007466\n" +
                        "631030236\t631030246\n" +
                        "-1197365630\t-1197365620\n" +
                        "-1455449542\t-1455449532\n" +
                        "147151402\t147151412\n" +
                        "-1896526678\t-1896526668\n" +
                        "-1642072812\t-1642072802\n" +
                        "1185719342\t1185719352\n" +
                        "-557519884\t-557519874\n" +
                        "-1695062096\t-1695062086\n",
                "select rnd_int() * 2 i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndIntUnary() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "1148479920\t1148479930\n" +
                        "-315515118\t-315515108\n" +
                        "-1548800833\t-1548800823\n" +
                        "727724771\t727724781\n" +
                        "-73575701\t-73575691\n" +
                        "948263339\t948263349\n" +
                        "-1326447242\t-1326447232\n" +
                        "-592859671\t-592859661\n" +
                        "-1868723706\t-1868723696\n" +
                        "847531048\t847531058\n",
                "select -rnd_int() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndLong() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "4689592037643856\t4689592037643866\n" +
                        "4729996258992366\t4729996258992376\n" +
                        "7746536061816329025\t7746536061816329035\n" +
                        "-6945921502384501475\t-6945921502384501465\n" +
                        "8260188555232587029\t8260188555232587039\n" +
                        "8920866532787660373\t8920866532787660383\n" +
                        "-7611843578141082998\t-7611843578141082988\n" +
                        "-5354193255228091881\t-5354193255228091871\n" +
                        "-2653407051020864006\t-2653407051020863996\n" +
                        "-1675638984090602536\t-1675638984090602526\n",
                "select rnd_long() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndLong256() throws Exception {
        allowFunctionMemoization();
        assertSql(
                "l\tcolumn\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\t0\n" +
                        "0xb5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa65572a215ba0462ad15\t0\n" +
                        "0x322a2198864beb14797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fa\t0\n",
                "select rnd_long256() l, l - l from long_sequence(3)"
        );
    }

    @Test
    public void testRndLongBinary() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "9379184075287712\t9379184075287722\n" +
                        "9459992517984732\t9459992517984742\n" +
                        "-2953671950076893566\t-2953671950076893556\n" +
                        "4554901068940548666\t4554901068940548676\n" +
                        "-1926366963244377558\t-1926366963244377548\n" +
                        "-605011008134230870\t-605011008134230860\n" +
                        "3223056917427385620\t3223056917427385630\n" +
                        "7738357563253367854\t7738357563253367864\n" +
                        "-5306814102041728012\t-5306814102041728002\n" +
                        "-3351277968181205072\t-3351277968181205062\n",
                "select rnd_long() * 2 i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndLongUnary() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "i\tk\n" +
                        "-4689592037643856\t-4689592037643846\n" +
                        "-4729996258992366\t-4729996258992356\n" +
                        "-7746536061816329025\t-7746536061816329015\n" +
                        "6945921502384501475\t6945921502384501485\n" +
                        "-8260188555232587029\t-8260188555232587019\n" +
                        "-8920866532787660373\t-8920866532787660363\n" +
                        "7611843578141082998\t7611843578141083008\n" +
                        "5354193255228091881\t5354193255228091891\n" +
                        "2653407051020864006\t2653407051020864016\n" +
                        "1675638984090602536\t1675638984090602546\n",
                "select -rnd_long() i, i + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndShort() throws SqlException {
        allowFunctionMemoization();
        // assertQuery does not reset rnd between SQL executions - using assertSql
        assertSql(
                "s\tk\n" +
                        "-27056\t-27046\n" +
                        "24814\t24824\n" +
                        "-11455\t-11445\n" +
                        "-13027\t-13017\n" +
                        "-21227\t-21217\n" +
                        "-22955\t-22945\n" +
                        "-1398\t-1388\n" +
                        "21015\t21025\n" +
                        "30202\t30212\n" +
                        "-19496\t-19486\n",
                "select rnd_short() s, s + 10 k from long_sequence(10)"
        );
    }

    @Test
    public void testRndUuid() throws Exception {
        allowFunctionMemoization();
        assertSql(
                "u\tcast\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\t0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\t9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\t7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n",
                "select rnd_uuid4() u, u::string from long_sequence(3)"
        );
    }

    @Test
    public void testTimestampSequenceNoRandomAccess() throws Exception {
        assertQuery(
                "ts\tcolumn\n" +
                        "1970-01-01T00:00:00.000042Z\t1970-01-01T00:16:40.000043Z\n" +
                        "1970-01-01T00:33:20.000042Z\t1970-01-01T00:50:00.000043Z\n" +
                        "1970-01-01T01:06:40.000042Z\t1970-01-01T01:23:20.000043Z\n",
                "select timestamp_sequence(0, 1000000000) + 42 ts, ts + 1 from long_sequence(3);",
                null,
                false,
                true
        );
    }
}
