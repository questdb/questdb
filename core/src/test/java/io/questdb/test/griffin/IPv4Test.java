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

import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IPv4Test extends AbstractCairoTest {

    @Test
    public void ipv4NullTest() throws Exception {
        ddl("create table x (b ipv4)");
        insert("insert into x values('128.0.0.0')");
        insert("insert into x values('0.0.0.0')");

        assertSql(
                "b\n" +
                        "128.0.0.0\n" +
                        "\n",
                "x"
        );
    }

    @Test
    public void testAggregateByIPv4() throws Exception {
        assertQuery(
                "ip\tsum\n" +
                        "0.0.0.2\t7360\n" +
                        "0.0.0.4\t10105\n" +
                        "0.0.0.3\t9230\n" +
                        "0.0.0.5\t11739\n" +
                        "0.0.0.1\t11644\n",
                "select ip, sum (bytes) from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000) time" +
                        "  from long_sequence(100)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testAlterTableIPv4NullCol() throws Exception {
        ddl("create table test (col1 ipv4)");
        insert("insert into test values ('0.0.0.1')");
        ddl("alter table test add col2 ipv4");

        assertSql(
                "col1\tcol2\n" +
                        "0.0.0.1\t\n",
                "test"
        );
    }

    @Test
    public void testBitAndStr() throws Exception {
        assertSql("column\n" +
                "2.0.0.0\n", "select ipv4 '2.1.1.1' & '2.2.2.2'");
    }

    @Test
    public void testBitAndStr2() throws Exception {
        assertSql("column\n" +
                "2.0.0.0\n", "select '2.2.2.2' & ipv4 '2.1.1.1'");
    }

    @Test
    public void testBroadcastAddrUseCase() throws Exception {
        assertSql(
                "column\n" +
                        "68.255.255.255\n",
                "select (~ netmask('68.11.9.2/8')) | ipv4 '68.11.9.2'"
        );
    }

    @Test
    public void testCaseIPv41() throws Exception {
        assertQuery(
                "ip\tbytes\tcase\n" +
                        "187.139.150.80\t580\tnay\n" +
                        "212.159.205.29\t23\tnay\n" +
                        "79.15.250.138\t850\tnay\n" +
                        "205.123.179.216\t167\tnay\n" +
                        "170.90.236.206\t572\tnay\n" +
                        "92.26.178.136\t7\tnay\n" +
                        "231.146.30.59\t766\tnay\n" +
                        "113.132.124.243\t522\tnay\n" +
                        "67.22.249.199\t203\tnay\n" +
                        "25.107.51.160\t827\tYAY\n" +
                        "146.16.210.119\t383\tnay\n" +
                        "187.63.210.97\t424\tnay\n" +
                        "188.239.72.25\t513\tnay\n" +
                        "181.82.42.148\t539\tnay\n" +
                        "129.172.181.73\t25\tnay\n" +
                        "66.56.51.126\t904\tnay\n" +
                        "230.202.108.161\t171\tnay\n" +
                        "180.48.50.141\t136\tnay\n" +
                        "128.225.84.244\t313\tnay\n" +
                        "254.93.251.9\t810\tnay\n" +
                        "227.40.250.157\t903\tnay\n" +
                        "180.36.62.54\t528\tnay\n" +
                        "136.166.51.222\t580\tnay\n" +
                        "24.123.12.210\t95\tYAY\n" +
                        "171.117.213.66\t720\tnay\n" +
                        "224.99.254.121\t619\tnay\n" +
                        "55.211.206.129\t785\tYAY\n" +
                        "144.131.72.77\t369\tnay\n" +
                        "97.159.145.120\t352\tnay\n" +
                        "164.153.242.17\t906\tnay\n" +
                        "165.166.233.251\t332\tnay\n" +
                        "114.126.117.26\t71\tnay\n" +
                        "164.74.203.45\t678\tnay\n" +
                        "241.248.184.75\t334\tnay\n" +
                        "255.95.177.227\t44\tnay\n" +
                        "216.150.248.30\t563\tnay\n" +
                        "71.73.196.29\t741\tnay\n" +
                        "180.91.244.55\t906\tnay\n" +
                        "111.221.228.130\t531\tnay\n" +
                        "171.30.189.77\t238\tnay\n" +
                        "73.153.126.70\t772\tnay\n" +
                        "105.218.160.179\t986\tnay\n" +
                        "201.100.238.229\t318\tnay\n" +
                        "12.214.12.100\t598\tYAY\n" +
                        "212.102.182.127\t984\tnay\n" +
                        "50.214.139.184\t574\tYAY\n" +
                        "186.33.243.40\t659\tnay\n" +
                        "74.196.176.71\t740\tnay\n" +
                        "150.153.88.133\t849\tnay\n" +
                        "63.60.82.184\t37\tYAY\n",
                "select ip, bytes, case when ip <<= '2.65.32.1/2' then 'YAY' else 'nay' end from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCaseIPv42() throws Exception {
        assertQuery(
                "ip\tbytes\tcase\n" +
                        "187.139.150.80\t580\t\n" +
                        "212.159.205.29\t23\t\n" +
                        "79.15.250.138\t850\t\n" +
                        "205.123.179.216\t167\t\n" +
                        "170.90.236.206\t572\t\n" +
                        "92.26.178.136\t7\t\n" +
                        "231.146.30.59\t766\t\n" +
                        "113.132.124.243\t522\t\n" +
                        "67.22.249.199\t203\t\n" +
                        "25.107.51.160\t827\tYAY\n" +
                        "146.16.210.119\t383\t\n" +
                        "187.63.210.97\t424\t\n" +
                        "188.239.72.25\t513\t\n" +
                        "181.82.42.148\t539\t\n" +
                        "129.172.181.73\t25\t\n" +
                        "66.56.51.126\t904\t\n" +
                        "230.202.108.161\t171\t\n" +
                        "180.48.50.141\t136\t\n" +
                        "128.225.84.244\t313\t\n" +
                        "254.93.251.9\t810\t\n" +
                        "227.40.250.157\t903\t\n" +
                        "180.36.62.54\t528\t\n" +
                        "136.166.51.222\t580\t\n" +
                        "24.123.12.210\t95\tYAY\n" +
                        "171.117.213.66\t720\t\n" +
                        "224.99.254.121\t619\t\n" +
                        "55.211.206.129\t785\tYAY\n" +
                        "144.131.72.77\t369\t\n" +
                        "97.159.145.120\t352\t\n" +
                        "164.153.242.17\t906\t\n" +
                        "165.166.233.251\t332\t\n" +
                        "114.126.117.26\t71\t\n" +
                        "164.74.203.45\t678\t\n" +
                        "241.248.184.75\t334\t\n" +
                        "255.95.177.227\t44\t\n" +
                        "216.150.248.30\t563\t\n" +
                        "71.73.196.29\t741\t\n" +
                        "180.91.244.55\t906\t\n" +
                        "111.221.228.130\t531\t\n" +
                        "171.30.189.77\t238\t\n" +
                        "73.153.126.70\t772\t\n" +
                        "105.218.160.179\t986\t\n" +
                        "201.100.238.229\t318\t\n" +
                        "12.214.12.100\t598\tYAY\n" +
                        "212.102.182.127\t984\t\n" +
                        "50.214.139.184\t574\tYAY\n" +
                        "186.33.243.40\t659\t\n" +
                        "74.196.176.71\t740\t\n" +
                        "150.153.88.133\t849\t\n" +
                        "63.60.82.184\t37\tYAY\n",
                "select ip, bytes, case when ip <<= '2.65.32.1/2' then 'YAY' end from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCaseIPv43() throws Exception {
        assertQuery(
                "ip\tbytes\tcase\n" +
                        "187.139.150.80\t580\t0\n" +
                        "212.159.205.29\t23\t0\n" +
                        "79.15.250.138\t850\t0\n" +
                        "205.123.179.216\t167\t0\n" +
                        "170.90.236.206\t572\t0\n" +
                        "92.26.178.136\t7\t0\n" +
                        "231.146.30.59\t766\t0\n" +
                        "113.132.124.243\t522\t0\n" +
                        "67.22.249.199\t203\t0\n" +
                        "25.107.51.160\t827\t1\n" +
                        "146.16.210.119\t383\t0\n" +
                        "187.63.210.97\t424\t0\n" +
                        "188.239.72.25\t513\t0\n" +
                        "181.82.42.148\t539\t0\n" +
                        "129.172.181.73\t25\t0\n" +
                        "66.56.51.126\t904\t0\n" +
                        "230.202.108.161\t171\t0\n" +
                        "180.48.50.141\t136\t0\n" +
                        "128.225.84.244\t313\t0\n" +
                        "254.93.251.9\t810\t0\n" +
                        "227.40.250.157\t903\t0\n" +
                        "180.36.62.54\t528\t0\n" +
                        "136.166.51.222\t580\t0\n" +
                        "24.123.12.210\t95\t1\n" +
                        "171.117.213.66\t720\t0\n" +
                        "224.99.254.121\t619\t0\n" +
                        "55.211.206.129\t785\t1\n" +
                        "144.131.72.77\t369\t0\n" +
                        "97.159.145.120\t352\t0\n" +
                        "164.153.242.17\t906\t0\n" +
                        "165.166.233.251\t332\t0\n" +
                        "114.126.117.26\t71\t0\n" +
                        "164.74.203.45\t678\t0\n" +
                        "241.248.184.75\t334\t0\n" +
                        "255.95.177.227\t44\t0\n" +
                        "216.150.248.30\t563\t0\n" +
                        "71.73.196.29\t741\t0\n" +
                        "180.91.244.55\t906\t0\n" +
                        "111.221.228.130\t531\t0\n" +
                        "171.30.189.77\t238\t0\n" +
                        "73.153.126.70\t772\t0\n" +
                        "105.218.160.179\t986\t0\n" +
                        "201.100.238.229\t318\t0\n" +
                        "12.214.12.100\t598\t1\n" +
                        "212.102.182.127\t984\t0\n" +
                        "50.214.139.184\t574\t1\n" +
                        "186.33.243.40\t659\t0\n" +
                        "74.196.176.71\t740\t0\n" +
                        "150.153.88.133\t849\t0\n" +
                        "63.60.82.184\t37\t1\n",
                "select ip, bytes, case when ip << '2.65.32.1/2' then 1 else 0 end from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCaseIPv44() throws Exception {
        assertQuery(
                "ip\tbytes\tcase\n" +
                        "187.139.150.80\t580\tNaN\n" +
                        "212.159.205.29\t23\tNaN\n" +
                        "79.15.250.138\t850\tNaN\n" +
                        "205.123.179.216\t167\tNaN\n" +
                        "170.90.236.206\t572\tNaN\n" +
                        "92.26.178.136\t7\tNaN\n" +
                        "231.146.30.59\t766\tNaN\n" +
                        "113.132.124.243\t522\tNaN\n" +
                        "67.22.249.199\t203\tNaN\n" +
                        "25.107.51.160\t827\t1\n" +
                        "146.16.210.119\t383\tNaN\n" +
                        "187.63.210.97\t424\tNaN\n" +
                        "188.239.72.25\t513\tNaN\n" +
                        "181.82.42.148\t539\tNaN\n" +
                        "129.172.181.73\t25\tNaN\n" +
                        "66.56.51.126\t904\tNaN\n" +
                        "230.202.108.161\t171\tNaN\n" +
                        "180.48.50.141\t136\tNaN\n" +
                        "128.225.84.244\t313\tNaN\n" +
                        "254.93.251.9\t810\tNaN\n" +
                        "227.40.250.157\t903\tNaN\n" +
                        "180.36.62.54\t528\tNaN\n" +
                        "136.166.51.222\t580\tNaN\n" +
                        "24.123.12.210\t95\t1\n" +
                        "171.117.213.66\t720\tNaN\n" +
                        "224.99.254.121\t619\tNaN\n" +
                        "55.211.206.129\t785\t1\n" +
                        "144.131.72.77\t369\tNaN\n" +
                        "97.159.145.120\t352\tNaN\n" +
                        "164.153.242.17\t906\tNaN\n" +
                        "165.166.233.251\t332\tNaN\n" +
                        "114.126.117.26\t71\tNaN\n" +
                        "164.74.203.45\t678\tNaN\n" +
                        "241.248.184.75\t334\tNaN\n" +
                        "255.95.177.227\t44\tNaN\n" +
                        "216.150.248.30\t563\tNaN\n" +
                        "71.73.196.29\t741\tNaN\n" +
                        "180.91.244.55\t906\tNaN\n" +
                        "111.221.228.130\t531\tNaN\n" +
                        "171.30.189.77\t238\tNaN\n" +
                        "73.153.126.70\t772\tNaN\n" +
                        "105.218.160.179\t986\tNaN\n" +
                        "201.100.238.229\t318\tNaN\n" +
                        "12.214.12.100\t598\t1\n" +
                        "212.102.182.127\t984\tNaN\n" +
                        "50.214.139.184\t574\t1\n" +
                        "186.33.243.40\t659\tNaN\n" +
                        "74.196.176.71\t740\tNaN\n" +
                        "150.153.88.133\t849\tNaN\n" +
                        "63.60.82.184\t37\t1\n",
                "select ip, bytes, case when ip << '2.65.32.1/2' then 1 end from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCaseIPv45() throws Exception {
        assertQuery(
                "ip\tbytes\tcase\n" +
                        "2.2.96.238\t774\tNOT NULL\n" +
                        "2.2.89.171\t404\tNOT NULL\n" +
                        "2.2.76.40\t167\tNOT NULL\n" +
                        "2.2.95.15\t803\tNOT NULL\n" +
                        "2.2.45.145\t182\tNOT NULL\n" +
                        "\t647\tNULL\n" +
                        "2.2.249.199\t203\tNOT NULL\n" +
                        "\t827\tNULL\n" +
                        "2.2.170.235\t987\tNOT NULL\n" +
                        "2.2.184.81\t614\tNOT NULL\n" +
                        "2.2.213.108\t539\tNOT NULL\n" +
                        "2.2.47.76\t585\tNOT NULL\n" +
                        "\t16\tNULL\n" +
                        "2.2.129.200\t981\tNOT NULL\n" +
                        "2.2.171.12\t313\tNOT NULL\n" +
                        "2.2.253.254\t297\tNOT NULL\n" +
                        "\t773\tNULL\n" +
                        "2.2.227.50\t411\tNOT NULL\n" +
                        "2.2.12.210\t95\tNOT NULL\n" +
                        "2.2.205.4\t916\tNOT NULL\n" +
                        "2.2.236.117\t983\tNOT NULL\n" +
                        "2.2.183.179\t369\tNOT NULL\n" +
                        "2.2.220.75\t12\tNOT NULL\n" +
                        "2.2.157.48\t613\tNOT NULL\n" +
                        "\t114\tNULL\n" +
                        "2.2.52.211\t678\tNOT NULL\n" +
                        "2.2.35.79\t262\tNOT NULL\n" +
                        "2.2.207.241\t497\tNOT NULL\n" +
                        "2.2.196.29\t741\tNOT NULL\n" +
                        "2.2.228.56\t993\tNOT NULL\n" +
                        "2.2.246.213\t562\tNOT NULL\n" +
                        "2.2.126.70\t772\tNOT NULL\n" +
                        "2.2.37.167\t907\tNOT NULL\n" +
                        "2.2.234.47\t314\tNOT NULL\n" +
                        "2.2.73.129\t984\tNOT NULL\n" +
                        "2.2.112.55\t175\tNOT NULL\n" +
                        "2.2.74.124\t254\tNOT NULL\n" +
                        "2.2.167.123\t849\tNOT NULL\n" +
                        "2.2.2.123\t941\tNOT NULL\n" +
                        "2.2.140.124\t551\tNOT NULL\n" +
                        "\t343\tNULL\n" +
                        "\t77\tNULL\n" +
                        "\t519\tNULL\n" +
                        "2.2.15.163\t606\tNOT NULL\n" +
                        "2.2.245.83\t446\tNOT NULL\n" +
                        "2.2.204.60\t835\tNOT NULL\n" +
                        "2.2.7.88\t308\tNOT NULL\n" +
                        "2.2.186.59\t875\tNOT NULL\n" +
                        "2.2.89.110\t421\tNOT NULL\n" +
                        "2.2.46.225\t470\tNOT NULL\n",
                "select ip, bytes, case when ip << null then 'NULL' else 'NOT NULL' end from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4('2.2.2.2/16', 2) ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCaseIPv46() throws Exception {
        assertQuery(
                "ip\tbytes\tcase\n" +
                        "2.2.96.238\t774\tNOT NULL\n" +
                        "2.2.89.171\t404\tNOT NULL\n" +
                        "2.2.76.40\t167\tNOT NULL\n" +
                        "2.2.95.15\t803\tNOT NULL\n" +
                        "2.2.45.145\t182\tNOT NULL\n" +
                        "\t647\tNULL\n" +
                        "2.2.249.199\t203\tNOT NULL\n" +
                        "\t827\tNULL\n" +
                        "2.2.170.235\t987\tNOT NULL\n" +
                        "2.2.184.81\t614\tNOT NULL\n" +
                        "2.2.213.108\t539\tNOT NULL\n" +
                        "2.2.47.76\t585\tNOT NULL\n" +
                        "\t16\tNULL\n" +
                        "2.2.129.200\t981\tNOT NULL\n" +
                        "2.2.171.12\t313\tNOT NULL\n" +
                        "2.2.253.254\t297\tNOT NULL\n" +
                        "\t773\tNULL\n" +
                        "2.2.227.50\t411\tNOT NULL\n" +
                        "2.2.12.210\t95\tNOT NULL\n" +
                        "2.2.205.4\t916\tNOT NULL\n" +
                        "2.2.236.117\t983\tNOT NULL\n" +
                        "2.2.183.179\t369\tNOT NULL\n" +
                        "2.2.220.75\t12\tNOT NULL\n" +
                        "2.2.157.48\t613\tNOT NULL\n" +
                        "\t114\tNULL\n" +
                        "2.2.52.211\t678\tNOT NULL\n" +
                        "2.2.35.79\t262\tNOT NULL\n" +
                        "2.2.207.241\t497\tNOT NULL\n" +
                        "2.2.196.29\t741\tNOT NULL\n" +
                        "2.2.228.56\t993\tNOT NULL\n" +
                        "2.2.246.213\t562\tNOT NULL\n" +
                        "2.2.126.70\t772\tNOT NULL\n" +
                        "2.2.37.167\t907\tNOT NULL\n" +
                        "2.2.234.47\t314\tNOT NULL\n" +
                        "2.2.73.129\t984\tNOT NULL\n" +
                        "2.2.112.55\t175\tNOT NULL\n" +
                        "2.2.74.124\t254\tNOT NULL\n" +
                        "2.2.167.123\t849\tNOT NULL\n" +
                        "2.2.2.123\t941\tNOT NULL\n" +
                        "2.2.140.124\t551\tNOT NULL\n" +
                        "\t343\tNULL\n" +
                        "\t77\tNULL\n" +
                        "\t519\tNULL\n" +
                        "2.2.15.163\t606\tNOT NULL\n" +
                        "2.2.245.83\t446\tNOT NULL\n" +
                        "2.2.204.60\t835\tNOT NULL\n" +
                        "2.2.7.88\t308\tNOT NULL\n" +
                        "2.2.186.59\t875\tNOT NULL\n" +
                        "2.2.89.110\t421\tNOT NULL\n" +
                        "2.2.46.225\t470\tNOT NULL\n",
                "select ip, bytes, case when ip <<= null then 'NULL' else 'NOT NULL' end from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4('2.2.2.2/16', 2) ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testContainsIPv4FunctionFactoryError() throws Exception {
        ddl("create table t (ip ipv4)");
        assertException("select * from t where ip << '1.1.1.1/35'", 28, "invalid argument: 1.1.1.1/35");
        assertException("select * from t where ip << '1.1.1.1/-1'", 28, "invalid argument: 1.1.1.1/-1");
        assertException("select * from t where ip << '1.1.1.1/A'", 28, "invalid argument: 1.1.1.1/A");
        assertException("select * from t where ip << '1.1/26'", 28, "invalid argument: 1.1/26");
    }

    @Test
    public void testCountIPv4() throws Exception {
        assertQuery(
                "count\tbytes\n" +
                        "8\t1\n" +
                        "10\t2\n" +
                        "12\t7\n" +
                        "12\t5\n" +
                        "6\t6\n" +
                        "12\t0\n" +
                        "14\t4\n" +
                        "6\t3\n" +
                        "9\t8\n" +
                        "7\t9\n" +
                        "4\t10\n",
                "select count(ip), bytes from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,10,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testCreateAsSelectCastIPv4ToStr() throws Exception {
        assertSql(
                "cast\n" +
                        "187.139.150.80\n" +
                        "18.206.96.238\n" +
                        "92.80.211.65\n" +
                        "212.159.205.29\n" +
                        "4.98.173.21\n" +
                        "199.122.166.85\n" +
                        "79.15.250.138\n" +
                        "35.86.82.23\n" +
                        "111.98.117.250\n" +
                        "205.123.179.216\n",
                "select rnd_ipv4()::string from long_sequence(10)"
        );
    }

    @Test
    public void testCreateAsSelectCastStrToIPv4() throws Exception {
        ddl("create table x as (select x::string col from long_sequence(0))");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into x values('0.0.0.6')");
        insert("insert into x values('0.0.0.7')");
        insert("insert into x values('0.0.0.8')");
        insert("insert into x values('0.0.0.9')");
        insert("insert into x values('0.0.0.10')");

        engine.releaseInactive();

        assertQuery(
                "col\n" +
                        "0.0.0.1" + '\n' +
                        "0.0.0.2" + '\n' +
                        "0.0.0.3" + '\n' +
                        "0.0.0.4" + '\n' +
                        "0.0.0.5" + '\n' +
                        "0.0.0.6" + '\n' +
                        "0.0.0.7" + '\n' +
                        "0.0.0.8" + '\n' +
                        "0.0.0.9" + '\n' +
                        "0.0.0.10" + '\n',
                "select * from y",
                "create table y as (x), cast(col as ipv4)",
                null,
                true,
                true
        );
    }

    @Test
    public void testExplicitCastIPv4ToStr() throws Exception {
        assertSql("cast\n" +
                "1.1.1.1\n", "select ipv4 '1.1.1.1'::string");
    }

    @Test
    public void testExplicitCastIPv4ToStr2() throws Exception {
        assertSql("cast\n" +
                "1.1.1.1\n", "select '1.1.1.1'::ipv4::string");
    }

    @Test
    public void testExplicitCastIntToIPv4() throws Exception {
        assertSql(
                "cast\n" +
                        "18.206.96.238\n" +
                        "212.159.205.29\n" +
                        "199.122.166.85\n" +
                        "35.86.82.23\n" +
                        "205.123.179.216\n" +
                        "134.75.235.20\n" +
                        "162.25.160.241\n" +
                        "92.26.178.136\n" +
                        "93.204.45.145\n" +
                        "20.62.93.114\n",
                "select rnd_int()::ipv4 from long_sequence(10)"
        );
    }

    @Test
    public void testExplicitCastNullToIPv4() throws Exception {
        assertSql(
                "cast\n" +
                        "\n" +
                        "187.139.150.80\n" +
                        "18.206.96.238\n" +
                        "92.80.211.65\n" +
                        "212.159.205.29\n" +
                        "4.98.173.21\n" +
                        "199.122.166.85\n" +
                        "79.15.250.138\n" +
                        "35.86.82.23\n" +
                        "111.98.117.250\n",
                "select cast(case when x = 1 then null else rnd_ipv4() end as string) from long_sequence(10)"
        );
    }

    @Test
    public void testExplicitCastStrToIPv4() throws Exception {
        assertSql(
                "cast\n" +
                        "18.206.96.238\n" +
                        "212.159.205.29\n" +
                        "199.122.166.85\n" +
                        "35.86.82.23\n" +
                        "205.123.179.216\n" +
                        "134.75.235.20\n" +
                        "162.25.160.241\n" +
                        "92.26.178.136\n" +
                        "93.204.45.145\n" +
                        "20.62.93.114\n",
                "select rnd_ipv4()::string::ipv4 from long_sequence(10)"
        );
    }

    @Test
    public void testExplicitCastStrToIPv4Null() throws Exception {
        assertSql(
                "cast\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                "select rnd_str()::ipv4 from long_sequence(10)"
        );
    }

    @Test
    public void testFirstIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
        assertSql(
                "first\n" +
                        "10.5.96.238\n",
                "select first(ip) from test"
        );
    }

    @Test
    public void testFullJoinIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(100))");
        ddl("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(100))");
        assertSql(
                "select a.count, a.ip, b.ip2, b.count2 from '*!*test' a join '*!*test2' b on b.ip2 = a.ip",
                "count\tip\tip2\tcount2\n" +
                        "1\t12.5.9.227\t12.5.9.227\t2\n" +
                        "1\t12.5.9.23\t12.5.9.23\t2\n" +
                        "1\t12.5.9.145\t12.5.9.145\t2\n" +
                        "1\t12.5.9.159\t12.5.9.159\t2\n" +
                        "1\t12.5.9.159\t12.5.9.159\t2\n" +
                        "1\t12.5.9.115\t12.5.9.115\t2\n" +
                        "1\t12.5.9.216\t12.5.9.216\t2\n" +
                        "1\t12.5.9.216\t12.5.9.216\t2\n" +
                        "1\t12.5.9.216\t12.5.9.216\t2\n" +
                        "1\t12.5.9.48\t12.5.9.48\t2\n" +
                        "1\t12.5.9.228\t12.5.9.228\t2\n" +
                        "1\t12.5.9.228\t12.5.9.228\t2\n" +
                        "1\t12.5.9.117\t12.5.9.117\t2\n" +
                        "1\t12.5.9.179\t12.5.9.179\t2\n" +
                        "1\t12.5.9.48\t12.5.9.48\t2\n" +
                        "1\t12.5.9.26\t12.5.9.26\t2\n" +
                        "1\t12.5.9.240\t12.5.9.240\t2\n" +
                        "1\t12.5.9.194\t12.5.9.194\t2\n" +
                        "1\t12.5.9.137\t12.5.9.137\t2\n" +
                        "1\t12.5.9.179\t12.5.9.179\t2\n" +
                        "1\t12.5.9.179\t12.5.9.179\t2\n" +
                        "1\t12.5.9.159\t12.5.9.159\t2\n" +
                        "1\t12.5.9.159\t12.5.9.159\t2\n" +
                        "1\t12.5.9.215\t12.5.9.215\t2\n" +
                        "1\t12.5.9.184\t12.5.9.184\t2\n" +
                        "1\t12.5.9.46\t12.5.9.46\t2\n" +
                        "1\t12.5.9.184\t12.5.9.184\t2\n" +
                        "1\t12.5.9.147\t12.5.9.147\t2\n" +
                        "1\t12.5.9.152\t12.5.9.152\t2\n" +
                        "1\t12.5.9.28\t12.5.9.28\t2\n" +
                        "1\t12.5.9.20\t12.5.9.20\t2\n" +
                        "1\t12.5.9.20\t12.5.9.20\t2\n",
                true
        );
    }

    @Test
    public void testFullJoinIPv4Fails() throws Exception {
        ddl("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(100))");
        ddl("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(100))");
        engine.releaseInactive();
        String query = "select a.count, a.ip, b.ip2, b.count2 from '*!*test' a join '*!*test2' b on b.ip2 = a.count";
        assertMemoryLeak(() -> {
            try {
                assertException(query, sqlExecutionContext, true);
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "join column type mismatch");
                Assert.assertEquals(Chars.toString(query), 76, ex.getPosition());
            }
        });
    }

    @Test
    public void testGreaterThanEqIPv4() throws Exception {
        assertSql("column\n" +
                "true\n", "select ipv4 '34.11.45.3' >= ipv4 '22.1.200.89'");
    }

    @Test
    public void testGreaterThanEqIPv4BadStr() throws Exception {
        assertSql("column\n" +
                "false\n", "select ipv4 '34.11.45.3' >= ipv4 'apple'");
    }

    @Test
    public void testGreaterThanEqIPv4Null() throws Exception {
        assertSql("column\n" +
                "false\n", "select ipv4 '34.11.45.3' >= ipv4 '0.0.0.0'");
    }

    @Test
    public void testGreaterThanEqIPv4Null2() throws Exception {
        assertSql("column\n" +
                "false\n", "select ipv4 '34.11.45.3' >= null");
    }

    @Test
    public void testGreaterThanIPv4() throws Exception {
        assertSql("column\n" +
                "true\n", "select ipv4 '34.11.45.3' > ipv4 '22.1.200.89'");
    }

    @Test
    public void testGroupByIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
        assertSql(
                "count\tip\n" +
                        "1\t10.5.96.238\n" +
                        "6\t\n" +
                        "1\t10.5.173.21\n" +
                        "1\t10.5.250.138\n" +
                        "1\t10.5.76.40\n" +
                        "1\t10.5.20.236\n" +
                        "1\t10.5.95.15\n" +
                        "1\t10.5.132.196\n" +
                        "1\t10.5.93.114\n" +
                        "1\t10.5.121.252\n" +
                        "1\t10.5.249.199\n" +
                        "1\t10.5.212.34\n" +
                        "1\t10.5.236.196\n" +
                        "1\t10.5.170.235\n" +
                        "1\t10.5.45.159\n",
                "select count(count), ip from test group by ip"
        );
    }

    @Test
    public void testGroupByIPv42() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
        assertSql(
                "sum\tip\n" +
                        "1\t10.5.96.238\n" +
                        "6\t\n" +
                        "1\t10.5.173.21\n" +
                        "1\t10.5.250.138\n" +
                        "1\t10.5.76.40\n" +
                        "1\t10.5.20.236\n" +
                        "1\t10.5.95.15\n" +
                        "1\t10.5.132.196\n" +
                        "1\t10.5.93.114\n" +
                        "1\t10.5.121.252\n" +
                        "1\t10.5.249.199\n" +
                        "1\t10.5.212.34\n" +
                        "1\t10.5.236.196\n" +
                        "1\t10.5.170.235\n" +
                        "1\t10.5.45.159\n",
                "select sum(count), ip from test group by ip"
        );
    }

    @Test
    public void testGroupByIPv43() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5.6/30', 2) ip, 1 count from long_sequence(20))");
        assertSql(
                "sum\tip\n" +
                        "4\t10.5.6.2\n" +
                        "6\t\n" +
                        "1\t10.5.6.1\n" +
                        "5\t10.5.6.0\n" +
                        "4\t10.5.6.3\n",
                "select sum(count), ip from test group by ip"
        );
    }

    @Test
    public void testIPv4BitOr() throws Exception {
        assertSql(
                "column\n" +
                        "255.1.1.1\n",
                "select ipv4 '1.1.1.1' | '255.0.0.0'"
        );
    }

    @Test
    public void testIPv4BitOr2() throws Exception {
        assertSql(
                "column\n" +
                        "255.1.1.1\n",
                "select '1.1.1.1' | ipv4 '255.0.0.0'"
        );
    }

    @Test
    public void testIPv4BitwiseAndConst() throws Exception {
        assertSql(
                "column\n" +
                        "0.0.1.1\n",
                "select ipv4 '1.1.1.1' & ipv4 '0.0.1.1'"
        );
    }

    @Test
    public void testIPv4BitwiseAndFails() throws Exception {
        assertQuery(
                "column\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                "select ip & ipv4 'apple' from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4BitwiseAndFailsConst() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select ipv4 '1.1.1.1' & ipv4 '0.0.1'"
        );
    }

    @Test
    public void testIPv4BitwiseAndFailsStr() throws Exception {
        assertException("select '1.1.1.1' & '0.0.1.1'", 0, "inconvertible value: `1.1.1.1` [STRING -> LONG]");
    }

    @Test
    public void testIPv4BitwiseAndHalfConst() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.0.12\n" +
                        "0.0.0.9\n" +
                        "0.0.0.13\n" +
                        "0.0.0.8\n" +
                        "0.0.0.9\n" +
                        "0.0.0.20\n" +
                        "0.0.0.9\n" +
                        "0.0.0.15\n" +
                        "0.0.0.8\n" +
                        "0.0.0.20\n" +
                        "0.0.0.15\n" +
                        "0.0.0.15\n" +
                        "0.0.0.4\n" +
                        "0.0.0.8\n" +
                        "0.0.0.16\n" +
                        "0.0.0.19\n" +
                        "0.0.0.14\n" +
                        "0.0.0.3\n" +
                        "0.0.0.2\n" +
                        "0.0.0.20\n" +
                        "0.0.0.8\n" +
                        "0.0.0.9\n" +
                        "0.0.0.13\n" +
                        "0.0.0.8\n" +
                        "0.0.0.5\n" +
                        "0.0.0.18\n" +
                        "0.0.0.20\n" +
                        "0.0.0.20\n" +
                        "0.0.0.5\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "0.0.0.10\n" +
                        "0.0.0.9\n" +
                        "0.0.0.16\n" +
                        "0.0.0.6\n" +
                        "0.0.0.7\n" +
                        "0.0.0.18\n" +
                        "0.0.0.2\n" +
                        "0.0.0.17\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.9\n" +
                        "0.0.0.9\n" +
                        "0.0.0.1\n" +
                        "0.0.0.7\n" +
                        "0.0.0.16\n" +
                        "0.0.0.4\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.4\n" +
                        "0.0.0.10\n" +
                        "0.0.0.17\n" +
                        "0.0.0.11\n" +
                        "0.0.0.5\n" +
                        "0.0.0.18\n" +
                        "0.0.0.15\n" +
                        "0.0.0.4\n" +
                        "0.0.0.2\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.1\n" +
                        "0.0.0.13\n" +
                        "0.0.0.8\n" +
                        "0.0.0.19\n" +
                        "0.0.0.7\n" +
                        "0.0.0.18\n" +
                        "0.0.0.6\n" +
                        "0.0.0.2\n" +
                        "0.0.0.3\n" +
                        "0.0.0.2\n" +
                        "0.0.0.16\n" +
                        "0.0.0.12\n" +
                        "0.0.0.1\n" +
                        "0.0.0.11\n" +
                        "0.0.0.6\n" +
                        "0.0.0.6\n" +
                        "0.0.0.3\n" +
                        "0.0.0.10\n" +
                        "0.0.0.15\n" +
                        "0.0.0.5\n" +
                        "0.0.0.6\n" +
                        "0.0.0.2\n" +
                        "0.0.0.9\n" +
                        "0.0.0.16\n" +
                        "0.0.0.18\n" +
                        "0.0.0.15\n" +
                        "0.0.0.16\n" +
                        "0.0.0.9\n" +
                        "0.0.0.1\n" +
                        "0.0.0.20\n" +
                        "0.0.0.18\n" +
                        "0.0.0.15\n" +
                        "0.0.0.10\n" +
                        "0.0.0.12\n" +
                        "0.0.0.1\n" +
                        "0.0.0.7\n" +
                        "0.0.0.5\n" +
                        "0.0.0.11\n" +
                        "0.0.0.16\n" +
                        "0.0.0.12\n",
                "select ip & ipv4 '255.255.255.255' from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4BitwiseAndVar() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.0.12\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "0.0.0.8\n" +
                        "0.0.0.16\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "0.0.0.20\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "\n" +
                        "0.0.0.9\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "\n" +
                        "0.0.0.6\n" +
                        "0.0.0.5\n" +
                        "0.0.0.1\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.18\n" +
                        "0.0.0.1\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.6\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.2\n" +
                        "0.0.0.14\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.8\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "0.0.0.8\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "0.0.0.6\n" +
                        "0.0.0.2\n" +
                        "0.0.0.16\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.16\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.5\n" +
                        "0.0.0.1\n" +
                        "\n" +
                        "0.0.0.12\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "0.0.0.10\n" +
                        "0.0.0.8\n" +
                        "0.0.0.9\n" +
                        "0.0.0.8\n" +
                        "0.0.0.16\n" +
                        "0.0.0.2\n" +
                        "0.0.0.16\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.6\n",
                "select ip & ip2 from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4BitwiseNotConst() throws Exception {
        assertSql(
                "column\n" +
                        "254.254.254.254\n",
                "select ~ ipv4 '1.1.1.1'"
        );
    }

    @Test
    public void testIPv4BitwiseNotFails() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select ~ ipv4 'apple'"
        );
    }

    @Test
    public void testIPv4BitwiseNotVar() throws Exception {
        assertQuery(
                "column\n" +
                        "255.255.255.243\n" +
                        "255.255.255.246\n" +
                        "255.255.255.242\n" +
                        "255.255.255.247\n" +
                        "255.255.255.246\n" +
                        "255.255.255.235\n" +
                        "255.255.255.246\n" +
                        "255.255.255.240\n" +
                        "255.255.255.247\n" +
                        "255.255.255.235\n" +
                        "255.255.255.240\n" +
                        "255.255.255.240\n" +
                        "255.255.255.251\n" +
                        "255.255.255.247\n" +
                        "255.255.255.239\n" +
                        "255.255.255.236\n" +
                        "255.255.255.241\n" +
                        "255.255.255.252\n" +
                        "255.255.255.253\n" +
                        "255.255.255.235\n" +
                        "255.255.255.247\n" +
                        "255.255.255.246\n" +
                        "255.255.255.242\n" +
                        "255.255.255.247\n" +
                        "255.255.255.250\n" +
                        "255.255.255.237\n" +
                        "255.255.255.235\n" +
                        "255.255.255.235\n" +
                        "255.255.255.250\n" +
                        "255.255.255.250\n" +
                        "255.255.255.251\n" +
                        "255.255.255.245\n" +
                        "255.255.255.246\n" +
                        "255.255.255.239\n" +
                        "255.255.255.249\n" +
                        "255.255.255.248\n" +
                        "255.255.255.237\n" +
                        "255.255.255.253\n" +
                        "255.255.255.238\n" +
                        "255.255.255.251\n" +
                        "255.255.255.250\n" +
                        "255.255.255.246\n" +
                        "255.255.255.246\n" +
                        "255.255.255.254\n" +
                        "255.255.255.248\n" +
                        "255.255.255.239\n" +
                        "255.255.255.251\n" +
                        "255.255.255.254\n" +
                        "255.255.255.253\n" +
                        "255.255.255.251\n" +
                        "255.255.255.245\n" +
                        "255.255.255.238\n" +
                        "255.255.255.244\n" +
                        "255.255.255.250\n" +
                        "255.255.255.237\n" +
                        "255.255.255.240\n" +
                        "255.255.255.251\n" +
                        "255.255.255.253\n" +
                        "255.255.255.251\n" +
                        "255.255.255.251\n" +
                        "255.255.255.254\n" +
                        "255.255.255.242\n" +
                        "255.255.255.247\n" +
                        "255.255.255.236\n" +
                        "255.255.255.248\n" +
                        "255.255.255.237\n" +
                        "255.255.255.249\n" +
                        "255.255.255.253\n" +
                        "255.255.255.252\n" +
                        "255.255.255.253\n" +
                        "255.255.255.239\n" +
                        "255.255.255.243\n" +
                        "255.255.255.254\n" +
                        "255.255.255.244\n" +
                        "255.255.255.249\n" +
                        "255.255.255.249\n" +
                        "255.255.255.252\n" +
                        "255.255.255.245\n" +
                        "255.255.255.240\n" +
                        "255.255.255.250\n" +
                        "255.255.255.249\n" +
                        "255.255.255.253\n" +
                        "255.255.255.246\n" +
                        "255.255.255.239\n" +
                        "255.255.255.237\n" +
                        "255.255.255.240\n" +
                        "255.255.255.239\n" +
                        "255.255.255.246\n" +
                        "255.255.255.254\n" +
                        "255.255.255.235\n" +
                        "255.255.255.237\n" +
                        "255.255.255.240\n" +
                        "255.255.255.245\n" +
                        "255.255.255.243\n" +
                        "255.255.255.254\n" +
                        "255.255.255.248\n" +
                        "255.255.255.250\n" +
                        "255.255.255.244\n" +
                        "255.255.255.239\n" +
                        "255.255.255.243\n",
                "select ~ip from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4BitwiseOr() throws Exception {
        assertQuery(
                "column\n" +
                        "255.0.0.12\n" +
                        "255.0.0.9\n" +
                        "255.0.0.13\n" +
                        "255.0.0.8\n" +
                        "255.0.0.9\n" +
                        "255.0.0.20\n" +
                        "255.0.0.9\n" +
                        "255.0.0.15\n" +
                        "255.0.0.8\n" +
                        "255.0.0.20\n" +
                        "255.0.0.15\n" +
                        "255.0.0.15\n" +
                        "255.0.0.4\n" +
                        "255.0.0.8\n" +
                        "255.0.0.16\n" +
                        "255.0.0.19\n" +
                        "255.0.0.14\n" +
                        "255.0.0.3\n" +
                        "255.0.0.2\n" +
                        "255.0.0.20\n" +
                        "255.0.0.8\n" +
                        "255.0.0.9\n" +
                        "255.0.0.13\n" +
                        "255.0.0.8\n" +
                        "255.0.0.5\n" +
                        "255.0.0.18\n" +
                        "255.0.0.20\n" +
                        "255.0.0.20\n" +
                        "255.0.0.5\n" +
                        "255.0.0.5\n" +
                        "255.0.0.4\n" +
                        "255.0.0.10\n" +
                        "255.0.0.9\n" +
                        "255.0.0.16\n" +
                        "255.0.0.6\n" +
                        "255.0.0.7\n" +
                        "255.0.0.18\n" +
                        "255.0.0.2\n" +
                        "255.0.0.17\n" +
                        "255.0.0.4\n" +
                        "255.0.0.5\n" +
                        "255.0.0.9\n" +
                        "255.0.0.9\n" +
                        "255.0.0.1\n" +
                        "255.0.0.7\n" +
                        "255.0.0.16\n" +
                        "255.0.0.4\n" +
                        "255.0.0.1\n" +
                        "255.0.0.2\n" +
                        "255.0.0.4\n" +
                        "255.0.0.10\n" +
                        "255.0.0.17\n" +
                        "255.0.0.11\n" +
                        "255.0.0.5\n" +
                        "255.0.0.18\n" +
                        "255.0.0.15\n" +
                        "255.0.0.4\n" +
                        "255.0.0.2\n" +
                        "255.0.0.4\n" +
                        "255.0.0.4\n" +
                        "255.0.0.1\n" +
                        "255.0.0.13\n" +
                        "255.0.0.8\n" +
                        "255.0.0.19\n" +
                        "255.0.0.7\n" +
                        "255.0.0.18\n" +
                        "255.0.0.6\n" +
                        "255.0.0.2\n" +
                        "255.0.0.3\n" +
                        "255.0.0.2\n" +
                        "255.0.0.16\n" +
                        "255.0.0.12\n" +
                        "255.0.0.1\n" +
                        "255.0.0.11\n" +
                        "255.0.0.6\n" +
                        "255.0.0.6\n" +
                        "255.0.0.3\n" +
                        "255.0.0.10\n" +
                        "255.0.0.15\n" +
                        "255.0.0.5\n" +
                        "255.0.0.6\n" +
                        "255.0.0.2\n" +
                        "255.0.0.9\n" +
                        "255.0.0.16\n" +
                        "255.0.0.18\n" +
                        "255.0.0.15\n" +
                        "255.0.0.16\n" +
                        "255.0.0.9\n" +
                        "255.0.0.1\n" +
                        "255.0.0.20\n" +
                        "255.0.0.18\n" +
                        "255.0.0.15\n" +
                        "255.0.0.10\n" +
                        "255.0.0.12\n" +
                        "255.0.0.1\n" +
                        "255.0.0.7\n" +
                        "255.0.0.5\n" +
                        "255.0.0.11\n" +
                        "255.0.0.16\n" +
                        "255.0.0.12\n",
                "select ip | ipv4 '255.0.0.0' from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4BitwiseOrConst() throws Exception {
        assertSql(
                "column\n" +
                        "255.1.1.1\n",
                "select ipv4 '1.1.1.1' | ipv4 '255.0.0.0'"
        );
    }

    @Test
    public void testIPv4BitwiseOrFailsStr() throws Exception {
        assertException("select '1.1.1.1' | '0.0.1.1'", 0, "inconvertible value: `1.1.1.1` [STRING -> LONG]");
    }

    @Test
    public void testIPv4BitwiseOrVar() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.0.12\n" +
                        "0.0.0.22\n" +
                        "0.0.0.27\n" +
                        "0.0.0.29\n" +
                        "0.0.0.17\n" +
                        "0.0.0.23\n" +
                        "0.0.0.15\n" +
                        "0.0.0.15\n" +
                        "0.0.0.19\n" +
                        "0.0.0.31\n" +
                        "0.0.0.11\n" +
                        "0.0.0.20\n" +
                        "0.0.0.12\n" +
                        "0.0.0.23\n" +
                        "0.0.0.7\n" +
                        "0.0.0.19\n" +
                        "0.0.0.15\n" +
                        "0.0.0.13\n" +
                        "0.0.0.20\n" +
                        "0.0.0.17\n" +
                        "0.0.0.6\n" +
                        "0.0.0.15\n" +
                        "0.0.0.25\n" +
                        "0.0.0.13\n" +
                        "0.0.0.13\n" +
                        "0.0.0.29\n" +
                        "0.0.0.15\n" +
                        "0.0.0.29\n" +
                        "0.0.0.12\n" +
                        "0.0.0.21\n" +
                        "0.0.0.27\n" +
                        "0.0.0.15\n" +
                        "0.0.0.18\n" +
                        "0.0.0.15\n" +
                        "0.0.0.21\n" +
                        "0.0.0.12\n" +
                        "0.0.0.7\n" +
                        "0.0.0.22\n" +
                        "0.0.0.7\n" +
                        "0.0.0.22\n" +
                        "0.0.0.7\n" +
                        "0.0.0.19\n" +
                        "0.0.0.26\n" +
                        "0.0.0.3\n" +
                        "0.0.0.14\n" +
                        "0.0.0.7\n" +
                        "0.0.0.30\n" +
                        "0.0.0.21\n" +
                        "0.0.0.7\n" +
                        "0.0.0.14\n" +
                        "0.0.0.30\n" +
                        "0.0.0.15\n" +
                        "0.0.0.19\n" +
                        "0.0.0.29\n" +
                        "0.0.0.23\n" +
                        "0.0.0.12\n" +
                        "0.0.0.19\n" +
                        "0.0.0.15\n" +
                        "0.0.0.13\n" +
                        "0.0.0.28\n" +
                        "0.0.0.11\n" +
                        "0.0.0.6\n" +
                        "0.0.0.22\n" +
                        "0.0.0.21\n" +
                        "0.0.0.3\n" +
                        "0.0.0.1\n" +
                        "0.0.0.29\n" +
                        "0.0.0.30\n" +
                        "0.0.0.23\n" +
                        "0.0.0.27\n" +
                        "0.0.0.30\n" +
                        "0.0.0.14\n" +
                        "0.0.0.11\n" +
                        "0.0.0.19\n" +
                        "0.0.0.20\n" +
                        "0.0.0.25\n" +
                        "0.0.0.26\n" +
                        "0.0.0.30\n" +
                        "0.0.0.30\n" +
                        "0.0.0.17\n" +
                        "0.0.0.15\n" +
                        "0.0.0.15\n" +
                        "0.0.0.15\n" +
                        "0.0.0.31\n" +
                        "0.0.0.23\n" +
                        "0.0.0.14\n" +
                        "0.0.0.11\n" +
                        "0.0.0.29\n" +
                        "0.0.0.15\n" +
                        "0.0.0.11\n" +
                        "0.0.0.13\n" +
                        "0.0.0.15\n" +
                        "0.0.0.23\n" +
                        "0.0.0.22\n" +
                        "0.0.0.23\n" +
                        "0.0.0.15\n" +
                        "0.0.0.15\n" +
                        "0.0.0.14\n" +
                        "0.0.0.11\n" +
                        "0.0.0.15\n",
                "select ip | ip2 from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4ContainsEqSubnet() throws Exception {
        ddl("create table test (col ipv4)");
        insert("insert into test values('12.67.45.3')");
        insert("insert into test values('160.5.22.8')");
        insert("insert into test values('240.110.88.22')");
        insert("insert into test values('1.6.2.0')");
        insert("insert into test values('255.255.255.255')");
        insert("insert into test values('0.0.0.0')");

        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where col <<= '12.67.50.2/20'", sink, "col\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where col <<= '12.67.50.2/1'", sink, "col\n" +
                "12.67.45.3\n" +
                "1.6.2.0\n" +
                "\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where col <<= '255.6.8.10/8'", sink, "col\n" +
                "255.255.255.255\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where col <<= '12.67.50.2/0'", sink, "col\n" +
                "12.67.45.3\n" +
                "160.5.22.8\n" +
                "240.110.88.22\n" +
                "1.6.2.0\n" +
                "255.255.255.255\n" +
                "\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where col <<= '1.6.2.0/32'", sink, "col\n" +
                "1.6.2.0\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where col <<= '1.6.2.0'", sink, "col\n" +
                "1.6.2.0\n");

    }

    @Test
    public void testIPv4ContainsEqSubnetAndMask() throws Exception {
        ddl("create table test as (select rnd_int(0,1000,0)::ipv4 ip from long_sequence(100))");

        assertSql(
                "ip\n" +
                        "0.0.0.167\n" +
                        "0.0.0.182\n" +
                        "0.0.0.108\n" +
                        "0.0.0.95\n" +
                        "0.0.0.12\n" +
                        "0.0.0.71\n" +
                        "0.0.0.10\n" +
                        "0.0.0.238\n" +
                        "0.0.0.105\n" +
                        "0.0.0.203\n" +
                        "0.0.0.86\n" +
                        "0.0.0.100\n" +
                        "0.0.0.144\n" +
                        "0.0.0.173\n" +
                        "0.0.0.121\n" +
                        "0.0.0.231\n" +
                        "0.0.0.181\n" +
                        "0.0.0.218\n" +
                        "0.0.0.34\n" +
                        "0.0.0.90\n" +
                        "\n" +
                        "0.0.0.161\n" +
                        "0.0.0.188\n" +
                        "\n" +
                        "0.0.0.29\n" +
                        "0.0.0.159\n",
                "select * from test where ip <<= '0.0.0/24'"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFails() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0.1/hello'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0.1/hello"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFails2() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0/hello'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0/hello"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFails3() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0.0/65'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0.0/65"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFails4() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0/65'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0/65"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFails5() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0.0/-1'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0.0/-1"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFails6() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0/-1'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0/-1"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsChars() throws Exception {
        assertException(
                "select * from test where ip <<= 'apple'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: apple"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsNetmaskOverflow() throws Exception {
        assertException(
                "select * from test where ip <<= '85.7.36/74'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 85.7.36/74"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsNums() throws Exception {
        assertException(
                "select * from test where ip <<= '8573674'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 8573674"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsOverflow() throws Exception {
        assertException(
                "select * from test where ip <<= '256.256.256.256'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 256.256.256.256"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetIncorrectMask() throws Exception {
        assertException(
                "select * from test where ip <<= '0.0.0/32'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                32,
                "invalid argument: 0.0.0/32"
        );
    }

    @Test
    public void testIPv4ContainsEqSubnetNoMask() throws Exception {
        ddl("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(100))");

        assertSql(
                "ip\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n",
                "select * from test where ip <<= '0.0.0.4'"
        );
    }

    @Test
    public void testIPv4ContainsSubnet() throws Exception {
        ddl("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
        assertSql("ip\n", "select * from test where ip << '0.0.0.1'");
    }

    @Test
    public void testIPv4ContainsSubnet2() throws Exception {
        ddl("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
        assertSql("ip\n", "select * from test where ip << '0.0.0.1/32'");
    }

    @Test
    public void testIPv4ContainsSubnet3() throws Exception {
        ddl("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
        assertSql(
                "ip\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n",
                "select * from test where ip << '0.0.0.1/24'"
        );
    }

    @Test
    public void testIPv4CountDistinct() throws Exception {
        assertQuery("count_distinct\n" +
                        "20\n", "select count_distinct(ip) from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                false,
                true
        );
    }

    @Test
    public void testIPv4Distinct() throws Exception {
        assertQuery(
                "ip\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.3\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n",
                "select distinct ip from test order by ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testIPv4EqArgsSwapped() throws Exception {
        ddl("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

        assertSql(
                "ip\n" +
                        "0.0.0.1\n",
                "select * from test where ipv4 '0.0.0.1' = ip"
        );
    }

    @Test
    public void testIPv4EqNegated() throws Exception {
        ddl("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

        assertSql(
                "ip\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "\n" +
                        "0.0.0.3\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "\n",
                "select * from test where ip != ipv4 '0.0.0.1'"
        );
    }

    @Test
    public void testIPv4EqNegated2() throws Exception {
        ddl("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

        assertSql(
                "ip\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "\n" +
                        "0.0.0.3\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "0.0.0.4\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "\n",
                "select * from test where ipv4 '0.0.0.1' != ip"
        );
    }

    @Test
    public void testIPv4EqNull() throws Exception {
        ddl("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(100))");
        assertSql(
                "ip\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                "select * from test where ip = null"
        );
    }

    @Test
    public void testIPv4Except() throws Exception {
        ddl("create table x (col1 ipv4)");
        ddl("create table y (col2 ipv4)");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into x values('0.0.0.6')");
        insert("insert into y values('0.0.0.1')");
        insert("insert into y values('0.0.0.2')");
        insert("insert into y values('0.0.0.3')");
        insert("insert into y values('0.0.0.4')");
        insert("insert into y values('0.0.0.5')");

        assertSql(
                "col1\n" +
                        "0.0.0.6\n",
                "select col1 from x except select col2 from y"
        );
    }

    @Test
    public void testIPv4Explain() throws Exception {
        assertQuery(
                "QUERY PLAN\n" +
                        "Sort light\n" +
                        "  keys: [ip desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: test\n",
                "explain select * from test order by ip desc",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testIPv4Intersect() throws Exception {
        ddl("create table x (col1 ipv4)");
        ddl("create table y (col2 ipv4)");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into x values('0.0.0.6')");
        insert("insert into y values('0.0.0.1')");
        insert("insert into y values('0.0.0.2')");

        assertSql(
                "col1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n",
                "select col1 from x intersect all select col2 from y"
        );
    }

    @Test
    public void testIPv4IsOrderedAsc() throws Exception {
        ddl("create table test (ip ipv4, bytes int)");
        insert("insert into test values ('0.0.0.1', 1)");
        insert("insert into test values ('0.0.0.2', 1)");
        insert("insert into test values ('0.0.0.3', 1)");
        insert("insert into test values ('0.0.0.4', 1)");
        insert("insert into test values ('0.0.0.5', 1)");

        assertSql(
                "isOrdered\n" +
                        "true\n",
                "select isOrdered(ip) from test"
        );
    }

    @Test
    public void testIPv4IsOrderedFalse() throws Exception {
        assertQuery(
                "isOrdered\n" +
                        "false\n",
                "select isOrdered(ip) from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                false,
                true
        );
    }

    @Test
    public void testIPv4IsOrderedNull() throws Exception {
        ddl("create table test (ip ipv4, bytes int)");
        insert("insert into test values ('0.0.0.0', 1)");
        insert("insert into test values ('0.0.0.0', 1)");
        insert("insert into test values ('0.0.0.0', 1)");
        insert("insert into test values ('0.0.0.0', 1)");
        insert("insert into test values ('0.0.0.0', 1)");

        assertSql(
                "isOrdered\n" +
                        "true\n",
                "select isOrdered(ip) from test"
        );
    }

    @Test
    public void testIPv4IsOrderedSame() throws Exception {
        ddl("create table test (ip ipv4, bytes int)");
        insert("insert into test values ('0.0.0.12', 1)");
        insert("insert into test values ('0.0.0.12', 1)");
        insert("insert into test values ('0.0.0.12', 1)");
        insert("insert into test values ('0.0.0.12', 1)");
        insert("insert into test values ('0.0.0.12', 1)");

        assertSql(
                "isOrdered\n" +
                        "true\n",
                "select isOrdered(ip) from test"
        );
    }

    @Test
    public void testIPv4MinusIPv4Const() throws Exception {
        assertSql(
                "column\n" +
                        "-4278124286\n",
                "select ipv4 '1.1.1.1' - ipv4 '255.255.255.255'"
        );
    }

    @Test
    public void testIPv4MinusIPv4ConstNull() throws Exception {
        assertSql(
                "column\n" +
                        "NaN\n",
                "select ipv4 '0.0.0.0' - ipv4 '1.1.1.1'"
        );
    }

    @Test
    public void testIPv4MinusIPv4HalfConst() throws Exception {
        assertQuery(
                "column\n" +
                        "2773848137\n" +
                        "-299063538\n" +
                        "1496084467\n" +
                        "2485446343\n" +
                        "1196850877\n" +
                        "1158191828\n" +
                        "752939968\n" +
                        "3837158002\n" +
                        "2820505953\n" +
                        "2797158930\n" +
                        "860245551\n" +
                        "1326914642\n" +
                        "3499386522\n" +
                        "3619032084\n" +
                        "3460716594\n" +
                        "3438474390\n" +
                        "-304373661\n" +
                        "86179701\n" +
                        "2503987003\n" +
                        "2347192664\n" +
                        "3255296908\n" +
                        "1265208177\n" +
                        "2135218764\n" +
                        "-211046476\n" +
                        "2383725862\n" +
                        "2622936746\n" +
                        "1484573162\n" +
                        "823377430\n" +
                        "2720404929\n" +
                        "2339832612\n" +
                        "862156863\n" +
                        "3548828754\n" +
                        "-257891288\n" +
                        "3190861944\n" +
                        "2198440386\n" +
                        "1846652797\n" +
                        "2153992830\n" +
                        "1422720116\n" +
                        "493192821\n" +
                        "466104543\n" +
                        "3304290560\n" +
                        "3854300225\n" +
                        "1834010571\n" +
                        "1811077867\n" +
                        "1226040229\n" +
                        "3639006165\n" +
                        "865851868\n" +
                        "642416689\n" +
                        "990194656\n" +
                        "3282022737\n",
                "select ip - ipv4 '22.54.6.7' from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_ipv4() ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(50)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4MinusIPv4Str() throws Exception {
        assertSql(
                "column\n" +
                        "16843008\n",
                "select ipv4 '1.1.1.1' - '0.0.0.1'"
        );
    }

    @Test
    public void testIPv4MinusIPv4Var() throws Exception {
        assertQuery(
                "column\n" +
                        "-11\n" +
                        "17\n" +
                        "121\n" +
                        "129\n" +
                        "40\n" +
                        "-76\n" +
                        "-88\n" +
                        "50\n" +
                        "-111\n" +
                        "47\n" +
                        "154\n" +
                        "-18\n" +
                        "131\n" +
                        "136\n" +
                        "164\n" +
                        "-74\n" +
                        "-11\n" +
                        "-15\n" +
                        "-49\n" +
                        "42\n" +
                        "67\n" +
                        "-59\n" +
                        "-31\n" +
                        "14\n" +
                        "57\n" +
                        "-45\n" +
                        "-165\n" +
                        "45\n" +
                        "-55\n" +
                        "31\n" +
                        "-16\n" +
                        "74\n" +
                        "8\n" +
                        "153\n" +
                        "-84\n" +
                        "-45\n" +
                        "-35\n" +
                        "169\n" +
                        "71\n" +
                        "92\n" +
                        "47\n" +
                        "-28\n" +
                        "113\n" +
                        "-77\n" +
                        "104\n" +
                        "25\n" +
                        "191\n" +
                        "213\n" +
                        "50\n" +
                        "-96\n",
                "select ip2 - ip from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4('2.65.11.1/24', 0) ip," +
                        "    rnd_ipv4('2.65.11.1/24', 0) ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(50)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4MinusIntConst() throws Exception {
        assertSql(
                "column\n" +
                        "1.1.1.0\n",
                "select ipv4 '1.1.1.1' - 1"
        );
    }

    @Test
    public void testIPv4MinusIntConst2() throws Exception {
        assertSql(
                "column\n" +
                        "0.0.0.1\n",
                "select ipv4 '1.1.1.1' - 16843008"
        );
    }

    @Test
    public void testIPv4MinusIntConstOverflow() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select ipv4 '1.1.1.1' - 16843010"
        );
    }

    @Test
    public void testIPv4MinusIntHalfConst() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.0.7\n" +
                        "\n" +
                        "0.0.0.13\n" +
                        "0.0.0.15\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.10\n" +
                        "0.0.0.4\n" +
                        "0.0.0.14\n" +
                        "0.0.0.14\n" +
                        "\n" +
                        "0.0.0.15\n" +
                        "0.0.0.3\n" +
                        "0.0.0.11\n" +
                        "0.0.0.2\n" +
                        "0.0.0.13\n" +
                        "0.0.0.6\n" +
                        "0.0.0.8\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.12\n" +
                        "0.0.0.8\n" +
                        "\n" +
                        "0.0.0.11\n" +
                        "0.0.0.4\n" +
                        "0.0.0.11\n" +
                        "0.0.0.7\n" +
                        "0.0.0.12\n" +
                        "0.0.0.5\n" +
                        "0.0.0.1\n" +
                        "0.0.0.13\n" +
                        "0.0.0.10\n" +
                        "\n" +
                        "0.0.0.3\n" +
                        "\n" +
                        "0.0.0.11\n" +
                        "\n" +
                        "0.0.0.13\n" +
                        "\n" +
                        "0.0.0.14\n" +
                        "0.0.0.11\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "0.0.0.1\n" +
                        "0.0.0.7\n" +
                        "0.0.0.12\n" +
                        "0.0.0.1\n" +
                        "0.0.0.3\n" +
                        "0.0.0.9\n" +
                        "0.0.0.10\n" +
                        "0.0.0.11\n" +
                        "0.0.0.4\n" +
                        "0.0.0.13\n" +
                        "0.0.0.3\n" +
                        "0.0.0.13\n" +
                        "0.0.0.2\n" +
                        "0.0.0.7\n" +
                        "0.0.0.11\n" +
                        "\n" +
                        "0.0.0.1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.12\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "0.0.0.13\n" +
                        "0.0.0.12\n" +
                        "0.0.0.13\n" +
                        "0.0.0.13\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.13\n" +
                        "\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.11\n" +
                        "0.0.0.13\n" +
                        "0.0.0.11\n" +
                        "\n" +
                        "0.0.0.2\n" +
                        "\n" +
                        "0.0.0.8\n" +
                        "0.0.0.14\n" +
                        "0.0.0.7\n" +
                        "\n" +
                        "0.0.0.15\n" +
                        "0.0.0.6\n" +
                        "0.0.0.3\n" +
                        "0.0.0.8\n" +
                        "0.0.0.5\n" +
                        "0.0.0.15\n" +
                        "0.0.0.1\n" +
                        "0.0.0.14\n" +
                        "\n" +
                        "\n" +
                        "0.0.0.5\n" +
                        "\n" +
                        "0.0.0.1\n",
                "select ip - 5 from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4MinusIntVar() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.4.203\n" +
                        "0.0.6.190\n" +
                        "0.0.4.145\n" +
                        "0.0.2.139\n" +
                        "0.0.5.33\n" +
                        "0.0.2.82\n" +
                        "0.0.6.43\n" +
                        "0.0.2.70\n" +
                        "0.0.1.179\n" +
                        "0.0.2.151\n" +
                        "0.0.5.56\n" +
                        "0.0.1.142\n" +
                        "0.0.1.58\n" +
                        "0.0.2.210\n" +
                        "0.0.2.197\n" +
                        "0.0.5.228\n" +
                        "0.0.4.42\n" +
                        "0.0.3.93\n" +
                        "0.0.5.171\n" +
                        "0.0.1.116\n" +
                        "0.0.3.143\n" +
                        "0.0.3.149\n" +
                        "0.0.5.119\n" +
                        "0.0.2.2\n" +
                        "0.0.7.46\n" +
                        "0.0.3.253\n" +
                        "0.0.8.160\n" +
                        "0.0.7.202\n" +
                        "0.0.6.59\n" +
                        "0.0.2.79\n" +
                        "0.0.5.134\n" +
                        "0.0.5.79\n" +
                        "0.0.1.228\n" +
                        "0.0.0.252\n" +
                        "0.0.4.25\n" +
                        "0.0.5.239\n" +
                        "0.0.3.20\n" +
                        "0.0.1.159\n" +
                        "0.0.4.168\n" +
                        "0.0.0.203\n" +
                        "0.0.6.12\n" +
                        "0.0.5.222\n" +
                        "0.0.2.100\n" +
                        "0.0.2.99\n" +
                        "0.0.7.92\n" +
                        "0.0.4.37\n" +
                        "0.0.0.231\n" +
                        "0.0.8.152\n" +
                        "0.0.1.189\n" +
                        "0.0.4.89\n" +
                        "0.0.3.224\n" +
                        "\n" +
                        "0.0.8.229\n" +
                        "0.0.6.127\n" +
                        "0.0.6.11\n" +
                        "0.0.5.26\n" +
                        "0.0.5.250\n" +
                        "0.0.4.64\n" +
                        "0.0.2.20\n" +
                        "0.0.4.16\n" +
                        "0.0.5.235\n" +
                        "0.0.3.162\n" +
                        "0.0.8.157\n" +
                        "0.0.0.19\n" +
                        "0.0.0.244\n" +
                        "0.0.0.42\n" +
                        "0.0.2.75\n" +
                        "0.0.1.226\n" +
                        "0.0.0.253\n" +
                        "0.0.5.171\n" +
                        "0.0.7.59\n" +
                        "0.0.6.183\n" +
                        "0.0.3.38\n" +
                        "0.0.4.113\n" +
                        "0.0.2.138\n" +
                        "0.0.0.47\n" +
                        "0.0.3.150\n" +
                        "0.0.2.138\n" +
                        "0.0.4.204\n" +
                        "0.0.5.3\n" +
                        "0.0.2.67\n" +
                        "0.0.2.63\n" +
                        "0.0.8.152\n" +
                        "0.0.6.195\n" +
                        "0.0.4.52\n" +
                        "0.0.2.253\n" +
                        "0.0.2.37\n" +
                        "\n" +
                        "0.0.3.70\n" +
                        "0.0.6.69\n" +
                        "0.0.2.19\n" +
                        "0.0.3.56\n" +
                        "0.0.4.130\n" +
                        "0.0.5.164\n" +
                        "0.0.5.5\n" +
                        "0.0.8.220\n" +
                        "0.0.6.186\n" +
                        "0.0.7.42\n" +
                        "0.0.0.196\n" +
                        "\n",
                "select ip - bytes from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(500,2500,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnet() throws Exception {
        ddl("create table test (col ipv4)");
        insert("insert into test values('12.67.45.3')");
        insert("insert into test values('160.5.22.8')");
        insert("insert into test values('240.110.88.22')");
        insert("insert into test values('1.6.2.0')");
        insert("insert into test values('255.255.255.255')");
        insert("insert into test values('0.0.0.0')");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where '12.67.50.2/20' >>= col", sink, "col\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where '12.67.50.2/1' >>= col", sink, "col\n" +
                "12.67.45.3\n" +
                "1.6.2.0\n" +
                "\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where '255.6.8.10/8' >>= col", sink, "col\n" +
                "255.255.255.255\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where '12.67.50.2/0' >>= col", sink, "col\n" +
                "12.67.45.3\n" +
                "160.5.22.8\n" +
                "240.110.88.22\n" +
                "1.6.2.0\n" +
                "255.255.255.255\n" +
                "\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where '1.6.2.0/32' >>= col", sink, "col\n" +
                "1.6.2.0\n");
        TestUtils.assertSql(engine, sqlExecutionContext, "select * from test where '1.6.2.0' >>= col", sink, "col\n" +
                "1.6.2.0\n");
    }

    @Test
    public void testIPv4NegContainsEqSubnetAndMask() throws Exception {
        ddl("create table test as (select rnd_int(0,2000,0)::ipv4 ip from long_sequence(100))");

        assertSql(
                "ip\n" +
                        "0.0.0.115\n" +
                        "0.0.0.208\n" +
                        "0.0.0.110\n" +
                        "0.0.0.90\n" +
                        "0.0.0.53\n" +
                        "0.0.0.143\n" +
                        "0.0.0.246\n" +
                        "0.0.0.158\n" +
                        "0.0.0.237\n" +
                        "0.0.0.220\n" +
                        "0.0.0.184\n" +
                        "0.0.0.103\n",
                "select * from test where '0.0.0/24' >>= ip"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails() throws Exception {
        assertException("select * from test where '0.0.0.1/hello' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0.1/hello");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails2() throws Exception {
        assertException("select * from test where '0.0.0/hello' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0/hello");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails3() throws Exception {
        assertException("select * from test where '0.0.0.0/65' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0.0/65");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails4() throws Exception {
        assertException("select * from test where '0.0.0/65' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0/65");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails5() throws Exception {
        assertException("select * from test where '0.0.0.0/-1' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0.0/-1");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails6() throws Exception {
        assertException("select * from test where '0.0.0/-1' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0/-1");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsChars() throws Exception {
        assertException("select * from test where 'apple' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: apple");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsNetmaskOverflow() throws Exception {
        assertException("select * from test where '85.7.36/74' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 85.7.36/74");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsNums() throws Exception {
        assertException("select * from test where '8573674' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 8573674");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsOverflow() throws Exception {
        assertException("select * from test where '256.256.256.256' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 256.256.256.256");
    }

    @Test
    public void testIPv4NegContainsEqSubnetIncorrectMask() throws Exception {
        assertException("select * from test where '0.0.0/32' >>= ip", "create table test as " +
                "(" +
                "  select" +
                "    rnd_int(1000,2000,0)::ipv4 ip," +
                "    rnd_int(0,1000,0) bytes," +
                "    timestamp_sequence(0,100000000) k" +
                "  from long_sequence(100)" +
                ") timestamp(k)", 25, "invalid argument: 0.0.0/32");
    }

    @Test
    public void testIPv4NegContainsEqSubnetNoMask() throws Exception {
        ddl("create table test as (select rnd_int(0,5,2)::ipv4 ip from long_sequence(100))");

        assertSql(
                "ip\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n" +
                        "0.0.0.4\n",
                "select * from test where '0.0.0.4' >>= ip"
        );
    }

    @Test
    public void testIPv4NegContainsSubnet() throws Exception {
        ddl("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
        assertSql("ip\n", "select * from test where '0.0.0.1' >> ip");
    }

    @Test
    public void testIPv4NegContainsSubnet2() throws Exception {
        ddl("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
        assertSql("ip\n", "select * from test where '0.0.0.1/32' >> ip");
    }

    @Test
    public void testIPv4NegContainsSubnet3() throws Exception {
        ddl("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
        assertSql(
                "ip\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.2\n" +
                        "0.0.0.1\n",
                "select * from test where '0.0.0.1/24' >> ip"
        );
    }

    @Test
    public void testIPv4Null() throws Exception {
        ddl("create table test (col ipv4)");
        insert("insert into test values(null)");
        assertSql(
                "col\n" +
                        "\n",
                "test"
        );
    }

    @Test
    public void testIPv4NullIf() throws Exception {
        assertQuery("k\tnullif\n" +
                        "1970-01-01T00:00:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T00:01:40.000000Z\t0.0.0.9\n" +
                        "1970-01-01T00:03:20.000000Z\t0.0.0.3\n" +
                        "1970-01-01T00:05:00.000000Z\t0.0.0.8\n" +
                        "1970-01-01T00:06:40.000000Z\t0.0.0.9\n" +
                        "1970-01-01T00:08:20.000000Z\t0.0.0.10\n" +
                        "1970-01-01T00:10:00.000000Z\t0.0.0.9\n" +
                        "1970-01-01T00:11:40.000000Z\t\n" +
                        "1970-01-01T00:13:20.000000Z\t0.0.0.8\n" +
                        "1970-01-01T00:15:00.000000Z\t0.0.0.10\n" +
                        "1970-01-01T00:16:40.000000Z\t\n" +
                        "1970-01-01T00:18:20.000000Z\t\n" +
                        "1970-01-01T00:20:00.000000Z\t0.0.0.4\n" +
                        "1970-01-01T00:21:40.000000Z\t0.0.0.8\n" +
                        "1970-01-01T00:23:20.000000Z\t0.0.0.6\n" +
                        "1970-01-01T00:25:00.000000Z\t0.0.0.9\n" +
                        "1970-01-01T00:26:40.000000Z\t0.0.0.4\n" +
                        "1970-01-01T00:28:20.000000Z\t0.0.0.3\n" +
                        "1970-01-01T00:30:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T00:31:40.000000Z\t0.0.0.10\n" +
                        "1970-01-01T00:33:20.000000Z\t0.0.0.8\n" +
                        "1970-01-01T00:35:00.000000Z\t0.0.0.9\n" +
                        "1970-01-01T00:36:40.000000Z\t0.0.0.3\n" +
                        "1970-01-01T00:38:20.000000Z\t0.0.0.8\n" +
                        "1970-01-01T00:40:00.000000Z\t\n" +
                        "1970-01-01T00:41:40.000000Z\t0.0.0.8\n" +
                        "1970-01-01T00:43:20.000000Z\t0.0.0.10\n" +
                        "1970-01-01T00:45:00.000000Z\t0.0.0.10\n" +
                        "1970-01-01T00:46:40.000000Z\t\n" +
                        "1970-01-01T00:48:20.000000Z\t\n" +
                        "1970-01-01T00:50:00.000000Z\t0.0.0.4\n" +
                        "1970-01-01T00:51:40.000000Z\t0.0.0.10\n" +
                        "1970-01-01T00:53:20.000000Z\t0.0.0.9\n" +
                        "1970-01-01T00:55:00.000000Z\t0.0.0.6\n" +
                        "1970-01-01T00:56:40.000000Z\t0.0.0.6\n" +
                        "1970-01-01T00:58:20.000000Z\t0.0.0.7\n" +
                        "1970-01-01T01:00:00.000000Z\t0.0.0.8\n" +
                        "1970-01-01T01:01:40.000000Z\t0.0.0.2\n" +
                        "1970-01-01T01:03:20.000000Z\t0.0.0.7\n" +
                        "1970-01-01T01:05:00.000000Z\t0.0.0.4\n" +
                        "1970-01-01T01:06:40.000000Z\t\n" +
                        "1970-01-01T01:08:20.000000Z\t0.0.0.9\n" +
                        "1970-01-01T01:10:00.000000Z\t0.0.0.9\n" +
                        "1970-01-01T01:11:40.000000Z\t0.0.0.1\n" +
                        "1970-01-01T01:13:20.000000Z\t0.0.0.7\n" +
                        "1970-01-01T01:15:00.000000Z\t0.0.0.6\n" +
                        "1970-01-01T01:16:40.000000Z\t0.0.0.4\n" +
                        "1970-01-01T01:18:20.000000Z\t0.0.0.1\n" +
                        "1970-01-01T01:20:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T01:21:40.000000Z\t0.0.0.4\n" +
                        "1970-01-01T01:23:20.000000Z\t0.0.0.10\n" +
                        "1970-01-01T01:25:00.000000Z\t0.0.0.7\n" +
                        "1970-01-01T01:26:40.000000Z\t0.0.0.1\n" +
                        "1970-01-01T01:28:20.000000Z\t\n" +
                        "1970-01-01T01:30:00.000000Z\t0.0.0.8\n" +
                        "1970-01-01T01:31:40.000000Z\t\n" +
                        "1970-01-01T01:33:20.000000Z\t0.0.0.4\n" +
                        "1970-01-01T01:35:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T01:36:40.000000Z\t0.0.0.4\n" +
                        "1970-01-01T01:38:20.000000Z\t0.0.0.4\n" +
                        "1970-01-01T01:40:00.000000Z\t0.0.0.1\n" +
                        "1970-01-01T01:41:40.000000Z\t0.0.0.3\n" +
                        "1970-01-01T01:43:20.000000Z\t0.0.0.8\n" +
                        "1970-01-01T01:45:00.000000Z\t0.0.0.9\n" +
                        "1970-01-01T01:46:40.000000Z\t0.0.0.7\n" +
                        "1970-01-01T01:48:20.000000Z\t0.0.0.8\n" +
                        "1970-01-01T01:50:00.000000Z\t0.0.0.6\n" +
                        "1970-01-01T01:51:40.000000Z\t0.0.0.2\n" +
                        "1970-01-01T01:53:20.000000Z\t0.0.0.3\n" +
                        "1970-01-01T01:55:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T01:56:40.000000Z\t0.0.0.6\n" +
                        "1970-01-01T01:58:20.000000Z\t0.0.0.2\n" +
                        "1970-01-01T02:00:00.000000Z\t0.0.0.1\n" +
                        "1970-01-01T02:01:40.000000Z\t0.0.0.1\n" +
                        "1970-01-01T02:03:20.000000Z\t0.0.0.6\n" +
                        "1970-01-01T02:05:00.000000Z\t0.0.0.6\n" +
                        "1970-01-01T02:06:40.000000Z\t0.0.0.3\n" +
                        "1970-01-01T02:08:20.000000Z\t0.0.0.10\n" +
                        "1970-01-01T02:10:00.000000Z\t\n" +
                        "1970-01-01T02:11:40.000000Z\t\n" +
                        "1970-01-01T02:13:20.000000Z\t0.0.0.6\n" +
                        "1970-01-01T02:15:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T02:16:40.000000Z\t0.0.0.9\n" +
                        "1970-01-01T02:18:20.000000Z\t0.0.0.6\n" +
                        "1970-01-01T02:20:00.000000Z\t0.0.0.8\n" +
                        "1970-01-01T02:21:40.000000Z\t\n" +
                        "1970-01-01T02:23:20.000000Z\t0.0.0.6\n" +
                        "1970-01-01T02:25:00.000000Z\t0.0.0.9\n" +
                        "1970-01-01T02:26:40.000000Z\t0.0.0.1\n" +
                        "1970-01-01T02:28:20.000000Z\t0.0.0.10\n" +
                        "1970-01-01T02:30:00.000000Z\t0.0.0.8\n" +
                        "1970-01-01T02:31:40.000000Z\t\n" +
                        "1970-01-01T02:33:20.000000Z\t0.0.0.10\n" +
                        "1970-01-01T02:35:00.000000Z\t0.0.0.2\n" +
                        "1970-01-01T02:36:40.000000Z\t0.0.0.1\n" +
                        "1970-01-01T02:38:20.000000Z\t0.0.0.7\n" +
                        "1970-01-01T02:40:00.000000Z\t\n" +
                        "1970-01-01T02:41:40.000000Z\t0.0.0.1\n" +
                        "1970-01-01T02:43:20.000000Z\t0.0.0.6\n" +
                        "1970-01-01T02:45:00.000000Z\t0.0.0.2\n", "select k, nullif(ip, '0.0.0.5') from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,10,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                "k",
                true,
                true
        );
    }

    @Test
    public void testIPv4PlusIntConst() throws Exception {
        assertSql(
                "column\n" +
                        "1.1.1.21\n",
                "select ipv4 '1.1.1.1' + 20"
        );
    }

    @Test
    public void testIPv4PlusIntConst2() throws Exception {
        assertSql(
                "column\n" +
                        "255.255.255.255\n",
                "select ipv4 '255.255.255.20' + 235"
        );
    }

    @Test
    public void testIPv4PlusIntConst3() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select  ('255.255.255.255')::ipv4 + 10"
        );
    }

    @Test
    public void testIPv4PlusIntConstNull() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select ipv4 '0.0.0.0' + 20"
        );
    }

    @Test
    public void testIPv4PlusIntConstOverflow() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select ipv4 '255.255.255.255' + 1"
        );
    }

    @Test
    public void testIPv4PlusIntConstOverflow2() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select ipv4 '255.255.255.20' + 236"
        );
    }

    @Test
    public void testIPv4PlusIntHalfConst() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.0.32\n" +
                        "0.0.0.24\n" +
                        "0.0.0.38\n" +
                        "0.0.0.40\n" +
                        "0.0.0.21\n" +
                        "0.0.0.23\n" +
                        "0.0.0.35\n" +
                        "0.0.0.29\n" +
                        "0.0.0.39\n" +
                        "0.0.0.39\n" +
                        "0.0.0.22\n" +
                        "0.0.0.40\n" +
                        "0.0.0.28\n" +
                        "0.0.0.36\n" +
                        "0.0.0.27\n" +
                        "0.0.0.38\n" +
                        "0.0.0.31\n" +
                        "0.0.0.33\n" +
                        "0.0.0.24\n" +
                        "0.0.0.21\n" +
                        "0.0.0.26\n" +
                        "0.0.0.27\n" +
                        "0.0.0.37\n" +
                        "0.0.0.33\n" +
                        "0.0.0.25\n" +
                        "0.0.0.36\n" +
                        "0.0.0.29\n" +
                        "0.0.0.36\n" +
                        "0.0.0.32\n" +
                        "0.0.0.37\n" +
                        "0.0.0.30\n" +
                        "0.0.0.26\n" +
                        "0.0.0.38\n" +
                        "0.0.0.35\n" +
                        "0.0.0.25\n" +
                        "0.0.0.28\n" +
                        "0.0.0.21\n" +
                        "0.0.0.36\n" +
                        "0.0.0.25\n" +
                        "0.0.0.38\n" +
                        "0.0.0.25\n" +
                        "0.0.0.39\n" +
                        "0.0.0.36\n" +
                        "0.0.0.23\n" +
                        "0.0.0.30\n" +
                        "0.0.0.26\n" +
                        "0.0.0.32\n" +
                        "0.0.0.37\n" +
                        "0.0.0.26\n" +
                        "0.0.0.28\n" +
                        "0.0.0.34\n" +
                        "0.0.0.35\n" +
                        "0.0.0.36\n" +
                        "0.0.0.29\n" +
                        "0.0.0.38\n" +
                        "0.0.0.28\n" +
                        "0.0.0.38\n" +
                        "0.0.0.27\n" +
                        "0.0.0.32\n" +
                        "0.0.0.36\n" +
                        "0.0.0.21\n" +
                        "0.0.0.26\n" +
                        "0.0.0.26\n" +
                        "0.0.0.37\n" +
                        "0.0.0.22\n" +
                        "0.0.0.21\n" +
                        "0.0.0.29\n" +
                        "0.0.0.38\n" +
                        "0.0.0.37\n" +
                        "0.0.0.38\n" +
                        "0.0.0.38\n" +
                        "0.0.0.22\n" +
                        "0.0.0.21\n" +
                        "0.0.0.38\n" +
                        "0.0.0.24\n" +
                        "0.0.0.29\n" +
                        "0.0.0.30\n" +
                        "0.0.0.36\n" +
                        "0.0.0.38\n" +
                        "0.0.0.36\n" +
                        "0.0.0.24\n" +
                        "0.0.0.27\n" +
                        "0.0.0.25\n" +
                        "0.0.0.33\n" +
                        "0.0.0.39\n" +
                        "0.0.0.32\n" +
                        "0.0.0.21\n" +
                        "0.0.0.40\n" +
                        "0.0.0.31\n" +
                        "0.0.0.28\n" +
                        "0.0.0.33\n" +
                        "0.0.0.30\n" +
                        "0.0.0.40\n" +
                        "0.0.0.26\n" +
                        "0.0.0.39\n" +
                        "0.0.0.21\n" +
                        "0.0.0.24\n" +
                        "0.0.0.30\n" +
                        "0.0.0.23\n" +
                        "0.0.0.26\n",
                "select ip + 20 from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4PlusIntVar() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.1.120\n" +
                        "0.0.0.186\n" +
                        "0.0.3.77\n" +
                        "0.0.2.202\n" +
                        "0.0.1.82\n" +
                        "0.0.3.45\n" +
                        "0.0.0.102\n" +
                        "0.0.3.224\n" +
                        "0.0.3.157\n" +
                        "0.0.0.29\n" +
                        "0.0.3.197\n" +
                        "0.0.1.2\n" +
                        "0.0.1.81\n" +
                        "0.0.2.114\n" +
                        "0.0.0.44\n" +
                        "0.0.1.201\n" +
                        "0.0.3.136\n" +
                        "0.0.1.65\n" +
                        "0.0.3.139\n" +
                        "0.0.2.169\n" +
                        "0.0.2.229\n" +
                        "0.0.2.132\n" +
                        "0.0.3.169\n" +
                        "0.0.0.194\n" +
                        "0.0.1.103\n" +
                        "0.0.1.100\n" +
                        "0.0.0.108\n" +
                        "0.0.1.236\n" +
                        "0.0.2.42\n" +
                        "0.0.3.132\n" +
                        "0.0.0.10\n" +
                        "0.0.0.29\n" +
                        "0.0.1.129\n" +
                        "0.0.2.160\n" +
                        "0.0.3.122\n" +
                        "0.0.2.7\n" +
                        "0.0.2.172\n" +
                        "0.0.0.175\n" +
                        "0.0.0.80\n" +
                        "0.0.3.123\n" +
                        "0.0.0.165\n" +
                        "0.0.0.112\n" +
                        "0.0.0.73\n" +
                        "0.0.2.150\n" +
                        "0.0.1.9\n" +
                        "0.0.3.3\n" +
                        "0.0.2.147\n" +
                        "0.0.0.210\n" +
                        "0.0.1.96\n" +
                        "0.0.1.249\n" +
                        "0.0.3.98\n" +
                        "0.0.3.6\n" +
                        "0.0.0.58\n" +
                        "0.0.2.82\n" +
                        "0.0.2.177\n" +
                        "0.0.0.87\n" +
                        "0.0.3.64\n" +
                        "0.0.2.206\n" +
                        "0.0.0.84\n" +
                        "0.0.1.86\n" +
                        "0.0.1.6\n" +
                        "0.0.3.176\n" +
                        "0.0.1.25\n" +
                        "0.0.2.127\n" +
                        "0.0.3.118\n" +
                        "0.0.3.222\n" +
                        "0.0.3.24\n" +
                        "0.0.1.187\n" +
                        "0.0.1.99\n" +
                        "0.0.1.63\n" +
                        "0.0.0.136\n" +
                        "0.0.1.71\n" +
                        "0.0.3.123\n" +
                        "0.0.3.25\n" +
                        "0.0.1.17\n" +
                        "0.0.3.191\n" +
                        "0.0.0.101\n" +
                        "0.0.3.236\n" +
                        "0.0.2.10\n" +
                        "0.0.2.188\n" +
                        "0.0.2.154\n" +
                        "0.0.1.171\n" +
                        "0.0.0.146\n" +
                        "0.0.0.153\n" +
                        "0.0.0.155\n" +
                        "0.0.2.146\n" +
                        "0.0.1.70\n" +
                        "0.0.2.226\n" +
                        "0.0.3.70\n" +
                        "0.0.0.176\n" +
                        "0.0.0.188\n" +
                        "0.0.0.251\n" +
                        "0.0.1.190\n" +
                        "0.0.1.42\n" +
                        "0.0.1.74\n" +
                        "0.0.0.151\n" +
                        "0.0.0.241\n" +
                        "0.0.2.134\n" +
                        "0.0.2.79\n" +
                        "0.0.3.98\n",
                "select ip + bytes from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4Rank() throws Exception {
        assertQuery(
                "ip\tbytes\trank\n" +
                        "0.0.0.1\t814\t1\n" +
                        "0.0.0.1\t30\t1\n" +
                        "0.0.0.1\t660\t1\n" +
                        "0.0.0.1\t368\t1\n" +
                        "0.0.0.1\t924\t1\n" +
                        "0.0.0.1\t887\t1\n" +
                        "0.0.0.2\t493\t7\n" +
                        "0.0.0.2\t93\t7\n" +
                        "0.0.0.2\t606\t7\n" +
                        "0.0.0.2\t288\t7\n" +
                        "0.0.0.2\t480\t7\n" +
                        "0.0.0.2\t345\t7\n" +
                        "0.0.0.2\t906\t7\n" +
                        "0.0.0.3\t624\t14\n" +
                        "0.0.0.3\t840\t14\n" +
                        "0.0.0.3\t563\t14\n" +
                        "0.0.0.4\t511\t17\n" +
                        "0.0.0.4\t907\t17\n" +
                        "0.0.0.4\t937\t17\n" +
                        "0.0.0.4\t883\t17\n" +
                        "0.0.0.4\t328\t17\n" +
                        "0.0.0.4\t181\t17\n" +
                        "0.0.0.4\t807\t17\n" +
                        "0.0.0.4\t619\t17\n" +
                        "0.0.0.5\t624\t25\n" +
                        "0.0.0.5\t193\t25\n" +
                        "0.0.0.5\t397\t25\n" +
                        "0.0.0.5\t697\t25\n" +
                        "0.0.0.5\t308\t25\n" +
                        "0.0.0.5\t877\t25\n" +
                        "0.0.0.5\t37\t25\n" +
                        "0.0.0.6\t240\t32\n" +
                        "0.0.0.6\t841\t32\n" +
                        "0.0.0.6\t255\t32\n" +
                        "0.0.0.6\t746\t32\n" +
                        "0.0.0.6\t735\t32\n" +
                        "0.0.0.7\t727\t37\n" +
                        "0.0.0.7\t75\t37\n" +
                        "0.0.0.7\t99\t37\n" +
                        "0.0.0.7\t173\t37\n" +
                        "0.0.0.8\t665\t41\n" +
                        "0.0.0.8\t740\t41\n" +
                        "0.0.0.8\t986\t41\n" +
                        "0.0.0.8\t369\t41\n" +
                        "0.0.0.8\t136\t41\n" +
                        "0.0.0.8\t522\t41\n" +
                        "0.0.0.9\t269\t47\n" +
                        "0.0.0.9\t487\t47\n" +
                        "0.0.0.9\t935\t47\n" +
                        "0.0.0.9\t34\t47\n" +
                        "0.0.0.9\t345\t47\n" +
                        "0.0.0.9\t598\t47\n" +
                        "0.0.0.9\t539\t47\n" +
                        "0.0.0.9\t827\t47\n" +
                        "0.0.0.9\t167\t47\n" +
                        "0.0.0.10\t868\t56\n" +
                        "0.0.0.10\t472\t56\n" +
                        "0.0.0.10\t644\t56\n" +
                        "0.0.0.10\t470\t56\n" +
                        "0.0.0.11\t511\t60\n" +
                        "0.0.0.11\t519\t60\n" +
                        "0.0.0.11\t188\t60\n" +
                        "0.0.0.12\t326\t63\n" +
                        "0.0.0.12\t884\t63\n" +
                        "0.0.0.12\t655\t63\n" +
                        "0.0.0.12\t23\t63\n" +
                        "0.0.0.13\t0\t67\n" +
                        "0.0.0.13\t574\t67\n" +
                        "0.0.0.13\t7\t67\n" +
                        "0.0.0.14\t334\t70\n" +
                        "0.0.0.15\t172\t71\n" +
                        "0.0.0.15\t149\t71\n" +
                        "0.0.0.15\t551\t71\n" +
                        "0.0.0.15\t910\t71\n" +
                        "0.0.0.15\t95\t71\n" +
                        "0.0.0.15\t528\t71\n" +
                        "0.0.0.15\t904\t71\n" +
                        "0.0.0.16\t25\t78\n" +
                        "0.0.0.16\t597\t78\n" +
                        "0.0.0.16\t777\t78\n" +
                        "0.0.0.16\t482\t78\n" +
                        "0.0.0.16\t428\t78\n" +
                        "0.0.0.16\t711\t78\n" +
                        "0.0.0.16\t906\t78\n" +
                        "0.0.0.17\t770\t85\n" +
                        "0.0.0.17\t417\t85\n" +
                        "0.0.0.18\t660\t87\n" +
                        "0.0.0.18\t852\t87\n" +
                        "0.0.0.18\t569\t87\n" +
                        "0.0.0.18\t367\t87\n" +
                        "0.0.0.18\t162\t87\n" +
                        "0.0.0.18\t594\t87\n" +
                        "0.0.0.19\t326\t93\n" +
                        "0.0.0.19\t71\t93\n" +
                        "0.0.0.20\t585\t95\n" +
                        "0.0.0.20\t606\t95\n" +
                        "0.0.0.20\t180\t95\n" +
                        "0.0.0.20\t238\t95\n" +
                        "0.0.0.20\t810\t95\n" +
                        "0.0.0.20\t424\t95\n",
                "select ip, bytes, rank() over (order by ip asc) rank from test order by rank",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                false
        );
    }

    @Test
    public void testIPv4RowNum() throws Exception {
        assertQuery(
                "ip\tbytes\trow_num\n" +
                        "0.0.0.12\t23\t1\n" +
                        "0.0.0.9\t167\t2\n" +
                        "0.0.0.13\t7\t3\n" +
                        "0.0.0.8\t522\t4\n" +
                        "0.0.0.9\t827\t5\n" +
                        "0.0.0.20\t424\t6\n" +
                        "0.0.0.9\t539\t7\n" +
                        "0.0.0.15\t904\t8\n" +
                        "0.0.0.8\t136\t9\n" +
                        "0.0.0.20\t810\t10\n" +
                        "0.0.0.15\t528\t11\n" +
                        "0.0.0.15\t95\t12\n" +
                        "0.0.0.4\t619\t13\n" +
                        "0.0.0.8\t369\t14\n" +
                        "0.0.0.16\t906\t15\n" +
                        "0.0.0.19\t71\t16\n" +
                        "0.0.0.14\t334\t17\n" +
                        "0.0.0.3\t563\t18\n" +
                        "0.0.0.2\t906\t19\n" +
                        "0.0.0.20\t238\t20\n" +
                        "0.0.0.8\t986\t21\n" +
                        "0.0.0.9\t598\t22\n" +
                        "0.0.0.13\t574\t23\n" +
                        "0.0.0.8\t740\t24\n" +
                        "0.0.0.5\t37\t25\n" +
                        "0.0.0.18\t594\t26\n" +
                        "0.0.0.20\t180\t27\n" +
                        "0.0.0.20\t606\t28\n" +
                        "0.0.0.5\t877\t29\n" +
                        "0.0.0.5\t308\t30\n" +
                        "0.0.0.4\t807\t31\n" +
                        "0.0.0.10\t470\t32\n" +
                        "0.0.0.9\t345\t33\n" +
                        "0.0.0.16\t711\t34\n" +
                        "0.0.0.6\t735\t35\n" +
                        "0.0.0.7\t173\t36\n" +
                        "0.0.0.18\t162\t37\n" +
                        "0.0.0.2\t345\t38\n" +
                        "0.0.0.17\t417\t39\n" +
                        "0.0.0.4\t181\t40\n" +
                        "0.0.0.5\t697\t41\n" +
                        "0.0.0.9\t34\t42\n" +
                        "0.0.0.9\t935\t43\n" +
                        "0.0.0.1\t887\t44\n" +
                        "0.0.0.7\t99\t45\n" +
                        "0.0.0.16\t428\t46\n" +
                        "0.0.0.4\t328\t47\n" +
                        "0.0.0.1\t924\t48\n" +
                        "0.0.0.2\t480\t49\n" +
                        "0.0.0.4\t883\t50\n" +
                        "0.0.0.10\t644\t51\n" +
                        "0.0.0.17\t770\t52\n" +
                        "0.0.0.11\t188\t53\n" +
                        "0.0.0.5\t397\t54\n" +
                        "0.0.0.18\t367\t55\n" +
                        "0.0.0.15\t910\t56\n" +
                        "0.0.0.4\t937\t57\n" +
                        "0.0.0.2\t288\t58\n" +
                        "0.0.0.4\t907\t59\n" +
                        "0.0.0.4\t511\t60\n" +
                        "0.0.0.1\t368\t61\n" +
                        "0.0.0.13\t0\t62\n" +
                        "0.0.0.8\t665\t63\n" +
                        "0.0.0.19\t326\t64\n" +
                        "0.0.0.7\t75\t65\n" +
                        "0.0.0.18\t569\t66\n" +
                        "0.0.0.6\t746\t67\n" +
                        "0.0.0.2\t606\t68\n" +
                        "0.0.0.3\t840\t69\n" +
                        "0.0.0.2\t93\t70\n" +
                        "0.0.0.16\t482\t71\n" +
                        "0.0.0.12\t655\t72\n" +
                        "0.0.0.1\t660\t73\n" +
                        "0.0.0.11\t519\t74\n" +
                        "0.0.0.6\t255\t75\n" +
                        "0.0.0.6\t841\t76\n" +
                        "0.0.0.3\t624\t77\n" +
                        "0.0.0.10\t472\t78\n" +
                        "0.0.0.15\t551\t79\n" +
                        "0.0.0.5\t193\t80\n" +
                        "0.0.0.6\t240\t81\n" +
                        "0.0.0.2\t493\t82\n" +
                        "0.0.0.9\t487\t83\n" +
                        "0.0.0.16\t777\t84\n" +
                        "0.0.0.18\t852\t85\n" +
                        "0.0.0.15\t149\t86\n" +
                        "0.0.0.16\t597\t87\n" +
                        "0.0.0.9\t269\t88\n" +
                        "0.0.0.1\t30\t89\n" +
                        "0.0.0.20\t585\t90\n" +
                        "0.0.0.18\t660\t91\n" +
                        "0.0.0.15\t172\t92\n" +
                        "0.0.0.10\t868\t93\n" +
                        "0.0.0.12\t884\t94\n" +
                        "0.0.0.1\t814\t95\n" +
                        "0.0.0.7\t727\t96\n" +
                        "0.0.0.5\t624\t97\n" +
                        "0.0.0.11\t511\t98\n" +
                        "0.0.0.16\t25\t99\n" +
                        "0.0.0.12\t326\t100\n",
                "select ip, bytes, row_number() over () as row_num from test order by row_num asc",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4StrBadStr() throws Exception {
        assertSql(
                "netmask\n" +
                        "\n",
                "select netmask('bdfsir/33')"
        );
    }

    @Test
    public void testIPv4StrBitwiseOrHalfConst() throws Exception {
        assertQuery(
                "column\n" +
                        "255.0.0.12\n" +
                        "255.0.0.9\n" +
                        "255.0.0.13\n" +
                        "255.0.0.8\n" +
                        "255.0.0.9\n" +
                        "255.0.0.20\n" +
                        "255.0.0.9\n" +
                        "255.0.0.15\n" +
                        "255.0.0.8\n" +
                        "255.0.0.20\n" +
                        "255.0.0.15\n" +
                        "255.0.0.15\n" +
                        "255.0.0.4\n" +
                        "255.0.0.8\n" +
                        "255.0.0.16\n" +
                        "255.0.0.19\n" +
                        "255.0.0.14\n" +
                        "255.0.0.3\n" +
                        "255.0.0.2\n" +
                        "255.0.0.20\n" +
                        "255.0.0.8\n" +
                        "255.0.0.9\n" +
                        "255.0.0.13\n" +
                        "255.0.0.8\n" +
                        "255.0.0.5\n" +
                        "255.0.0.18\n" +
                        "255.0.0.20\n" +
                        "255.0.0.20\n" +
                        "255.0.0.5\n" +
                        "255.0.0.5\n" +
                        "255.0.0.4\n" +
                        "255.0.0.10\n" +
                        "255.0.0.9\n" +
                        "255.0.0.16\n" +
                        "255.0.0.6\n" +
                        "255.0.0.7\n" +
                        "255.0.0.18\n" +
                        "255.0.0.2\n" +
                        "255.0.0.17\n" +
                        "255.0.0.4\n" +
                        "255.0.0.5\n" +
                        "255.0.0.9\n" +
                        "255.0.0.9\n" +
                        "255.0.0.1\n" +
                        "255.0.0.7\n" +
                        "255.0.0.16\n" +
                        "255.0.0.4\n" +
                        "255.0.0.1\n" +
                        "255.0.0.2\n" +
                        "255.0.0.4\n" +
                        "255.0.0.10\n" +
                        "255.0.0.17\n" +
                        "255.0.0.11\n" +
                        "255.0.0.5\n" +
                        "255.0.0.18\n" +
                        "255.0.0.15\n" +
                        "255.0.0.4\n" +
                        "255.0.0.2\n" +
                        "255.0.0.4\n" +
                        "255.0.0.4\n" +
                        "255.0.0.1\n" +
                        "255.0.0.13\n" +
                        "255.0.0.8\n" +
                        "255.0.0.19\n" +
                        "255.0.0.7\n" +
                        "255.0.0.18\n" +
                        "255.0.0.6\n" +
                        "255.0.0.2\n" +
                        "255.0.0.3\n" +
                        "255.0.0.2\n" +
                        "255.0.0.16\n" +
                        "255.0.0.12\n" +
                        "255.0.0.1\n" +
                        "255.0.0.11\n" +
                        "255.0.0.6\n" +
                        "255.0.0.6\n" +
                        "255.0.0.3\n" +
                        "255.0.0.10\n" +
                        "255.0.0.15\n" +
                        "255.0.0.5\n" +
                        "255.0.0.6\n" +
                        "255.0.0.2\n" +
                        "255.0.0.9\n" +
                        "255.0.0.16\n" +
                        "255.0.0.18\n" +
                        "255.0.0.15\n" +
                        "255.0.0.16\n" +
                        "255.0.0.9\n" +
                        "255.0.0.1\n" +
                        "255.0.0.20\n" +
                        "255.0.0.18\n" +
                        "255.0.0.15\n" +
                        "255.0.0.10\n" +
                        "255.0.0.12\n" +
                        "255.0.0.1\n" +
                        "255.0.0.7\n" +
                        "255.0.0.5\n" +
                        "255.0.0.11\n" +
                        "255.0.0.16\n" +
                        "255.0.0.12\n",
                "select ip | ipv4 '255.0.0.0' from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4StrMinusIPv4() throws Exception {
        assertSql(
                "column\n" +
                        "16843008\n",
                "select '1.1.1.1' - ipv4 '0.0.0.1'"
        );
    }

    @Test
    public void testIPv4StrMinusIPv4Str() throws Exception {
        assertSql(
                "column\n" +
                        "16843008\n",
                "select '1.1.1.1' - '0.0.0.1'"
        );
    }

    @Test
    public void testIPv4StrMinusInt() throws Exception {
        assertSql(
                "column\n" +
                        "1.1.0.252\n",
                "select '1.1.1.1' - 5"
        );
    }

    @Test
    public void testIPv4StrNetmask() throws Exception {
        assertSql(
                "netmask\n" +
                        "255.255.255.240\n",
                "select netmask('68.11.22.1/28')"
        );
    }

    @Test
    public void testIPv4StrNetmaskNull() throws Exception {
        assertSql(
                "netmask\n" +
                        "\n",
                "select netmask('68.11.22.1/33')"
        );
    }

    @Test
    public void testIPv4StrPlusInt() throws Exception {
        assertSql(
                "column\n" +
                        "1.1.1.6\n",
                "select '1.1.1.1' + 5"
        );
    }

    @Test
    public void testIPv4StringUnionFails() throws Exception {
        ddl("create table x (col1 ipv4)");
        ddl("create table y (col2 string)");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into y values('0.0.0.1')");
        insert("insert into y values('0.0.0.2')");
        insert("insert into y values('0.0.0.3')");
        insert("insert into y values('0.0.0.4')");
        insert("insert into y values('0.0.0.5')");

        engine.releaseInactive();

        assertException("select col1 from x union select col2 from y", 25, "unsupported cast [column=col2, from=STRING, to=IPv4]");
    }

    @Test
    public void testIPv4Union() throws Exception {
        ddl("create table x (col1 ipv4)");
        ddl("create table y (col2 ipv4)");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into y values('0.0.0.1')");
        insert("insert into y values('0.0.0.2')");
        insert("insert into y values('0.0.0.3')");
        insert("insert into y values('0.0.0.4')");
        insert("insert into y values('0.0.0.5')");

        assertSql(
                "col1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.3\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n",
                "select col1 from x union select col2 from y"
        );
    }

    @Test
    public void testIPv4UnionAll() throws Exception {
        ddl("create table x (col1 ipv4)");
        ddl("create table y (col2 ipv4)");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into y values('0.0.0.1')");
        insert("insert into y values('0.0.0.2')");
        insert("insert into y values('0.0.0.3')");
        insert("insert into y values('0.0.0.4')");
        insert("insert into y values('0.0.0.5')");

        assertSql(
                "col1\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.3\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n" +
                        "0.0.0.1\n" +
                        "0.0.0.2\n" +
                        "0.0.0.3\n" +
                        "0.0.0.4\n" +
                        "0.0.0.5\n",
                "select col1 from x union all select col2 from y"
        );
    }

    @Test
    public void testImplicitCastBinaryToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_bin() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: BINARY -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastBoolToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_boolean() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: BOOLEAN -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastByteToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_byte() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: BYTE -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastCharToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_char() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: CHAR -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastDateToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_date() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: DATE -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastDoubleToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_double() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: DOUBLE -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastFloatToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_float() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: FLOAT -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToBinary() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col binary)",
                21,
                "inconvertible types: IPv4 -> BINARY [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToBool() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col boolean)",
                21,
                "inconvertible types: IPv4 -> BOOLEAN [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToByte() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col byte)",
                21,
                "inconvertible types: IPv4 -> BYTE [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToChar() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col char)",
                21,
                "inconvertible types: IPv4 -> CHAR [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToDate() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col date)",
                21,
                "inconvertible types: IPv4 -> DATE [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToDouble() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col double)",
                21,
                "inconvertible types: IPv4 -> DOUBLE [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToFloat() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col float)",
                21,
                "inconvertible types: IPv4 -> FLOAT [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToInt() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col int)",
                21,
                "inconvertible types: IPv4 -> INT [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToLong() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col long)",
                21,
                "inconvertible types: IPv4 -> LONG [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToLong128() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col long128)",
                21,
                "inconvertible types: IPv4 -> LONG128 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToLong256() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col long256)",
                21,
                "inconvertible types: IPv4 -> LONG256 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToShort() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col short)",
                21,
                "inconvertible types: IPv4 -> SHORT [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToStr() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col string)",
                21,
                "inconvertible types: IPv4 -> STRING [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToSymbol() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col symbol)",
                21,
                "inconvertible types: IPv4 -> SYMBOL [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToTimestamp() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col timestamp)",
                21,
                "inconvertible types: IPv4 -> TIMESTAMP [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIPv4ToUUID() throws Exception {
        assertException(
                "insert into y select rnd_ipv4() col from long_sequence(10)",
                "create table y (col uuid)",
                21,
                "inconvertible types: IPv4 -> UUID [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastIntToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_int() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: INT -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastLong256ToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_long256() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: LONG256 -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastLongToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_long() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: LONG -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastShortToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_short() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: SHORT -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastStrIPv4() throws Exception {
        assertQuery(
                "ip\tbytes\tts\n" +
                        "187.139.150.80\t580\t1970-01-01T00:00:00.000000Z\n",
                "select * from test where ip = '187.139.150.80'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                false
        );

        assertSql("ip\tbytes\tts\n" +
                        "187.139.150.80\t580\t1970-01-01T00:00:00.000000Z\n",
                "select * from test where '187.139.150.80' = ip");
    }

    @Test
    public void testImplicitCastStrIPv42() throws Exception {
        assertSql("column\n" +
                "false\n", "select ipv4 '2.2.2.2' <= '1.1.1.1'");
    }

    @Test
    public void testImplicitCastStrIPv43() throws Exception {
        assertSql("column\n" +
                "253.253.253.253\n", "select ~ '2.2.2.2'");
    }

    @Test
    public void testImplicitCastStrIPv4BadStr() throws Exception {
        assertException("select 'dhukdsvhiu' < ipv4 '1.1.1.1'", 0, "invalid ipv4 format: dhukdsvhiu");
    }

    @Test
    public void testImplicitCastStrToIpv4() throws Exception {
        ddl("create table x (b string)");
        insert("insert into x values('0.0.0.1')");
        insert("insert into x values('0.0.0.2')");
        insert("insert into x values('0.0.0.3')");
        insert("insert into x values('0.0.0.4')");
        insert("insert into x values('0.0.0.5')");
        insert("insert into x values('0.0.0.6')");
        insert("insert into x values('0.0.0.7')");
        insert("insert into x values('0.0.0.8')");
        insert("insert into x values('0.0.0.9')");
        insert("insert into x values('0.0.0.10')");
        ddl("create table y (a ipv4)");

        engine.releaseInactive();

        assertQuery(
                "a\n" +
                        "0.0.0.1" + '\n' +
                        "0.0.0.2" + '\n' +
                        "0.0.0.3" + '\n' +
                        "0.0.0.4" + '\n' +
                        "0.0.0.5" + '\n' +
                        "0.0.0.6" + '\n' +
                        "0.0.0.7" + '\n' +
                        "0.0.0.8" + '\n' +
                        "0.0.0.9" + '\n' +
                        "0.0.0.10" + '\n',
                "select * from y",
                "insert into y select * from x",
                null,
                true,
                true
        );
    }

    @Test
    public void testImplicitCastSymbolToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_symbol(4,4,4,2) col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: SYMBOL -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastTimestampToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_timestamp(10,100000,356) col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: TIMESTAMP -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testImplicitCastUUIDToIPv4() throws Exception {
        assertException(
                "insert into y select rnd_uuid4() col from long_sequence(10)",
                "create table y (col ipv4)",
                21,
                "inconvertible types: UUID -> IPv4 [from=col, to=col]"
        );
    }

    @Test
    public void testInnerJoinIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('1.1.1.1/32', 0) ip, 1 count from long_sequence(5))");
        ddl("create table test2 as (select rnd_ipv4('1.1.1.1/32', 0) ip2, 2 count2 from long_sequence(5))");
        assertSql(
                "count\tcount2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n" +
                        "1\t2\n",
                "select test.count, test2.count2 from test inner join test2 on test2.ip2 = test.ip"
        );
    }

    @Test
    public void testIntPlusIPv4Const() throws Exception {
        assertSql(
                "column\n" +
                        "1.1.1.21\n",
                "select 20 + ipv4 '1.1.1.1'"
        );
    }

    @Test
    public void testIntPlusIPv4Const2() throws Exception {
        assertSql(
                "column\n" +
                        "255.255.255.255\n",
                "select 235 + ipv4 '255.255.255.20'"
        );
    }

    @Test
    public void testIntPlusIPv4ConstNull() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select 20 + ipv4 '0.0.0.0'"
        );
    }

    @Test
    public void testIntPlusIPv4ConstOverflow() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select 1 + ipv4 '255.255.255.255'"
        );
    }

    @Test
    public void testIntPlusIPv4ConstOverflow2() throws Exception {
        assertSql(
                "column\n" +
                        "\n",
                "select 236 + ipv4 '255.255.255.20'"
        );
    }

    @Test
    public void testIntPlusIPv4HalfConst() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.0.32\n" +
                        "0.0.0.24\n" +
                        "0.0.0.38\n" +
                        "0.0.0.40\n" +
                        "0.0.0.21\n" +
                        "0.0.0.23\n" +
                        "0.0.0.35\n" +
                        "0.0.0.29\n" +
                        "0.0.0.39\n" +
                        "0.0.0.39\n" +
                        "0.0.0.22\n" +
                        "0.0.0.40\n" +
                        "0.0.0.28\n" +
                        "0.0.0.36\n" +
                        "0.0.0.27\n" +
                        "0.0.0.38\n" +
                        "0.0.0.31\n" +
                        "0.0.0.33\n" +
                        "0.0.0.24\n" +
                        "0.0.0.21\n" +
                        "0.0.0.26\n" +
                        "0.0.0.27\n" +
                        "0.0.0.37\n" +
                        "0.0.0.33\n" +
                        "0.0.0.25\n" +
                        "0.0.0.36\n" +
                        "0.0.0.29\n" +
                        "0.0.0.36\n" +
                        "0.0.0.32\n" +
                        "0.0.0.37\n" +
                        "0.0.0.30\n" +
                        "0.0.0.26\n" +
                        "0.0.0.38\n" +
                        "0.0.0.35\n" +
                        "0.0.0.25\n" +
                        "0.0.0.28\n" +
                        "0.0.0.21\n" +
                        "0.0.0.36\n" +
                        "0.0.0.25\n" +
                        "0.0.0.38\n" +
                        "0.0.0.25\n" +
                        "0.0.0.39\n" +
                        "0.0.0.36\n" +
                        "0.0.0.23\n" +
                        "0.0.0.30\n" +
                        "0.0.0.26\n" +
                        "0.0.0.32\n" +
                        "0.0.0.37\n" +
                        "0.0.0.26\n" +
                        "0.0.0.28\n" +
                        "0.0.0.34\n" +
                        "0.0.0.35\n" +
                        "0.0.0.36\n" +
                        "0.0.0.29\n" +
                        "0.0.0.38\n" +
                        "0.0.0.28\n" +
                        "0.0.0.38\n" +
                        "0.0.0.27\n" +
                        "0.0.0.32\n" +
                        "0.0.0.36\n" +
                        "0.0.0.21\n" +
                        "0.0.0.26\n" +
                        "0.0.0.26\n" +
                        "0.0.0.37\n" +
                        "0.0.0.22\n" +
                        "0.0.0.21\n" +
                        "0.0.0.29\n" +
                        "0.0.0.38\n" +
                        "0.0.0.37\n" +
                        "0.0.0.38\n" +
                        "0.0.0.38\n" +
                        "0.0.0.22\n" +
                        "0.0.0.21\n" +
                        "0.0.0.38\n" +
                        "0.0.0.24\n" +
                        "0.0.0.29\n" +
                        "0.0.0.30\n" +
                        "0.0.0.36\n" +
                        "0.0.0.38\n" +
                        "0.0.0.36\n" +
                        "0.0.0.24\n" +
                        "0.0.0.27\n" +
                        "0.0.0.25\n" +
                        "0.0.0.33\n" +
                        "0.0.0.39\n" +
                        "0.0.0.32\n" +
                        "0.0.0.21\n" +
                        "0.0.0.40\n" +
                        "0.0.0.31\n" +
                        "0.0.0.28\n" +
                        "0.0.0.33\n" +
                        "0.0.0.30\n" +
                        "0.0.0.40\n" +
                        "0.0.0.26\n" +
                        "0.0.0.39\n" +
                        "0.0.0.21\n" +
                        "0.0.0.24\n" +
                        "0.0.0.30\n" +
                        "0.0.0.23\n" +
                        "0.0.0.26\n",
                "select 20 + ip from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntPlusIPv4Str() throws Exception {
        assertSql(
                "column\n" +
                        "1.1.1.6\n",
                "select 5 + '1.1.1.1'"
        );
    }

    @Test
    public void testIntPlusIPv4Var() throws Exception {
        assertQuery(
                "column\n" +
                        "0.0.1.120\n" +
                        "0.0.0.186\n" +
                        "0.0.3.77\n" +
                        "0.0.2.202\n" +
                        "0.0.1.82\n" +
                        "0.0.3.45\n" +
                        "0.0.0.102\n" +
                        "0.0.3.224\n" +
                        "0.0.3.157\n" +
                        "0.0.0.29\n" +
                        "0.0.3.197\n" +
                        "0.0.1.2\n" +
                        "0.0.1.81\n" +
                        "0.0.2.114\n" +
                        "0.0.0.44\n" +
                        "0.0.1.201\n" +
                        "0.0.3.136\n" +
                        "0.0.1.65\n" +
                        "0.0.3.139\n" +
                        "0.0.2.169\n" +
                        "0.0.2.229\n" +
                        "0.0.2.132\n" +
                        "0.0.3.169\n" +
                        "0.0.0.194\n" +
                        "0.0.1.103\n" +
                        "0.0.1.100\n" +
                        "0.0.0.108\n" +
                        "0.0.1.236\n" +
                        "0.0.2.42\n" +
                        "0.0.3.132\n" +
                        "0.0.0.10\n" +
                        "0.0.0.29\n" +
                        "0.0.1.129\n" +
                        "0.0.2.160\n" +
                        "0.0.3.122\n" +
                        "0.0.2.7\n" +
                        "0.0.2.172\n" +
                        "0.0.0.175\n" +
                        "0.0.0.80\n" +
                        "0.0.3.123\n" +
                        "0.0.0.165\n" +
                        "0.0.0.112\n" +
                        "0.0.0.73\n" +
                        "0.0.2.150\n" +
                        "0.0.1.9\n" +
                        "0.0.3.3\n" +
                        "0.0.2.147\n" +
                        "0.0.0.210\n" +
                        "0.0.1.96\n" +
                        "0.0.1.249\n" +
                        "0.0.3.98\n" +
                        "0.0.3.6\n" +
                        "0.0.0.58\n" +
                        "0.0.2.82\n" +
                        "0.0.2.177\n" +
                        "0.0.0.87\n" +
                        "0.0.3.64\n" +
                        "0.0.2.206\n" +
                        "0.0.0.84\n" +
                        "0.0.1.86\n" +
                        "0.0.1.6\n" +
                        "0.0.3.176\n" +
                        "0.0.1.25\n" +
                        "0.0.2.127\n" +
                        "0.0.3.118\n" +
                        "0.0.3.222\n" +
                        "0.0.3.24\n" +
                        "0.0.1.187\n" +
                        "0.0.1.99\n" +
                        "0.0.1.63\n" +
                        "0.0.0.136\n" +
                        "0.0.1.71\n" +
                        "0.0.3.123\n" +
                        "0.0.3.25\n" +
                        "0.0.1.17\n" +
                        "0.0.3.191\n" +
                        "0.0.0.101\n" +
                        "0.0.3.236\n" +
                        "0.0.2.10\n" +
                        "0.0.2.188\n" +
                        "0.0.2.154\n" +
                        "0.0.1.171\n" +
                        "0.0.0.146\n" +
                        "0.0.0.153\n" +
                        "0.0.0.155\n" +
                        "0.0.2.146\n" +
                        "0.0.1.70\n" +
                        "0.0.2.226\n" +
                        "0.0.3.70\n" +
                        "0.0.0.176\n" +
                        "0.0.0.188\n" +
                        "0.0.0.251\n" +
                        "0.0.1.190\n" +
                        "0.0.1.42\n" +
                        "0.0.1.74\n" +
                        "0.0.0.151\n" +
                        "0.0.0.241\n" +
                        "0.0.2.134\n" +
                        "0.0.2.79\n" +
                        "0.0.3.98\n",
                "select bytes + ip from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testLastIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 2) ip, rnd_symbol('ab', '$a', 'ac') sym from long_sequence(20))");
        assertSql(
                "sym\tlast\n" +
                        "$a\t10.5.237.229\n" +
                        "ac\t10.5.235.200\n" +
                        "ab\t\n",
                "select sym, last(ip) from test"
        );
    }

    @Test
    public void testLatestByIPv4() throws Exception {
        assertQuery(
                "ip\tbytes\ttime\n" +
                        "0.0.0.4\t269\t1970-01-01T00:00:08.700000Z\n" +
                        "0.0.0.3\t660\t1970-01-01T00:00:09.000000Z\n" +
                        "0.0.0.5\t624\t1970-01-01T00:00:09.600000Z\n" +
                        "0.0.0.1\t25\t1970-01-01T00:00:09.800000Z\n" +
                        "0.0.0.2\t326\t1970-01-01T00:00:09.900000Z\n",
                "select * from test latest by ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000) time" +
                        "  from long_sequence(100)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testLeftJoinIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('1.1.1.1/16', 0) ip, 1 count from long_sequence(5))");
        ddl("create table test2 as (select rnd_ipv4('1.1.1.1/32', 0) ip2, 2 count2 from long_sequence(5))");
        assertSql(
                "ip\tip2\tcount\tcount2\n" +
                        "1.1.96.238\t\t1\tNaN\n" +
                        "1.1.50.227\t\t1\tNaN\n" +
                        "1.1.89.171\t\t1\tNaN\n" +
                        "1.1.82.23\t\t1\tNaN\n" +
                        "1.1.76.40\t\t1\tNaN\n",
                "select test.ip, test2.ip2, test.count, test2.count2 from test left join test2 on test2.ip2 = test.ip"
        );
    }

    @Test
    public void testLeftJoinIPv42() throws Exception {
        ddl("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(50))");
        ddl("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(50))");
        assertSql(
                "ip\tip2\tcount\tcount2\n" +
                        "12.5.9.238\t\t1\tNaN\n" +
                        "12.5.9.227\t\t1\tNaN\n" +
                        "12.5.9.171\t\t1\tNaN\n" +
                        "12.5.9.23\t\t1\tNaN\n" +
                        "12.5.9.40\t\t1\tNaN\n" +
                        "12.5.9.236\t\t1\tNaN\n" +
                        "12.5.9.15\t\t1\tNaN\n" +
                        "12.5.9.136\t\t1\tNaN\n" +
                        "12.5.9.145\t\t1\tNaN\n" +
                        "12.5.9.114\t12.5.9.114\t1\t2\n" +
                        "12.5.9.243\t\t1\tNaN\n" +
                        "12.5.9.229\t\t1\tNaN\n" +
                        "12.5.9.120\t\t1\tNaN\n" +
                        "12.5.9.160\t\t1\tNaN\n" +
                        "12.5.9.196\t\t1\tNaN\n" +
                        "12.5.9.235\t\t1\tNaN\n" +
                        "12.5.9.159\t12.5.9.159\t1\t2\n" +
                        "12.5.9.81\t\t1\tNaN\n" +
                        "12.5.9.196\t\t1\tNaN\n" +
                        "12.5.9.108\t\t1\tNaN\n" +
                        "12.5.9.173\t\t1\tNaN\n" +
                        "12.5.9.76\t\t1\tNaN\n" +
                        "12.5.9.126\t\t1\tNaN\n" +
                        "12.5.9.248\t\t1\tNaN\n" +
                        "12.5.9.226\t12.5.9.226\t1\t2\n" +
                        "12.5.9.115\t\t1\tNaN\n" +
                        "12.5.9.98\t\t1\tNaN\n" +
                        "12.5.9.200\t\t1\tNaN\n" +
                        "12.5.9.247\t\t1\tNaN\n" +
                        "12.5.9.216\t\t1\tNaN\n" +
                        "12.5.9.48\t\t1\tNaN\n" +
                        "12.5.9.202\t\t1\tNaN\n" +
                        "12.5.9.50\t\t1\tNaN\n" +
                        "12.5.9.228\t\t1\tNaN\n" +
                        "12.5.9.210\t\t1\tNaN\n" +
                        "12.5.9.7\t\t1\tNaN\n" +
                        "12.5.9.4\t\t1\tNaN\n" +
                        "12.5.9.135\t\t1\tNaN\n" +
                        "12.5.9.117\t\t1\tNaN\n" +
                        "12.5.9.43\t12.5.9.43\t1\t2\n" +
                        "12.5.9.43\t12.5.9.43\t1\t2\n" +
                        "12.5.9.179\t12.5.9.179\t1\t2\n" +
                        "12.5.9.179\t12.5.9.179\t1\t2\n" +
                        "12.5.9.142\t\t1\tNaN\n" +
                        "12.5.9.75\t\t1\tNaN\n" +
                        "12.5.9.239\t\t1\tNaN\n" +
                        "12.5.9.48\t\t1\tNaN\n" +
                        "12.5.9.100\t12.5.9.100\t1\t2\n" +
                        "12.5.9.100\t12.5.9.100\t1\t2\n" +
                        "12.5.9.100\t12.5.9.100\t1\t2\n" +
                        "12.5.9.26\t\t1\tNaN\n" +
                        "12.5.9.240\t\t1\tNaN\n" +
                        "12.5.9.192\t\t1\tNaN\n" +
                        "12.5.9.181\t\t1\tNaN\n",
                "select test.ip, test2.ip2, test.count, test2.count2 from test left join test2 on test2.ip2 = test.ip"
        );
    }

    @Test
    public void testLessThanEqIPv4() throws Exception {
        assertSql("column\n" +
                "true\n", "select ipv4 '34.11.45.3' <= ipv4 '34.11.45.3'");
    }

    @Test
    public void testLessThanIPv4() throws Exception {
        assertSql("column\n" +
                "false\n", "select ipv4 '34.11.45.3' < ipv4 '22.1.200.89'");
    }

    @Test
    public void testLimitIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
        assertSql(
                "ip\tcount\n" +
                        "10.5.96.238\t1\n" +
                        "10.5.50.227\t1\n" +
                        "10.5.89.171\t1\n" +
                        "10.5.82.23\t1\n" +
                        "10.5.76.40\t1\n",
                "select * from test limit 5"
        );
    }

    @Test
    public void testLimitIPv42() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
        assertSql(
                "ip\tcount\n" +
                        "10.5.170.235\t1\n" +
                        "10.5.45.159\t1\n" +
                        "10.5.184.81\t1\n" +
                        "10.5.207.196\t1\n" +
                        "10.5.213.108\t1\n",
                "select * from test limit -5"
        );
    }

    @Test
    public void testLimitIPv43() throws Exception {
        ddl("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
        assertSql(
                "ip\tcount\n" +
                        "10.5.89.171\t1\n" +
                        "10.5.82.23\t1\n" +
                        "10.5.76.40\t1\n" +
                        "10.5.20.236\t1\n" +
                        "10.5.95.15\t1\n" +
                        "10.5.178.136\t1\n" +
                        "10.5.45.145\t1\n" +
                        "10.5.93.114\t1\n",
                "select * from test limit 2, 10"
        );
    }

    @Test
    public void testMaxIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
        insert("insert into test values ('255.255.255.255', 1)");
        assertSql(
                "max\n" +
                        "255.255.255.255\n",
                "select max(ip) from test"
        );
    }

    @Test
    public void testMinIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
        insert("insert into test values ('0.0.0.1', 1)");
        assertSql(
                "min\n" +
                        "0.0.0.1\n",
                "select min(ip) from test"
        );
    }

    @Test
    public void testMinIPv4Null() throws Exception {
        ddl("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
        insert("insert into test values ('0.0.0.0', 1)");
        assertSql(
                "min\n" +
                        "4.98.173.21\n",
                "select min(ip) from test"
        );
    }

    @Test
    public void testNegContainsIPv4FunctionFactoryError() throws Exception {
        ddl("create table t (ip ipv4)");
        assertException("select * from t where '1.1.1.1/35' >> ip", 22, "invalid argument: 1.1.1.1/35");
        assertException("select * from t where '1.1.1.1/-1' >> ip", 22, "invalid argument: 1.1.1.1/-1");
        assertException("select * from t where '1.1.1.1/A' >> ip ", 22, "invalid argument: 1.1.1.1/A");
        assertException("select * from t where '1.1/26' >> ip ", 22, "invalid argument: 1.1/26");
    }

    @Test
    public void testNetmask() throws SqlException {
        ddl("create table tipv4 ( ip ipv4)");
        insert("insert into tipv4 values ('255.255.255.254')");
        assertSql("ip\n" +
                "255.255.255.254\n", "select * from tipv4 where '255.255.255.255/31' >>= ip");
        assertSql("ip\n", "select * from tipv4 where '255.255.255.255/32' >>= ip");
        assertSql("ip\n" +
                "255.255.255.254\n", "select * from tipv4 where '255.255.255.255/30' >>= ip");
        assertSql("ip\n" +
                "255.255.255.254\n", "select * from tipv4 where '255.255.255.0/24' >>= ip");

        assertSql("column\n" +
                "true\n", "select '255.255.255.255'::ipv4 <<= '255.255.255.255'");
        assertSql("column\n" +
                "false\n", "select '255.255.255.255'::ipv4 <<= '255.255.255.254'");
        assertSql("column\n" +
                "true\n", "select '255.255.255.255'::ipv4 <<= '255.255.255.254/31'");

        assertSql("netmask\n\n", "select netmask('1.1.1.1/0');");
        assertSql("netmask\n255.255.255.252\n", "select netmask('1.1.1.1/30');");
        assertSql("netmask\n255.255.255.254\n", "select netmask('1.1.1.1/31');");
        assertSql("netmask\n255.255.255.255\n", "select netmask('1.1.1.1/32');");
        assertSql("netmask\n\n", "select netmask('1.1.1.1/33');");
    }

    @Test
    public void testNullNetmask() throws Exception {
        assertSql(
                "netmask\n" +
                        "\n",
                "select netmask(null)"
        );
    }

    @Test
    public void testOrderByIPv4Ascending() throws Exception {
        assertQuery(
                "ip\tbytes\tts\n" +
                        "12.214.12.100\t598\t1970-01-01T01:11:40.000000Z\n" +
                        "24.123.12.210\t95\t1970-01-01T00:38:20.000000Z\n" +
                        "25.107.51.160\t827\t1970-01-01T00:15:00.000000Z\n" +
                        "50.214.139.184\t574\t1970-01-01T01:15:00.000000Z\n" +
                        "55.211.206.129\t785\t1970-01-01T00:43:20.000000Z\n" +
                        "63.60.82.184\t37\t1970-01-01T01:21:40.000000Z\n" +
                        "66.56.51.126\t904\t1970-01-01T00:25:00.000000Z\n" +
                        "67.22.249.199\t203\t1970-01-01T00:13:20.000000Z\n" +
                        "71.73.196.29\t741\t1970-01-01T01:00:00.000000Z\n" +
                        "73.153.126.70\t772\t1970-01-01T01:06:40.000000Z\n" +
                        "74.196.176.71\t740\t1970-01-01T01:18:20.000000Z\n" +
                        "79.15.250.138\t850\t1970-01-01T00:03:20.000000Z\n" +
                        "92.26.178.136\t7\t1970-01-01T00:08:20.000000Z\n" +
                        "97.159.145.120\t352\t1970-01-01T00:46:40.000000Z\n" +
                        "105.218.160.179\t986\t1970-01-01T01:08:20.000000Z\n" +
                        "111.221.228.130\t531\t1970-01-01T01:03:20.000000Z\n" +
                        "113.132.124.243\t522\t1970-01-01T00:11:40.000000Z\n" +
                        "114.126.117.26\t71\t1970-01-01T00:51:40.000000Z\n" +
                        "128.225.84.244\t313\t1970-01-01T00:30:00.000000Z\n" +
                        "129.172.181.73\t25\t1970-01-01T00:23:20.000000Z\n" +
                        "136.166.51.222\t580\t1970-01-01T00:36:40.000000Z\n" +
                        "144.131.72.77\t369\t1970-01-01T00:45:00.000000Z\n" +
                        "146.16.210.119\t383\t1970-01-01T00:16:40.000000Z\n" +
                        "150.153.88.133\t849\t1970-01-01T01:20:00.000000Z\n" +
                        "164.74.203.45\t678\t1970-01-01T00:53:20.000000Z\n" +
                        "164.153.242.17\t906\t1970-01-01T00:48:20.000000Z\n" +
                        "165.166.233.251\t332\t1970-01-01T00:50:00.000000Z\n" +
                        "170.90.236.206\t572\t1970-01-01T00:06:40.000000Z\n" +
                        "171.30.189.77\t238\t1970-01-01T01:05:00.000000Z\n" +
                        "171.117.213.66\t720\t1970-01-01T00:40:00.000000Z\n" +
                        "180.36.62.54\t528\t1970-01-01T00:35:00.000000Z\n" +
                        "180.48.50.141\t136\t1970-01-01T00:28:20.000000Z\n" +
                        "180.91.244.55\t906\t1970-01-01T01:01:40.000000Z\n" +
                        "181.82.42.148\t539\t1970-01-01T00:21:40.000000Z\n" +
                        "186.33.243.40\t659\t1970-01-01T01:16:40.000000Z\n" +
                        "187.63.210.97\t424\t1970-01-01T00:18:20.000000Z\n" +
                        "187.139.150.80\t580\t1970-01-01T00:00:00.000000Z\n" +
                        "188.239.72.25\t513\t1970-01-01T00:20:00.000000Z\n" +
                        "201.100.238.229\t318\t1970-01-01T01:10:00.000000Z\n" +
                        "205.123.179.216\t167\t1970-01-01T00:05:00.000000Z\n" +
                        "212.102.182.127\t984\t1970-01-01T01:13:20.000000Z\n" +
                        "212.159.205.29\t23\t1970-01-01T00:01:40.000000Z\n" +
                        "216.150.248.30\t563\t1970-01-01T00:58:20.000000Z\n" +
                        "224.99.254.121\t619\t1970-01-01T00:41:40.000000Z\n" +
                        "227.40.250.157\t903\t1970-01-01T00:33:20.000000Z\n" +
                        "230.202.108.161\t171\t1970-01-01T00:26:40.000000Z\n" +
                        "231.146.30.59\t766\t1970-01-01T00:10:00.000000Z\n" +
                        "241.248.184.75\t334\t1970-01-01T00:55:00.000000Z\n" +
                        "254.93.251.9\t810\t1970-01-01T00:31:40.000000Z\n" +
                        "255.95.177.227\t44\t1970-01-01T00:56:40.000000Z\n",
                "select * from test order by ip, bytes, ts",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByIPv4Descending() throws Exception {
        assertQuery(
                "ip\tbytes\tts\n" +
                        "255.95.177.227\t44\t1970-01-01T00:56:40.000000Z\n" +
                        "254.93.251.9\t810\t1970-01-01T00:31:40.000000Z\n" +
                        "241.248.184.75\t334\t1970-01-01T00:55:00.000000Z\n" +
                        "231.146.30.59\t766\t1970-01-01T00:10:00.000000Z\n" +
                        "230.202.108.161\t171\t1970-01-01T00:26:40.000000Z\n" +
                        "227.40.250.157\t903\t1970-01-01T00:33:20.000000Z\n" +
                        "224.99.254.121\t619\t1970-01-01T00:41:40.000000Z\n" +
                        "216.150.248.30\t563\t1970-01-01T00:58:20.000000Z\n" +
                        "212.159.205.29\t23\t1970-01-01T00:01:40.000000Z\n" +
                        "212.102.182.127\t984\t1970-01-01T01:13:20.000000Z\n" +
                        "205.123.179.216\t167\t1970-01-01T00:05:00.000000Z\n" +
                        "201.100.238.229\t318\t1970-01-01T01:10:00.000000Z\n" +
                        "188.239.72.25\t513\t1970-01-01T00:20:00.000000Z\n" +
                        "187.139.150.80\t580\t1970-01-01T00:00:00.000000Z\n" +
                        "187.63.210.97\t424\t1970-01-01T00:18:20.000000Z\n" +
                        "186.33.243.40\t659\t1970-01-01T01:16:40.000000Z\n" +
                        "181.82.42.148\t539\t1970-01-01T00:21:40.000000Z\n" +
                        "180.91.244.55\t906\t1970-01-01T01:01:40.000000Z\n" +
                        "180.48.50.141\t136\t1970-01-01T00:28:20.000000Z\n" +
                        "180.36.62.54\t528\t1970-01-01T00:35:00.000000Z\n" +
                        "171.117.213.66\t720\t1970-01-01T00:40:00.000000Z\n" +
                        "171.30.189.77\t238\t1970-01-01T01:05:00.000000Z\n" +
                        "170.90.236.206\t572\t1970-01-01T00:06:40.000000Z\n" +
                        "165.166.233.251\t332\t1970-01-01T00:50:00.000000Z\n" +
                        "164.153.242.17\t906\t1970-01-01T00:48:20.000000Z\n" +
                        "164.74.203.45\t678\t1970-01-01T00:53:20.000000Z\n" +
                        "150.153.88.133\t849\t1970-01-01T01:20:00.000000Z\n" +
                        "146.16.210.119\t383\t1970-01-01T00:16:40.000000Z\n" +
                        "144.131.72.77\t369\t1970-01-01T00:45:00.000000Z\n" +
                        "136.166.51.222\t580\t1970-01-01T00:36:40.000000Z\n" +
                        "129.172.181.73\t25\t1970-01-01T00:23:20.000000Z\n" +
                        "128.225.84.244\t313\t1970-01-01T00:30:00.000000Z\n" +
                        "114.126.117.26\t71\t1970-01-01T00:51:40.000000Z\n" +
                        "113.132.124.243\t522\t1970-01-01T00:11:40.000000Z\n" +
                        "111.221.228.130\t531\t1970-01-01T01:03:20.000000Z\n" +
                        "105.218.160.179\t986\t1970-01-01T01:08:20.000000Z\n" +
                        "97.159.145.120\t352\t1970-01-01T00:46:40.000000Z\n" +
                        "92.26.178.136\t7\t1970-01-01T00:08:20.000000Z\n" +
                        "79.15.250.138\t850\t1970-01-01T00:03:20.000000Z\n" +
                        "74.196.176.71\t740\t1970-01-01T01:18:20.000000Z\n" +
                        "73.153.126.70\t772\t1970-01-01T01:06:40.000000Z\n" +
                        "71.73.196.29\t741\t1970-01-01T01:00:00.000000Z\n" +
                        "67.22.249.199\t203\t1970-01-01T00:13:20.000000Z\n" +
                        "66.56.51.126\t904\t1970-01-01T00:25:00.000000Z\n" +
                        "63.60.82.184\t37\t1970-01-01T01:21:40.000000Z\n" +
                        "55.211.206.129\t785\t1970-01-01T00:43:20.000000Z\n" +
                        "50.214.139.184\t574\t1970-01-01T01:15:00.000000Z\n" +
                        "25.107.51.160\t827\t1970-01-01T00:15:00.000000Z\n" +
                        "24.123.12.210\t95\t1970-01-01T00:38:20.000000Z\n" +
                        "12.214.12.100\t598\t1970-01-01T01:11:40.000000Z\n",
                "select * from test order by ip DESC",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testRandomIPv4Error() throws Exception {
        assertException("select rnd_ipv4('1.1.1.1/35', 1)", 16, "invalid argument: 1.1.1.1/35");
        assertException("select rnd_ipv4('1.1.1.1/-1', 1)", 16, "invalid argument: 1.1.1.1/-1");
        assertException("select rnd_ipv4('1.1.1.1/A', 1)", 16, "invalid argument: 1.1.1.1/A");
        assertException("select rnd_ipv4('1.1/26', 1)", 16, "invalid argument: 1.1/26");
    }

    @Test
    public void testRndIPv4() throws Exception {
        assertSql(
                "rnd_ipv4\n" +
                        "12.6.96.238\n" +
                        "12.6.50.227\n" +
                        "12.6.89.171\n" +
                        "12.6.82.23\n" +
                        "12.6.76.40\n" +
                        "12.6.20.236\n" +
                        "12.6.95.15\n" +
                        "12.6.178.136\n" +
                        "12.6.45.145\n" +
                        "12.6.93.114\n",
                "select rnd_ipv4('12.6/16', 0) from long_sequence(10)"
        );
    }

    @Test
    public void testRndIPv42() throws Exception {
        assertSql(
                "rnd_ipv4\n" +
                        "12.6.96.238\n" +
                        "12.6.50.227\n" +
                        "12.6.89.171\n" +
                        "12.6.82.23\n" +
                        "12.6.76.40\n" +
                        "12.6.20.236\n" +
                        "12.6.95.15\n" +
                        "12.6.178.136\n" +
                        "12.6.45.145\n" +
                        "12.6.93.114\n",
                "select rnd_ipv4('12.6.8/16', 0) from long_sequence(10)"
        );
    }

    @Test
    public void testSampleByIPv4() throws Exception {
        assertQuery(
                "ip\tts\tsum\n" +
                        "12.214.12.100\t1970-01-01T00:00:00.000001Z\t598\n" +
                        "24.123.12.210\t1970-01-01T00:00:00.000001Z\t95\n" +
                        "25.107.51.160\t1970-01-01T00:00:00.000001Z\t827\n" +
                        "50.214.139.184\t1970-01-01T00:00:00.000001Z\t574\n" +
                        "55.211.206.129\t1970-01-01T00:00:00.000001Z\t785\n" +
                        "63.60.82.184\t1970-01-01T00:00:00.000001Z\t37\n" +
                        "66.56.51.126\t1970-01-01T00:00:00.000001Z\t904\n" +
                        "67.22.249.199\t1970-01-01T00:00:00.000001Z\t203\n" +
                        "71.73.196.29\t1970-01-01T00:00:00.000001Z\t741\n" +
                        "73.153.126.70\t1970-01-01T00:00:00.000001Z\t772\n" +
                        "74.196.176.71\t1970-01-01T00:00:00.000001Z\t740\n" +
                        "79.15.250.138\t1970-01-01T00:00:00.000001Z\t850\n" +
                        "92.26.178.136\t1970-01-01T00:00:00.000001Z\t7\n" +
                        "97.159.145.120\t1970-01-01T00:00:00.000001Z\t352\n" +
                        "105.218.160.179\t1970-01-01T00:00:00.000001Z\t986\n" +
                        "111.221.228.130\t1970-01-01T00:00:00.000001Z\t531\n" +
                        "113.132.124.243\t1970-01-01T00:00:00.000001Z\t522\n" +
                        "114.126.117.26\t1970-01-01T00:00:00.000001Z\t71\n" +
                        "128.225.84.244\t1970-01-01T00:00:00.000001Z\t313\n" +
                        "129.172.181.73\t1970-01-01T00:00:00.000001Z\t25\n" +
                        "136.166.51.222\t1970-01-01T00:00:00.000001Z\t580\n" +
                        "144.131.72.77\t1970-01-01T00:00:00.000001Z\t369\n" +
                        "146.16.210.119\t1970-01-01T00:00:00.000001Z\t383\n" +
                        "150.153.88.133\t1970-01-01T00:00:00.000001Z\t849\n" +
                        "164.74.203.45\t1970-01-01T00:00:00.000001Z\t678\n" +
                        "164.153.242.17\t1970-01-01T00:00:00.000001Z\t906\n" +
                        "165.166.233.251\t1970-01-01T00:00:00.000001Z\t332\n" +
                        "170.90.236.206\t1970-01-01T00:00:00.000001Z\t572\n" +
                        "171.30.189.77\t1970-01-01T00:00:00.000001Z\t238\n" +
                        "171.117.213.66\t1970-01-01T00:00:00.000001Z\t720\n" +
                        "180.36.62.54\t1970-01-01T00:00:00.000001Z\t528\n" +
                        "180.48.50.141\t1970-01-01T00:00:00.000001Z\t136\n" +
                        "180.91.244.55\t1970-01-01T00:00:00.000001Z\t906\n" +
                        "181.82.42.148\t1970-01-01T00:00:00.000001Z\t539\n" +
                        "186.33.243.40\t1970-01-01T00:00:00.000001Z\t659\n" +
                        "187.63.210.97\t1970-01-01T00:00:00.000001Z\t424\n" +
                        "187.139.150.80\t1970-01-01T00:00:00.000001Z\t580\n" +
                        "188.239.72.25\t1970-01-01T00:00:00.000001Z\t513\n" +
                        "201.100.238.229\t1970-01-01T00:00:00.000001Z\t318\n" +
                        "205.123.179.216\t1970-01-01T00:00:00.000001Z\t167\n" +
                        "212.102.182.127\t1970-01-01T00:00:00.000001Z\t984\n" +
                        "212.159.205.29\t1970-01-01T00:00:00.000001Z\t23\n" +
                        "216.150.248.30\t1970-01-01T00:00:00.000001Z\t563\n" +
                        "224.99.254.121\t1970-01-01T00:00:00.000001Z\t619\n" +
                        "227.40.250.157\t1970-01-01T00:00:00.000001Z\t903\n" +
                        "230.202.108.161\t1970-01-01T00:00:00.000001Z\t171\n" +
                        "231.146.30.59\t1970-01-01T00:00:00.000001Z\t766\n" +
                        "241.248.184.75\t1970-01-01T00:00:00.000001Z\t334\n" +
                        "254.93.251.9\t1970-01-01T00:00:00.000001Z\t810\n" +
                        "255.95.177.227\t1970-01-01T00:00:00.000001Z\t44\n",
                "select ip, ts, sum(bytes) from test sample by 1y order by 2,1",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(1,10000) ts" +
                        "  from long_sequence(50)" +
                        ") timestamp(ts)",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testStrIPv4Cast() throws Exception {
        assertSql(
                "cast\n" +
                        "1.1.1.1\n",
                "select ipv4 '1.1.1.1'"
        );
    }

    @Test
    public void testStrIPv4CastInvalid() throws Exception {
        assertSql(
                "cast\n" +
                        "1.1.1.1\n",
                "select ipv4 '1.1.1.1'"
        );
    }

    @Test
    public void testStrIPv4CastNull() throws Exception {
        assertSql(
                "cast\n" +
                        "1.1.1.1\n",
                "select ipv4 '1.1.1.1'"
        );
    }

    @Test
    public void testStrIPv4CastOverflow() throws Exception {
        assertSql(
                "cast\n" +
                        "\n",
                "select ipv4 '256.5.5.5'"
        );
    }

    @Test
    public void testTruncateIPv4() throws Exception {
        ddl("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
        assertSql(
                "ip\tcount\n" +
                        "187.139.150.80\t1\n" +
                        "18.206.96.238\t1\n" +
                        "92.80.211.65\t1\n" +
                        "212.159.205.29\t1\n" +
                        "4.98.173.21\t1\n" +
                        "199.122.166.85\t1\n" +
                        "79.15.250.138\t1\n" +
                        "35.86.82.23\t1\n" +
                        "111.98.117.250\t1\n" +
                        "205.123.179.216\t1\n" +
                        "184.254.198.204\t1\n" +
                        "134.75.235.20\t1\n" +
                        "170.90.236.206\t1\n" +
                        "162.25.160.241\t1\n" +
                        "48.21.128.89\t1\n" +
                        "92.26.178.136\t1\n" +
                        "93.140.132.196\t1\n" +
                        "93.204.45.145\t1\n" +
                        "231.146.30.59\t1\n" +
                        "20.62.93.114\t1\n",
                "test"
        );
        ddl("truncate table test");
        assertSql("ip\tcount\n", "test");
    }

    @Test
    public void testUpdateTableIPv4ToString() throws Exception {
        ddl("create table test (col1 ipv4)");
        insert("insert into test values('0.0.0.1')");
        insert("insert into test values('0.0.0.2')");
        insert("insert into test values('0.0.0.3')");
        insert("insert into test values('0.0.0.4')");
        insert("insert into test values('0.0.0.5')");
        ddl("alter table test add col2 string");
        update("update test set col2 = col1");
        TestUtils.assertSql(engine, sqlExecutionContext, "test", sink, "col1\tcol2\n" +
                "0.0.0.1\t0.0.0.1\n" +
                "0.0.0.2\t0.0.0.2\n" +
                "0.0.0.3\t0.0.0.3\n" +
                "0.0.0.4\t0.0.0.4\n" +
                "0.0.0.5\t0.0.0.5\n");
    }

    @Test
    public void testUpdateTableIPv4ToStringWal() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (" +
                    "select " +
                    " rnd_ipv4 col1, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            ddl("alter table test add column col2 string");
            update("update test set col2 = col1");

            drainWalQueue();

            assertSql(
                    "col1\tts\tcol2\n" +
                            "187.139.150.80\t2022-02-24T00:00:00.000000Z\t187.139.150.80\n" +
                            "18.206.96.238\t2022-02-24T00:00:01.000000Z\t18.206.96.238\n" +
                            "92.80.211.65\t2022-02-24T00:00:02.000000Z\t92.80.211.65\n" +
                            "212.159.205.29\t2022-02-24T00:00:03.000000Z\t212.159.205.29\n" +
                            "4.98.173.21\t2022-02-24T00:00:04.000000Z\t4.98.173.21\n",
                    "test"
            );
        });
    }

    @Test
    public void testUpdateTableStringToIPv4() throws Exception {
        ddl("create table test (col1 string)");
        insert("insert into test values('0.0.0.1')");
        insert("insert into test values('0.0.0.2')");
        insert("insert into test values('0.0.0.3')");
        insert("insert into test values('0.0.0.4')");
        insert("insert into test values('0.0.0.5')");
        ddl("alter table test add col2 ipv4");
        update("update test set col2 = col1");
        TestUtils.assertSql(engine, sqlExecutionContext, "test", sink, "col1\tcol2\n" +
                "0.0.0.1\t0.0.0.1\n" +
                "0.0.0.2\t0.0.0.2\n" +
                "0.0.0.3\t0.0.0.3\n" +
                "0.0.0.4\t0.0.0.4\n" +
                "0.0.0.5\t0.0.0.5\n");
    }

    @Test
    public void testUpdateTableStringToIPv4Wal() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (" +
                    "select " +
                    " rnd_ipv4::string col1, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            ddl("alter table test add column col2 ipv4");
            update("update test set col2 = col1");

            drainWalQueue();

            assertSql(
                    "col1\tts\tcol2\n" +
                            "187.139.150.80\t2022-02-24T00:00:00.000000Z\t187.139.150.80\n" +
                            "18.206.96.238\t2022-02-24T00:00:01.000000Z\t18.206.96.238\n" +
                            "92.80.211.65\t2022-02-24T00:00:02.000000Z\t92.80.211.65\n" +
                            "212.159.205.29\t2022-02-24T00:00:03.000000Z\t212.159.205.29\n" +
                            "4.98.173.21\t2022-02-24T00:00:04.000000Z\t4.98.173.21\n",
                    "test"
            );
        });
    }

    @Test
    public void testWalIPv4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test as (" +
                    "select " +
                    " rnd_ipv4 col1, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertSql(
                    "col1\tts\n" +
                            "187.139.150.80\t2022-02-24T00:00:00.000000Z\n" +
                            "18.206.96.238\t2022-02-24T00:00:01.000000Z\n" +
                            "92.80.211.65\t2022-02-24T00:00:02.000000Z\n" +
                            "212.159.205.29\t2022-02-24T00:00:03.000000Z\n" +
                            "4.98.173.21\t2022-02-24T00:00:04.000000Z\n",
                    "test"
            );
        });
    }

    @Test
    public void testWhereIPv4() throws Exception {
        assertQuery(
                "ip\tbytes\tk\n" +
                        "0.0.0.1\t906\t1970-01-01T00:23:20.000000Z\n" +
                        "0.0.0.1\t711\t1970-01-01T00:55:00.000000Z\n" +
                        "0.0.0.1\t735\t1970-01-01T00:56:40.000000Z\n" +
                        "0.0.0.1\t887\t1970-01-01T01:11:40.000000Z\n" +
                        "0.0.0.1\t428\t1970-01-01T01:15:00.000000Z\n" +
                        "0.0.0.1\t924\t1970-01-01T01:18:20.000000Z\n" +
                        "0.0.0.1\t188\t1970-01-01T01:26:40.000000Z\n" +
                        "0.0.0.1\t368\t1970-01-01T01:40:00.000000Z\n" +
                        "0.0.0.1\t746\t1970-01-01T01:50:00.000000Z\n" +
                        "0.0.0.1\t482\t1970-01-01T01:56:40.000000Z\n" +
                        "0.0.0.1\t660\t1970-01-01T02:00:00.000000Z\n" +
                        "0.0.0.1\t519\t1970-01-01T02:01:40.000000Z\n" +
                        "0.0.0.1\t255\t1970-01-01T02:03:20.000000Z\n" +
                        "0.0.0.1\t841\t1970-01-01T02:05:00.000000Z\n" +
                        "0.0.0.1\t240\t1970-01-01T02:13:20.000000Z\n" +
                        "0.0.0.1\t777\t1970-01-01T02:18:20.000000Z\n" +
                        "0.0.0.1\t597\t1970-01-01T02:23:20.000000Z\n" +
                        "0.0.0.1\t30\t1970-01-01T02:26:40.000000Z\n" +
                        "0.0.0.1\t814\t1970-01-01T02:36:40.000000Z\n" +
                        "0.0.0.1\t511\t1970-01-01T02:41:40.000000Z\n" +
                        "0.0.0.1\t25\t1970-01-01T02:43:20.000000Z\n",
                "select * from test where ip = ipv4 '0.0.0.1'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                "k",
                true,
                false
        );
    }

    @Test
    public void testWhereIPv4Var() throws Exception {
        assertQuery(
                "ip1\tip2\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "\t\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "\t\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "\t\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "\t\n" +
                        "\t\n" +
                        "\t\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "\t\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "\t\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "\t\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "\t\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "\t\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "\t\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "\t\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "\t\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "\t\n" +
                        "\t\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "\t\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "\t\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.8\t0.0.0.8\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "\t\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "\t\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "\t\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "\t\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.2\t0.0.0.2\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.7\t0.0.0.7\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "0.0.0.3\t0.0.0.3\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "\t\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "0.0.0.9\t0.0.0.9\n" +
                        "0.0.0.1\t0.0.0.1\n" +
                        "0.0.0.5\t0.0.0.5\n" +
                        "0.0.0.4\t0.0.0.4\n" +
                        "0.0.0.6\t0.0.0.6\n" +
                        "0.0.0.8\t0.0.0.8\n",
                "select * from test where ip1 = ip2",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(0,9,0)::ipv4 ip1," +
                        "    rnd_int(0,9,0)::ipv4 ip2" +
                        "  from long_sequence(1000)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testWhereInvalidIPv4() throws Exception {
        assertQuery("ip\tbytes\tk\n",
                "select * from test where ip = ipv4 'hello'",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                "k",
                true,
                false
        );
    }
}
