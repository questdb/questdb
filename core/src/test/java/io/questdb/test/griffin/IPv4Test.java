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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IPv4Test extends AbstractCairoTest {

    @Test
    public void testAggregateByIPv4() throws Exception {
        assertQuery(
                """
                        ip\tsum
                        0.0.0.1\t11644
                        0.0.0.2\t7360
                        0.0.0.3\t9230
                        0.0.0.4\t10105
                        0.0.0.5\t11739
                        """,
                "select ip, sum(bytes) from test order by ip",
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
        assertMemoryLeak(() -> {
            execute("create table test (col1 ipv4)");
            execute("insert into test values ('0.0.0.1')");
            execute("alter table test add col2 ipv4");

            assertSql(
                    """
                            col1\tcol2
                            0.0.0.1\t
                            """,
                    "test"
            );
        });
    }

    @Test
    public void testBindVariableInEqFilterInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "foobar");
            assertException("x where b = :ip", 0, "invalid IPv4 format: foobar");
        });
    }

    @Test
    public void testBitAndStr() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        2.0.0.0
                        """,
                "select ipv4 '2.1.1.1' & '2.2.2.2'"
        ));
    }

    @Test
    public void testBitAndStr2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        2.0.0.0
                        """,
                "select '2.2.2.2' & ipv4 '2.1.1.1'"
        ));
    }

    @Test
    public void testBroadcastAddrUseCase() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        68.255.255.255
                        """,
                "select (~ netmask('68.11.9.2/8')) | ipv4 '68.11.9.2'"
        ));
    }

    @Test
    public void testCaseIPv41() throws Exception {
        assertQuery(
                """
                        ip\tbytes\tcase
                        187.139.150.80\t580\tnay
                        212.159.205.29\t23\tnay
                        79.15.250.138\t850\tnay
                        205.123.179.216\t167\tnay
                        170.90.236.206\t572\tnay
                        92.26.178.136\t7\tnay
                        231.146.30.59\t766\tnay
                        113.132.124.243\t522\tnay
                        67.22.249.199\t203\tnay
                        25.107.51.160\t827\tYAY
                        146.16.210.119\t383\tnay
                        187.63.210.97\t424\tnay
                        188.239.72.25\t513\tnay
                        181.82.42.148\t539\tnay
                        129.172.181.73\t25\tnay
                        66.56.51.126\t904\tnay
                        230.202.108.161\t171\tnay
                        180.48.50.141\t136\tnay
                        128.225.84.244\t313\tnay
                        254.93.251.9\t810\tnay
                        227.40.250.157\t903\tnay
                        180.36.62.54\t528\tnay
                        136.166.51.222\t580\tnay
                        24.123.12.210\t95\tYAY
                        171.117.213.66\t720\tnay
                        224.99.254.121\t619\tnay
                        55.211.206.129\t785\tYAY
                        144.131.72.77\t369\tnay
                        97.159.145.120\t352\tnay
                        164.153.242.17\t906\tnay
                        165.166.233.251\t332\tnay
                        114.126.117.26\t71\tnay
                        164.74.203.45\t678\tnay
                        241.248.184.75\t334\tnay
                        255.95.177.227\t44\tnay
                        216.150.248.30\t563\tnay
                        71.73.196.29\t741\tnay
                        180.91.244.55\t906\tnay
                        111.221.228.130\t531\tnay
                        171.30.189.77\t238\tnay
                        73.153.126.70\t772\tnay
                        105.218.160.179\t986\tnay
                        201.100.238.229\t318\tnay
                        12.214.12.100\t598\tYAY
                        212.102.182.127\t984\tnay
                        50.214.139.184\t574\tYAY
                        186.33.243.40\t659\tnay
                        74.196.176.71\t740\tnay
                        150.153.88.133\t849\tnay
                        63.60.82.184\t37\tYAY
                        """,
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
                """
                        ip\tbytes\tcase
                        187.139.150.80\t580\t
                        212.159.205.29\t23\t
                        79.15.250.138\t850\t
                        205.123.179.216\t167\t
                        170.90.236.206\t572\t
                        92.26.178.136\t7\t
                        231.146.30.59\t766\t
                        113.132.124.243\t522\t
                        67.22.249.199\t203\t
                        25.107.51.160\t827\tYAY
                        146.16.210.119\t383\t
                        187.63.210.97\t424\t
                        188.239.72.25\t513\t
                        181.82.42.148\t539\t
                        129.172.181.73\t25\t
                        66.56.51.126\t904\t
                        230.202.108.161\t171\t
                        180.48.50.141\t136\t
                        128.225.84.244\t313\t
                        254.93.251.9\t810\t
                        227.40.250.157\t903\t
                        180.36.62.54\t528\t
                        136.166.51.222\t580\t
                        24.123.12.210\t95\tYAY
                        171.117.213.66\t720\t
                        224.99.254.121\t619\t
                        55.211.206.129\t785\tYAY
                        144.131.72.77\t369\t
                        97.159.145.120\t352\t
                        164.153.242.17\t906\t
                        165.166.233.251\t332\t
                        114.126.117.26\t71\t
                        164.74.203.45\t678\t
                        241.248.184.75\t334\t
                        255.95.177.227\t44\t
                        216.150.248.30\t563\t
                        71.73.196.29\t741\t
                        180.91.244.55\t906\t
                        111.221.228.130\t531\t
                        171.30.189.77\t238\t
                        73.153.126.70\t772\t
                        105.218.160.179\t986\t
                        201.100.238.229\t318\t
                        12.214.12.100\t598\tYAY
                        212.102.182.127\t984\t
                        50.214.139.184\t574\tYAY
                        186.33.243.40\t659\t
                        74.196.176.71\t740\t
                        150.153.88.133\t849\t
                        63.60.82.184\t37\tYAY
                        """,
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
                """
                        ip\tbytes\tcase
                        187.139.150.80\t580\t0
                        212.159.205.29\t23\t0
                        79.15.250.138\t850\t0
                        205.123.179.216\t167\t0
                        170.90.236.206\t572\t0
                        92.26.178.136\t7\t0
                        231.146.30.59\t766\t0
                        113.132.124.243\t522\t0
                        67.22.249.199\t203\t0
                        25.107.51.160\t827\t1
                        146.16.210.119\t383\t0
                        187.63.210.97\t424\t0
                        188.239.72.25\t513\t0
                        181.82.42.148\t539\t0
                        129.172.181.73\t25\t0
                        66.56.51.126\t904\t0
                        230.202.108.161\t171\t0
                        180.48.50.141\t136\t0
                        128.225.84.244\t313\t0
                        254.93.251.9\t810\t0
                        227.40.250.157\t903\t0
                        180.36.62.54\t528\t0
                        136.166.51.222\t580\t0
                        24.123.12.210\t95\t1
                        171.117.213.66\t720\t0
                        224.99.254.121\t619\t0
                        55.211.206.129\t785\t1
                        144.131.72.77\t369\t0
                        97.159.145.120\t352\t0
                        164.153.242.17\t906\t0
                        165.166.233.251\t332\t0
                        114.126.117.26\t71\t0
                        164.74.203.45\t678\t0
                        241.248.184.75\t334\t0
                        255.95.177.227\t44\t0
                        216.150.248.30\t563\t0
                        71.73.196.29\t741\t0
                        180.91.244.55\t906\t0
                        111.221.228.130\t531\t0
                        171.30.189.77\t238\t0
                        73.153.126.70\t772\t0
                        105.218.160.179\t986\t0
                        201.100.238.229\t318\t0
                        12.214.12.100\t598\t1
                        212.102.182.127\t984\t0
                        50.214.139.184\t574\t1
                        186.33.243.40\t659\t0
                        74.196.176.71\t740\t0
                        150.153.88.133\t849\t0
                        63.60.82.184\t37\t1
                        """,
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
                """
                        ip\tbytes\tcase
                        187.139.150.80\t580\tnull
                        212.159.205.29\t23\tnull
                        79.15.250.138\t850\tnull
                        205.123.179.216\t167\tnull
                        170.90.236.206\t572\tnull
                        92.26.178.136\t7\tnull
                        231.146.30.59\t766\tnull
                        113.132.124.243\t522\tnull
                        67.22.249.199\t203\tnull
                        25.107.51.160\t827\t1
                        146.16.210.119\t383\tnull
                        187.63.210.97\t424\tnull
                        188.239.72.25\t513\tnull
                        181.82.42.148\t539\tnull
                        129.172.181.73\t25\tnull
                        66.56.51.126\t904\tnull
                        230.202.108.161\t171\tnull
                        180.48.50.141\t136\tnull
                        128.225.84.244\t313\tnull
                        254.93.251.9\t810\tnull
                        227.40.250.157\t903\tnull
                        180.36.62.54\t528\tnull
                        136.166.51.222\t580\tnull
                        24.123.12.210\t95\t1
                        171.117.213.66\t720\tnull
                        224.99.254.121\t619\tnull
                        55.211.206.129\t785\t1
                        144.131.72.77\t369\tnull
                        97.159.145.120\t352\tnull
                        164.153.242.17\t906\tnull
                        165.166.233.251\t332\tnull
                        114.126.117.26\t71\tnull
                        164.74.203.45\t678\tnull
                        241.248.184.75\t334\tnull
                        255.95.177.227\t44\tnull
                        216.150.248.30\t563\tnull
                        71.73.196.29\t741\tnull
                        180.91.244.55\t906\tnull
                        111.221.228.130\t531\tnull
                        171.30.189.77\t238\tnull
                        73.153.126.70\t772\tnull
                        105.218.160.179\t986\tnull
                        201.100.238.229\t318\tnull
                        12.214.12.100\t598\t1
                        212.102.182.127\t984\tnull
                        50.214.139.184\t574\t1
                        186.33.243.40\t659\tnull
                        74.196.176.71\t740\tnull
                        150.153.88.133\t849\tnull
                        63.60.82.184\t37\t1
                        """,
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
                """
                        ip\tbytes\tcase
                        2.2.96.238\t774\tNOT NULL
                        2.2.89.171\t404\tNOT NULL
                        2.2.76.40\t167\tNOT NULL
                        2.2.95.15\t803\tNOT NULL
                        2.2.45.145\t182\tNOT NULL
                        \t647\tNOT NULL
                        2.2.249.199\t203\tNOT NULL
                        \t827\tNOT NULL
                        2.2.170.235\t987\tNOT NULL
                        2.2.184.81\t614\tNOT NULL
                        2.2.213.108\t539\tNOT NULL
                        2.2.47.76\t585\tNOT NULL
                        \t16\tNOT NULL
                        2.2.129.200\t981\tNOT NULL
                        2.2.171.12\t313\tNOT NULL
                        2.2.253.254\t297\tNOT NULL
                        \t773\tNOT NULL
                        2.2.227.50\t411\tNOT NULL
                        2.2.12.210\t95\tNOT NULL
                        2.2.205.4\t916\tNOT NULL
                        2.2.236.117\t983\tNOT NULL
                        2.2.183.179\t369\tNOT NULL
                        2.2.220.75\t12\tNOT NULL
                        2.2.157.48\t613\tNOT NULL
                        \t114\tNOT NULL
                        2.2.52.211\t678\tNOT NULL
                        2.2.35.79\t262\tNOT NULL
                        2.2.207.241\t497\tNOT NULL
                        2.2.196.29\t741\tNOT NULL
                        2.2.228.56\t993\tNOT NULL
                        2.2.246.213\t562\tNOT NULL
                        2.2.126.70\t772\tNOT NULL
                        2.2.37.167\t907\tNOT NULL
                        2.2.234.47\t314\tNOT NULL
                        2.2.73.129\t984\tNOT NULL
                        2.2.112.55\t175\tNOT NULL
                        2.2.74.124\t254\tNOT NULL
                        2.2.167.123\t849\tNOT NULL
                        2.2.2.123\t941\tNOT NULL
                        2.2.140.124\t551\tNOT NULL
                        \t343\tNOT NULL
                        \t77\tNOT NULL
                        \t519\tNOT NULL
                        2.2.15.163\t606\tNOT NULL
                        2.2.245.83\t446\tNOT NULL
                        2.2.204.60\t835\tNOT NULL
                        2.2.7.88\t308\tNOT NULL
                        2.2.186.59\t875\tNOT NULL
                        2.2.89.110\t421\tNOT NULL
                        2.2.46.225\t470\tNOT NULL
                        """,
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
                """
                        ip\tbytes\tcase
                        2.2.96.238\t774\tNOT NULL
                        2.2.89.171\t404\tNOT NULL
                        2.2.76.40\t167\tNOT NULL
                        2.2.95.15\t803\tNOT NULL
                        2.2.45.145\t182\tNOT NULL
                        \t647\tNOT NULL
                        2.2.249.199\t203\tNOT NULL
                        \t827\tNOT NULL
                        2.2.170.235\t987\tNOT NULL
                        2.2.184.81\t614\tNOT NULL
                        2.2.213.108\t539\tNOT NULL
                        2.2.47.76\t585\tNOT NULL
                        \t16\tNOT NULL
                        2.2.129.200\t981\tNOT NULL
                        2.2.171.12\t313\tNOT NULL
                        2.2.253.254\t297\tNOT NULL
                        \t773\tNOT NULL
                        2.2.227.50\t411\tNOT NULL
                        2.2.12.210\t95\tNOT NULL
                        2.2.205.4\t916\tNOT NULL
                        2.2.236.117\t983\tNOT NULL
                        2.2.183.179\t369\tNOT NULL
                        2.2.220.75\t12\tNOT NULL
                        2.2.157.48\t613\tNOT NULL
                        \t114\tNOT NULL
                        2.2.52.211\t678\tNOT NULL
                        2.2.35.79\t262\tNOT NULL
                        2.2.207.241\t497\tNOT NULL
                        2.2.196.29\t741\tNOT NULL
                        2.2.228.56\t993\tNOT NULL
                        2.2.246.213\t562\tNOT NULL
                        2.2.126.70\t772\tNOT NULL
                        2.2.37.167\t907\tNOT NULL
                        2.2.234.47\t314\tNOT NULL
                        2.2.73.129\t984\tNOT NULL
                        2.2.112.55\t175\tNOT NULL
                        2.2.74.124\t254\tNOT NULL
                        2.2.167.123\t849\tNOT NULL
                        2.2.2.123\t941\tNOT NULL
                        2.2.140.124\t551\tNOT NULL
                        \t343\tNOT NULL
                        \t77\tNOT NULL
                        \t519\tNOT NULL
                        2.2.15.163\t606\tNOT NULL
                        2.2.245.83\t446\tNOT NULL
                        2.2.204.60\t835\tNOT NULL
                        2.2.7.88\t308\tNOT NULL
                        2.2.186.59\t875\tNOT NULL
                        2.2.89.110\t421\tNOT NULL
                        2.2.46.225\t470\tNOT NULL
                        """,
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
    public void testConstantInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");
            assertSql(
                    """
                            b
                            192.168.0.1
                            """,
                    "x where b = '192.168.0.1'"
            );
        });
    }

    @Test
    public void testContainsIPv4FunctionFactoryError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ip ipv4)");
            assertExceptionNoLeakCheck("select * from t where ip << '1.1.1.1/35'", 28, "invalid argument: 1.1.1.1/35");
            assertExceptionNoLeakCheck("select * from t where ip << '1.1.1.1/-1'", 28, "invalid argument: 1.1.1.1/-1");
            assertExceptionNoLeakCheck("select * from t where ip << '1.1.1.1/A'", 28, "invalid argument: 1.1.1.1/A");
            assertExceptionNoLeakCheck("select * from t where ip << '1.1/26'", 28, "invalid argument: 1.1/26");
        });
    }

    @Test
    public void testCountIPv4() throws Exception {
        assertQuery(
                """
                        count\tbytes
                        12\t0
                        8\t1
                        10\t2
                        6\t3
                        14\t4
                        12\t5
                        6\t6
                        12\t7
                        9\t8
                        7\t9
                        4\t10
                        """,
                "select count(ip), bytes from test order by bytes",
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
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        187.139.150.80
                        18.206.96.238
                        92.80.211.65
                        212.159.205.29
                        4.98.173.21
                        199.122.166.85
                        79.15.250.138
                        35.86.82.23
                        111.98.117.250
                        205.123.179.216
                        """,
                "select rnd_ipv4()::string from long_sequence(10)"
        ));
    }

    @Test
    public void testCreateAsSelectCastStrToIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::string col from long_sequence(0))");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into x values('0.0.0.6')");
            execute("insert into x values('0.0.0.7')");
            execute("insert into x values('0.0.0.8')");
            execute("insert into x values('0.0.0.9')");
            execute("insert into x values('0.0.0.10')");

            engine.releaseInactive();

            assertQueryNoLeakCheck(
                    """
                            col
                            0.0.0.1
                            \
                            0.0.0.2
                            \
                            0.0.0.3
                            \
                            0.0.0.4
                            \
                            0.0.0.5
                            \
                            0.0.0.6
                            \
                            0.0.0.7
                            \
                            0.0.0.8
                            \
                            0.0.0.9
                            \
                            0.0.0.10
                            """,
                    "select * from y",
                    "create table y as (x), cast(col as ipv4)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testCreateAsSelectCastVarcharToIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::varchar col from long_sequence(0))");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into x values('0.0.0.6')");
            execute("insert into x values('0.0.0.7')");
            execute("insert into x values('0.0.0.8')");
            execute("insert into x values('0.0.0.9')");
            execute("insert into x values('0.0.0.10')");

            engine.releaseInactive();

            assertQueryNoLeakCheck(
                    """
                            col
                            0.0.0.1
                            \
                            0.0.0.2
                            \
                            0.0.0.3
                            \
                            0.0.0.4
                            \
                            0.0.0.5
                            \
                            0.0.0.6
                            \
                            0.0.0.7
                            \
                            0.0.0.8
                            \
                            0.0.0.9
                            \
                            0.0.0.10
                            """,
                    "select * from y",
                    "create table y as (x), cast(col as ipv4)",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testExplicitCastIPv4ToStr() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        1.1.1.1
                        """,
                "select ipv4 '1.1.1.1'::string"
        ));
    }

    @Test
    public void testExplicitCastIPv4ToStr2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        1.1.1.1
                        """,
                "select '1.1.1.1'::ipv4::string"
        ));
    }

    @Test
    public void testExplicitCastIntToIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        18.206.96.238
                        212.159.205.29
                        199.122.166.85
                        35.86.82.23
                        205.123.179.216
                        134.75.235.20
                        162.25.160.241
                        92.26.178.136
                        93.204.45.145
                        20.62.93.114
                        """,
                "select rnd_int()::ipv4 from long_sequence(10)"
        ));
    }

    @Test
    public void testExplicitCastNullToIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        
                        187.139.150.80
                        18.206.96.238
                        92.80.211.65
                        212.159.205.29
                        4.98.173.21
                        199.122.166.85
                        79.15.250.138
                        35.86.82.23
                        111.98.117.250
                        """,
                "select cast(case when x = 1 then null else rnd_ipv4() end as string) from long_sequence(10)"
        ));
    }

    @Test
    public void testExplicitCastStrIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        253.253.253.253
                        """,
                "select ~ ipv4 '2.2.2.2'"
        ));
    }

    @Test
    public void testExplicitCastStrToIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        187.139.150.80
                        18.206.96.238
                        92.80.211.65
                        212.159.205.29
                        4.98.173.21
                        199.122.166.85
                        79.15.250.138
                        35.86.82.23
                        111.98.117.250
                        205.123.179.216
                        """,
                "select rnd_ipv4()::string::ipv4 from long_sequence(10)"
        ));
    }

    @Test
    public void testExplicitCastStrToIPv4Null() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        """,
                "select rnd_str()::ipv4 from long_sequence(10)"
        ));
    }

    @Test
    public void testFirstIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            first
                            10.5.96.238
                            """,
                    "select first(ip) from test"
            );
        });
    }

    @Test
    public void testFullJoinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(100))");
            execute("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(100))");
            assertQueryNoLeakCheckWithFatJoin(
                    "select a.count, a.ip, b.ip2, b.count2 from '*!*test' a join '*!*test2' b on b.ip2 = a.ip",
                    """
                            count\tip\tip2\tcount2
                            1\t12.5.9.227\t12.5.9.227\t2
                            1\t12.5.9.23\t12.5.9.23\t2
                            1\t12.5.9.145\t12.5.9.145\t2
                            1\t12.5.9.159\t12.5.9.159\t2
                            1\t12.5.9.159\t12.5.9.159\t2
                            1\t12.5.9.115\t12.5.9.115\t2
                            1\t12.5.9.216\t12.5.9.216\t2
                            1\t12.5.9.216\t12.5.9.216\t2
                            1\t12.5.9.216\t12.5.9.216\t2
                            1\t12.5.9.48\t12.5.9.48\t2
                            1\t12.5.9.228\t12.5.9.228\t2
                            1\t12.5.9.228\t12.5.9.228\t2
                            1\t12.5.9.117\t12.5.9.117\t2
                            1\t12.5.9.179\t12.5.9.179\t2
                            1\t12.5.9.48\t12.5.9.48\t2
                            1\t12.5.9.26\t12.5.9.26\t2
                            1\t12.5.9.240\t12.5.9.240\t2
                            1\t12.5.9.194\t12.5.9.194\t2
                            1\t12.5.9.137\t12.5.9.137\t2
                            1\t12.5.9.179\t12.5.9.179\t2
                            1\t12.5.9.179\t12.5.9.179\t2
                            1\t12.5.9.159\t12.5.9.159\t2
                            1\t12.5.9.159\t12.5.9.159\t2
                            1\t12.5.9.215\t12.5.9.215\t2
                            1\t12.5.9.184\t12.5.9.184\t2
                            1\t12.5.9.46\t12.5.9.46\t2
                            1\t12.5.9.184\t12.5.9.184\t2
                            1\t12.5.9.147\t12.5.9.147\t2
                            1\t12.5.9.152\t12.5.9.152\t2
                            1\t12.5.9.28\t12.5.9.28\t2
                            1\t12.5.9.20\t12.5.9.20\t2
                            1\t12.5.9.20\t12.5.9.20\t2
                            """,
                    null,
                    true,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFullJoinIPv4Fails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(100))");
            execute("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(100))");
            engine.releaseInactive();
            String query = "select a.count, a.ip, b.ip2, b.count2 from '*!*test' a join '*!*test2' b on b.ip2 = a.count";

            try {
                assertExceptionNoLeakCheck(query, sqlExecutionContext, true);
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "join column type mismatch");
                Assert.assertEquals(Chars.toString(query), 76, ex.getPosition());
            }
        });
    }

    @Test
    public void testGreaterThanEqIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        true
                        """,
                "select ipv4 '34.11.45.3' >= ipv4 '22.1.200.89'"
        ));
    }

    @Test
    public void testGreaterThanEqIPv4BadStr() throws Exception {
        assertException(
                "select ipv4 '34.11.45.3' >= ipv4 'apple'",
                33,
                "invalid IPv4 constant"
        );
    }

    @Test
    public void testGreaterThanEqIPv4Null() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        false
                        """,
                "select ipv4 '34.11.45.3' >= ipv4 '0.0.0.0'"
        ));
    }

    @Test
    public void testGreaterThanEqIPv4Null2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        false
                        """,
                "select ipv4 '34.11.45.3' >= null"
        ));
    }

    @Test
    public void testGreaterThanEqIPv4Null3() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        false
                        """,
                "select null >= ipv4 '34.11.45.3'"
        ));
    }

    @Test
    public void testGreaterThanIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        true
                        """,
                "select ipv4 '34.11.45.3' > ipv4 '22.1.200.89'"
        ));
    }

    @Test
    public void testGroupByIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            count\tip
                            6\t
                            1\t10.5.20.236
                            1\t10.5.45.159
                            1\t10.5.76.40
                            1\t10.5.93.114
                            1\t10.5.95.15
                            1\t10.5.96.238
                            1\t10.5.121.252
                            1\t10.5.132.196
                            1\t10.5.170.235
                            1\t10.5.173.21
                            1\t10.5.212.34
                            1\t10.5.236.196
                            1\t10.5.249.199
                            1\t10.5.250.138
                            """,
                    "select count(count), ip from test group by ip order by ip"
            );
        });
    }

    @Test
    public void testGroupByIPv42() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            sum\tip
                            6\t
                            1\t10.5.20.236
                            1\t10.5.45.159
                            1\t10.5.76.40
                            1\t10.5.93.114
                            1\t10.5.95.15
                            1\t10.5.96.238
                            1\t10.5.121.252
                            1\t10.5.132.196
                            1\t10.5.170.235
                            1\t10.5.173.21
                            1\t10.5.212.34
                            1\t10.5.236.196
                            1\t10.5.249.199
                            1\t10.5.250.138
                            """,
                    "select sum(count), ip from test group by ip order by ip"
            );
        });
    }

    @Test
    public void testGroupByIPv43() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5.6/30', 2) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            sum\tip
                            6\t
                            5\t10.5.6.0
                            1\t10.5.6.1
                            4\t10.5.6.2
                            4\t10.5.6.3
                            """,
                    "select sum(count), ip from test group by ip order by ip"
            );
        });
    }

    @Test
    public void testIPv4BitOr() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        255.1.1.1
                        """,
                "select ipv4 '1.1.1.1' | '255.0.0.0'"
        ));
    }

    @Test
    public void testIPv4BitOr2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        255.1.1.1
                        """,
                "select '1.1.1.1' | ipv4 '255.0.0.0'"
        ));
    }

    @Test
    public void testIPv4BitwiseAndChar() throws Exception {
        assertException(
                "select ipv4 '1.1.1.1' & '0'",
                22,
                "there is no matching operator `&` with the argument types: IPv4 & CHAR"
        );

        assertException(
                "select ipv4 '1.1.1.1' | '0'",
                22,
                "there is no matching operator `|` with the argument types: IPv4 | CHAR"
        );

        assertException(
                "select '0' & ipv4 '1.1.1.1'",
                11,
                "there is no matching operator `&` with the argument types: CHAR & IPv4"
        );

        assertException(
                "select '0' | ipv4 '1.1.1.1'",
                11,
                "there is no matching operator `|` with the argument types: CHAR | IPv4"
        );
    }

    @Test
    public void testIPv4BitwiseAndConst() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        0.0.1.1
                        """,
                "select ipv4 '1.1.1.1' & ipv4 '0.0.1.1'"
        ));
    }

    @Test
    public void testIPv4BitwiseAndFails() throws Exception {
        assertQuery(
                """
                        column
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        """,
                "select ip & cast(s as ipv4) from test",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    'apple' s," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(10)" +
                        ") ",
                null,
                true,
                true
        );
    }

    @Test
    public void testIPv4BitwiseAndFailsConst() throws Exception {
        assertException(
                "select ipv4 '1.1.1.1' & ipv4 '0.0.1'",
                29,
                "invalid IPv4 constant"
        );
    }

    @Test
    public void testIPv4BitwiseAndHalfConst() throws Exception {
        assertQuery(
                """
                        column
                        0.0.0.12
                        0.0.0.9
                        0.0.0.13
                        0.0.0.8
                        0.0.0.9
                        0.0.0.20
                        0.0.0.9
                        0.0.0.15
                        0.0.0.8
                        0.0.0.20
                        0.0.0.15
                        0.0.0.15
                        0.0.0.4
                        0.0.0.8
                        0.0.0.16
                        0.0.0.19
                        0.0.0.14
                        0.0.0.3
                        0.0.0.2
                        0.0.0.20
                        0.0.0.8
                        0.0.0.9
                        0.0.0.13
                        0.0.0.8
                        0.0.0.5
                        0.0.0.18
                        0.0.0.20
                        0.0.0.20
                        0.0.0.5
                        0.0.0.5
                        0.0.0.4
                        0.0.0.10
                        0.0.0.9
                        0.0.0.16
                        0.0.0.6
                        0.0.0.7
                        0.0.0.18
                        0.0.0.2
                        0.0.0.17
                        0.0.0.4
                        0.0.0.5
                        0.0.0.9
                        0.0.0.9
                        0.0.0.1
                        0.0.0.7
                        0.0.0.16
                        0.0.0.4
                        0.0.0.1
                        0.0.0.2
                        0.0.0.4
                        0.0.0.10
                        0.0.0.17
                        0.0.0.11
                        0.0.0.5
                        0.0.0.18
                        0.0.0.15
                        0.0.0.4
                        0.0.0.2
                        0.0.0.4
                        0.0.0.4
                        0.0.0.1
                        0.0.0.13
                        0.0.0.8
                        0.0.0.19
                        0.0.0.7
                        0.0.0.18
                        0.0.0.6
                        0.0.0.2
                        0.0.0.3
                        0.0.0.2
                        0.0.0.16
                        0.0.0.12
                        0.0.0.1
                        0.0.0.11
                        0.0.0.6
                        0.0.0.6
                        0.0.0.3
                        0.0.0.10
                        0.0.0.15
                        0.0.0.5
                        0.0.0.6
                        0.0.0.2
                        0.0.0.9
                        0.0.0.16
                        0.0.0.18
                        0.0.0.15
                        0.0.0.16
                        0.0.0.9
                        0.0.0.1
                        0.0.0.20
                        0.0.0.18
                        0.0.0.15
                        0.0.0.10
                        0.0.0.12
                        0.0.0.1
                        0.0.0.7
                        0.0.0.5
                        0.0.0.11
                        0.0.0.16
                        0.0.0.12
                        """,
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
    public void testIPv4BitwiseAndStr() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                """
                        column
                        0.0.1.1
                        """,
                "select '1.1.1.1' & '0.0.1.1'",
                null,
                true,
                true
        ));
    }

    @Test
    public void testIPv4BitwiseAndVar() throws Exception {
        assertQuery(
                """
                        column
                        0.0.0.12
                        
                        
                        
                        0.0.0.1
                        
                        0.0.0.1
                        0.0.0.8
                        0.0.0.16
                        0.0.0.1
                        
                        0.0.0.20
                        
                        
                        0.0.0.5
                        
                        0.0.0.9
                        0.0.0.5
                        0.0.0.4
                        
                        0.0.0.6
                        0.0.0.5
                        0.0.0.1
                        0.0.0.4
                        0.0.0.5
                        
                        0.0.0.1
                        
                        0.0.0.4
                        
                        
                        
                        0.0.0.18
                        0.0.0.1
                        0.0.0.1
                        
                        
                        
                        0.0.0.5
                        0.0.0.2
                        
                        0.0.0.2
                        
                        0.0.0.1
                        0.0.0.2
                        0.0.0.6
                        
                        0.0.0.1
                        
                        
                        0.0.0.2
                        0.0.0.14
                        
                        
                        
                        0.0.0.8
                        
                        0.0.0.4
                        0.0.0.8
                        
                        0.0.0.1
                        0.0.0.6
                        0.0.0.2
                        0.0.0.16
                        0.0.0.2
                        0.0.0.1
                        
                        
                        
                        0.0.0.2
                        0.0.0.2
                        0.0.0.2
                        0.0.0.1
                        0.0.0.2
                        
                        0.0.0.1
                        
                        
                        
                        0.0.0.16
                        0.0.0.4
                        0.0.0.5
                        0.0.0.5
                        0.0.0.1
                        
                        0.0.0.12
                        
                        0.0.0.4
                        0.0.0.10
                        0.0.0.8
                        0.0.0.9
                        0.0.0.8
                        0.0.0.16
                        0.0.0.2
                        0.0.0.16
                        
                        
                        0.0.0.2
                        
                        0.0.0.6
                        """,
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
    public void testIPv4BitwiseFails() throws Exception {
        assertException(
                "select ~ ipv4 'apple'",
                14,
                "invalid IPv4 constant"
        );
    }

    @Test
    public void testIPv4BitwiseNotConst() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        254.254.254.254
                        """,
                "select ~ ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testIPv4BitwiseNotVar() throws Exception {
        assertQuery(
                """
                        column
                        255.255.255.243
                        255.255.255.246
                        255.255.255.242
                        255.255.255.247
                        255.255.255.246
                        255.255.255.235
                        255.255.255.246
                        255.255.255.240
                        255.255.255.247
                        255.255.255.235
                        255.255.255.240
                        255.255.255.240
                        255.255.255.251
                        255.255.255.247
                        255.255.255.239
                        255.255.255.236
                        255.255.255.241
                        255.255.255.252
                        255.255.255.253
                        255.255.255.235
                        255.255.255.247
                        255.255.255.246
                        255.255.255.242
                        255.255.255.247
                        255.255.255.250
                        255.255.255.237
                        255.255.255.235
                        255.255.255.235
                        255.255.255.250
                        255.255.255.250
                        255.255.255.251
                        255.255.255.245
                        255.255.255.246
                        255.255.255.239
                        255.255.255.249
                        255.255.255.248
                        255.255.255.237
                        255.255.255.253
                        255.255.255.238
                        255.255.255.251
                        255.255.255.250
                        255.255.255.246
                        255.255.255.246
                        255.255.255.254
                        255.255.255.248
                        255.255.255.239
                        255.255.255.251
                        255.255.255.254
                        255.255.255.253
                        255.255.255.251
                        255.255.255.245
                        255.255.255.238
                        255.255.255.244
                        255.255.255.250
                        255.255.255.237
                        255.255.255.240
                        255.255.255.251
                        255.255.255.253
                        255.255.255.251
                        255.255.255.251
                        255.255.255.254
                        255.255.255.242
                        255.255.255.247
                        255.255.255.236
                        255.255.255.248
                        255.255.255.237
                        255.255.255.249
                        255.255.255.253
                        255.255.255.252
                        255.255.255.253
                        255.255.255.239
                        255.255.255.243
                        255.255.255.254
                        255.255.255.244
                        255.255.255.249
                        255.255.255.249
                        255.255.255.252
                        255.255.255.245
                        255.255.255.240
                        255.255.255.250
                        255.255.255.249
                        255.255.255.253
                        255.255.255.246
                        255.255.255.239
                        255.255.255.237
                        255.255.255.240
                        255.255.255.239
                        255.255.255.246
                        255.255.255.254
                        255.255.255.235
                        255.255.255.237
                        255.255.255.240
                        255.255.255.245
                        255.255.255.243
                        255.255.255.254
                        255.255.255.248
                        255.255.255.250
                        255.255.255.244
                        255.255.255.239
                        255.255.255.243
                        """,
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
                """
                        column
                        255.0.0.12
                        255.0.0.9
                        255.0.0.13
                        255.0.0.8
                        255.0.0.9
                        255.0.0.20
                        255.0.0.9
                        255.0.0.15
                        255.0.0.8
                        255.0.0.20
                        255.0.0.15
                        255.0.0.15
                        255.0.0.4
                        255.0.0.8
                        255.0.0.16
                        255.0.0.19
                        255.0.0.14
                        255.0.0.3
                        255.0.0.2
                        255.0.0.20
                        255.0.0.8
                        255.0.0.9
                        255.0.0.13
                        255.0.0.8
                        255.0.0.5
                        255.0.0.18
                        255.0.0.20
                        255.0.0.20
                        255.0.0.5
                        255.0.0.5
                        255.0.0.4
                        255.0.0.10
                        255.0.0.9
                        255.0.0.16
                        255.0.0.6
                        255.0.0.7
                        255.0.0.18
                        255.0.0.2
                        255.0.0.17
                        255.0.0.4
                        255.0.0.5
                        255.0.0.9
                        255.0.0.9
                        255.0.0.1
                        255.0.0.7
                        255.0.0.16
                        255.0.0.4
                        255.0.0.1
                        255.0.0.2
                        255.0.0.4
                        255.0.0.10
                        255.0.0.17
                        255.0.0.11
                        255.0.0.5
                        255.0.0.18
                        255.0.0.15
                        255.0.0.4
                        255.0.0.2
                        255.0.0.4
                        255.0.0.4
                        255.0.0.1
                        255.0.0.13
                        255.0.0.8
                        255.0.0.19
                        255.0.0.7
                        255.0.0.18
                        255.0.0.6
                        255.0.0.2
                        255.0.0.3
                        255.0.0.2
                        255.0.0.16
                        255.0.0.12
                        255.0.0.1
                        255.0.0.11
                        255.0.0.6
                        255.0.0.6
                        255.0.0.3
                        255.0.0.10
                        255.0.0.15
                        255.0.0.5
                        255.0.0.6
                        255.0.0.2
                        255.0.0.9
                        255.0.0.16
                        255.0.0.18
                        255.0.0.15
                        255.0.0.16
                        255.0.0.9
                        255.0.0.1
                        255.0.0.20
                        255.0.0.18
                        255.0.0.15
                        255.0.0.10
                        255.0.0.12
                        255.0.0.1
                        255.0.0.7
                        255.0.0.5
                        255.0.0.11
                        255.0.0.16
                        255.0.0.12
                        """,
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
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        255.1.1.1
                        """,
                "select ipv4 '1.1.1.1' | ipv4 '255.0.0.0'"
        ));
    }

    @Test
    public void testIPv4BitwiseOrStr() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                """
                        column
                        1.1.1.1
                        """,
                "select '1.1.1.1' | '0.0.1.1'",
                null,
                true,
                true
        ));
    }

    @Test
    public void testIPv4BitwiseOrVar() throws Exception {
        assertQuery(
                """
                        column
                        0.0.0.12
                        0.0.0.22
                        0.0.0.27
                        0.0.0.29
                        0.0.0.17
                        0.0.0.23
                        0.0.0.15
                        0.0.0.15
                        0.0.0.19
                        0.0.0.31
                        0.0.0.11
                        0.0.0.20
                        0.0.0.12
                        0.0.0.23
                        0.0.0.7
                        0.0.0.19
                        0.0.0.15
                        0.0.0.13
                        0.0.0.20
                        0.0.0.17
                        0.0.0.6
                        0.0.0.15
                        0.0.0.25
                        0.0.0.13
                        0.0.0.13
                        0.0.0.29
                        0.0.0.15
                        0.0.0.29
                        0.0.0.12
                        0.0.0.21
                        0.0.0.27
                        0.0.0.15
                        0.0.0.18
                        0.0.0.15
                        0.0.0.21
                        0.0.0.12
                        0.0.0.7
                        0.0.0.22
                        0.0.0.7
                        0.0.0.22
                        0.0.0.7
                        0.0.0.19
                        0.0.0.26
                        0.0.0.3
                        0.0.0.14
                        0.0.0.7
                        0.0.0.30
                        0.0.0.21
                        0.0.0.7
                        0.0.0.14
                        0.0.0.30
                        0.0.0.15
                        0.0.0.19
                        0.0.0.29
                        0.0.0.23
                        0.0.0.12
                        0.0.0.19
                        0.0.0.15
                        0.0.0.13
                        0.0.0.28
                        0.0.0.11
                        0.0.0.6
                        0.0.0.22
                        0.0.0.21
                        0.0.0.3
                        0.0.0.1
                        0.0.0.29
                        0.0.0.30
                        0.0.0.23
                        0.0.0.27
                        0.0.0.30
                        0.0.0.14
                        0.0.0.11
                        0.0.0.19
                        0.0.0.20
                        0.0.0.25
                        0.0.0.26
                        0.0.0.30
                        0.0.0.30
                        0.0.0.17
                        0.0.0.15
                        0.0.0.15
                        0.0.0.15
                        0.0.0.31
                        0.0.0.23
                        0.0.0.14
                        0.0.0.11
                        0.0.0.29
                        0.0.0.15
                        0.0.0.11
                        0.0.0.13
                        0.0.0.15
                        0.0.0.23
                        0.0.0.22
                        0.0.0.23
                        0.0.0.15
                        0.0.0.15
                        0.0.0.14
                        0.0.0.11
                        0.0.0.15
                        """,
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
        assertMemoryLeak(() -> {
            execute("create table test (col ipv4)");
            execute("insert into test values('12.67.45.3')");
            execute("insert into test values('160.5.22.8')");
            execute("insert into test values('240.110.88.22')");
            execute("insert into test values('1.6.2.0')");
            execute("insert into test values('255.255.255.255')");
            execute("insert into test values('0.0.0.0')");

            assertSql("col\n", "select * from test where col <<= '12.67.50.2/20'");
            assertSql(
                    """
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """,
                    "select * from test where col <<= '12.67.50.2/1'"
            );
            assertSql(
                    """
                            col
                            255.255.255.255
                            """,
                    "select * from test where col <<= '255.6.8.10/8'"
            );
            assertSql(
                    """
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """,
                    "select * from test where col <<= '12.67.50.2/0'"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where col <<= '1.6.2.0/32'"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where col <<= '1.6.2.0'"
            );
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetAndMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,1000,0)::ipv4 ip from long_sequence(100))");

            assertSql(
                    """
                            ip
                            0.0.0.167
                            0.0.0.182
                            0.0.0.108
                            0.0.0.95
                            0.0.0.12
                            0.0.0.71
                            0.0.0.10
                            0.0.0.238
                            0.0.0.105
                            0.0.0.203
                            0.0.0.86
                            0.0.0.100
                            0.0.0.144
                            0.0.0.173
                            0.0.0.121
                            0.0.0.231
                            0.0.0.181
                            0.0.0.218
                            0.0.0.34
                            0.0.0.90
                            0.0.0.114
                            0.0.0.23
                            0.0.0.150
                            0.0.0.147
                            
                            0.0.0.29
                            0.0.0.159
                            """,
                    "select * from test where ip <<= '0.0.0/24'"
            );
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """,
                    "select * from test where ip <<= subnet"
            );
        });
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
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(100))");

            assertSql(
                    """
                            ip
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            """,
                    "select * from test where ip <<= '0.0.0.4'"
            );
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col ipv4)");
            execute("insert into test values('12.67.45.3')");
            execute("insert into test values('160.5.22.8')");
            execute("insert into test values('240.110.88.22')");
            execute("insert into test values('1.6.2.0')");
            execute("insert into test values('255.255.255.255')");
            execute("insert into test values('0.0.0.0')");

            assertSql("col\n", "select * from test where col <<= '12.67.50.2/20'::varchar");
            assertSql(
                    """
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """,
                    "select * from test where col <<= '12.67.50.2/1'::varchar"
            );
            assertSql(
                    """
                            col
                            255.255.255.255
                            """,
                    "select * from test where col <<= '255.6.8.10/8'::varchar"
            );
            assertSql(
                    """
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """,
                    "select * from test where col <<= '12.67.50.2/0'::varchar"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where col <<= '1.6.2.0/32'::varchar"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where col <<= '1.6.2.0'::varchar"
            );
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetVarcharColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """,
                    "select * from test where ip <<= subnet"
            );
        });
    }

    @Test
    public void testIPv4ContainsSubnet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertSql("ip\n", "select * from test where ip << '0.0.0.1'");
        });
    }

    @Test
    public void testIPv4ContainsSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertSql("ip\n", "select * from test where ip << '0.0.0.1/32'");
        });
    }

    @Test
    public void testIPv4ContainsSubnet3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertSql(
                    """
                            ip
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.1
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            """,
                    "select * from test where ip << '0.0.0.1/24'"
            );
        });
    }

    @Test
    public void testIPv4ContainsSubnet4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """,
                    "select * from test where ip << subnet"
            );
        });
    }

    @Test
    public void testIPv4ContainsVarcharSubnet1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertSql(
                    """
                            ip
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.1
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            """,
                    "select * from test where ip << '0.0.0.1/24'::varchar"
            );
        });
    }

    @Test
    public void testIPv4ContainsVarcharSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """,
                    "select * from test where ip << subnet"
            );
        });
    }

    @Test
    public void testIPv4CountDistinct() throws Exception {
        String expected = """
                count_distinct
                20
                """;
        assertQuery(
                expected,
                "select count_distinct(ip) from test",
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
        assertSql(expected, "select count(distinct ip) from test");
    }

    @Test
    public void testIPv4Distinct() throws Exception {
        assertQuery(
                """
                        ip
                        0.0.0.1
                        0.0.0.2
                        0.0.0.3
                        0.0.0.4
                        0.0.0.5
                        """,
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
                true
        );
    }

    @Test
    public void testIPv4EqArgsSwapped() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

            assertSql(
                    """
                            ip
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            """,
                    "select * from test where ipv4 '0.0.0.1' = ip"
            );
        });
    }

    @Test
    public void testIPv4EqNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

            assertSql(
                    """
                            ip
                            0.0.0.5
                            0.0.0.2
                            
                            0.0.0.4
                            0.0.0.5
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            
                            0.0.0.5
                            0.0.0.4
                            0.0.0.2
                            0.0.0.2
                            0.0.0.4
                            0.0.0.3
                            0.0.0.5
                            """,
                    "select * from test where ip != ipv4 '0.0.0.1'"
            );
        });
    }

    @Test
    public void testIPv4EqNegated2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

            assertSql(
                    """
                            ip
                            0.0.0.5
                            0.0.0.2
                            
                            0.0.0.4
                            0.0.0.5
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            
                            0.0.0.5
                            0.0.0.4
                            0.0.0.2
                            0.0.0.2
                            0.0.0.4
                            0.0.0.3
                            0.0.0.5
                            """,
                    "select * from test where ipv4 '0.0.0.1' != ip"
            );
        });
    }

    @Test
    public void testIPv4EqNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(100))");

            assertSql(
                    """
                            ip
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            """,
                    "select * from test where ip = null"
            );
        });
    }

    @Test
    public void testIPv4Except() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (col1 ipv4)");
            execute("create table y (col2 ipv4)");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into x values('0.0.0.6')");
            execute("insert into y values('0.0.0.1')");
            execute("insert into y values('0.0.0.2')");
            execute("insert into y values('0.0.0.3')");
            execute("insert into y values('0.0.0.4')");
            execute("insert into y values('0.0.0.5')");

            assertSql(
                    """
                            col1
                            0.0.0.6
                            """,
                    "select col1 from x except select col2 from y"
            );
        });
    }

    @Test
    public void testIPv4Explain() throws Exception {
        assertQuery(
                """
                        QUERY PLAN
                        Radix sort light
                          keys: [ip desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                        """,
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
        assertMemoryLeak(() -> {
            execute("create table x (col1 ipv4)");
            execute("create table y (col2 ipv4)");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into x values('0.0.0.6')");
            execute("insert into y values('0.0.0.1')");
            execute("insert into y values('0.0.0.2')");

            assertSql(
                    """
                            col1
                            0.0.0.1
                            0.0.0.2
                            """,
                    "select col1 from x intersect all select col2 from y"
            );
        });
    }

    @Test
    public void testIPv4IsOrderedAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, bytes int)");
            execute("insert into test values ('0.0.0.1', 1)");
            execute("insert into test values ('0.0.0.2', 1)");
            execute("insert into test values ('0.0.0.3', 1)");
            execute("insert into test values ('0.0.0.4', 1)");
            execute("insert into test values ('0.0.0.5', 1)");

            assertSql(
                    """
                            isOrdered
                            true
                            """,
                    "select isOrdered(ip) from test"
            );
        });
    }

    @Test
    public void testIPv4IsOrderedFalse() throws Exception {
        assertQuery(
                """
                        isOrdered
                        false
                        """,
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
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, bytes int)");
            execute("insert into test values ('0.0.0.0', 1)");
            execute("insert into test values ('0.0.0.0', 1)");
            execute("insert into test values ('0.0.0.0', 1)");
            execute("insert into test values ('0.0.0.0', 1)");
            execute("insert into test values ('0.0.0.0', 1)");

            assertSql(
                    """
                            isOrdered
                            true
                            """,
                    "select isOrdered(ip) from test"
            );
        });
    }

    @Test
    public void testIPv4IsOrderedSame() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ip ipv4, bytes int)");
            execute("insert into test values ('0.0.0.12', 1)");
            execute("insert into test values ('0.0.0.12', 1)");
            execute("insert into test values ('0.0.0.12', 1)");
            execute("insert into test values ('0.0.0.12', 1)");
            execute("insert into test values ('0.0.0.12', 1)");

            assertSql(
                    """
                            isOrdered
                            true
                            """,
                    "select isOrdered(ip) from test"
            );
        });
    }

    @Test
    public void testIPv4MinusIPv4Char() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                """
                        column
                        1.1.0.248
                        """,
                "select ipv4 '1.1.1.1' - '9'",
                true
        ));
    }

    @Test
    public void testIPv4MinusIPv4Const() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        -4278124286
                        """,
                "select ipv4 '1.1.1.1' - ipv4 '255.255.255.255'"
        ));
    }

    @Test
    public void testIPv4MinusIPv4ConstNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        null
                        """,
                "select ipv4 '0.0.0.0' - ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testIPv4MinusIPv4HalfConst() throws Exception {
        assertQuery(
                """
                        column
                        2773848137
                        -299063538
                        1496084467
                        2485446343
                        1196850877
                        1158191828
                        752939968
                        3837158002
                        2820505953
                        2797158930
                        860245551
                        1326914642
                        3499386522
                        3619032084
                        3460716594
                        3438474390
                        -304373661
                        86179701
                        2503987003
                        2347192664
                        3255296908
                        1265208177
                        2135218764
                        -211046476
                        2383725862
                        2622936746
                        1484573162
                        823377430
                        2720404929
                        2339832612
                        862156863
                        3548828754
                        -257891288
                        3190861944
                        2198440386
                        1846652797
                        2153992830
                        1422720116
                        493192821
                        466104543
                        3304290560
                        3854300225
                        1834010571
                        1811077867
                        1226040229
                        3639006165
                        865851868
                        642416689
                        990194656
                        3282022737
                        """,
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
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        16843008
                        """,
                "select ipv4 '1.1.1.1' - '0.0.0.1'"
        ));
    }

    @Test
    public void testIPv4MinusIPv4Var() throws Exception {
        assertQuery(
                """
                        column
                        -11
                        17
                        121
                        129
                        40
                        -76
                        -88
                        50
                        -111
                        47
                        154
                        -18
                        131
                        136
                        164
                        -74
                        -11
                        -15
                        -49
                        42
                        67
                        -59
                        -31
                        14
                        57
                        -45
                        -165
                        45
                        -55
                        31
                        -16
                        74
                        8
                        153
                        -84
                        -45
                        -35
                        169
                        71
                        92
                        47
                        -28
                        113
                        -77
                        104
                        25
                        191
                        213
                        50
                        -96
                        """,
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
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        1.1.1.0
                        """,
                "select ipv4 '1.1.1.1' - 1"
        ));
    }

    @Test
    public void testIPv4MinusIntConst2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        0.0.0.1
                        """,
                "select ipv4 '1.1.1.1' - 16843008"
        ));
    }

    @Test
    public void testIPv4MinusIntConstOverflow() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select ipv4 '1.1.1.1' - 16843010"
        ));
    }

    @Test
    public void testIPv4MinusIntHalfConst() throws Exception {
        assertQuery(
                """
                        column
                        0.0.0.7
                        
                        0.0.0.13
                        0.0.0.15
                        
                        
                        0.0.0.10
                        0.0.0.4
                        0.0.0.14
                        0.0.0.14
                        
                        0.0.0.15
                        0.0.0.3
                        0.0.0.11
                        0.0.0.2
                        0.0.0.13
                        0.0.0.6
                        0.0.0.8
                        
                        
                        0.0.0.1
                        0.0.0.2
                        0.0.0.12
                        0.0.0.8
                        
                        0.0.0.11
                        0.0.0.4
                        0.0.0.11
                        0.0.0.7
                        0.0.0.12
                        0.0.0.5
                        0.0.0.1
                        0.0.0.13
                        0.0.0.10
                        
                        0.0.0.3
                        
                        0.0.0.11
                        
                        0.0.0.13
                        
                        0.0.0.14
                        0.0.0.11
                        
                        0.0.0.5
                        0.0.0.1
                        0.0.0.7
                        0.0.0.12
                        0.0.0.1
                        0.0.0.3
                        0.0.0.9
                        0.0.0.10
                        0.0.0.11
                        0.0.0.4
                        0.0.0.13
                        0.0.0.3
                        0.0.0.13
                        0.0.0.2
                        0.0.0.7
                        0.0.0.11
                        
                        0.0.0.1
                        0.0.0.1
                        0.0.0.12
                        
                        
                        0.0.0.4
                        0.0.0.13
                        0.0.0.12
                        0.0.0.13
                        0.0.0.13
                        
                        
                        0.0.0.13
                        
                        0.0.0.4
                        0.0.0.5
                        0.0.0.11
                        0.0.0.13
                        0.0.0.11
                        
                        0.0.0.2
                        
                        0.0.0.8
                        0.0.0.14
                        0.0.0.7
                        
                        0.0.0.15
                        0.0.0.6
                        0.0.0.3
                        0.0.0.8
                        0.0.0.5
                        0.0.0.15
                        0.0.0.1
                        0.0.0.14
                        
                        
                        0.0.0.5
                        
                        0.0.0.1
                        """,
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
                """
                        column
                        0.0.4.203
                        0.0.6.190
                        0.0.4.145
                        0.0.2.139
                        0.0.5.33
                        0.0.2.82
                        0.0.6.43
                        0.0.2.70
                        0.0.1.179
                        0.0.2.151
                        0.0.5.56
                        0.0.1.142
                        0.0.1.58
                        0.0.2.210
                        0.0.2.197
                        0.0.5.228
                        0.0.4.42
                        0.0.3.93
                        0.0.5.171
                        0.0.1.116
                        0.0.3.143
                        0.0.3.149
                        0.0.5.119
                        0.0.2.2
                        0.0.7.46
                        0.0.3.253
                        0.0.8.160
                        0.0.7.202
                        0.0.6.59
                        0.0.2.79
                        0.0.5.134
                        0.0.5.79
                        0.0.1.228
                        0.0.0.252
                        0.0.4.25
                        0.0.5.239
                        0.0.3.20
                        0.0.1.159
                        0.0.4.168
                        0.0.0.203
                        0.0.6.12
                        0.0.5.222
                        0.0.2.100
                        0.0.2.99
                        0.0.7.92
                        0.0.4.37
                        0.0.0.231
                        0.0.8.152
                        0.0.1.189
                        0.0.4.89
                        0.0.3.224
                        
                        0.0.8.229
                        0.0.6.127
                        0.0.6.11
                        0.0.5.26
                        0.0.5.250
                        0.0.4.64
                        0.0.2.20
                        0.0.4.16
                        0.0.5.235
                        0.0.3.162
                        0.0.8.157
                        0.0.0.19
                        0.0.0.244
                        0.0.0.42
                        0.0.2.75
                        0.0.1.226
                        0.0.0.253
                        0.0.5.171
                        0.0.7.59
                        0.0.6.183
                        0.0.3.38
                        0.0.4.113
                        0.0.2.138
                        0.0.0.47
                        0.0.3.150
                        0.0.2.138
                        0.0.4.204
                        0.0.5.3
                        0.0.2.67
                        0.0.2.63
                        0.0.8.152
                        0.0.6.195
                        0.0.4.52
                        0.0.2.253
                        0.0.2.37
                        
                        0.0.3.70
                        0.0.6.69
                        0.0.2.19
                        0.0.3.56
                        0.0.4.130
                        0.0.5.164
                        0.0.5.5
                        0.0.8.220
                        0.0.6.186
                        0.0.7.42
                        0.0.0.196
                        
                        """,
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
        assertMemoryLeak(() -> {
            execute("create table test (col ipv4)");
            execute("insert into test values('12.67.45.3')");
            execute("insert into test values('160.5.22.8')");
            execute("insert into test values('240.110.88.22')");
            execute("insert into test values('1.6.2.0')");
            execute("insert into test values('255.255.255.255')");
            execute("insert into test values('0.0.0.0')");

            assertSql("col\n", "select * from test where '12.67.50.2/20' >>= col");
            assertSql(
                    """
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """,
                    "select * from test where '12.67.50.2/1' >>= col"
            );
            assertSql(
                    """
                            col
                            255.255.255.255
                            """,
                    "select * from test where '255.6.8.10/8' >>= col"
            );
            assertSql(
                    """
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """,
                    "select * from test where '12.67.50.2/0' >>= col"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where '1.6.2.0/32' >>= col"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where '1.6.2.0' >>= col"
            );
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetAndMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,2000,0)::ipv4 ip from long_sequence(100))");

            assertSql(
                    """
                            ip
                            0.0.0.115
                            0.0.0.208
                            0.0.0.110
                            0.0.0.90
                            0.0.0.53
                            0.0.0.143
                            0.0.0.246
                            0.0.0.158
                            0.0.0.237
                            0.0.0.220
                            0.0.0.184
                            0.0.0.103
                            """,
                    "select * from test where '0.0.0/24' >>= ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """,
                    "select * from test where subnet >>= ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails() throws Exception {
        assertException(
                "select * from test where '0.0.0.1/hello' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0.1/hello"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails2() throws Exception {
        assertException(
                "select * from test where '0.0.0/hello' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0/hello"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails3() throws Exception {
        assertException(
                "select * from test where '0.0.0.0/65' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0.0/65"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails4() throws Exception {
        assertException(
                "select * from test where '0.0.0/65' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0/65"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails5() throws Exception {
        assertException(
                "select * from test where '0.0.0.0/-1' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0.0/-1"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails6() throws Exception {
        assertException(
                "select * from test where '0.0.0/-1' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0/-1"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsChars() throws Exception {
        assertException(
                "select * from test where 'apple' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: apple"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsNetmaskOverflow() throws Exception {
        assertException(
                "select * from test where '85.7.36/74' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 85.7.36/74"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsNums() throws Exception {
        assertException(
                "select * from test where '8573674' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 8573674"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsOverflow() throws Exception {
        assertException(
                "select * from test where '256.256.256.256' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 256.256.256.256"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetIncorrectMask() throws Exception {
        assertException(
                "select * from test where '0.0.0/32' >>= ip",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)",
                25,
                "invalid argument: 0.0.0/32"
        );
    }

    @Test
    public void testIPv4NegContainsEqSubnetNoMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,2)::ipv4 ip from long_sequence(20))");

            assertSql(
                    """
                            ip
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            """,
                    "select * from test where '0.0.0.4' >>= ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col ipv4)");
            execute("insert into test values('12.67.45.3')");
            execute("insert into test values('160.5.22.8')");
            execute("insert into test values('240.110.88.22')");
            execute("insert into test values('1.6.2.0')");
            execute("insert into test values('255.255.255.255')");
            execute("insert into test values('0.0.0.0')");

            assertSql("col\n", "select * from test where '12.67.50.2/20'::varchar >>= col");
            assertSql(
                    """
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """,
                    "select * from test where '12.67.50.2/1'::varchar >>= col"
            );
            assertSql(
                    """
                            col
                            255.255.255.255
                            """,
                    "select * from test where '255.6.8.10/8'::varchar >>= col"
            );
            assertSql(
                    """
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """,
                    "select * from test where '12.67.50.2/0'::varchar >>= col"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where '1.6.2.0/32'::varchar >>= col"
            );
            assertSql(
                    """
                            col
                            1.6.2.0
                            """,
                    "select * from test where '1.6.2.0'::varchar >>= col"
            );
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetVarcharColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """,
                    "select * from test where subnet >>= ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsSubnet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertSql("ip\n", "select * from test where '0.0.0.1' >> ip");
        });
    }

    @Test
    public void testIPv4NegContainsSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertSql("ip\n", "select * from test where '0.0.0.1/32' >> ip");
        });
    }

    @Test
    public void testIPv4NegContainsSubnet3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertSql(
                    """
                            ip
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.1
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            """,
                    "select * from test where '0.0.0.1/24' >> ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsSubnet4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """,
                    "select * from test where subnet >> ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsVarcharSubnet1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertSql(
                    """
                            ip
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.1
                            0.0.0.2
                            0.0.0.2
                            0.0.0.1
                            """,
                    "select * from test where '0.0.0.1/24'::varchar >> ip"
            );
        });
    }

    @Test
    public void testIPv4NegContainsVarcharSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertSql(
                    """
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """,
                    "select * from test where subnet >> ip"
            );
        });
    }

    @Test
    public void testIPv4Null() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col ipv4)");
            execute("insert into test values(null)");
            assertSql(
                    """
                            col
                            
                            """,
                    "test"
            );
        });
    }

    @Test
    public void testIPv4NullIf() throws Exception {
        assertQuery(
                """
                        k\tnullif
                        1970-01-01T00:00:00.000000Z\t0.0.0.2
                        1970-01-01T00:01:40.000000Z\t0.0.0.9
                        1970-01-01T00:03:20.000000Z\t0.0.0.3
                        1970-01-01T00:05:00.000000Z\t0.0.0.8
                        1970-01-01T00:06:40.000000Z\t0.0.0.9
                        1970-01-01T00:08:20.000000Z\t0.0.0.10
                        1970-01-01T00:10:00.000000Z\t0.0.0.9
                        1970-01-01T00:11:40.000000Z\t
                        1970-01-01T00:13:20.000000Z\t0.0.0.8
                        1970-01-01T00:15:00.000000Z\t0.0.0.10
                        1970-01-01T00:16:40.000000Z\t
                        1970-01-01T00:18:20.000000Z\t
                        1970-01-01T00:20:00.000000Z\t0.0.0.4
                        1970-01-01T00:21:40.000000Z\t0.0.0.8
                        1970-01-01T00:23:20.000000Z\t0.0.0.6
                        1970-01-01T00:25:00.000000Z\t0.0.0.9
                        1970-01-01T00:26:40.000000Z\t0.0.0.4
                        1970-01-01T00:28:20.000000Z\t0.0.0.3
                        1970-01-01T00:30:00.000000Z\t0.0.0.2
                        1970-01-01T00:31:40.000000Z\t0.0.0.10
                        1970-01-01T00:33:20.000000Z\t0.0.0.8
                        1970-01-01T00:35:00.000000Z\t0.0.0.9
                        1970-01-01T00:36:40.000000Z\t0.0.0.3
                        1970-01-01T00:38:20.000000Z\t0.0.0.8
                        1970-01-01T00:40:00.000000Z\t
                        1970-01-01T00:41:40.000000Z\t0.0.0.8
                        1970-01-01T00:43:20.000000Z\t0.0.0.10
                        1970-01-01T00:45:00.000000Z\t0.0.0.10
                        1970-01-01T00:46:40.000000Z\t
                        1970-01-01T00:48:20.000000Z\t
                        1970-01-01T00:50:00.000000Z\t0.0.0.4
                        1970-01-01T00:51:40.000000Z\t0.0.0.10
                        1970-01-01T00:53:20.000000Z\t0.0.0.9
                        1970-01-01T00:55:00.000000Z\t0.0.0.6
                        1970-01-01T00:56:40.000000Z\t0.0.0.6
                        1970-01-01T00:58:20.000000Z\t0.0.0.7
                        1970-01-01T01:00:00.000000Z\t0.0.0.8
                        1970-01-01T01:01:40.000000Z\t0.0.0.2
                        1970-01-01T01:03:20.000000Z\t0.0.0.7
                        1970-01-01T01:05:00.000000Z\t0.0.0.4
                        1970-01-01T01:06:40.000000Z\t
                        1970-01-01T01:08:20.000000Z\t0.0.0.9
                        1970-01-01T01:10:00.000000Z\t0.0.0.9
                        1970-01-01T01:11:40.000000Z\t0.0.0.1
                        1970-01-01T01:13:20.000000Z\t0.0.0.7
                        1970-01-01T01:15:00.000000Z\t0.0.0.6
                        1970-01-01T01:16:40.000000Z\t0.0.0.4
                        1970-01-01T01:18:20.000000Z\t0.0.0.1
                        1970-01-01T01:20:00.000000Z\t0.0.0.2
                        1970-01-01T01:21:40.000000Z\t0.0.0.4
                        1970-01-01T01:23:20.000000Z\t0.0.0.10
                        1970-01-01T01:25:00.000000Z\t0.0.0.7
                        1970-01-01T01:26:40.000000Z\t0.0.0.1
                        1970-01-01T01:28:20.000000Z\t
                        1970-01-01T01:30:00.000000Z\t0.0.0.8
                        1970-01-01T01:31:40.000000Z\t
                        1970-01-01T01:33:20.000000Z\t0.0.0.4
                        1970-01-01T01:35:00.000000Z\t0.0.0.2
                        1970-01-01T01:36:40.000000Z\t0.0.0.4
                        1970-01-01T01:38:20.000000Z\t0.0.0.4
                        1970-01-01T01:40:00.000000Z\t0.0.0.1
                        1970-01-01T01:41:40.000000Z\t0.0.0.3
                        1970-01-01T01:43:20.000000Z\t0.0.0.8
                        1970-01-01T01:45:00.000000Z\t0.0.0.9
                        1970-01-01T01:46:40.000000Z\t0.0.0.7
                        1970-01-01T01:48:20.000000Z\t0.0.0.8
                        1970-01-01T01:50:00.000000Z\t0.0.0.6
                        1970-01-01T01:51:40.000000Z\t0.0.0.2
                        1970-01-01T01:53:20.000000Z\t0.0.0.3
                        1970-01-01T01:55:00.000000Z\t0.0.0.2
                        1970-01-01T01:56:40.000000Z\t0.0.0.6
                        1970-01-01T01:58:20.000000Z\t0.0.0.2
                        1970-01-01T02:00:00.000000Z\t0.0.0.1
                        1970-01-01T02:01:40.000000Z\t0.0.0.1
                        1970-01-01T02:03:20.000000Z\t0.0.0.6
                        1970-01-01T02:05:00.000000Z\t0.0.0.6
                        1970-01-01T02:06:40.000000Z\t0.0.0.3
                        1970-01-01T02:08:20.000000Z\t0.0.0.10
                        1970-01-01T02:10:00.000000Z\t
                        1970-01-01T02:11:40.000000Z\t
                        1970-01-01T02:13:20.000000Z\t0.0.0.6
                        1970-01-01T02:15:00.000000Z\t0.0.0.2
                        1970-01-01T02:16:40.000000Z\t0.0.0.9
                        1970-01-01T02:18:20.000000Z\t0.0.0.6
                        1970-01-01T02:20:00.000000Z\t0.0.0.8
                        1970-01-01T02:21:40.000000Z\t
                        1970-01-01T02:23:20.000000Z\t0.0.0.6
                        1970-01-01T02:25:00.000000Z\t0.0.0.9
                        1970-01-01T02:26:40.000000Z\t0.0.0.1
                        1970-01-01T02:28:20.000000Z\t0.0.0.10
                        1970-01-01T02:30:00.000000Z\t0.0.0.8
                        1970-01-01T02:31:40.000000Z\t
                        1970-01-01T02:33:20.000000Z\t0.0.0.10
                        1970-01-01T02:35:00.000000Z\t0.0.0.2
                        1970-01-01T02:36:40.000000Z\t0.0.0.1
                        1970-01-01T02:38:20.000000Z\t0.0.0.7
                        1970-01-01T02:40:00.000000Z\t
                        1970-01-01T02:41:40.000000Z\t0.0.0.1
                        1970-01-01T02:43:20.000000Z\t0.0.0.6
                        1970-01-01T02:45:00.000000Z\t0.0.0.2
                        """,
                "select k, nullif(ip, '0.0.0.5') from test",
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
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        1.1.1.21
                        """,
                "select ipv4 '1.1.1.1' + 20"
        ));
    }

    @Test
    public void testIPv4PlusIntConst2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        255.255.255.255
                        """,
                "select ipv4 '255.255.255.20' + 235"
        ));
    }

    @Test
    public void testIPv4PlusIntConst3() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select  ('255.255.255.255')::ipv4 + 10"
        ));
    }

    @Test
    public void testIPv4PlusIntConstNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select ipv4 '0.0.0.0' + 20"
        ));
    }

    @Test
    public void testIPv4PlusIntConstOverflow() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select ipv4 '255.255.255.255' + 1"
        ));
    }

    @Test
    public void testIPv4PlusIntConstOverflow2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select ipv4 '255.255.255.20' + 236"
        ));
    }

    @Test
    public void testIPv4PlusIntHalfConst() throws Exception {
        assertQuery(
                """
                        column
                        0.0.0.32
                        0.0.0.24
                        0.0.0.38
                        0.0.0.40
                        0.0.0.21
                        0.0.0.23
                        0.0.0.35
                        0.0.0.29
                        0.0.0.39
                        0.0.0.39
                        0.0.0.22
                        0.0.0.40
                        0.0.0.28
                        0.0.0.36
                        0.0.0.27
                        0.0.0.38
                        0.0.0.31
                        0.0.0.33
                        0.0.0.24
                        0.0.0.21
                        0.0.0.26
                        0.0.0.27
                        0.0.0.37
                        0.0.0.33
                        0.0.0.25
                        0.0.0.36
                        0.0.0.29
                        0.0.0.36
                        0.0.0.32
                        0.0.0.37
                        0.0.0.30
                        0.0.0.26
                        0.0.0.38
                        0.0.0.35
                        0.0.0.25
                        0.0.0.28
                        0.0.0.21
                        0.0.0.36
                        0.0.0.25
                        0.0.0.38
                        0.0.0.25
                        0.0.0.39
                        0.0.0.36
                        0.0.0.23
                        0.0.0.30
                        0.0.0.26
                        0.0.0.32
                        0.0.0.37
                        0.0.0.26
                        0.0.0.28
                        0.0.0.34
                        0.0.0.35
                        0.0.0.36
                        0.0.0.29
                        0.0.0.38
                        0.0.0.28
                        0.0.0.38
                        0.0.0.27
                        0.0.0.32
                        0.0.0.36
                        0.0.0.21
                        0.0.0.26
                        0.0.0.26
                        0.0.0.37
                        0.0.0.22
                        0.0.0.21
                        0.0.0.29
                        0.0.0.38
                        0.0.0.37
                        0.0.0.38
                        0.0.0.38
                        0.0.0.22
                        0.0.0.21
                        0.0.0.38
                        0.0.0.24
                        0.0.0.29
                        0.0.0.30
                        0.0.0.36
                        0.0.0.38
                        0.0.0.36
                        0.0.0.24
                        0.0.0.27
                        0.0.0.25
                        0.0.0.33
                        0.0.0.39
                        0.0.0.32
                        0.0.0.21
                        0.0.0.40
                        0.0.0.31
                        0.0.0.28
                        0.0.0.33
                        0.0.0.30
                        0.0.0.40
                        0.0.0.26
                        0.0.0.39
                        0.0.0.21
                        0.0.0.24
                        0.0.0.30
                        0.0.0.23
                        0.0.0.26
                        """,
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
                """
                        column
                        0.0.1.120
                        0.0.0.186
                        0.0.3.77
                        0.0.2.202
                        0.0.1.82
                        0.0.3.45
                        0.0.0.102
                        0.0.3.224
                        0.0.3.157
                        0.0.0.29
                        0.0.3.197
                        0.0.1.2
                        0.0.1.81
                        0.0.2.114
                        0.0.0.44
                        0.0.1.201
                        0.0.3.136
                        0.0.1.65
                        0.0.3.139
                        0.0.2.169
                        0.0.2.229
                        0.0.2.132
                        0.0.3.169
                        0.0.0.194
                        0.0.1.103
                        0.0.1.100
                        0.0.0.108
                        0.0.1.236
                        0.0.2.42
                        0.0.3.132
                        0.0.0.10
                        0.0.0.29
                        0.0.1.129
                        0.0.2.160
                        0.0.3.122
                        0.0.2.7
                        0.0.2.172
                        0.0.0.175
                        0.0.0.80
                        0.0.3.123
                        0.0.0.165
                        0.0.0.112
                        0.0.0.73
                        0.0.2.150
                        0.0.1.9
                        0.0.3.3
                        0.0.2.147
                        0.0.0.210
                        0.0.1.96
                        0.0.1.249
                        0.0.3.98
                        0.0.3.6
                        0.0.0.58
                        0.0.2.82
                        0.0.2.177
                        0.0.0.87
                        0.0.3.64
                        0.0.2.206
                        0.0.0.84
                        0.0.1.86
                        0.0.1.6
                        0.0.3.176
                        0.0.1.25
                        0.0.2.127
                        0.0.3.118
                        0.0.3.222
                        0.0.3.24
                        0.0.1.187
                        0.0.1.99
                        0.0.1.63
                        0.0.0.136
                        0.0.1.71
                        0.0.3.123
                        0.0.3.25
                        0.0.1.17
                        0.0.3.191
                        0.0.0.101
                        0.0.3.236
                        0.0.2.10
                        0.0.2.188
                        0.0.2.154
                        0.0.1.171
                        0.0.0.146
                        0.0.0.153
                        0.0.0.155
                        0.0.2.146
                        0.0.1.70
                        0.0.2.226
                        0.0.3.70
                        0.0.0.176
                        0.0.0.188
                        0.0.0.251
                        0.0.1.190
                        0.0.1.42
                        0.0.1.74
                        0.0.0.151
                        0.0.0.241
                        0.0.2.134
                        0.0.2.79
                        0.0.3.98
                        """,
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
                """
                        ip\tbytes\trank
                        0.0.0.1\t30\t1
                        0.0.0.1\t368\t1
                        0.0.0.1\t660\t1
                        0.0.0.1\t814\t1
                        0.0.0.1\t887\t1
                        0.0.0.1\t924\t1
                        0.0.0.2\t93\t7
                        0.0.0.2\t288\t7
                        0.0.0.2\t345\t7
                        0.0.0.2\t480\t7
                        0.0.0.2\t493\t7
                        0.0.0.2\t606\t7
                        0.0.0.2\t906\t7
                        0.0.0.3\t563\t14
                        0.0.0.3\t624\t14
                        0.0.0.3\t840\t14
                        0.0.0.4\t181\t17
                        0.0.0.4\t328\t17
                        0.0.0.4\t511\t17
                        0.0.0.4\t619\t17
                        0.0.0.4\t807\t17
                        0.0.0.4\t883\t17
                        0.0.0.4\t907\t17
                        0.0.0.4\t937\t17
                        0.0.0.5\t37\t25
                        0.0.0.5\t193\t25
                        0.0.0.5\t308\t25
                        0.0.0.5\t397\t25
                        0.0.0.5\t624\t25
                        0.0.0.5\t697\t25
                        0.0.0.5\t877\t25
                        0.0.0.6\t240\t32
                        0.0.0.6\t255\t32
                        0.0.0.6\t735\t32
                        0.0.0.6\t746\t32
                        0.0.0.6\t841\t32
                        0.0.0.7\t75\t37
                        0.0.0.7\t99\t37
                        0.0.0.7\t173\t37
                        0.0.0.7\t727\t37
                        0.0.0.8\t136\t41
                        0.0.0.8\t369\t41
                        0.0.0.8\t522\t41
                        0.0.0.8\t665\t41
                        0.0.0.8\t740\t41
                        0.0.0.8\t986\t41
                        0.0.0.9\t34\t47
                        0.0.0.9\t167\t47
                        0.0.0.9\t269\t47
                        0.0.0.9\t345\t47
                        0.0.0.9\t487\t47
                        0.0.0.9\t539\t47
                        0.0.0.9\t598\t47
                        0.0.0.9\t827\t47
                        0.0.0.9\t935\t47
                        0.0.0.10\t470\t56
                        0.0.0.10\t472\t56
                        0.0.0.10\t644\t56
                        0.0.0.10\t868\t56
                        0.0.0.11\t188\t60
                        0.0.0.11\t511\t60
                        0.0.0.11\t519\t60
                        0.0.0.12\t23\t63
                        0.0.0.12\t326\t63
                        0.0.0.12\t655\t63
                        0.0.0.12\t884\t63
                        0.0.0.13\t0\t67
                        0.0.0.13\t7\t67
                        0.0.0.13\t574\t67
                        0.0.0.14\t334\t70
                        0.0.0.15\t95\t71
                        0.0.0.15\t149\t71
                        0.0.0.15\t172\t71
                        0.0.0.15\t528\t71
                        0.0.0.15\t551\t71
                        0.0.0.15\t904\t71
                        0.0.0.15\t910\t71
                        0.0.0.16\t25\t78
                        0.0.0.16\t428\t78
                        0.0.0.16\t482\t78
                        0.0.0.16\t597\t78
                        0.0.0.16\t711\t78
                        0.0.0.16\t777\t78
                        0.0.0.16\t906\t78
                        0.0.0.17\t417\t85
                        0.0.0.17\t770\t85
                        0.0.0.18\t162\t87
                        0.0.0.18\t367\t87
                        0.0.0.18\t569\t87
                        0.0.0.18\t594\t87
                        0.0.0.18\t660\t87
                        0.0.0.18\t852\t87
                        0.0.0.19\t71\t93
                        0.0.0.19\t326\t93
                        0.0.0.20\t180\t95
                        0.0.0.20\t238\t95
                        0.0.0.20\t424\t95
                        0.0.0.20\t585\t95
                        0.0.0.20\t606\t95
                        0.0.0.20\t810\t95
                        """,
                "select ip, bytes, rank() over (order by ip asc) rank from test order by rank, bytes",
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
    public void testIPv4RowNum() throws Exception {
        assertQuery(
                """
                        ip\tbytes\trow_num
                        0.0.0.12\t23\t1
                        0.0.0.9\t167\t2
                        0.0.0.13\t7\t3
                        0.0.0.8\t522\t4
                        0.0.0.9\t827\t5
                        0.0.0.20\t424\t6
                        0.0.0.9\t539\t7
                        0.0.0.15\t904\t8
                        0.0.0.8\t136\t9
                        0.0.0.20\t810\t10
                        0.0.0.15\t528\t11
                        0.0.0.15\t95\t12
                        0.0.0.4\t619\t13
                        0.0.0.8\t369\t14
                        0.0.0.16\t906\t15
                        0.0.0.19\t71\t16
                        0.0.0.14\t334\t17
                        0.0.0.3\t563\t18
                        0.0.0.2\t906\t19
                        0.0.0.20\t238\t20
                        0.0.0.8\t986\t21
                        0.0.0.9\t598\t22
                        0.0.0.13\t574\t23
                        0.0.0.8\t740\t24
                        0.0.0.5\t37\t25
                        0.0.0.18\t594\t26
                        0.0.0.20\t180\t27
                        0.0.0.20\t606\t28
                        0.0.0.5\t877\t29
                        0.0.0.5\t308\t30
                        0.0.0.4\t807\t31
                        0.0.0.10\t470\t32
                        0.0.0.9\t345\t33
                        0.0.0.16\t711\t34
                        0.0.0.6\t735\t35
                        0.0.0.7\t173\t36
                        0.0.0.18\t162\t37
                        0.0.0.2\t345\t38
                        0.0.0.17\t417\t39
                        0.0.0.4\t181\t40
                        0.0.0.5\t697\t41
                        0.0.0.9\t34\t42
                        0.0.0.9\t935\t43
                        0.0.0.1\t887\t44
                        0.0.0.7\t99\t45
                        0.0.0.16\t428\t46
                        0.0.0.4\t328\t47
                        0.0.0.1\t924\t48
                        0.0.0.2\t480\t49
                        0.0.0.4\t883\t50
                        0.0.0.10\t644\t51
                        0.0.0.17\t770\t52
                        0.0.0.11\t188\t53
                        0.0.0.5\t397\t54
                        0.0.0.18\t367\t55
                        0.0.0.15\t910\t56
                        0.0.0.4\t937\t57
                        0.0.0.2\t288\t58
                        0.0.0.4\t907\t59
                        0.0.0.4\t511\t60
                        0.0.0.1\t368\t61
                        0.0.0.13\t0\t62
                        0.0.0.8\t665\t63
                        0.0.0.19\t326\t64
                        0.0.0.7\t75\t65
                        0.0.0.18\t569\t66
                        0.0.0.6\t746\t67
                        0.0.0.2\t606\t68
                        0.0.0.3\t840\t69
                        0.0.0.2\t93\t70
                        0.0.0.16\t482\t71
                        0.0.0.12\t655\t72
                        0.0.0.1\t660\t73
                        0.0.0.11\t519\t74
                        0.0.0.6\t255\t75
                        0.0.0.6\t841\t76
                        0.0.0.3\t624\t77
                        0.0.0.10\t472\t78
                        0.0.0.15\t551\t79
                        0.0.0.5\t193\t80
                        0.0.0.6\t240\t81
                        0.0.0.2\t493\t82
                        0.0.0.9\t487\t83
                        0.0.0.16\t777\t84
                        0.0.0.18\t852\t85
                        0.0.0.15\t149\t86
                        0.0.0.16\t597\t87
                        0.0.0.9\t269\t88
                        0.0.0.1\t30\t89
                        0.0.0.20\t585\t90
                        0.0.0.18\t660\t91
                        0.0.0.15\t172\t92
                        0.0.0.10\t868\t93
                        0.0.0.12\t884\t94
                        0.0.0.1\t814\t95
                        0.0.0.7\t727\t96
                        0.0.0.5\t624\t97
                        0.0.0.11\t511\t98
                        0.0.0.16\t25\t99
                        0.0.0.12\t326\t100
                        """,
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
        assertMemoryLeak(() -> assertSql(
                """
                        netmask
                        
                        """,
                "select netmask('bdfsir/33')"
        ));
    }

    @Test
    public void testIPv4StrBitwiseOrHalfConst() throws Exception {
        assertQuery(
                """
                        column
                        255.0.0.12
                        255.0.0.9
                        255.0.0.13
                        255.0.0.8
                        255.0.0.9
                        255.0.0.20
                        255.0.0.9
                        255.0.0.15
                        255.0.0.8
                        255.0.0.20
                        255.0.0.15
                        255.0.0.15
                        255.0.0.4
                        255.0.0.8
                        255.0.0.16
                        255.0.0.19
                        255.0.0.14
                        255.0.0.3
                        255.0.0.2
                        255.0.0.20
                        255.0.0.8
                        255.0.0.9
                        255.0.0.13
                        255.0.0.8
                        255.0.0.5
                        255.0.0.18
                        255.0.0.20
                        255.0.0.20
                        255.0.0.5
                        255.0.0.5
                        255.0.0.4
                        255.0.0.10
                        255.0.0.9
                        255.0.0.16
                        255.0.0.6
                        255.0.0.7
                        255.0.0.18
                        255.0.0.2
                        255.0.0.17
                        255.0.0.4
                        255.0.0.5
                        255.0.0.9
                        255.0.0.9
                        255.0.0.1
                        255.0.0.7
                        255.0.0.16
                        255.0.0.4
                        255.0.0.1
                        255.0.0.2
                        255.0.0.4
                        255.0.0.10
                        255.0.0.17
                        255.0.0.11
                        255.0.0.5
                        255.0.0.18
                        255.0.0.15
                        255.0.0.4
                        255.0.0.2
                        255.0.0.4
                        255.0.0.4
                        255.0.0.1
                        255.0.0.13
                        255.0.0.8
                        255.0.0.19
                        255.0.0.7
                        255.0.0.18
                        255.0.0.6
                        255.0.0.2
                        255.0.0.3
                        255.0.0.2
                        255.0.0.16
                        255.0.0.12
                        255.0.0.1
                        255.0.0.11
                        255.0.0.6
                        255.0.0.6
                        255.0.0.3
                        255.0.0.10
                        255.0.0.15
                        255.0.0.5
                        255.0.0.6
                        255.0.0.2
                        255.0.0.9
                        255.0.0.16
                        255.0.0.18
                        255.0.0.15
                        255.0.0.16
                        255.0.0.9
                        255.0.0.1
                        255.0.0.20
                        255.0.0.18
                        255.0.0.15
                        255.0.0.10
                        255.0.0.12
                        255.0.0.1
                        255.0.0.7
                        255.0.0.5
                        255.0.0.11
                        255.0.0.16
                        255.0.0.12
                        """,
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
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        16843008
                        """,
                "select '1.1.1.1' - ipv4 '0.0.0.1'"
        ));
    }

    @Test
    public void testIPv4StrMinusIPv4Str() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        16843008
                        """,
                "select '1.1.1.1' - '0.0.0.1'"
        ));
    }

    @Test
    public void testIPv4StrMinusInt() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        1.1.0.252
                        """,
                "select ipv4 '1.1.1.1' - 5"
        ));
    }

    @Test
    public void testIPv4StrNetmask() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        netmask
                        255.255.255.240
                        """,
                "select netmask('68.11.22.1/28')"
        ));
    }

    @Test
    public void testIPv4StrNetmaskNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        netmask
                        
                        """,
                "select netmask('68.11.22.1/33')"
        ));
    }

    @Test
    public void testIPv4StrPlusInt() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        1.1.1.6
                        """,
                "select ipv4 '1.1.1.1' + 5"
        ));
    }

    @Test
    public void testIPv4Union() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (col1 ipv4)");
            execute("create table y (col2 ipv4)");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into y values('0.0.0.1')");
            execute("insert into y values('0.0.0.2')");
            execute("insert into y values('0.0.0.3')");
            execute("insert into y values('0.0.0.4')");
            execute("insert into y values('0.0.0.5')");

            assertSql(
                    """
                            col1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.3
                            0.0.0.4
                            0.0.0.5
                            """,
                    "select col1 from x union select col2 from y"
            );
        });
    }

    @Test
    public void testIPv4UnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (col1 ipv4)");
            execute("create table y (col2 ipv4)");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into y values('0.0.0.1')");
            execute("insert into y values('0.0.0.2')");
            execute("insert into y values('0.0.0.3')");
            execute("insert into y values('0.0.0.4')");
            execute("insert into y values('0.0.0.5')");

            assertSql(
                    """
                            col1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.3
                            0.0.0.4
                            0.0.0.5
                            0.0.0.1
                            0.0.0.2
                            0.0.0.3
                            0.0.0.4
                            0.0.0.5
                            """,
                    "select col1 from x union all select col2 from y"
            );
        });
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
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    """
                            ip\tbytes\tts
                            187.139.150.80\t580\t1970-01-01T00:00:00.000000Z
                            """,
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

            assertSql(
                    """
                            ip\tbytes\tts
                            187.139.150.80\t580\t1970-01-01T00:00:00.000000Z
                            """,
                    "select * from test where '187.139.150.80' = ip"
            );
        });
    }

    @Test
    public void testImplicitCastStrIPv42() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        false
                        """,
                "select ipv4 '2.2.2.2' <= '1.1.1.1'"
        ));
    }

    @Test
    public void testImplicitCastStrIPv4BadStr() throws Exception {
        assertException("select 'dhukdsvhiu' < ipv4 '1.1.1.1'", 0, "invalid IPv4 format: dhukdsvhiu");
    }

    @Test
    public void testImplicitCastStrToIpv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b string)");
            execute("insert into x values('0.0.0.1')");
            execute("insert into x values('0.0.0.2')");
            execute("insert into x values('0.0.0.3')");
            execute("insert into x values('0.0.0.4')");
            execute("insert into x values('0.0.0.5')");
            execute("insert into x values('0.0.0.6')");
            execute("insert into x values('0.0.0.7')");
            execute("insert into x values('0.0.0.8')");
            execute("insert into x values('0.0.0.9')");
            execute("insert into x values('0.0.0.10')");
            execute("create table y (a ipv4)");

            engine.releaseInactive();

            assertQueryNoLeakCheck(
                    """
                            a
                            0.0.0.1
                            \
                            0.0.0.2
                            \
                            0.0.0.3
                            \
                            0.0.0.4
                            \
                            0.0.0.5
                            \
                            0.0.0.6
                            \
                            0.0.0.7
                            \
                            0.0.0.8
                            \
                            0.0.0.9
                            \
                            0.0.0.10
                            """,
                    "select * from y",
                    "insert into y select * from x",
                    null,
                    true,
                    true
            );
        });
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
    public void testIndexedBindVariableInContainsEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where b <<= $1"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInContainsEqFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertExceptionNoLeakCheck("x where b <<= $1", 14, "invalid argument: foobar");
        });
    }

    @Test
    public void testIndexedBindVariableInContainsEqFilterNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a int)");
            execute("insert into x values('127.0.0.1', 0)");
            execute("insert into x values('192.168.0.1', 1)");
            execute("insert into x values('255.255.255.255', 2)");
            execute("insert into x values(null, 3)");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, null);
            assertSql(
                    "b\ta\n",
                    "x where b <<= $1"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInContainsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where b << $1"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInContainsFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertExceptionNoLeakCheck("x where b << $1", 13, "invalid argument: foobar");
        });
    }

    @Test
    public void testIndexedBindVariableInContainsFilterNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a int)");
            execute("insert into x values('127.0.0.1', 0)");
            execute("insert into x values('192.168.0.1', 1)");
            execute("insert into x values('255.255.255.255', 2)");
            execute("insert into x values(null, 3)");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, null);
            assertSql(
                    "b\ta\n",
                    "x where b << $1"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "192.168.0.1");
            assertSql(
                    """
                            b
                            192.168.0.1
                            """,
                    "x where b = $1"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInLtFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "192.168.0.1");
            assertSql(
                    """
                            b
                            127.0.0.1
                            """,
                    "x where b < $1"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInLtFilter2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "192.168.0.1");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where $1 < b"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where $1 >>= b"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsEqFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertExceptionNoLeakCheck("x where $1 >>= b", 8, "invalid argument: foobar");
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsEqFilterNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a int)");
            execute("insert into x values('127.0.0.1', 0)");
            execute("insert into x values('192.168.0.1', 1)");
            execute("insert into x values('255.255.255.255', 2)");
            execute("insert into x values(null, 3)");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, null);
            assertSql(
                    "b\ta\n",
                    "x where $1 >>= b"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where $1 >> b"
            );
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertExceptionNoLeakCheck("x where $1 >> b", 8, "invalid argument: foobar");
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsFilterNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a int)");
            execute("insert into x values('127.0.0.1', 0)");
            execute("insert into x values('192.168.0.1', 1)");
            execute("insert into x values('255.255.255.255', 2)");
            execute("insert into x values(null, 3)");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, null);
            assertSql(
                    "b\ta\n",
                    "x where $1 >> b"
            );
        });
    }

    @Test
    public void testInnerJoinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('1.1.1.1/32', 0) ip, 1 count from long_sequence(5))");
            execute("create table test2 as (select rnd_ipv4('1.1.1.1/32', 0) ip2, 2 count2 from long_sequence(5))");
            assertSql(
                    """
                            count\tcount2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            1\t2
                            """,
                    "select test.count, test2.count2 from test inner join test2 on test2.ip2 = test.ip"
            );
        });
    }

    @Test
    public void testIntPlusIPv4Const() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        1.1.1.21
                        """,
                "select 20 + ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testIntPlusIPv4Const2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        255.255.255.255
                        """,
                "select 235 + ipv4 '255.255.255.20'"
        ));
    }

    @Test
    public void testIntPlusIPv4ConstNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select 20 + ipv4 '0.0.0.0'"
        ));
    }

    @Test
    public void testIntPlusIPv4ConstOverflow() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select 1 + ipv4 '255.255.255.255'"
        ));
    }

    @Test
    public void testIntPlusIPv4ConstOverflow2() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        
                        """,
                "select 236 + ipv4 '255.255.255.20'"
        ));
    }

    @Test
    public void testIntPlusIPv4HalfConst() throws Exception {
        assertQuery(
                """
                        column
                        0.0.0.32
                        0.0.0.24
                        0.0.0.38
                        0.0.0.40
                        0.0.0.21
                        0.0.0.23
                        0.0.0.35
                        0.0.0.29
                        0.0.0.39
                        0.0.0.39
                        0.0.0.22
                        0.0.0.40
                        0.0.0.28
                        0.0.0.36
                        0.0.0.27
                        0.0.0.38
                        0.0.0.31
                        0.0.0.33
                        0.0.0.24
                        0.0.0.21
                        0.0.0.26
                        0.0.0.27
                        0.0.0.37
                        0.0.0.33
                        0.0.0.25
                        0.0.0.36
                        0.0.0.29
                        0.0.0.36
                        0.0.0.32
                        0.0.0.37
                        0.0.0.30
                        0.0.0.26
                        0.0.0.38
                        0.0.0.35
                        0.0.0.25
                        0.0.0.28
                        0.0.0.21
                        0.0.0.36
                        0.0.0.25
                        0.0.0.38
                        0.0.0.25
                        0.0.0.39
                        0.0.0.36
                        0.0.0.23
                        0.0.0.30
                        0.0.0.26
                        0.0.0.32
                        0.0.0.37
                        0.0.0.26
                        0.0.0.28
                        0.0.0.34
                        0.0.0.35
                        0.0.0.36
                        0.0.0.29
                        0.0.0.38
                        0.0.0.28
                        0.0.0.38
                        0.0.0.27
                        0.0.0.32
                        0.0.0.36
                        0.0.0.21
                        0.0.0.26
                        0.0.0.26
                        0.0.0.37
                        0.0.0.22
                        0.0.0.21
                        0.0.0.29
                        0.0.0.38
                        0.0.0.37
                        0.0.0.38
                        0.0.0.38
                        0.0.0.22
                        0.0.0.21
                        0.0.0.38
                        0.0.0.24
                        0.0.0.29
                        0.0.0.30
                        0.0.0.36
                        0.0.0.38
                        0.0.0.36
                        0.0.0.24
                        0.0.0.27
                        0.0.0.25
                        0.0.0.33
                        0.0.0.39
                        0.0.0.32
                        0.0.0.21
                        0.0.0.40
                        0.0.0.31
                        0.0.0.28
                        0.0.0.33
                        0.0.0.30
                        0.0.0.40
                        0.0.0.26
                        0.0.0.39
                        0.0.0.21
                        0.0.0.24
                        0.0.0.30
                        0.0.0.23
                        0.0.0.26
                        """,
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
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        1.1.1.6
                        """,
                "select 5 + ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testIntPlusIPv4Var() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                """
                        column
                        0.0.1.120
                        0.0.0.186
                        0.0.3.77
                        0.0.2.202
                        0.0.1.82
                        0.0.3.45
                        0.0.0.102
                        0.0.3.224
                        0.0.3.157
                        0.0.0.29
                        0.0.3.197
                        0.0.1.2
                        0.0.1.81
                        0.0.2.114
                        0.0.0.44
                        0.0.1.201
                        0.0.3.136
                        0.0.1.65
                        0.0.3.139
                        0.0.2.169
                        0.0.2.229
                        0.0.2.132
                        0.0.3.169
                        0.0.0.194
                        0.0.1.103
                        0.0.1.100
                        0.0.0.108
                        0.0.1.236
                        0.0.2.42
                        0.0.3.132
                        0.0.0.10
                        0.0.0.29
                        0.0.1.129
                        0.0.2.160
                        0.0.3.122
                        0.0.2.7
                        0.0.2.172
                        0.0.0.175
                        0.0.0.80
                        0.0.3.123
                        0.0.0.165
                        0.0.0.112
                        0.0.0.73
                        0.0.2.150
                        0.0.1.9
                        0.0.3.3
                        0.0.2.147
                        0.0.0.210
                        0.0.1.96
                        0.0.1.249
                        0.0.3.98
                        0.0.3.6
                        0.0.0.58
                        0.0.2.82
                        0.0.2.177
                        0.0.0.87
                        0.0.3.64
                        0.0.2.206
                        0.0.0.84
                        0.0.1.86
                        0.0.1.6
                        0.0.3.176
                        0.0.1.25
                        0.0.2.127
                        0.0.3.118
                        0.0.3.222
                        0.0.3.24
                        0.0.1.187
                        0.0.1.99
                        0.0.1.63
                        0.0.0.136
                        0.0.1.71
                        0.0.3.123
                        0.0.3.25
                        0.0.1.17
                        0.0.3.191
                        0.0.0.101
                        0.0.3.236
                        0.0.2.10
                        0.0.2.188
                        0.0.2.154
                        0.0.1.171
                        0.0.0.146
                        0.0.0.153
                        0.0.0.155
                        0.0.2.146
                        0.0.1.70
                        0.0.2.226
                        0.0.3.70
                        0.0.0.176
                        0.0.0.188
                        0.0.0.251
                        0.0.1.190
                        0.0.1.42
                        0.0.1.74
                        0.0.0.151
                        0.0.0.241
                        0.0.2.134
                        0.0.2.79
                        0.0.3.98
                        """,
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
        ));
    }

    @Test
    public void testInvalidConstantInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            assertExceptionNoLeakCheck("x where b = 'foobar'", 0, "invalid IPv4 format: foobar");
        });
    }

    @Test
    public void testLastIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, rnd_symbol('ab', '$a', 'ac') sym from long_sequence(20))");
            assertSql(
                    """
                            sym\tlast
                            $a\t10.5.237.229
                            ab\t
                            ac\t10.5.235.200
                            """,
                    "select sym, last(ip) from test order by sym"
            );
        });
    }

    @Test
    public void testLatestByIPv4() throws Exception {
        assertQuery(
                """
                        ip\tbytes\ttime
                        0.0.0.4\t269\t1970-01-01T00:00:08.700000Z
                        0.0.0.3\t660\t1970-01-01T00:00:09.000000Z
                        0.0.0.5\t624\t1970-01-01T00:00:09.600000Z
                        0.0.0.1\t25\t1970-01-01T00:00:09.800000Z
                        0.0.0.2\t326\t1970-01-01T00:00:09.900000Z
                        """,
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
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('1.1.1.1/16', 0) ip, 1 count from long_sequence(5))");
            execute("create table test2 as (select rnd_ipv4('1.1.1.1/32', 0) ip2, 2 count2 from long_sequence(5))");
            assertSql(
                    """
                            ip\tip2\tcount\tcount2
                            1.1.96.238\t\t1\tnull
                            1.1.50.227\t\t1\tnull
                            1.1.89.171\t\t1\tnull
                            1.1.82.23\t\t1\tnull
                            1.1.76.40\t\t1\tnull
                            """,
                    "select test.ip, test2.ip2, test.count, test2.count2 from test left join test2 on test2.ip2 = test.ip"
            );
        });
    }

    @Test
    public void testLeftJoinIPv42() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(50))");
            execute("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(50))");
            assertSql(
                    """
                            ip\tip2\tcount\tcount2
                            12.5.9.238\t\t1\tnull
                            12.5.9.227\t\t1\tnull
                            12.5.9.171\t\t1\tnull
                            12.5.9.23\t\t1\tnull
                            12.5.9.40\t\t1\tnull
                            12.5.9.236\t\t1\tnull
                            12.5.9.15\t\t1\tnull
                            12.5.9.136\t\t1\tnull
                            12.5.9.145\t\t1\tnull
                            12.5.9.114\t12.5.9.114\t1\t2
                            12.5.9.243\t\t1\tnull
                            12.5.9.229\t\t1\tnull
                            12.5.9.120\t\t1\tnull
                            12.5.9.160\t\t1\tnull
                            12.5.9.196\t\t1\tnull
                            12.5.9.235\t\t1\tnull
                            12.5.9.159\t12.5.9.159\t1\t2
                            12.5.9.81\t\t1\tnull
                            12.5.9.196\t\t1\tnull
                            12.5.9.108\t\t1\tnull
                            12.5.9.173\t\t1\tnull
                            12.5.9.76\t\t1\tnull
                            12.5.9.126\t\t1\tnull
                            12.5.9.248\t\t1\tnull
                            12.5.9.226\t12.5.9.226\t1\t2
                            12.5.9.115\t\t1\tnull
                            12.5.9.98\t\t1\tnull
                            12.5.9.200\t\t1\tnull
                            12.5.9.247\t\t1\tnull
                            12.5.9.216\t\t1\tnull
                            12.5.9.48\t\t1\tnull
                            12.5.9.202\t\t1\tnull
                            12.5.9.50\t\t1\tnull
                            12.5.9.228\t\t1\tnull
                            12.5.9.210\t\t1\tnull
                            12.5.9.7\t\t1\tnull
                            12.5.9.4\t\t1\tnull
                            12.5.9.135\t\t1\tnull
                            12.5.9.117\t\t1\tnull
                            12.5.9.43\t12.5.9.43\t1\t2
                            12.5.9.43\t12.5.9.43\t1\t2
                            12.5.9.179\t12.5.9.179\t1\t2
                            12.5.9.179\t12.5.9.179\t1\t2
                            12.5.9.142\t\t1\tnull
                            12.5.9.75\t\t1\tnull
                            12.5.9.239\t\t1\tnull
                            12.5.9.48\t\t1\tnull
                            12.5.9.100\t12.5.9.100\t1\t2
                            12.5.9.100\t12.5.9.100\t1\t2
                            12.5.9.100\t12.5.9.100\t1\t2
                            12.5.9.26\t\t1\tnull
                            12.5.9.240\t\t1\tnull
                            12.5.9.192\t\t1\tnull
                            12.5.9.181\t\t1\tnull
                            """,
                    "select test.ip, test2.ip2, test.count, test2.count2 from test left join test2 on test2.ip2 = test.ip"
            );
        });
    }

    @Test
    public void testLessThanEqIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        true
                        """,
                "select ipv4 '34.11.45.3' <= ipv4 '34.11.45.3'"
        ));
    }

    @Test
    public void testLessThanIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column
                        false
                        """,
                "select ipv4 '34.11.45.3' < ipv4 '22.1.200.89'"
        ));
    }

    @Test
    public void testLimitIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            ip\tcount
                            10.5.96.238\t1
                            10.5.50.227\t1
                            10.5.89.171\t1
                            10.5.82.23\t1
                            10.5.76.40\t1
                            """,
                    "select * from test limit 5"
            );
        });
    }

    @Test
    public void testLimitIPv42() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            ip\tcount
                            10.5.170.235\t1
                            10.5.45.159\t1
                            10.5.184.81\t1
                            10.5.207.196\t1
                            10.5.213.108\t1
                            """,
                    "select * from test limit -5"
            );
        });
    }

    @Test
    public void testLimitIPv43() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            ip\tcount
                            10.5.89.171\t1
                            10.5.82.23\t1
                            10.5.76.40\t1
                            10.5.20.236\t1
                            10.5.95.15\t1
                            10.5.178.136\t1
                            10.5.45.145\t1
                            10.5.93.114\t1
                            """,
                    "select * from test limit 2, 10"
            );
        });
    }

    @Test
    public void testMaxIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            execute("insert into test values ('255.255.255.255', 1)");
            assertSql(
                    """
                            max
                            255.255.255.255
                            """,
                    "select max(ip) from test"
            );
        });
    }

    @Test
    public void testMinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            execute("insert into test values ('0.0.0.1', 1)");
            assertSql(
                    """
                            min
                            0.0.0.1
                            """,
                    "select min(ip) from test"
            );
        });
    }

    @Test
    public void testMinIPv4Null() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            execute("insert into test values ('0.0.0.0', 1)");
            assertSql(
                    """
                            min
                            4.98.173.21
                            """,
                    "select min(ip) from test"
            );
        });
    }

    @Test
    public void testNamedBindVariableInContainsEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where b <<= :ip"
            );
        });
    }

    @Test
    public void testNamedBindVariableInContainsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where b << :ip"
            );
        });
    }

    @Test
    public void testNamedBindVariableInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "192.168.0.1");
            assertSql(
                    """
                            b
                            192.168.0.1
                            """,
                    "x where b = :ip"
            );
        });
    }

    @Test
    public void testNamedBindVariableInLtFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "192.168.0.1");
            assertSql(
                    """
                            b
                            127.0.0.1
                            """,
                    "x where b < :ip"
            );
        });
    }

    @Test
    public void testNamedBindVariableInLtFilter2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "192.168.0.1");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where :ip < b"
            );
        });
    }

    @Test
    public void testNamedBindVariableInNegContainsEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where :ip >>= b"
            );
        });
    }

    @Test
    public void testNamedBindVariableInNegContainsFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("ip", "255.255.255.255/31");
            assertSql(
                    """
                            b
                            255.255.255.255
                            """,
                    "x where :ip >> b"
            );
        });
    }

    @Test
    public void testNegContainsIPv4FunctionFactoryError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ip ipv4)");
            assertExceptionNoLeakCheck("select * from t where '1.1.1.1/35' >> ip", 22, "invalid argument: 1.1.1.1/35");
            assertExceptionNoLeakCheck("select * from t where '1.1.1.1/-1' >> ip", 22, "invalid argument: 1.1.1.1/-1");
            assertExceptionNoLeakCheck("select * from t where '1.1.1.1/A' >> ip ", 22, "invalid argument: 1.1.1.1/A");
            assertExceptionNoLeakCheck("select * from t where '1.1/26' >> ip ", 22, "invalid argument: 1.1/26");
        });
    }

    @Test
    public void testNetmask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tipv4 (ip ipv4)");
            execute("insert into tipv4 values ('255.255.255.254')");
            assertSql("""
                    ip
                    255.255.255.254
                    """, "select * from tipv4 where '255.255.255.255/31' >>= ip");
            assertSql("ip\n", "select * from tipv4 where '255.255.255.255/32' >>= ip");
            assertSql("""
                    ip
                    255.255.255.254
                    """, "select * from tipv4 where '255.255.255.255/30' >>= ip");
            assertSql("""
                    ip
                    255.255.255.254
                    """, "select * from tipv4 where '255.255.255.0/24' >>= ip");

            assertSql("""
                    column
                    true
                    """, "select '255.255.255.255'::ipv4 <<= '255.255.255.255'");
            assertSql("""
                    column
                    false
                    """, "select '255.255.255.255'::ipv4 <<= '255.255.255.254'");
            assertSql("""
                    column
                    true
                    """, "select '255.255.255.255'::ipv4 <<= '255.255.255.254/31'");

            assertSql("netmask\n\n", "select netmask('1.1.1.1/0');");
            assertSql("netmask\n255.255.255.252\n", "select netmask('1.1.1.1/30');");
            assertSql("netmask\n255.255.255.254\n", "select netmask('1.1.1.1/31');");
            assertSql("netmask\n255.255.255.255\n", "select netmask('1.1.1.1/32');");
            assertSql("netmask\n\n", "select netmask('1.1.1.1/33');");
        });
    }

    @Test
    public void testNetmaskColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (msk string)");
            execute("insert into test values ('255.255.255.255/30')");
            execute("insert into test values ('255.255.255.255/31')");
            execute("insert into test values ('255.255.255.255/32')");

            assertSql(
                    """
                            netmask
                            255.255.255.252
                            255.255.255.254
                            255.255.255.255
                            """,
                    "select netmask(msk) from test;"
            );
        });
    }

    @Test
    public void testNetmaskVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tipv4 (ip ipv4)");
            execute("insert into tipv4 values ('255.255.255.254')");
            assertSql("""
                    ip
                    255.255.255.254
                    """, "select * from tipv4 where '255.255.255.255/31'::varchar >>= ip");
            assertSql("ip\n", "select * from tipv4 where '255.255.255.255/32'::varchar >>= ip");
            assertSql("""
                    ip
                    255.255.255.254
                    """, "select * from tipv4 where '255.255.255.255/30'::varchar >>= ip");
            assertSql("""
                    ip
                    255.255.255.254
                    """, "select * from tipv4 where '255.255.255.0/24'::varchar >>= ip");

            assertSql("""
                    column
                    true
                    """, "select '255.255.255.255'::ipv4 <<= '255.255.255.255'::varchar");
            assertSql("""
                    column
                    false
                    """, "select '255.255.255.255'::ipv4 <<= '255.255.255.254'::varchar");
            assertSql("""
                    column
                    true
                    """, "select '255.255.255.255'::ipv4 <<= '255.255.255.254/31'::varchar");

            assertSql("netmask\n\n", "select netmask('1.1.1.1/0'::varchar);");
            assertSql("netmask\n255.255.255.252\n", "select netmask('1.1.1.1/30'::varchar);");
            assertSql("netmask\n255.255.255.254\n", "select netmask('1.1.1.1/31'::varchar);");
            assertSql("netmask\n255.255.255.255\n", "select netmask('1.1.1.1/32'::varchar);");
            assertSql("netmask\n\n", "select netmask('1.1.1.1/33'::varchar);");
        });
    }

    @Test
    public void testNetmaskVarcharColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (msk varchar)");
            execute("insert into test values ('255.255.255.255/30')");
            execute("insert into test values ('255.255.255.255/31')");
            execute("insert into test values ('255.255.255.255/32')");

            assertSql(
                    """
                            netmask
                            255.255.255.252
                            255.255.255.254
                            255.255.255.255
                            """,
                    "select netmask(msk) from test;"
            );
        });
    }

    @Test
    public void testNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('128.0.0.0')");
            execute("insert into x values('0.0.0.0')");

            assertSql(
                    """
                            b
                            128.0.0.0
                            
                            """,
                    "x"
            );
        });
    }

    @Test
    public void testNullNetmask() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        netmask
                        
                        """,
                "select netmask(null)"
        ));
    }

    @Test
    public void testOrderByIPv4Ascending() throws Exception {
        assertQuery(
                """
                        ip\tbytes\tts
                        12.214.12.100\t598\t1970-01-01T01:11:40.000000Z
                        24.123.12.210\t95\t1970-01-01T00:38:20.000000Z
                        25.107.51.160\t827\t1970-01-01T00:15:00.000000Z
                        50.214.139.184\t574\t1970-01-01T01:15:00.000000Z
                        55.211.206.129\t785\t1970-01-01T00:43:20.000000Z
                        63.60.82.184\t37\t1970-01-01T01:21:40.000000Z
                        66.56.51.126\t904\t1970-01-01T00:25:00.000000Z
                        67.22.249.199\t203\t1970-01-01T00:13:20.000000Z
                        71.73.196.29\t741\t1970-01-01T01:00:00.000000Z
                        73.153.126.70\t772\t1970-01-01T01:06:40.000000Z
                        74.196.176.71\t740\t1970-01-01T01:18:20.000000Z
                        79.15.250.138\t850\t1970-01-01T00:03:20.000000Z
                        92.26.178.136\t7\t1970-01-01T00:08:20.000000Z
                        97.159.145.120\t352\t1970-01-01T00:46:40.000000Z
                        105.218.160.179\t986\t1970-01-01T01:08:20.000000Z
                        111.221.228.130\t531\t1970-01-01T01:03:20.000000Z
                        113.132.124.243\t522\t1970-01-01T00:11:40.000000Z
                        114.126.117.26\t71\t1970-01-01T00:51:40.000000Z
                        128.225.84.244\t313\t1970-01-01T00:30:00.000000Z
                        129.172.181.73\t25\t1970-01-01T00:23:20.000000Z
                        136.166.51.222\t580\t1970-01-01T00:36:40.000000Z
                        144.131.72.77\t369\t1970-01-01T00:45:00.000000Z
                        146.16.210.119\t383\t1970-01-01T00:16:40.000000Z
                        150.153.88.133\t849\t1970-01-01T01:20:00.000000Z
                        164.74.203.45\t678\t1970-01-01T00:53:20.000000Z
                        164.153.242.17\t906\t1970-01-01T00:48:20.000000Z
                        165.166.233.251\t332\t1970-01-01T00:50:00.000000Z
                        170.90.236.206\t572\t1970-01-01T00:06:40.000000Z
                        171.30.189.77\t238\t1970-01-01T01:05:00.000000Z
                        171.117.213.66\t720\t1970-01-01T00:40:00.000000Z
                        180.36.62.54\t528\t1970-01-01T00:35:00.000000Z
                        180.48.50.141\t136\t1970-01-01T00:28:20.000000Z
                        180.91.244.55\t906\t1970-01-01T01:01:40.000000Z
                        181.82.42.148\t539\t1970-01-01T00:21:40.000000Z
                        186.33.243.40\t659\t1970-01-01T01:16:40.000000Z
                        187.63.210.97\t424\t1970-01-01T00:18:20.000000Z
                        187.139.150.80\t580\t1970-01-01T00:00:00.000000Z
                        188.239.72.25\t513\t1970-01-01T00:20:00.000000Z
                        201.100.238.229\t318\t1970-01-01T01:10:00.000000Z
                        205.123.179.216\t167\t1970-01-01T00:05:00.000000Z
                        212.102.182.127\t984\t1970-01-01T01:13:20.000000Z
                        212.159.205.29\t23\t1970-01-01T00:01:40.000000Z
                        216.150.248.30\t563\t1970-01-01T00:58:20.000000Z
                        224.99.254.121\t619\t1970-01-01T00:41:40.000000Z
                        227.40.250.157\t903\t1970-01-01T00:33:20.000000Z
                        230.202.108.161\t171\t1970-01-01T00:26:40.000000Z
                        231.146.30.59\t766\t1970-01-01T00:10:00.000000Z
                        241.248.184.75\t334\t1970-01-01T00:55:00.000000Z
                        254.93.251.9\t810\t1970-01-01T00:31:40.000000Z
                        255.95.177.227\t44\t1970-01-01T00:56:40.000000Z
                        """,
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
                """
                        ip\tbytes\tts
                        255.95.177.227\t44\t1970-01-01T00:56:40.000000Z
                        254.93.251.9\t810\t1970-01-01T00:31:40.000000Z
                        241.248.184.75\t334\t1970-01-01T00:55:00.000000Z
                        231.146.30.59\t766\t1970-01-01T00:10:00.000000Z
                        230.202.108.161\t171\t1970-01-01T00:26:40.000000Z
                        227.40.250.157\t903\t1970-01-01T00:33:20.000000Z
                        224.99.254.121\t619\t1970-01-01T00:41:40.000000Z
                        216.150.248.30\t563\t1970-01-01T00:58:20.000000Z
                        212.159.205.29\t23\t1970-01-01T00:01:40.000000Z
                        212.102.182.127\t984\t1970-01-01T01:13:20.000000Z
                        205.123.179.216\t167\t1970-01-01T00:05:00.000000Z
                        201.100.238.229\t318\t1970-01-01T01:10:00.000000Z
                        188.239.72.25\t513\t1970-01-01T00:20:00.000000Z
                        187.139.150.80\t580\t1970-01-01T00:00:00.000000Z
                        187.63.210.97\t424\t1970-01-01T00:18:20.000000Z
                        186.33.243.40\t659\t1970-01-01T01:16:40.000000Z
                        181.82.42.148\t539\t1970-01-01T00:21:40.000000Z
                        180.91.244.55\t906\t1970-01-01T01:01:40.000000Z
                        180.48.50.141\t136\t1970-01-01T00:28:20.000000Z
                        180.36.62.54\t528\t1970-01-01T00:35:00.000000Z
                        171.117.213.66\t720\t1970-01-01T00:40:00.000000Z
                        171.30.189.77\t238\t1970-01-01T01:05:00.000000Z
                        170.90.236.206\t572\t1970-01-01T00:06:40.000000Z
                        165.166.233.251\t332\t1970-01-01T00:50:00.000000Z
                        164.153.242.17\t906\t1970-01-01T00:48:20.000000Z
                        164.74.203.45\t678\t1970-01-01T00:53:20.000000Z
                        150.153.88.133\t849\t1970-01-01T01:20:00.000000Z
                        146.16.210.119\t383\t1970-01-01T00:16:40.000000Z
                        144.131.72.77\t369\t1970-01-01T00:45:00.000000Z
                        136.166.51.222\t580\t1970-01-01T00:36:40.000000Z
                        129.172.181.73\t25\t1970-01-01T00:23:20.000000Z
                        128.225.84.244\t313\t1970-01-01T00:30:00.000000Z
                        114.126.117.26\t71\t1970-01-01T00:51:40.000000Z
                        113.132.124.243\t522\t1970-01-01T00:11:40.000000Z
                        111.221.228.130\t531\t1970-01-01T01:03:20.000000Z
                        105.218.160.179\t986\t1970-01-01T01:08:20.000000Z
                        97.159.145.120\t352\t1970-01-01T00:46:40.000000Z
                        92.26.178.136\t7\t1970-01-01T00:08:20.000000Z
                        79.15.250.138\t850\t1970-01-01T00:03:20.000000Z
                        74.196.176.71\t740\t1970-01-01T01:18:20.000000Z
                        73.153.126.70\t772\t1970-01-01T01:06:40.000000Z
                        71.73.196.29\t741\t1970-01-01T01:00:00.000000Z
                        67.22.249.199\t203\t1970-01-01T00:13:20.000000Z
                        66.56.51.126\t904\t1970-01-01T00:25:00.000000Z
                        63.60.82.184\t37\t1970-01-01T01:21:40.000000Z
                        55.211.206.129\t785\t1970-01-01T00:43:20.000000Z
                        50.214.139.184\t574\t1970-01-01T01:15:00.000000Z
                        25.107.51.160\t827\t1970-01-01T00:15:00.000000Z
                        24.123.12.210\t95\t1970-01-01T00:38:20.000000Z
                        12.214.12.100\t598\t1970-01-01T01:11:40.000000Z
                        """,
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
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck("select rnd_ipv4('1.1.1.1/35', 1)", 16, "invalid argument: 1.1.1.1/35");
            assertExceptionNoLeakCheck("select rnd_ipv4('1.1.1.1/-1', 1)", 16, "invalid argument: 1.1.1.1/-1");
            assertExceptionNoLeakCheck("select rnd_ipv4('1.1.1.1/A', 1)", 16, "invalid argument: 1.1.1.1/A");
            assertExceptionNoLeakCheck("select rnd_ipv4('1.1/26', 1)", 16, "invalid argument: 1.1/26");
        });
    }

    @Test
    public void testRndIPv4() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        rnd_ipv4
                        12.6.96.238
                        12.6.50.227
                        12.6.89.171
                        12.6.82.23
                        12.6.76.40
                        12.6.20.236
                        12.6.95.15
                        12.6.178.136
                        12.6.45.145
                        12.6.93.114
                        """,
                "select rnd_ipv4('12.6/16', 0) from long_sequence(10)"
        ));
    }

    @Test
    public void testRndIPv42() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        rnd_ipv4
                        12.6.96.238
                        12.6.50.227
                        12.6.89.171
                        12.6.82.23
                        12.6.76.40
                        12.6.20.236
                        12.6.95.15
                        12.6.178.136
                        12.6.45.145
                        12.6.93.114
                        """,
                "select rnd_ipv4('12.6.8/16', 0) from long_sequence(10)"
        ));
    }

    @Test
    public void testSampleByIPv4() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    """
                            ip\tts\tsum
                            12.214.12.100\t1970-01-01T00:00:00.000001Z\t598
                            24.123.12.210\t1970-01-01T00:00:00.000001Z\t95
                            25.107.51.160\t1970-01-01T00:00:00.000001Z\t827
                            50.214.139.184\t1970-01-01T00:00:00.000001Z\t574
                            55.211.206.129\t1970-01-01T00:00:00.000001Z\t785
                            63.60.82.184\t1970-01-01T00:00:00.000001Z\t37
                            66.56.51.126\t1970-01-01T00:00:00.000001Z\t904
                            67.22.249.199\t1970-01-01T00:00:00.000001Z\t203
                            71.73.196.29\t1970-01-01T00:00:00.000001Z\t741
                            73.153.126.70\t1970-01-01T00:00:00.000001Z\t772
                            74.196.176.71\t1970-01-01T00:00:00.000001Z\t740
                            79.15.250.138\t1970-01-01T00:00:00.000001Z\t850
                            92.26.178.136\t1970-01-01T00:00:00.000001Z\t7
                            97.159.145.120\t1970-01-01T00:00:00.000001Z\t352
                            105.218.160.179\t1970-01-01T00:00:00.000001Z\t986
                            111.221.228.130\t1970-01-01T00:00:00.000001Z\t531
                            113.132.124.243\t1970-01-01T00:00:00.000001Z\t522
                            114.126.117.26\t1970-01-01T00:00:00.000001Z\t71
                            128.225.84.244\t1970-01-01T00:00:00.000001Z\t313
                            129.172.181.73\t1970-01-01T00:00:00.000001Z\t25
                            136.166.51.222\t1970-01-01T00:00:00.000001Z\t580
                            144.131.72.77\t1970-01-01T00:00:00.000001Z\t369
                            146.16.210.119\t1970-01-01T00:00:00.000001Z\t383
                            150.153.88.133\t1970-01-01T00:00:00.000001Z\t849
                            164.74.203.45\t1970-01-01T00:00:00.000001Z\t678
                            164.153.242.17\t1970-01-01T00:00:00.000001Z\t906
                            165.166.233.251\t1970-01-01T00:00:00.000001Z\t332
                            170.90.236.206\t1970-01-01T00:00:00.000001Z\t572
                            171.30.189.77\t1970-01-01T00:00:00.000001Z\t238
                            171.117.213.66\t1970-01-01T00:00:00.000001Z\t720
                            180.36.62.54\t1970-01-01T00:00:00.000001Z\t528
                            180.48.50.141\t1970-01-01T00:00:00.000001Z\t136
                            180.91.244.55\t1970-01-01T00:00:00.000001Z\t906
                            181.82.42.148\t1970-01-01T00:00:00.000001Z\t539
                            186.33.243.40\t1970-01-01T00:00:00.000001Z\t659
                            187.63.210.97\t1970-01-01T00:00:00.000001Z\t424
                            187.139.150.80\t1970-01-01T00:00:00.000001Z\t580
                            188.239.72.25\t1970-01-01T00:00:00.000001Z\t513
                            201.100.238.229\t1970-01-01T00:00:00.000001Z\t318
                            205.123.179.216\t1970-01-01T00:00:00.000001Z\t167
                            212.102.182.127\t1970-01-01T00:00:00.000001Z\t984
                            212.159.205.29\t1970-01-01T00:00:00.000001Z\t23
                            216.150.248.30\t1970-01-01T00:00:00.000001Z\t563
                            224.99.254.121\t1970-01-01T00:00:00.000001Z\t619
                            227.40.250.157\t1970-01-01T00:00:00.000001Z\t903
                            230.202.108.161\t1970-01-01T00:00:00.000001Z\t171
                            231.146.30.59\t1970-01-01T00:00:00.000001Z\t766
                            241.248.184.75\t1970-01-01T00:00:00.000001Z\t334
                            254.93.251.9\t1970-01-01T00:00:00.000001Z\t810
                            255.95.177.227\t1970-01-01T00:00:00.000001Z\t44
                            """,
                    "select ip, ts, sum(bytes) from test sample by 1y align to first observation order by 2,1",
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

            assertQueryNoLeakCheck(
                    """
                            ip\tts\tsum
                            12.214.12.100\t1970-01-01T00:00:00.000000Z\t598
                            24.123.12.210\t1970-01-01T00:00:00.000000Z\t95
                            25.107.51.160\t1970-01-01T00:00:00.000000Z\t827
                            50.214.139.184\t1970-01-01T00:00:00.000000Z\t574
                            55.211.206.129\t1970-01-01T00:00:00.000000Z\t785
                            63.60.82.184\t1970-01-01T00:00:00.000000Z\t37
                            66.56.51.126\t1970-01-01T00:00:00.000000Z\t904
                            67.22.249.199\t1970-01-01T00:00:00.000000Z\t203
                            71.73.196.29\t1970-01-01T00:00:00.000000Z\t741
                            73.153.126.70\t1970-01-01T00:00:00.000000Z\t772
                            74.196.176.71\t1970-01-01T00:00:00.000000Z\t740
                            79.15.250.138\t1970-01-01T00:00:00.000000Z\t850
                            92.26.178.136\t1970-01-01T00:00:00.000000Z\t7
                            97.159.145.120\t1970-01-01T00:00:00.000000Z\t352
                            105.218.160.179\t1970-01-01T00:00:00.000000Z\t986
                            111.221.228.130\t1970-01-01T00:00:00.000000Z\t531
                            113.132.124.243\t1970-01-01T00:00:00.000000Z\t522
                            114.126.117.26\t1970-01-01T00:00:00.000000Z\t71
                            128.225.84.244\t1970-01-01T00:00:00.000000Z\t313
                            129.172.181.73\t1970-01-01T00:00:00.000000Z\t25
                            136.166.51.222\t1970-01-01T00:00:00.000000Z\t580
                            144.131.72.77\t1970-01-01T00:00:00.000000Z\t369
                            146.16.210.119\t1970-01-01T00:00:00.000000Z\t383
                            150.153.88.133\t1970-01-01T00:00:00.000000Z\t849
                            164.74.203.45\t1970-01-01T00:00:00.000000Z\t678
                            164.153.242.17\t1970-01-01T00:00:00.000000Z\t906
                            165.166.233.251\t1970-01-01T00:00:00.000000Z\t332
                            170.90.236.206\t1970-01-01T00:00:00.000000Z\t572
                            171.30.189.77\t1970-01-01T00:00:00.000000Z\t238
                            171.117.213.66\t1970-01-01T00:00:00.000000Z\t720
                            180.36.62.54\t1970-01-01T00:00:00.000000Z\t528
                            180.48.50.141\t1970-01-01T00:00:00.000000Z\t136
                            180.91.244.55\t1970-01-01T00:00:00.000000Z\t906
                            181.82.42.148\t1970-01-01T00:00:00.000000Z\t539
                            186.33.243.40\t1970-01-01T00:00:00.000000Z\t659
                            187.63.210.97\t1970-01-01T00:00:00.000000Z\t424
                            187.139.150.80\t1970-01-01T00:00:00.000000Z\t580
                            188.239.72.25\t1970-01-01T00:00:00.000000Z\t513
                            201.100.238.229\t1970-01-01T00:00:00.000000Z\t318
                            205.123.179.216\t1970-01-01T00:00:00.000000Z\t167
                            212.102.182.127\t1970-01-01T00:00:00.000000Z\t984
                            212.159.205.29\t1970-01-01T00:00:00.000000Z\t23
                            216.150.248.30\t1970-01-01T00:00:00.000000Z\t563
                            224.99.254.121\t1970-01-01T00:00:00.000000Z\t619
                            227.40.250.157\t1970-01-01T00:00:00.000000Z\t903
                            230.202.108.161\t1970-01-01T00:00:00.000000Z\t171
                            231.146.30.59\t1970-01-01T00:00:00.000000Z\t766
                            241.248.184.75\t1970-01-01T00:00:00.000000Z\t334
                            254.93.251.9\t1970-01-01T00:00:00.000000Z\t810
                            255.95.177.227\t1970-01-01T00:00:00.000000Z\t44
                            """,
                    "select ip, ts, sum(bytes) from test sample by 1y align to calendar order by 2,1",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testStrColumnInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a string)");
            assertExceptionNoLeakCheck("x where b = a", 12, "STRING constant expected");
        });
    }

    @Test
    public void testStrColumnInLtFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a string)");
            assertExceptionNoLeakCheck("x where b < a", 12, "STRING constant expected");
        });
    }

    @Test
    public void testStrColumnInLtFilter2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a string)");
            assertExceptionNoLeakCheck("x where b > a", 12, "STRING constant expected");
        });
    }

    @Test
    public void testStrIPv4Cast() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        1.1.1.1
                        """,
                "select ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testStrIPv4CastInvalid() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        1.1.1.1
                        """,
                "select ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testStrIPv4CastNull() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        cast
                        1.1.1.1
                        """,
                "select ipv4 '1.1.1.1'"
        ));
    }

    @Test
    public void testStrIPv4CastOverflow() throws Exception {
        assertException(
                "select ipv4 '256.5.5.5'",
                12,
                "invalid IPv4 constant"
        );
    }

    @Test
    public void testTruncateIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            assertSql(
                    """
                            ip\tcount
                            187.139.150.80\t1
                            18.206.96.238\t1
                            92.80.211.65\t1
                            212.159.205.29\t1
                            4.98.173.21\t1
                            199.122.166.85\t1
                            79.15.250.138\t1
                            35.86.82.23\t1
                            111.98.117.250\t1
                            205.123.179.216\t1
                            184.254.198.204\t1
                            134.75.235.20\t1
                            170.90.236.206\t1
                            162.25.160.241\t1
                            48.21.128.89\t1
                            92.26.178.136\t1
                            93.140.132.196\t1
                            93.204.45.145\t1
                            231.146.30.59\t1
                            20.62.93.114\t1
                            """,
                    "test"
            );
            execute("truncate table test");
            assertSql("ip\tcount\n", "test");
        });
    }

    @Test
    public void testUpdateTableIPv4ToString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col1 ipv4)");
            execute("insert into test values('0.0.0.1')");
            execute("insert into test values('0.0.0.2')");
            execute("insert into test values('0.0.0.3')");
            execute("insert into test values('0.0.0.4')");
            execute("insert into test values('0.0.0.5')");
            execute("alter table test add col2 string");
            update("update test set col2 = col1");
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "test",
                    sink,
                    """
                            col1\tcol2
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.2
                            0.0.0.3\t0.0.0.3
                            0.0.0.4\t0.0.0.4
                            0.0.0.5\t0.0.0.5
                            """
            );
        });
    }

    @Test
    public void testUpdateTableIPv4ToStringWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select " +
                    " rnd_ipv4 col1, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table test add column col2 string");
            update("update test set col2 = col1");

            drainWalQueue();

            assertSql(
                    """
                            col1\tts\tcol2
                            187.139.150.80\t2022-02-24T00:00:00.000000Z\t187.139.150.80
                            18.206.96.238\t2022-02-24T00:00:01.000000Z\t18.206.96.238
                            92.80.211.65\t2022-02-24T00:00:02.000000Z\t92.80.211.65
                            212.159.205.29\t2022-02-24T00:00:03.000000Z\t212.159.205.29
                            4.98.173.21\t2022-02-24T00:00:04.000000Z\t4.98.173.21
                            """,
                    "test"
            );
        });
    }

    @Test
    public void testUpdateTableStringToIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col1 string)");
            execute("insert into test values('0.0.0.1')");
            execute("insert into test values('0.0.0.2')");
            execute("insert into test values('0.0.0.3')");
            execute("insert into test values('0.0.0.4')");
            execute("insert into test values('0.0.0.5')");
            execute("alter table test add col2 ipv4");
            update("update test set col2 = col1");
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "test",
                    sink,
                    """
                            col1\tcol2
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.2
                            0.0.0.3\t0.0.0.3
                            0.0.0.4\t0.0.0.4
                            0.0.0.5\t0.0.0.5
                            """
            );
        });
    }

    @Test
    public void testUpdateTableStringToIPv4Wal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select " +
                    " rnd_ipv4::string col1, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table test add column col2 ipv4");
            update("update test set col2 = col1");

            drainWalQueue();

            assertSql(
                    """
                            col1\tts\tcol2
                            187.139.150.80\t2022-02-24T00:00:00.000000Z\t187.139.150.80
                            18.206.96.238\t2022-02-24T00:00:01.000000Z\t18.206.96.238
                            92.80.211.65\t2022-02-24T00:00:02.000000Z\t92.80.211.65
                            212.159.205.29\t2022-02-24T00:00:03.000000Z\t212.159.205.29
                            4.98.173.21\t2022-02-24T00:00:04.000000Z\t4.98.173.21
                            """,
                    "test"
            );
        });
    }

    @Test
    public void testWalIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (" +
                    "select " +
                    " rnd_ipv4 col1, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertSql(
                    """
                            col1\tts
                            187.139.150.80\t2022-02-24T00:00:00.000000Z
                            18.206.96.238\t2022-02-24T00:00:01.000000Z
                            92.80.211.65\t2022-02-24T00:00:02.000000Z
                            212.159.205.29\t2022-02-24T00:00:03.000000Z
                            4.98.173.21\t2022-02-24T00:00:04.000000Z
                            """,
                    "test"
            );
        });
    }

    @Test
    public void testWhereIPv4() throws Exception {
        assertQuery(
                """
                        ip\tbytes\tk
                        0.0.0.1\t906\t1970-01-01T00:23:20.000000Z
                        0.0.0.1\t711\t1970-01-01T00:55:00.000000Z
                        0.0.0.1\t735\t1970-01-01T00:56:40.000000Z
                        0.0.0.1\t887\t1970-01-01T01:11:40.000000Z
                        0.0.0.1\t428\t1970-01-01T01:15:00.000000Z
                        0.0.0.1\t924\t1970-01-01T01:18:20.000000Z
                        0.0.0.1\t188\t1970-01-01T01:26:40.000000Z
                        0.0.0.1\t368\t1970-01-01T01:40:00.000000Z
                        0.0.0.1\t746\t1970-01-01T01:50:00.000000Z
                        0.0.0.1\t482\t1970-01-01T01:56:40.000000Z
                        0.0.0.1\t660\t1970-01-01T02:00:00.000000Z
                        0.0.0.1\t519\t1970-01-01T02:01:40.000000Z
                        0.0.0.1\t255\t1970-01-01T02:03:20.000000Z
                        0.0.0.1\t841\t1970-01-01T02:05:00.000000Z
                        0.0.0.1\t240\t1970-01-01T02:13:20.000000Z
                        0.0.0.1\t777\t1970-01-01T02:18:20.000000Z
                        0.0.0.1\t597\t1970-01-01T02:23:20.000000Z
                        0.0.0.1\t30\t1970-01-01T02:26:40.000000Z
                        0.0.0.1\t814\t1970-01-01T02:36:40.000000Z
                        0.0.0.1\t511\t1970-01-01T02:41:40.000000Z
                        0.0.0.1\t25\t1970-01-01T02:43:20.000000Z
                        """,
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
                """
                        ip1\tip2
                        0.0.0.1\t0.0.0.1
                        0.0.0.2\t0.0.0.2
                        0.0.0.5\t0.0.0.5
                        0.0.0.3\t0.0.0.3
                        0.0.0.1\t0.0.0.1
                        0.0.0.7\t0.0.0.7
                        0.0.0.1\t0.0.0.1
                        0.0.0.7\t0.0.0.7
                        \t
                        0.0.0.5\t0.0.0.5
                        0.0.0.7\t0.0.0.7
                        \t
                        """,
                "select * from test where ip1 = ip2",
                "create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(0,9,0)::ipv4 ip1," +
                        "    rnd_int(0,9,0)::ipv4 ip2" +
                        "  from long_sequence(100)" +
                        ")",
                null,
                true,
                false
        );
    }

    @Test
    public void testWhereInvalidIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table test as " +
                            "(" +
                            "  select" +
                            "    rnd_int(1,5,0)::ipv4 ip," +
                            "    rnd_int(0,1000,0) bytes," +
                            "    timestamp_sequence(0,100000000) k" +
                            "  from long_sequence(100)" +
                            ") timestamp(k)"
            );

            assertExceptionNoLeakCheck(
                    "select * from test where ip = 'apple'",
                    0,
                    "invalid IPv4 format"
            );
        });
    }
}
