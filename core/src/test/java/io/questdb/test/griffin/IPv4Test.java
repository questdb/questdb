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
        assertQuery("select ip, sum(bytes) from test order by ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000) time" +
                        "  from long_sequence(100)" +
                        ")")
                .expectSize()
                .returns("""
                        ip\tsum
                        0.0.0.1\t11644
                        0.0.0.2\t7360
                        0.0.0.3\t9230
                        0.0.0.4\t10105
                        0.0.0.5\t11739
                        """);
    }

    @Test
    public void testAlterTableIPv4NullCol() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col1 ipv4)");
            execute("insert into test values ('0.0.0.1')");
            execute("alter table test add col2 ipv4");

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            col1\tcol2
                            0.0.0.1\t
                            """);
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
            assertQuery("x where b = :ip")
                    .fails(0, "invalid IPv4 format: foobar");
        });
    }

    @Test
    public void testBitAndStr() throws Exception {
        assertQuery("select ipv4 '2.1.1.1' & '2.2.2.2'")
                .expectSize()
                .returns("""
                        column
                        2.0.0.0
                        """);
    }

    @Test
    public void testBitAndStr2() throws Exception {
        assertQuery("select '2.2.2.2' & ipv4 '2.1.1.1'")
                .expectSize()
                .returns("""
                        column
                        2.0.0.0
                        """);
    }

    @Test
    public void testBroadcastAddrUseCase() throws Exception {
        assertQuery("select (~ netmask('68.11.9.2/8')) | ipv4 '68.11.9.2'")
                .expectSize()
                .returns("""
                        column
                        68.255.255.255
                        """);
    }

    @Test
    public void testCaseIPv41() throws Exception {
        assertQuery("select ip, bytes, case when ip <<= '2.65.32.1/2' then 'YAY' else 'nay' end from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testCaseIPv42() throws Exception {
        assertQuery("select ip, bytes, case when ip <<= '2.65.32.1/2' then 'YAY' end from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testCaseIPv43() throws Exception {
        assertQuery("select ip, bytes, case when ip << '2.65.32.1/2' then 1 else 0 end from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testCaseIPv44() throws Exception {
        assertQuery("select ip, bytes, case when ip << '2.65.32.1/2' then 1 end from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testCaseIPv45() throws Exception {
        assertQuery("select ip, bytes, case when ip << null then 'NULL' else 'NOT NULL' end from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4('2.2.2.2/16', 2) ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testCaseIPv46() throws Exception {
        assertQuery("select ip, bytes, case when ip <<= null then 'NULL' else 'NOT NULL' end from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4('2.2.2.2/16', 2) ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testConstantInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into x values('192.168.0.1')");
            execute("insert into x values('255.255.255.255')");
            assertQuery("x where b = '192.168.0.1'")
                    .noLeakCheck()
                    .returns("""
                            b
                            192.168.0.1
                            """);
        });
    }

    @Test
    public void testContainsIPv4FunctionFactoryError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ip ipv4)");
            assertQuery("select * from t where ip << '1.1.1.1/35'")
                    .noLeakCheck()
                    .fails(28, "invalid argument: 1.1.1.1/35");
            assertQuery("select * from t where ip << '1.1.1.1/-1'")
                    .noLeakCheck()
                    .fails(28, "invalid argument: 1.1.1.1/-1");
            assertQuery("select * from t where ip << '1.1.1.1/A'")
                    .noLeakCheck()
                    .fails(28, "invalid argument: 1.1.1.1/A");
            assertQuery("select * from t where ip << '1.1/26'")
                    .noLeakCheck()
                    .fails(28, "invalid argument: 1.1/26");
        });
    }

    @Test
    public void testCountIPv4() throws Exception {
        assertQuery("select count(ip), bytes from test order by bytes")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,10,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testCreateAsSelectCastIPv4ToStr() throws Exception {
        assertMemoryLeak(() -> assertQuery("select rnd_ipv4()::string from long_sequence(10)")
        .noLeakCheck()
        .returnsOnce("""
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
                        """));
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

            assertQuery("select * from y")
                    .noLeakCheck()
                    .ddl("create table y as (x), cast(col as ipv4)")
                    .expectSize()
                    .returns("""
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
                            """);
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

            assertQuery("select * from y")
                    .noLeakCheck()
                    .ddl("create table y as (x), cast(col as ipv4)")
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testExplicitCastIPv4ToStr() throws Exception {
        assertQuery("select ipv4 '1.1.1.1'::string")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testExplicitCastIPv4ToStr2() throws Exception {
        assertQuery("select '1.1.1.1'::ipv4::string")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testExplicitCastIPv4ToVarchar() throws Exception {
        assertQuery("select ipv4 '1.1.1.1'::varchar")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testExplicitCastIPv4ToVarchar2() throws Exception {
        assertQuery("select '1.1.1.1'::ipv4::varchar")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testExplicitCastIntToIPv4() throws Exception {
        assertMemoryLeak(() -> assertQuery("select rnd_int()::ipv4 from long_sequence(10)")
        .noLeakCheck()
        .returnsOnce("""
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
                        """));
    }

    @Test
    public void testExplicitCastNullToIPv4() throws Exception {
        assertMemoryLeak(() -> assertQuery("select cast(case when x = 1 then null else rnd_ipv4() end as string) from long_sequence(10)")
        .noLeakCheck()
        .returnsOnce("""
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
                        """));
    }

    @Test
    public void testExplicitCastStrIPv4() throws Exception {
        assertQuery("select ~ ipv4 '2.2.2.2'")
                .expectSize()
                .returns("""
                        column
                        253.253.253.253
                        """);
    }

    @Test
    public void testExplicitCastStrToIPv4() throws Exception {
        assertMemoryLeak(() -> assertQuery("select rnd_ipv4()::string::ipv4 from long_sequence(10)")
        .noLeakCheck()
        .returnsOnce("""
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
                        """));
    }

    @Test
    public void testExplicitCastStrToIPv4Null() throws Exception {
        assertMemoryLeak(() -> assertQuery("select rnd_str()::ipv4 from long_sequence(10)")
        .noRandomAccess()
        .expectSize()
        .noLeakCheck()
        .returns("""
                        cast
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        """));
    }

    @Test
    public void testFirstIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
            assertQuery("select first(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            first
                            10.5.96.238
                            """);
        });
    }

    @Test
    public void testFullJoinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(100))");
            execute("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(100))");
            assertQuery("select a.count, a.ip, b.ip2, b.count2 from '*!*test' a join '*!*test2' b on b.ip2 = a.ip")
                    .noLeakCheck()
                    .fullFatJoins()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
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
                            """);
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
        assertQuery("select ipv4 '34.11.45.3' >= ipv4 '22.1.200.89'")
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testGreaterThanEqIPv4BadStr() throws Exception {
        assertQuery("select ipv4 '34.11.45.3' >= ipv4 'apple'")
                .fails(33, "invalid IPv4 constant");
    }

    @Test
    public void testGreaterThanEqIPv4Null() throws Exception {
        assertQuery("select ipv4 '34.11.45.3' >= ipv4 '0.0.0.0'")
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testGreaterThanEqIPv4Null2() throws Exception {
        assertQuery("select ipv4 '34.11.45.3' >= null")
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testGreaterThanEqIPv4Null3() throws Exception {
        assertQuery("select null >= ipv4 '34.11.45.3'")
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testGreaterThanIPv4() throws Exception {
        assertQuery("select ipv4 '34.11.45.3' > ipv4 '22.1.200.89'")
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testGroupByIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
            assertQuery("select count(count), ip from test group by ip order by ip")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testGroupByIPv42() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, 1 count from long_sequence(20))");
            assertQuery("select sum(count), ip from test group by ip order by ip")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testGroupByIPv43() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5.6/30', 2) ip, 1 count from long_sequence(20))");
            assertQuery("select sum(count), ip from test group by ip order by ip")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sum\tip
                            6\t
                            5\t10.5.6.0
                            1\t10.5.6.1
                            4\t10.5.6.2
                            4\t10.5.6.3
                            """);
        });
    }

    @Test
    public void testIPv4BitOr() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' | '255.0.0.0'")
                .expectSize()
                .returns("""
                        column
                        255.1.1.1
                        """);
    }

    @Test
    public void testIPv4BitOr2() throws Exception {
        assertQuery("select '1.1.1.1' | ipv4 '255.0.0.0'")
                .expectSize()
                .returns("""
                        column
                        255.1.1.1
                        """);
    }

    @Test
    public void testIPv4BitwiseAndChar() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' & '0'")
                .fails(22, "there is no matching operator `&` with the argument types: IPv4 & CHAR");

        assertQuery("select ipv4 '1.1.1.1' | '0'")
                .fails(22, "there is no matching operator `|` with the argument types: IPv4 | CHAR");

        assertQuery("select '0' & ipv4 '1.1.1.1'")
                .fails(11, "there is no matching operator `&` with the argument types: CHAR & IPv4");

        assertQuery("select '0' | ipv4 '1.1.1.1'")
                .fails(11, "there is no matching operator `|` with the argument types: CHAR | IPv4");
    }

    @Test
    public void testIPv4BitwiseAndConst() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' & ipv4 '0.0.1.1'")
                .expectSize()
                .returns("""
                        column
                        0.0.1.1
                        """);
    }

    @Test
    public void testIPv4BitwiseAndFails() throws Exception {
        assertQuery("select ip & cast(s as ipv4) from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    'apple' s," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(10)" +
                        ") ")
                .expectSize()
                .returns("""
                        column
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        
                        """);
    }

    @Test
    public void testIPv4BitwiseAndFailsConst() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' & ipv4 '0.0.1'")
                .fails(29, "invalid IPv4 constant");
    }

    @Test
    public void testIPv4BitwiseAndHalfConst() throws Exception {
        assertQuery("select ip & ipv4 '255.255.255.255' from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4BitwiseAndStr() throws Exception {
        assertQuery("select '1.1.1.1' & '0.0.1.1'")
                .expectSize()
                .returns("""
                        column
                        0.0.1.1
                        """);
    }

    @Test
    public void testIPv4BitwiseAndVar() throws Exception {
        assertQuery("select ip & ip2 from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4BitwiseFails() throws Exception {
        assertQuery("select ~ ipv4 'apple'")
                .fails(14, "invalid IPv4 constant");
    }

    @Test
    public void testIPv4BitwiseNotConst() throws Exception {
        assertQuery("select ~ ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        column
                        254.254.254.254
                        """);
    }

    @Test
    public void testIPv4BitwiseNotVar() throws Exception {
        assertQuery("select ~ip from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4BitwiseOr() throws Exception {
        assertQuery("select ip | ipv4 '255.0.0.0' from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4BitwiseOrConst() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' | ipv4 '255.0.0.0'")
                .expectSize()
                .returns("""
                        column
                        255.1.1.1
                        """);
    }

    @Test
    public void testIPv4BitwiseOrStr() throws Exception {
        assertQuery("select '1.1.1.1' | '0.0.1.1'")
                .expectSize()
                .returns("""
                        column
                        1.1.1.1
                        """);
    }

    @Test
    public void testIPv4BitwiseOrVar() throws Exception {
        assertQuery("select ip | ip2 from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
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

            assertQuery("select * from test where col <<= '12.67.50.2/20'")
                    .noLeakCheck()
                    .returns("col\n");
            assertQuery("select * from test where col <<= '12.67.50.2/1'")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """);
            assertQuery("select * from test where col <<= '255.6.8.10/8'")
                    .noLeakCheck()
                    .returns("""
                            col
                            255.255.255.255
                            """);
            assertQuery("select * from test where col <<= '12.67.50.2/0'")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """);
            assertQuery("select * from test where col <<= '1.6.2.0/32'")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
            assertQuery("select * from test where col <<= '1.6.2.0'")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetAndMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,1000,0)::ipv4 ip from long_sequence(100))");

            assertQuery("select * from test where ip <<= '0.0.0/24'")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where ip <<= subnet")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """);
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetFails() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0.1/hello'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0.1/hello");
    }

    @Test
    public void testIPv4ContainsEqSubnetFails2() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0/hello'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0/hello");
    }

    @Test
    public void testIPv4ContainsEqSubnetFails3() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0.0/65'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0.0/65");
    }

    @Test
    public void testIPv4ContainsEqSubnetFails4() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0/65'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0/65");
    }

    @Test
    public void testIPv4ContainsEqSubnetFails5() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0.0/-1'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0.0/-1");
    }

    @Test
    public void testIPv4ContainsEqSubnetFails6() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0/-1'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0/-1");
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsChars() throws Exception {
        assertQuery("select * from test where ip <<= 'apple'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: apple");
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsNetmaskOverflow() throws Exception {
        assertQuery("select * from test where ip <<= '85.7.36/74'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 85.7.36/74");
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsNums() throws Exception {
        assertQuery("select * from test where ip <<= '8573674'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 8573674");
    }

    @Test
    public void testIPv4ContainsEqSubnetFailsOverflow() throws Exception {
        assertQuery("select * from test where ip <<= '256.256.256.256'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 256.256.256.256");
    }

    @Test
    public void testIPv4ContainsEqSubnetIncorrectMask() throws Exception {
        assertQuery("select * from test where ip <<= '0.0.0/32'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(32, "invalid argument: 0.0.0/32");
    }

    @Test
    public void testIPv4ContainsEqSubnetNoMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(100))");

            assertQuery("select * from test where ip <<= '0.0.0.4'")
                    .noLeakCheck()
                    .returns("""
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
                            """);
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

            assertQuery("select * from test where col <<= '12.67.50.2/20'::varchar")
                    .noLeakCheck()
                    .returns("col\n");
            assertQuery("select * from test where col <<= '12.67.50.2/1'::varchar")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """);
            assertQuery("select * from test where col <<= '255.6.8.10/8'::varchar")
                    .noLeakCheck()
                    .returns("""
                            col
                            255.255.255.255
                            """);
            assertQuery("select * from test where col <<= '12.67.50.2/0'::varchar")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """);
            assertQuery("select * from test where col <<= '1.6.2.0/32'::varchar")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
            assertQuery("select * from test where col <<= '1.6.2.0'::varchar")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
        });
    }

    @Test
    public void testIPv4ContainsEqSubnetVarcharColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where ip <<= subnet")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """);
        });
    }

    @Test
    public void testIPv4ContainsSubnet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertQuery("select * from test where ip << '0.0.0.1'")
                    .noLeakCheck()
                    .returns("ip\n");
        });
    }

    @Test
    public void testIPv4ContainsSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertQuery("select * from test where ip << '0.0.0.1/32'")
                    .noLeakCheck()
                    .returns("ip\n");
        });
    }

    @Test
    public void testIPv4ContainsSubnet3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertQuery("select * from test where ip << '0.0.0.1/24'")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4ContainsSubnet4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where ip << subnet")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """);
        });
    }

    @Test
    public void testIPv4ContainsVarcharSubnet1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertQuery("select * from test where ip << '0.0.0.1/24'::varchar")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4ContainsVarcharSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where ip << subnet")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """);
        });
    }

    @Test
    public void testIPv4CountDistinct() throws Exception {
        String expected = """
                count_distinct
                20
                """;
        assertQuery("select count_distinct(ip) from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .noRandomAccess()
                .expectSize()
                .returns(expected);
        assertQuery("select count(distinct ip) from test")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns(expected);
    }

    @Test
    public void testIPv4Distinct() throws Exception {
        assertQuery("select distinct ip from test order by ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
                        ip
                        0.0.0.1
                        0.0.0.2
                        0.0.0.3
                        0.0.0.4
                        0.0.0.5
                        """);
    }

    @Test
    public void testIPv4EqArgsSwapped() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

            assertQuery("select * from test where ipv4 '0.0.0.1' = ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            0.0.0.1
                            """);
        });
    }

    @Test
    public void testIPv4EqNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

            assertQuery("select * from test where ip != ipv4 '0.0.0.1'")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4EqNegated2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(20))");

            assertQuery("select * from test where ipv4 '0.0.0.1' != ip")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4EqNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,0)::ipv4 ip from long_sequence(100))");

            assertQuery("select * from test where ip = null")
                    .noLeakCheck()
                    .returns("""
                            ip
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            """);
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

            assertQuery("select col1 from x except select col2 from y")
                    .noLeakCheck()
                    .returns("""
                            col1
                            0.0.0.6
                            """);
        });
    }

    @Test
    public void testIPv4Explain() throws Exception {
        assertQuery("explain select * from test order by ip desc")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        QUERY PLAN
                        Encode sort light
                          keys: [ip desc]
                            PageFrame
                                Row forward scan
                                Frame forward scan on: test
                        """);
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

            assertQuery("select col1 from x intersect all select col2 from y")
                    .noLeakCheck()
                    .returns("""
                            col1
                            0.0.0.1
                            0.0.0.2
                            """);
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

            assertQuery("select isOrdered(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            isOrdered
                            true
                            """);
        });
    }

    @Test
    public void testIPv4IsOrderedFalse() throws Exception {
        assertQuery("select isOrdered(ip) from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        isOrdered
                        false
                        """);
    }

    @Test
    public void testIPv4IsOrderedKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT" +
                    " ('k' || lpad((x % 100)::string, 5, '0')) key," +
                    " ((x % 100)::int)::ipv4 ip," +
                    " (x * 1000000)::timestamp ts" +
                    " FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY HOUR");
            assertQuery("SELECT key, isOrdered(ip) FROM test ORDER BY key LIMIT 1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("key\tisOrdered\nk00000\ttrue\n");
        });
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

            assertQuery("select isOrdered(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            isOrdered
                            true
                            """);
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

            assertQuery("select isOrdered(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            isOrdered
                            true
                            """);
        });
    }

    @Test
    public void testIPv4MinusIPv4Char() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - '9'")
                .expectSize()
                .returns("""
                        column
                        1.1.0.248
                        """);
    }

    @Test
    public void testIPv4MinusIPv4Const() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - ipv4 '255.255.255.255'")
                .expectSize()
                .returns("""
                        column
                        -4278124286
                        """);
    }

    @Test
    public void testIPv4MinusIPv4ConstNull() throws Exception {
        assertQuery("select ipv4 '0.0.0.0' - ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        column
                        null
                        """);
    }

    @Test
    public void testIPv4MinusIPv4HalfConst() throws Exception {
        assertQuery("select ip - ipv4 '22.54.6.7' from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_ipv4() ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(50)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4MinusIPv4Str() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - '0.0.0.1'")
                .expectSize()
                .returns("""
                        column
                        16843008
                        """);
    }

    @Test
    public void testIPv4MinusIPv4Var() throws Exception {
        assertQuery("select ip2 - ip from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4('2.65.11.1/24', 0) ip," +
                        "    rnd_ipv4('2.65.11.1/24', 0) ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(50)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4MinusIntConst() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - 1")
                .expectSize()
                .returns("""
                        column
                        1.1.1.0
                        """);
    }

    @Test
    public void testIPv4MinusIntConst2() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - 16843008")
                .expectSize()
                .returns("""
                        column
                        0.0.0.1
                        """);
    }

    @Test
    public void testIPv4MinusIntConstOverflow() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - 16843010")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIPv4MinusIntHalfConst() throws Exception {
        assertQuery("select ip - 5 from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4MinusIntVar() throws Exception {
        assertQuery("select ip - bytes from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(500,2500,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        
                        """);
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

            assertQuery("select * from test where '12.67.50.2/20' >>= col")
                    .noLeakCheck()
                    .returns("col\n");
            assertQuery("select * from test where '12.67.50.2/1' >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """);
            assertQuery("select * from test where '255.6.8.10/8' >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            255.255.255.255
                            """);
            assertQuery("select * from test where '12.67.50.2/0' >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """);
            assertQuery("select * from test where '1.6.2.0/32' >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
            assertQuery("select * from test where '1.6.2.0' >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetAndMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,2000,0)::ipv4 ip from long_sequence(100))");

            assertQuery("select * from test where '0.0.0/24' >>= ip")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where subnet >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails() throws Exception {
        assertQuery("select * from test where '0.0.0.1/hello' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0.1/hello");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails2() throws Exception {
        assertQuery("select * from test where '0.0.0/hello' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0/hello");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails3() throws Exception {
        assertQuery("select * from test where '0.0.0.0/65' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0.0/65");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails4() throws Exception {
        assertQuery("select * from test where '0.0.0/65' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0/65");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails5() throws Exception {
        assertQuery("select * from test where '0.0.0.0/-1' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0.0/-1");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFails6() throws Exception {
        assertQuery("select * from test where '0.0.0/-1' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0/-1");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsChars() throws Exception {
        assertQuery("select * from test where 'apple' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: apple");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsNetmaskOverflow() throws Exception {
        assertQuery("select * from test where '85.7.36/74' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 85.7.36/74");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsNums() throws Exception {
        assertQuery("select * from test where '8573674' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 8573674");
    }

    @Test
    public void testIPv4NegContainsEqSubnetFailsOverflow() throws Exception {
        assertQuery("select * from test where '256.256.256.256' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 256.256.256.256");
    }

    @Test
    public void testIPv4NegContainsEqSubnetIncorrectMask() throws Exception {
        assertQuery("select * from test where '0.0.0/32' >>= ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1000,2000,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .fails(25, "invalid argument: 0.0.0/32");
    }

    @Test
    public void testIPv4NegContainsEqSubnetNoMask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(0,5,2)::ipv4 ip from long_sequence(20))");

            assertQuery("select * from test where '0.0.0.4' >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            0.0.0.4
                            0.0.0.4
                            0.0.0.4
                            """);
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

            assertQuery("select * from test where '12.67.50.2/20'::varchar >>= col")
                    .noLeakCheck()
                    .returns("col\n");
            assertQuery("select * from test where '12.67.50.2/1'::varchar >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            1.6.2.0
                            
                            """);
            assertQuery("select * from test where '255.6.8.10/8'::varchar >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            255.255.255.255
                            """);
            assertQuery("select * from test where '12.67.50.2/0'::varchar >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            12.67.45.3
                            160.5.22.8
                            240.110.88.22
                            1.6.2.0
                            255.255.255.255
                            
                            """);
            assertQuery("select * from test where '1.6.2.0/32'::varchar >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
            assertQuery("select * from test where '1.6.2.0'::varchar >>= col")
                    .noLeakCheck()
                    .returns("""
                            col
                            1.6.2.0
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsEqSubnetVarcharColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where subnet >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1
                            0.0.0.1\t0.0.0.1
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsSubnet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertQuery("select * from test where '0.0.0.1' >> ip")
                    .noLeakCheck()
                    .returns("ip\n");
        });
    }

    @Test
    public void testIPv4NegContainsSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(100))");
            assertQuery("select * from test where '0.0.0.1/32' >> ip")
                    .noLeakCheck()
                    .returns("ip\n");
        });
    }

    @Test
    public void testIPv4NegContainsSubnet3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertQuery("select * from test where '0.0.0.1/24' >> ip")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsSubnet4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_str(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where subnet >> ip")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsVarcharSubnet1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip from long_sequence(10))");
            assertQuery("select * from test where '0.0.0.1/24'::varchar >> ip")
                    .noLeakCheck()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIPv4NegContainsVarcharSubnet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_int(1,2,0)::ipv4 ip, rnd_varchar(null,'0.0.0.1','0.0.0.1/24','0.0.0.1/32') subnet from long_sequence(20))");
            assertQuery("select * from test where subnet >> ip")
                    .noLeakCheck()
                    .returns("""
                            ip\tsubnet
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.2\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            0.0.0.1\t0.0.0.1/24
                            """);
        });
    }

    @Test
    public void testIPv4Null() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (col ipv4)");
            execute("insert into test values(null)");
            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            col
                            
                            """);
        });
    }

    @Test
    public void testIPv4NullIf() throws Exception {
        assertQuery("select k, nullif(ip, '0.0.0.5') from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,10,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .timestamp("k")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4PlusIntConst() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' + 20")
                .expectSize()
                .returns("""
                        column
                        1.1.1.21
                        """);
    }

    @Test
    public void testIPv4PlusIntConst2() throws Exception {
        assertQuery("select ipv4 '255.255.255.20' + 235")
                .expectSize()
                .returns("""
                        column
                        255.255.255.255
                        """);
    }

    @Test
    public void testIPv4PlusIntConst3() throws Exception {
        assertQuery("select  ('255.255.255.255')::ipv4 + 10")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIPv4PlusIntConstNull() throws Exception {
        assertQuery("select ipv4 '0.0.0.0' + 20")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIPv4PlusIntConstOverflow() throws Exception {
        assertQuery("select ipv4 '255.255.255.255' + 1")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIPv4PlusIntConstOverflow2() throws Exception {
        assertQuery("select ipv4 '255.255.255.20' + 236")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIPv4PlusIntHalfConst() throws Exception {
        assertQuery("select ip + 20 from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4PlusIntVar() throws Exception {
        assertQuery("select ip + bytes from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4Rank() throws Exception {
        assertQuery("select ip, bytes, rank() over (order by ip asc) rank from test order by rank, bytes")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4RowNum() throws Exception {
        assertQuery("select ip, bytes, row_number() over () as row_num from test order by row_num asc")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4StrBadStr() throws Exception {
        assertQuery("select netmask('bdfsir/33')")
                .expectSize()
                .returns("""
                        netmask
                        
                        """);
    }

    @Test
    public void testIPv4StrBitwiseOrHalfConst() throws Exception {
        assertQuery("select ip | ipv4 '255.0.0.0' from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIPv4StrMinusIPv4() throws Exception {
        assertQuery("select '1.1.1.1' - ipv4 '0.0.0.1'")
                .expectSize()
                .returns("""
                        column
                        16843008
                        """);
    }

    @Test
    public void testIPv4StrMinusIPv4Str() throws Exception {
        assertQuery("select '1.1.1.1' - '0.0.0.1'")
                .expectSize()
                .returns("""
                        column
                        16843008
                        """);
    }

    @Test
    public void testIPv4StrMinusInt() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' - 5")
                .expectSize()
                .returns("""
                        column
                        1.1.0.252
                        """);
    }

    @Test
    public void testIPv4StrNetmask() throws Exception {
        assertQuery("select netmask('68.11.22.1/28')")
                .expectSize()
                .returns("""
                        netmask
                        255.255.255.240
                        """);
    }

    @Test
    public void testIPv4StrNetmaskNull() throws Exception {
        assertQuery("select netmask('68.11.22.1/33')")
                .expectSize()
                .returns("""
                        netmask
                        
                        """);
    }

    @Test
    public void testIPv4StrPlusInt() throws Exception {
        assertQuery("select ipv4 '1.1.1.1' + 5")
                .expectSize()
                .returns("""
                        column
                        1.1.1.6
                        """);
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

            assertQuery("select col1 from x union select col2 from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            col1
                            0.0.0.1
                            0.0.0.2
                            0.0.0.3
                            0.0.0.4
                            0.0.0.5
                            """);
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

            assertQuery("select col1 from x union all select col2 from y")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testImplicitCastBinaryToIPv4() throws Exception {
        assertQuery("insert into y select rnd_bin() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: BINARY -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastBoolToIPv4() throws Exception {
        assertQuery("insert into y select rnd_boolean() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: BOOLEAN -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastByteToIPv4() throws Exception {
        assertQuery("insert into y select rnd_byte() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: BYTE -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastCharToIPv4() throws Exception {
        assertQuery("insert into y select rnd_char() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: CHAR -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastDateToIPv4() throws Exception {
        assertQuery("insert into y select rnd_date() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: DATE -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastDoubleToIPv4() throws Exception {
        assertQuery("insert into y select rnd_double() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: DOUBLE -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastFloatToIPv4() throws Exception {
        assertQuery("insert into y select rnd_float() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: FLOAT -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToBinary() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col binary)")
                .fails(21, "inconvertible types: IPv4 -> BINARY [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToBool() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col boolean)")
                .fails(21, "inconvertible types: IPv4 -> BOOLEAN [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToByte() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col byte)")
                .fails(21, "inconvertible types: IPv4 -> BYTE [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToChar() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col char)")
                .fails(21, "inconvertible types: IPv4 -> CHAR [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToDate() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col date)")
                .fails(21, "inconvertible types: IPv4 -> DATE [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToDouble() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col double)")
                .fails(21, "inconvertible types: IPv4 -> DOUBLE [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToFloat() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col float)")
                .fails(21, "inconvertible types: IPv4 -> FLOAT [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToInt() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col int)")
                .fails(21, "inconvertible types: IPv4 -> INT [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToLong() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col long)")
                .fails(21, "inconvertible types: IPv4 -> LONG [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToLong128() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col long128)")
                .fails(21, "inconvertible types: IPv4 -> LONG128 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToLong256() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col long256)")
                .fails(21, "inconvertible types: IPv4 -> LONG256 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToShort() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col short)")
                .fails(21, "inconvertible types: IPv4 -> SHORT [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToStr() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col string)")
                .fails(21, "inconvertible types: IPv4 -> STRING [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToSymbol() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col symbol)")
                .fails(21, "inconvertible types: IPv4 -> SYMBOL [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToTimestamp() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col timestamp)")
                .fails(21, "inconvertible types: IPv4 -> TIMESTAMP [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToUUID() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col uuid)")
                .fails(21, "inconvertible types: IPv4 -> UUID [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIPv4ToVarchar() throws Exception {
        assertQuery("insert into y select rnd_ipv4() col from long_sequence(10)")
                .ddl("create table y (col varchar)")
                .fails(21, "inconvertible types: IPv4 -> VARCHAR [from=col, to=col]");
    }

    @Test
    public void testImplicitCastIntToIPv4() throws Exception {
        assertQuery("insert into y select rnd_int() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: INT -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastLong256ToIPv4() throws Exception {
        assertQuery("insert into y select rnd_long256() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: LONG256 -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastLongToIPv4() throws Exception {
        assertQuery("insert into y select rnd_long() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: LONG -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastShortToIPv4() throws Exception {
        assertQuery("insert into y select rnd_short() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: SHORT -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastStrIPv4() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select * from test where ip = '187.139.150.80'")
                    .noLeakCheck()
                    .ddl("create table test as " +
                            "(" +
                            "  select" +
                            "    rnd_ipv4() ip," +
                            "    rnd_int(0,1000,0) bytes," +
                            "    timestamp_sequence(0,100000000) ts" +
                            "  from long_sequence(50)" +
                            ")")
                    .returns("""
                            ip\tbytes\tts
                            187.139.150.80\t580\t1970-01-01T00:00:00.000000Z
                            """);

            assertQuery("select * from test where '187.139.150.80' = ip")
                    .noLeakCheck()
                    .returns("""
                            ip\tbytes\tts
                            187.139.150.80\t580\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testImplicitCastStrIPv42() throws Exception {
        assertQuery("select ipv4 '2.2.2.2' <= '1.1.1.1'")
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testImplicitCastStrIPv4BadStr() throws Exception {
        assertQuery("select 'dhukdsvhiu' < ipv4 '1.1.1.1'")
                .fails(0, "invalid IPv4 format: dhukdsvhiu");
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

            assertQuery("select * from y")
                    .noLeakCheck()
                    .ddl("insert into y select * from x")
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testImplicitCastSymbolToIPv4() throws Exception {
        assertQuery("insert into y select rnd_symbol(4,4,4,2) col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: SYMBOL -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastTimestampToIPv4() throws Exception {
        assertQuery("insert into y select rnd_timestamp(10,100000,356) col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: TIMESTAMP -> IPv4 [from=col, to=col]");
    }

    @Test
    public void testImplicitCastUUIDToIPv4() throws Exception {
        assertQuery("insert into y select rnd_uuid4() col from long_sequence(10)")
                .ddl("create table y (col ipv4)")
                .fails(21, "inconvertible types: UUID -> IPv4 [from=col, to=col]");
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
            assertQuery("x where b <<= $1")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testIndexedBindVariableInContainsEqFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertQuery("x where b <<= $1")
                    .noLeakCheck()
                    .fails(14, "invalid argument: foobar");
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
            assertQuery("x where b <<= $1")
                    .noLeakCheck()
                    .returns("b\ta\n");
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
            assertQuery("x where b << $1")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testIndexedBindVariableInContainsFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertQuery("x where b << $1")
                    .noLeakCheck()
                    .fails(13, "invalid argument: foobar");
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
            assertQuery("x where b << $1")
                    .noLeakCheck()
                    .returns("b\ta\n");
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
            assertQuery("x where b = $1")
                    .noLeakCheck()
                    .returns("""
                            b
                            192.168.0.1
                            """);
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
            assertQuery("x where b < $1")
                    .noLeakCheck()
                    .returns("""
                            b
                            127.0.0.1
                            """);
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
            assertQuery("x where $1 < b")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
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
            assertQuery("x where $1 >>= b")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsEqFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertQuery("x where $1 >>= b")
                    .noLeakCheck()
                    .fails(8, "invalid argument: foobar");
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
            assertQuery("x where $1 >>= b")
                    .noLeakCheck()
                    .returns("b\ta\n");
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
            assertQuery("x where $1 >> b")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testIndexedBindVariableInNegContainsFilterInvalidValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('127.0.0.1')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertQuery("x where $1 >> b")
                    .noLeakCheck()
                    .fails(8, "invalid argument: foobar");
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
            assertQuery("x where $1 >> b")
                    .noLeakCheck()
                    .returns("b\ta\n");
        });
    }

    @Test
    public void testInnerJoinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('1.1.1.1/32', 0) ip, 1 count from long_sequence(5))");
            execute("create table test2 as (select rnd_ipv4('1.1.1.1/32', 0) ip2, 2 count2 from long_sequence(5))");
            assertQuery("select test.count, test2.count2 from test inner join test2 on test2.ip2 = test.ip")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testIntPlusIPv4Const() throws Exception {
        assertQuery("select 20 + ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        column
                        1.1.1.21
                        """);
    }

    @Test
    public void testIntPlusIPv4Const2() throws Exception {
        assertQuery("select 235 + ipv4 '255.255.255.20'")
                .expectSize()
                .returns("""
                        column
                        255.255.255.255
                        """);
    }

    @Test
    public void testIntPlusIPv4ConstNull() throws Exception {
        assertQuery("select 20 + ipv4 '0.0.0.0'")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIntPlusIPv4ConstOverflow() throws Exception {
        assertQuery("select 1 + ipv4 '255.255.255.255'")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIntPlusIPv4ConstOverflow2() throws Exception {
        assertQuery("select 236 + ipv4 '255.255.255.20'")
                .expectSize()
                .returns("""
                        column
                        
                        """);
    }

    @Test
    public void testIntPlusIPv4HalfConst() throws Exception {
        assertQuery("select 20 + ip from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testIntPlusIPv4Str() throws Exception {
        assertQuery("select 5 + ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        column
                        1.1.1.6
                        """);
    }

    @Test
    public void testIntPlusIPv4Var() throws Exception {
        assertQuery("select bytes + ip from test")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,20,0)::ipv4 ip," +
                        "    rnd_int(1,20,0)::ipv4 ip2," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") ")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testInvalidConstantInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            assertQuery("x where b = 'foobar'")
                    .noLeakCheck()
                    .fails(0, "invalid IPv4 format: foobar");
        });
    }

    @Test
    public void testLastIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 2) ip, rnd_symbol('ab', '$a', 'ac') sym from long_sequence(20))");
            assertQuery("select sym, last(ip) from test order by sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            sym\tlast
                            $a\t10.5.237.229
                            ab\t
                            ac\t10.5.235.200
                            """);
        });
    }

    @Test
    public void testLatestByIPv4() throws Exception {
        assertQuery("select * from test latest by ip")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000) time" +
                        "  from long_sequence(100)" +
                        ")")
                .expectSize()
                .returns("""
                        ip\tbytes\ttime
                        0.0.0.4\t269\t1970-01-01T00:00:08.700000Z
                        0.0.0.3\t660\t1970-01-01T00:00:09.000000Z
                        0.0.0.5\t624\t1970-01-01T00:00:09.600000Z
                        0.0.0.1\t25\t1970-01-01T00:00:09.800000Z
                        0.0.0.2\t326\t1970-01-01T00:00:09.900000Z
                        """);
    }

    @Test
    public void testLeftJoinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('1.1.1.1/16', 0) ip, 1 count from long_sequence(5))");
            execute("create table test2 as (select rnd_ipv4('1.1.1.1/32', 0) ip2, 2 count2 from long_sequence(5))");
            assertQuery("select test.ip, test2.ip2, test.count, test2.count2 from test left join test2 on test2.ip2 = test.ip")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ip\tip2\tcount\tcount2
                            1.1.96.238\t\t1\tnull
                            1.1.50.227\t\t1\tnull
                            1.1.89.171\t\t1\tnull
                            1.1.82.23\t\t1\tnull
                            1.1.76.40\t\t1\tnull
                            """);
        });
    }

    @Test
    public void testLeftJoinIPv42() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('12.5.9/24', 0) ip, 1 count from long_sequence(50))");
            execute("create table test2 as (select rnd_ipv4('12.5.9/24', 0) ip2, 2 count2 from long_sequence(50))");
            assertQuery("select test.ip, test2.ip2, test.count, test2.count2 from test left join test2 on test2.ip2 = test.ip")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testLessThanEqIPv4() throws Exception {
        assertQuery("select ipv4 '34.11.45.3' <= ipv4 '34.11.45.3'")
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testLessThanIPv4() throws Exception {
        assertQuery("select ipv4 '34.11.45.3' < ipv4 '22.1.200.89'")
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testLimitIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
            assertQuery("select * from test limit 5")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ip\tcount
                            10.5.96.238\t1
                            10.5.50.227\t1
                            10.5.89.171\t1
                            10.5.82.23\t1
                            10.5.76.40\t1
                            """);
        });
    }

    @Test
    public void testLimitIPv42() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
            assertQuery("select * from test limit -5")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ip\tcount
                            10.5.170.235\t1
                            10.5.45.159\t1
                            10.5.184.81\t1
                            10.5.207.196\t1
                            10.5.213.108\t1
                            """);
        });
    }

    @Test
    public void testLimitIPv43() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4('10.5/16', 0) ip, 1 count from long_sequence(20))");
            assertQuery("select * from test limit 2, 10")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ip\tcount
                            10.5.89.171\t1
                            10.5.82.23\t1
                            10.5.76.40\t1
                            10.5.20.236\t1
                            10.5.95.15\t1
                            10.5.178.136\t1
                            10.5.45.145\t1
                            10.5.93.114\t1
                            """);
        });
    }

    @Test
    public void testMaxIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            execute("insert into test values ('255.255.255.255', 1)");
            assertQuery("select max(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            max
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testMinIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            execute("insert into test values ('0.0.0.1', 1)");
            assertQuery("select min(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min
                            0.0.0.1
                            """);
        });
    }

    @Test
    public void testMinIPv4Null() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            execute("insert into test values ('0.0.0.0', 1)");
            assertQuery("select min(ip) from test")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min
                            4.98.173.21
                            """);
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
            assertQuery("x where b <<= :ip")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
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
            assertQuery("x where b << :ip")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
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
            assertQuery("x where b = :ip")
                    .noLeakCheck()
                    .returns("""
                            b
                            192.168.0.1
                            """);
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
            assertQuery("x where b < :ip")
                    .noLeakCheck()
                    .returns("""
                            b
                            127.0.0.1
                            """);
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
            assertQuery("x where :ip < b")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
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
            assertQuery("x where :ip >>= b")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
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
            assertQuery("x where :ip >> b")
                    .noLeakCheck()
                    .returns("""
                            b
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testNegContainsIPv4FunctionFactoryError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ip ipv4)");
            assertQuery("select * from t where '1.1.1.1/35' >> ip")
                    .noLeakCheck()
                    .fails(22, "invalid argument: 1.1.1.1/35");
            assertQuery("select * from t where '1.1.1.1/-1' >> ip")
                    .noLeakCheck()
                    .fails(22, "invalid argument: 1.1.1.1/-1");
            assertQuery("select * from t where '1.1.1.1/A' >> ip ")
                    .noLeakCheck()
                    .fails(22, "invalid argument: 1.1.1.1/A");
            assertQuery("select * from t where '1.1/26' >> ip ")
                    .noLeakCheck()
                    .fails(22, "invalid argument: 1.1/26");
        });
    }

    @Test
    public void testNetmask() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tipv4 (ip ipv4)");
            execute("insert into tipv4 values ('255.255.255.254')");
            assertQuery("select * from tipv4 where '255.255.255.255/31' >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            255.255.255.254
                            """);
            assertQuery("select * from tipv4 where '255.255.255.255/32' >>= ip")
                    .noLeakCheck()
                    .returns("ip\n");
            assertQuery("select * from tipv4 where '255.255.255.255/30' >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            255.255.255.254
                            """);
            assertQuery("select * from tipv4 where '255.255.255.0/24' >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            255.255.255.254
                            """);

            assertQuery("select '255.255.255.255'::ipv4 <<= '255.255.255.255'")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);
            assertQuery("select '255.255.255.255'::ipv4 <<= '255.255.255.254'")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            false
                            """);
            assertQuery("select '255.255.255.255'::ipv4 <<= '255.255.255.254/31'")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);

            assertQuery("select netmask('1.1.1.1/0');")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n\n");
            assertQuery("select netmask('1.1.1.1/30');")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n255.255.255.252\n");
            assertQuery("select netmask('1.1.1.1/31');")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n255.255.255.254\n");
            assertQuery("select netmask('1.1.1.1/32');")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n255.255.255.255\n");
            assertQuery("select netmask('1.1.1.1/33');")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n\n");
        });
    }

    @Test
    public void testNetmaskColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (msk string)");
            execute("insert into test values ('255.255.255.255/30')");
            execute("insert into test values ('255.255.255.255/31')");
            execute("insert into test values ('255.255.255.255/32')");

            assertQuery("select netmask(msk) from test;")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            netmask
                            255.255.255.252
                            255.255.255.254
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testNetmaskVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tipv4 (ip ipv4)");
            execute("insert into tipv4 values ('255.255.255.254')");
            assertQuery("select * from tipv4 where '255.255.255.255/31'::varchar >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            255.255.255.254
                            """);
            assertQuery("select * from tipv4 where '255.255.255.255/32'::varchar >>= ip")
                    .noLeakCheck()
                    .returns("ip\n");
            assertQuery("select * from tipv4 where '255.255.255.255/30'::varchar >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            255.255.255.254
                            """);
            assertQuery("select * from tipv4 where '255.255.255.0/24'::varchar >>= ip")
                    .noLeakCheck()
                    .returns("""
                            ip
                            255.255.255.254
                            """);

            assertQuery("select '255.255.255.255'::ipv4 <<= '255.255.255.255'::varchar")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);
            assertQuery("select '255.255.255.255'::ipv4 <<= '255.255.255.254'::varchar")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            false
                            """);
            assertQuery("select '255.255.255.255'::ipv4 <<= '255.255.255.254/31'::varchar")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);

            assertQuery("select netmask('1.1.1.1/0'::varchar);")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n\n");
            assertQuery("select netmask('1.1.1.1/30'::varchar);")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n255.255.255.252\n");
            assertQuery("select netmask('1.1.1.1/31'::varchar);")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n255.255.255.254\n");
            assertQuery("select netmask('1.1.1.1/32'::varchar);")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n255.255.255.255\n");
            assertQuery("select netmask('1.1.1.1/33'::varchar);")
                    .noLeakCheck()
                    .expectSize()
                    .returns("netmask\n\n");
        });
    }

    @Test
    public void testNetmaskVarcharColumnInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (msk varchar)");
            execute("insert into test values ('255.255.255.255/30')");
            execute("insert into test values ('255.255.255.255/31')");
            execute("insert into test values ('255.255.255.255/32')");

            assertQuery("select netmask(msk) from test;")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            netmask
                            255.255.255.252
                            255.255.255.254
                            255.255.255.255
                            """);
        });
    }

    @Test
    public void testNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4)");
            execute("insert into x values('128.0.0.0')");
            execute("insert into x values('0.0.0.0')");

            assertQuery("x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b
                            128.0.0.0
                            
                            """);
        });
    }

    @Test
    public void testNullNetmask() throws Exception {
        assertQuery("select netmask(null)")
                .expectSize()
                .returns("""
                        netmask
                        
                        """);
    }

    @Test
    public void testOrderByIPv4Ascending() throws Exception {
        assertQuery("select * from test order by ip, bytes, ts")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testOrderByIPv4Descending() throws Exception {
        assertQuery("select * from test order by ip DESC")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_ipv4() ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) ts" +
                        "  from long_sequence(50)" +
                        ")")
                .expectSize()
                .returns("""
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
                        """);
    }

    @Test
    public void testRandomIPv4Error() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select rnd_ipv4('1.1.1.1/35', 1)")
                    .noLeakCheck()
                    .fails(16, "invalid argument: 1.1.1.1/35");
            assertQuery("select rnd_ipv4('1.1.1.1/-1', 1)")
                    .noLeakCheck()
                    .fails(16, "invalid argument: 1.1.1.1/-1");
            assertQuery("select rnd_ipv4('1.1.1.1/A', 1)")
                    .noLeakCheck()
                    .fails(16, "invalid argument: 1.1.1.1/A");
            assertQuery("select rnd_ipv4('1.1/26', 1)")
                    .noLeakCheck()
                    .fails(16, "invalid argument: 1.1/26");
        });
    }

    @Test
    public void testRndIPv4() throws Exception {
        assertMemoryLeak(() -> assertQuery("select rnd_ipv4('12.6/16', 0) from long_sequence(10)")
        .noLeakCheck()
        .returnsOnce("""
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
                        """));
    }

    @Test
    public void testRndIPv42() throws Exception {
        assertMemoryLeak(() -> assertQuery("select rnd_ipv4('12.6.8/16', 0) from long_sequence(10)")
        .noLeakCheck()
        .returnsOnce("""
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
                        """));
    }

    @Test
    public void testSampleByIPv4() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select ip, ts, sum(bytes) from test sample by 1y align to first observation order by 2,1")
                    .noLeakCheck()
                    .ddl("create table test as " +
                            "(" +
                            "  select" +
                            "    rnd_ipv4() ip," +
                            "    rnd_int(0,1000,0) bytes," +
                            "    timestamp_sequence(1,10000) ts" +
                            "  from long_sequence(50)" +
                            ") timestamp(ts)")
                    .timestamp("ts")
                    .returns("""
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
                            """);

            assertQuery("select ip, ts, sum(bytes) from test sample by 1y align to calendar order by 2,1")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
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
                            """);
        });
    }

    @Test
    public void testStrColumnInEqFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a string)");
            assertQuery("x where b = a")
                    .noLeakCheck()
                    .fails(12, "STRING constant expected");
        });
    }

    @Test
    public void testStrColumnInLtFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a string)");
            assertQuery("x where b < a")
                    .noLeakCheck()
                    .fails(12, "STRING constant expected");
        });
    }

    @Test
    public void testStrColumnInLtFilter2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b ipv4, a string)");
            assertQuery("x where b > a")
                    .noLeakCheck()
                    .fails(12, "STRING constant expected");
        });
    }

    @Test
    public void testStrIPv4Cast() throws Exception {
        assertQuery("select ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testStrIPv4CastInvalid() throws Exception {
        assertQuery("select ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testStrIPv4CastNull() throws Exception {
        assertQuery("select ipv4 '1.1.1.1'")
                .expectSize()
                .returns("""
                        cast
                        1.1.1.1
                        """);
    }

    @Test
    public void testStrIPv4CastOverflow() throws Exception {
        assertQuery("select ipv4 '256.5.5.5'")
                .fails(12, "invalid IPv4 constant");
    }

    @Test
    public void testTruncateIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select rnd_ipv4() ip, 1 count from long_sequence(20))");
            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
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
                            """);
            execute("truncate table test");
            assertQuery("test")
                    .noLeakCheck()
                    .returns("ip\tcount\n");
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

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            col1\tts\tcol2
                            187.139.150.80\t2022-02-24T00:00:00.000000Z\t187.139.150.80
                            18.206.96.238\t2022-02-24T00:00:01.000000Z\t18.206.96.238
                            92.80.211.65\t2022-02-24T00:00:02.000000Z\t92.80.211.65
                            212.159.205.29\t2022-02-24T00:00:03.000000Z\t212.159.205.29
                            4.98.173.21\t2022-02-24T00:00:04.000000Z\t4.98.173.21
                            """);
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

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            col1\tts\tcol2
                            187.139.150.80\t2022-02-24T00:00:00.000000Z\t187.139.150.80
                            18.206.96.238\t2022-02-24T00:00:01.000000Z\t18.206.96.238
                            92.80.211.65\t2022-02-24T00:00:02.000000Z\t92.80.211.65
                            212.159.205.29\t2022-02-24T00:00:03.000000Z\t212.159.205.29
                            4.98.173.21\t2022-02-24T00:00:04.000000Z\t4.98.173.21
                            """);
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

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            col1\tts
                            187.139.150.80\t2022-02-24T00:00:00.000000Z
                            18.206.96.238\t2022-02-24T00:00:01.000000Z
                            92.80.211.65\t2022-02-24T00:00:02.000000Z
                            212.159.205.29\t2022-02-24T00:00:03.000000Z
                            4.98.173.21\t2022-02-24T00:00:04.000000Z
                            """);
        });
    }

    @Test
    public void testWhereIPv4() throws Exception {
        assertQuery("select * from test where ip = ipv4 '0.0.0.1'")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(1,5,0)::ipv4 ip," +
                        "    rnd_int(0,1000,0) bytes," +
                        "    timestamp_sequence(0,100000000) k" +
                        "  from long_sequence(100)" +
                        ") timestamp(k)")
                .timestamp("k")
                .returns("""
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
                        """);
    }

    @Test
    public void testWhereIPv4Var() throws Exception {
        assertQuery("select * from test where ip1 = ip2")
                .ddl("create table test as " +
                        "(" +
                        "  select" +
                        "    rnd_int(0,9,0)::ipv4 ip1," +
                        "    rnd_int(0,9,0)::ipv4 ip2" +
                        "  from long_sequence(100)" +
                        ")")
                .returns("""
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
                        """);
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

            assertQuery("select * from test where ip = 'apple'")
                    .noLeakCheck()
                    .fails(0, "invalid IPv4 format");
        });
    }
}
