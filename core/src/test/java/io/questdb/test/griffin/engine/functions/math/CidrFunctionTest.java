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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CidrFunctionTest extends AbstractCairoTest {

    // ==================== cidr(string) tests ====================

    @Test
    public void testCidrFromString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            execute("insert into t values ('192.168.1.0/24')");
            // Just verify it returns a non-null long
            assertSql(
                    """
                            ok
                            true
                            """,
                    "select cidr(s) is not null as ok from t"
            );
        });
    }

    @Test
    public void testCidrFromStringNormalizesHostBits() throws Exception {
        assertMemoryLeak(() -> {
            // 192.168.1.100/24 should normalize to same value as 192.168.1.0/24
            execute("create table t (s1 string, s2 string)");
            execute("insert into t values ('192.168.1.0/24', '192.168.1.100/24')");
            assertSql(
                    """
                            same
                            true
                            """,
                    "select cidr(s1) = cidr(s2) as same from t"
            );
        });
    }

    @Test
    public void testCidrFromStringNull() throws Exception {
        assertSql(
                """
                        cidr
                        null
                        """,
                "select cidr(null::string)"
        );
    }

    @Test
    public void testCidrFromStringInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            execute("insert into t values ('invalid')");
            assertSql(
                    """
                            cidr
                            null
                            """,
                    "select cidr(s) from t"
            );
        });
    }

    @Test
    public void testCidrFromStringDifferentPrefixes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            execute("insert into t values ('10.0.0.0/8')");
            execute("insert into t values ('172.16.0.0/16')");
            execute("insert into t values ('192.168.1.0/24')");
            execute("insert into t values ('192.168.1.1/32')");
            // Verify all return non-null and different values
            assertSql(
                    """
                            cnt
                            4
                            """,
                    "select count(distinct cidr(s)) as cnt from t"
            );
        });
    }

    // ==================== cidr(varchar) tests ====================

    @Test
    public void testCidrFromVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (v varchar)");
            execute("insert into t values ('192.168.1.0/24'::varchar)");
            assertSql(
                    """
                            ok
                            true
                            """,
                    "select cidr(v) is not null as ok from t"
            );
        });
    }

    @Test
    public void testCidrFromVarcharNull() throws Exception {
        assertSql(
                """
                        cidr
                        null
                        """,
                "select cidr(null::varchar)"
        );
    }

    @Test
    public void testCidrStringAndVarcharSameResult() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string, v varchar)");
            execute("insert into t values ('192.168.1.0/24', '192.168.1.0/24'::varchar)");
            assertSql(
                    """
                            same
                            true
                            """,
                    "select cidr(s) = cidr(v) as same from t"
            );
        });
    }

    // ==================== cidr(long) - decode tests ====================

    @Test
    public void testCidrToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            execute("insert into t values ('192.168.1.0/24')");
            assertSql(
                    """
                            decoded
                            192.168.1.0/24
                            """,
                    "select cidr(cidr(s)) as decoded from t"
            );
        });
    }

    @Test
    public void testCidrToVarcharNull() throws Exception {
        assertSql(
                """
                        cidr
                        
                        """,
                "select cidr(null::long)"
        );
    }

    @Test
    public void testCidrRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            execute("insert into t values ('192.168.1.0/24')");
            assertSql(
                    """
                            original\tdecoded
                            192.168.1.0/24\t192.168.1.0/24
                            """,
                    "select s as original, cidr(cidr(s)) as decoded from t"
            );
        });
    }

    @Test
    public void testCidrRoundTripVariousPrefixes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            execute("insert into t values ('10.0.0.0/8')");
            execute("insert into t values ('172.16.0.0/16')");
            execute("insert into t values ('192.168.1.0/24')");
            execute("insert into t values ('192.168.1.1/32')");
            assertSql(
                    """
                            s\tdecoded
                            10.0.0.0/8\t10.0.0.0/8
                            172.16.0.0/16\t172.16.0.0/16
                            192.168.1.0/24\t192.168.1.0/24
                            192.168.1.1/32\t192.168.1.1/32
                            """,
                    "select s, cidr(cidr(s)) as decoded from t"
            );
        });
    }

    // ==================== cidr_contains tests ====================

    @Test
    public void testCidrContainsTrue() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (network string, ip ipv4)");
            execute("insert into t values ('192.168.1.0/24', '192.168.1.100'::ipv4)");
            assertSql(
                    """
                            cidr_contains
                            true
                            """,
                    "select cidr_contains(cidr(network), ip) from t"
            );
        });
    }

    @Test
    public void testCidrContainsFalse() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (network string, ip ipv4)");
            execute("insert into t values ('192.168.1.0/24', '192.168.2.1'::ipv4)");
            assertSql(
                    """
                            cidr_contains
                            false
                            """,
                    "select cidr_contains(cidr(network), ip) from t"
            );
        });
    }

    @Test
    public void testCidrContainsNetworkBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (network string, ip ipv4)");
            execute("insert into t values ('192.168.1.0/24', '192.168.1.0'::ipv4)");
            execute("insert into t values ('192.168.1.0/24', '192.168.1.255'::ipv4)");
            execute("insert into t values ('192.168.1.0/24', '192.168.2.0'::ipv4)");
            assertSql(
                    """
                            ip\tcontains
                            192.168.1.0\ttrue
                            192.168.1.255\ttrue
                            192.168.2.0\tfalse
                            """,
                    "select ip, cidr_contains(cidr(network), ip) as contains from t"
            );
        });
    }

    @Test
    public void testCidrContainsNullCidr() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ip ipv4)");
            execute("insert into t values ('192.168.1.1'::ipv4)");
            assertSql(
                    """
                            cidr_contains
                            false
                            """,
                    "select cidr_contains(null::long, ip) from t"
            );
        });
    }

    @Test
    public void testCidrContainsNullIpv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (network string)");
            execute("insert into t values ('192.168.1.0/24')");
            assertSql(
                    """
                            cidr_contains
                            false
                            """,
                    "select cidr_contains(cidr(network), null::ipv4) from t"
            );
        });
    }

    @Test
    public void testCidrContainsSlash32() throws Exception {
        assertMemoryLeak(() -> {
            // /32 should only match exact IP
            execute("create table t (network string, ip ipv4)");
            execute("insert into t values ('192.168.1.1/32', '192.168.1.1'::ipv4)");
            execute("insert into t values ('192.168.1.1/32', '192.168.1.2'::ipv4)");
            assertSql(
                    """
                            ip\tcontains
                            192.168.1.1\ttrue
                            192.168.1.2\tfalse
                            """,
                    "select ip, cidr_contains(cidr(network), ip) as contains from t"
            );
        });
    }

    @Test
    public void testCidrContainsSlash0() throws Exception {
        assertMemoryLeak(() -> {
            // /0 should match any IP
            execute("create table t (network string, ip ipv4)");
            execute("insert into t values ('0.0.0.0/0', '192.168.1.1'::ipv4)");
            execute("insert into t values ('0.0.0.0/0', '10.0.0.1'::ipv4)");
            assertSql(
                    """
                            ip\tcontains
                            192.168.1.1\ttrue
                            10.0.0.1\ttrue
                            """,
                    "select ip, cidr_contains(cidr(network), ip) as contains from t"
            );
        });
    }

    // ==================== rnd_cidr tests ====================

    @Test
    public void testRndCidr() throws Exception {
        assertSql(
                """
                        count
                        10
                        """,
                "select count(*) from (select rnd_cidr() from long_sequence(10))"
        );
    }

    @Test
    public void testRndCidrProducesValidCidrs() throws Exception {
        // Verify that rnd_cidr produces values that can be decoded back to valid CIDR strings
        assertSql(
                """
                        count
                        100
                        """,
                "select count(*) from (select cidr(rnd_cidr()) as c from long_sequence(100)) where c is not null"
        );
    }

    // ==================== Table tests ====================

    @Test
    public void testCidrWithTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name string, network string)");
            execute("insert into networks values ('office', '192.168.1.0/24')");
            execute("insert into networks values ('datacenter', '10.0.0.0/8')");
            execute("insert into networks values ('vpn', '172.16.0.0/16')");

            assertSql(
                    """
                            name\tcidr
                            office\t192.168.1.0/24
                            datacenter\t10.0.0.0/8
                            vpn\t172.16.0.0/16
                            """,
                    "select name, cidr(cidr(network)) from networks"
            );
        });
    }

    @Test
    public void testCidrContainsWithTableColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table hosts (ip ipv4)");
            execute("insert into hosts values ('192.168.1.1'::ipv4)");
            execute("insert into hosts values ('192.168.1.100'::ipv4)");
            execute("insert into hosts values ('10.0.0.1'::ipv4)");
            execute("insert into hosts values ('172.16.5.10'::ipv4)");

            assertSql(
                    """
                            ip\tin_office
                            192.168.1.1\ttrue
                            192.168.1.100\ttrue
                            10.0.0.1\tfalse
                            172.16.5.10\tfalse
                            """,
                    "select ip, cidr_contains(cidr('192.168.1.0/24'), ip) as in_office from hosts"
            );
        });
    }

    // ==================== Join tests ====================

    @Test
    public void testCidrContainsCrossJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, cidr_encoded long)");
            execute("insert into networks values ('office', cidr('192.168.1.0/24'))");
            execute("insert into networks values ('datacenter', cidr('10.0.0.0/8'))");

            execute("create table hosts (hostname symbol, ip ipv4)");
            execute("insert into hosts values ('web1', '192.168.1.10'::ipv4)");
            execute("insert into hosts values ('web2', '192.168.1.20'::ipv4)");
            execute("insert into hosts values ('db1', '10.1.2.3'::ipv4)");
            execute("insert into hosts values ('external', '8.8.8.8'::ipv4)");

            assertSql(
                    """
                            hostname\tip\tname
                            db1\t10.1.2.3\tdatacenter
                            web1\t192.168.1.10\toffice
                            web2\t192.168.1.20\toffice
                            """,
                    "select h.hostname, h.ip, n.name " +
                            "from hosts h " +
                            "cross join networks n " +
                            "where cidr_contains(n.cidr_encoded, h.ip) " +
                            "order by h.hostname"
            );
        });
    }

    @Test
    public void testCidrContainsLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, cidr_encoded long)");
            execute("insert into networks values ('office', cidr('192.168.1.0/24'))");
            execute("insert into networks values ('datacenter', cidr('10.0.0.0/8'))");

            execute("create table hosts (hostname symbol, ip ipv4)");
            execute("insert into hosts values ('web1', '192.168.1.10'::ipv4)");
            execute("insert into hosts values ('db1', '10.1.2.3'::ipv4)");
            execute("insert into hosts values ('external', '8.8.8.8'::ipv4)");

            // Find all hosts and their matching networks (if any)
            assertSql(
                    """
                            hostname\tip\tname
                            db1\t10.1.2.3\tdatacenter
                            external\t8.8.8.8\t
                            web1\t192.168.1.10\toffice
                            """,
                    "select h.hostname, h.ip, n.name " +
                            "from hosts h " +
                            "left join networks n on cidr_contains(n.cidr_encoded, h.ip) " +
                            "order by h.hostname"
            );
        });
    }

    @Test
    public void testCidrContainsWithRandomData() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with random CIDRs
            execute("create table networks as (select rnd_cidr() as cidr_encoded, x as id from long_sequence(5))");

            // Create table with random IPs
            execute("create table hosts as (select rnd_ipv4() as ip, x as id from long_sequence(20))");

            // This should execute without error - we're just testing the join works
            // The count will vary based on random data, so we just check it returns >= 0
            assertSql(
                    """
                            ok
                            true
                            """,
                    "select count(*) >= 0 as ok " +
                            "from hosts h " +
                            "cross join networks n " +
                            "where cidr_contains(n.cidr_encoded, h.ip)"
            );
        });
    }

    @Test
    public void testCidrContainsMultipleNetworksPerHost() throws Exception {
        assertMemoryLeak(() -> {
            // Create overlapping networks
            execute("create table networks (name symbol, cidr_encoded long)");
            execute("insert into networks values ('class_a', cidr('10.0.0.0/8'))");
            execute("insert into networks values ('class_b', cidr('10.1.0.0/16'))");
            execute("insert into networks values ('class_c', cidr('10.1.2.0/24'))");

            execute("create table hosts (hostname symbol, ip ipv4)");
            execute("insert into hosts values ('server1', '10.1.2.5'::ipv4)");

            // server1 should match all three networks
            assertSql(
                    """
                            hostname\tname
                            server1\tclass_a
                            server1\tclass_b
                            server1\tclass_c
                            """,
                    "select h.hostname, n.name " +
                            "from hosts h " +
                            "cross join networks n " +
                            "where cidr_contains(n.cidr_encoded, h.ip) " +
                            "order by n.name"
            );
        });
    }

    @Test
    public void testCidrGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, cidr_encoded long)");
            execute("insert into networks values ('office', cidr('192.168.1.0/24'))");
            execute("insert into networks values ('datacenter', cidr('10.0.0.0/8'))");

            execute("create table hosts (hostname symbol, ip ipv4)");
            execute("insert into hosts values ('web1', '192.168.1.10'::ipv4)");
            execute("insert into hosts values ('web2', '192.168.1.20'::ipv4)");
            execute("insert into hosts values ('web3', '192.168.1.30'::ipv4)");
            execute("insert into hosts values ('db1', '10.1.2.3'::ipv4)");
            execute("insert into hosts values ('db2', '10.2.3.4'::ipv4)");

            // Count hosts per network
            assertSql(
                    """
                            name\thost_count
                            datacenter\t2
                            office\t3
                            """,
                    "select n.name, count(*) as host_count " +
                            "from hosts h " +
                            "cross join networks n " +
                            "where cidr_contains(n.cidr_encoded, h.ip) " +
                            "group by n.name " +
                            "order by n.name"
            );
        });
    }

    // ==================== EXPLAIN / toPlan tests ====================

    @Test
    public void testExplainCidrFromString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            assertPlanNoLeakCheck(
                    "select cidr(s) from t",
                    """
                            VirtualRecord
                              functions: [cidr(s)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainCidrFromVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (v varchar)");
            assertPlanNoLeakCheck(
                    "select cidr(v) from t",
                    """
                            VirtualRecord
                              functions: [cidr(v)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainCidrFromLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (l long)");
            assertPlanNoLeakCheck(
                    "select cidr(l) from t",
                    """
                            VirtualRecord
                              functions: [cidr(l)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainCidrContains() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (cidr_val long, ip ipv4)");
            assertPlanNoLeakCheck(
                    "select cidr_contains(cidr_val, ip) from t",
                    """
                            VirtualRecord
                              functions: [cidr_contains(cidr_val,ip)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testExplainCidrContainsInFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (cidr_val long)");
            execute("create table hosts (ip ipv4)");
            assertPlanNoLeakCheck(
                    "select * from hosts h cross join networks n where cidr_contains(n.cidr_val, h.ip)",
                    """
                            SelectedRecord
                                Filter filter: cidr_contains(n.cidr_val,h.ip)
                                    Cross Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: hosts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: networks
                            """
            );
        });
    }

    @Test
    public void testExplainRndCidr() throws Exception {
        assertPlanNoLeakCheck(
                "select rnd_cidr() from long_sequence(1)",
                """
                        VirtualRecord
                          functions: [memoize(rnd_cidr())]
                            long_sequence count: 1
                        """
        );
    }

    @Test
    public void testExplainNestedCidrFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s string)");
            assertPlanNoLeakCheck(
                    "select cidr(cidr(s)) from t",
                    """
                            VirtualRecord
                              functions: [cidr(cidr(s))]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    // ==================== ORDER BY / sorting tests ====================
    // These tests focus on the proper use case: storing CIDR as encoded long

    @Test
    public void testOrderByCidrEncodedAsc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, network long)");
            execute("insert into networks values ('net_c', cidr('192.168.1.0/24'))");
            execute("insert into networks values ('net_a', cidr('10.0.0.0/8'))");
            execute("insert into networks values ('net_b', cidr('172.16.0.0/16'))");

            // Sorting by encoded CIDR (long) - network address is the primary sort key
            // 10.x < 172.x < 192.x
            assertSql(
                    """
                            name\tcidr
                            net_a\t10.0.0.0/8
                            net_b\t172.16.0.0/16
                            net_c\t192.168.1.0/24
                            """,
                    "select name, cidr(network) as cidr from networks order by network asc"
            );
        });
    }

    @Test
    public void testOrderByCidrEncodedDesc() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, network long)");
            execute("insert into networks values ('net_c', cidr('192.168.1.0/24'))");
            execute("insert into networks values ('net_a', cidr('10.0.0.0/8'))");
            execute("insert into networks values ('net_b', cidr('172.16.0.0/16'))");

            assertSql(
                    """
                            name\tcidr
                            net_c\t192.168.1.0/24
                            net_b\t172.16.0.0/16
                            net_a\t10.0.0.0/8
                            """,
                    "select name, cidr(network) as cidr from networks order by network desc"
            );
        });
    }

    @Test
    public void testOrderByCidrSameNetworkDifferentPrefix() throws Exception {
        assertMemoryLeak(() -> {
            // Use different base addresses so normalization produces different network addresses
            // 10.0.0.0/8 -> 10.0.0.0, 10.1.0.0/16 -> 10.1.0.0, 10.1.1.0/24 -> 10.1.1.0
            execute("create table networks (name symbol, network long)");
            execute("insert into networks values ('net_24', cidr('10.1.1.0/24'))");
            execute("insert into networks values ('net_8', cidr('10.0.0.0/8'))");
            execute("insert into networks values ('net_16', cidr('10.1.0.0/16'))");

            // Sorted by encoded long value (address << 6 | prefix)
            // 10.0.0.0/8 < 10.1.0.0/16 < 10.1.1.0/24
            assertSql(
                    """
                            name\tcidr
                            net_8\t10.0.0.0/8
                            net_16\t10.1.0.0/16
                            net_24\t10.1.1.0/24
                            """,
                    "select name, cidr(network) as cidr from networks order by network asc"
            );
        });
    }

    @Test
    public void testOrderByCidrSameAddressDifferentPrefix() throws Exception {
        assertMemoryLeak(() -> {
            // Same normalized address (10.0.0.0), different prefixes
            // All normalize to 10.0.0.0 because host bits are zeroed
            execute("create table networks (name symbol, network long)");
            execute("insert into networks values ('net_24', cidr('10.0.0.0/24'))");
            execute("insert into networks values ('net_8', cidr('10.0.0.0/8'))");
            execute("insert into networks values ('net_16', cidr('10.0.0.0/16'))");

            // Same address, sorted by prefix: /8 < /16 < /24
            // Encoding is (addr << 6) | prefix, so smaller prefix = smaller encoded value
            assertSql(
                    """
                            name\tcidr
                            net_8\t10.0.0.0/8
                            net_16\t10.0.0.0/16
                            net_24\t10.0.0.0/24
                            """,
                    "select name, cidr(network) as cidr from networks order by network asc"
            );
        });
    }

    @Test
    public void testOrderByCidrWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, network long)");
            execute("insert into networks values ('net_b', cidr('172.16.0.0/16'))");
            execute("insert into networks values ('net_null', null)");
            execute("insert into networks values ('net_a', cidr('10.0.0.0/8'))");

            // NULLs (LONG_NULL = Long.MIN_VALUE) sort first in ASC order for long type
            assertSql(
                    """
                            name\tcidr
                            net_null\t
                            net_a\t10.0.0.0/8
                            net_b\t172.16.0.0/16
                            """,
                    "select name, cidr(network) as cidr from networks order by network asc"
            );
        });
    }

    @Test
    public void testOrderByCidrInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table networks (name symbol, network long)");
            execute("insert into networks values ('net_c', cidr('192.168.1.0/24'))");
            execute("insert into networks values ('net_a', cidr('10.0.0.0/8'))");
            execute("insert into networks values ('net_b', cidr('172.16.0.0/16'))");

            assertSql(
                    """
                            name\tcidr
                            net_a\t10.0.0.0/8
                            net_b\t172.16.0.0/16
                            net_c\t192.168.1.0/24
                            """,
                    "select * from (select name, cidr(network) as cidr from networks order by network)"
            );
        });
    }

    @Test
    public void testGroupByNetworkWithOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table logs (network long, event_count int)");
            execute("insert into logs values (cidr('192.168.1.0/24'), 10)");
            execute("insert into logs values (cidr('10.0.0.0/8'), 5)");
            execute("insert into logs values (cidr('192.168.1.0/24'), 15)");
            execute("insert into logs values (cidr('172.16.0.0/16'), 8)");
            execute("insert into logs values (cidr('10.0.0.0/8'), 3)");

            // Group by long network column, order by long network column, display decoded CIDR
            assertSql(
                    """
                            cidr\ttotal
                            10.0.0.0/8\t8
                            172.16.0.0/16\t8
                            192.168.1.0/24\t25
                            """,
                    "select cidr(network) as cidr, sum(event_count) as total " +
                            "from logs " +
                            "group by network " +
                            "order by network"
            );
        });
    }

    @Test
    public void testCidrJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cidrs AS (SELECT rnd_double() freq, rnd_cidr() AS cidr FROM long_sequence(10000));");
            execute("CREATE TABLE ips AS (SELECT rnd_double() lat, rnd_double() lon, rnd_ipv4() AS ip FROM long_sequence(100));");

            assertQuery("""
                            lon	lat	freq	ip	cidr
                            0.34298120390667264	0.4165402862519392	0.6809728959613008	128.72.44.9	128.0.0.0/8
                            0.34298120390667264	0.4165402862519392	0.8091102688627468	128.72.44.9	128.0.0.0/8
                            0.34298120390667264	0.4165402862519392	0.486440482380041	128.72.44.9	128.0.0.0/8
                            0.7893957383033777	0.97574041992045	0.9295560391436746	169.54.169.67	169.0.0.0/8
                            0.3541108022289895	0.7221178377273948	0.8291244749282449	21.98.144.47	21.0.0.0/8
                            0.3541108022289895	0.7221178377273948	0.8738285191383893	21.98.144.47	21.0.0.0/8
                            0.7902335845194232	0.43283257070787595	0.33534286623257725	246.68.73.139	246.0.0.0/8
                            0.7902335845194232	0.43283257070787595	0.4801327005909507	246.68.73.139	246.0.0.0/8
                            0.8790325075418108	0.4388805566551185	0.17727653217821715	64.25.213.23	64.0.0.0/8
                            0.7733662151732446	0.2813486835440798	0.7150196400197047	247.8.9.42	247.0.0.0/8
                            0.2991711626309773	0.08054374395821295	0.595239515706855	66.245.127.102	66.128.0.0/9
                            0.2991711626309773	0.08054374395821295	0.599576831467573	66.245.127.102	66.128.0.0/9
                            0.8858701868424244	0.6967864665056102	0.6998532253417439	75.95.196.53	75.0.0.0/8
                            0.38063728360040183	0.3171251168842145	0.1588921769771906	36.164.6.1	36.0.0.0/8
                            0.38063728360040183	0.3171251168842145	0.938578034256313	36.164.6.1	36.0.0.0/8
                            0.38063728360040183	0.3171251168842145	0.7434178702877948	36.164.6.1	36.0.0.0/8
                            0.38063728360040183	0.3171251168842145	0.14806568853298208	36.164.6.1	36.0.0.0/8
                            0.7716316825119158	0.7330211553394504	0.24811364788551915	212.250.62.164	212.128.0.0/9
                            0.7716316825119158	0.7330211553394504	0.7482617469643522	212.250.62.164	212.128.0.0/9
                            0.5982378164070223	0.3110081476737323	0.13487506431018292	158.20.5.79	158.0.0.0/10
                            0.833768300611128	0.9958165702587816	0.10487400122897084	116.128.219.229	116.0.0.0/8
                            0.13739013223184138	0.07953793946566123	0.65916173119594	140.19.22.36	140.0.0.0/8
                            0.13739013223184138	0.07953793946566123	0.1783580856629664	140.19.22.36	140.0.0.0/8
                            0.6861509600029287	0.6195628551544847	0.7323913822706717	32.226.210.230	32.0.0.0/8
                            0.6861509600029287	0.6195628551544847	0.1914083998752949	32.226.210.230	32.0.0.0/8
                            0.9424977704548868	0.17085730139169952	0.2472550192502918	166.83.32.165	166.0.0.0/8
                            0.9424977704548868	0.17085730139169952	0.05527730335543868	166.83.32.165	166.0.0.0/8
                            0.9424977704548868	0.17085730139169952	0.06746067395870348	166.83.32.165	166.0.0.0/8
                            0.12007048966691614	0.18615996720772676	0.4677219994415258	237.252.88.7	237.0.0.0/8
                            0.12007048966691614	0.18615996720772676	0.200858508361334	237.252.88.7	237.0.0.0/8
                            0.12007048966691614	0.18615996720772676	0.9534844124580377	237.252.88.7	237.0.0.0/8
                            0.5462850544839577	0.8282999933593382	0.028230014661173808	8.233.87.213	8.128.0.0/9
                            0.5462850544839577	0.8282999933593382	0.6651733155025821	8.233.87.213	8.128.0.0/9
                            0.5462850544839577	0.8282999933593382	0.11371841836123953	8.233.87.213	8.128.0.0/9
                            0.8803759780206643	0.18381132240000198	0.998906104811058	138.197.92.127	138.0.0.0/8
                            0.5193693900360402	0.3029586144044665	0.9056569683912397	51.253.164.181	51.0.0.0/8
                            0.34315042380820804	0.2740491634559663	0.9285953102404939	245.252.44.29	245.0.0.0/8
                            0.6086137940548573	0.2842182954338869	0.4679239248658552	24.77.137.145	24.0.0.0/8
                            0.6086137940548573	0.2842182954338869	0.37813089076951223	24.77.137.145	24.0.0.0/8
                            0.0322370272963618	0.8538189951320124	0.7894691816624596	210.234.27.150	210.0.0.0/8
                            0.0322370272963618	0.8538189951320124	0.40272050740167153	210.234.27.150	210.0.0.0/8
                            0.0322370272963618	0.8538189951320124	0.3677307979802963	210.234.27.150	210.0.0.0/8
                            0.5373616742596297	0.40893313015643906	0.04776893358286183	172.60.49.66	172.0.0.0/8
                            0.6478649874580092	0.8845125352850193	0.8778092838835108	33.131.13.61	33.0.0.0/8
                            0.6478649874580092	0.8845125352850193	0.22723843512267772	33.131.13.61	33.0.0.0/8
                            0.6478649874580092	0.8845125352850193	0.11073061704549947	33.131.13.61	33.0.0.0/8
                            0.6478649874580092	0.8845125352850193	0.6149108167651209	33.131.13.61	33.0.0.0/8
                            0.2948369526824167	0.28902773467426446	0.17727653217821715	64.14.95.88	64.0.0.0/8
                            0.46546595409973957	0.7727911519438595	0.4769773127402621	227.126.101.109	227.0.0.0/8
                            0.46546595409973957	0.7727911519438595	0.588121196265547	227.126.101.109	227.0.0.0/8
                            0.46546595409973957	0.7727911519438595	0.6195921739922231	227.126.101.109	227.0.0.0/8
                            0.7252573129914128	0.49057437395908643	0.06900656203994782	182.193.254.44	182.0.0.0/8
                            0.7252573129914128	0.49057437395908643	0.907542027828362	182.193.254.44	182.0.0.0/8
                            0.30163699668392707	0.34120267856286357	0.8454641560049379	190.153.183.240	190.0.0.0/8
                            0.7587643835816594	0.7831204153588878	0.35586048103803125	45.92.125.206	45.0.0.0/8
                            0.9486165364123571	0.37693007482657603	0.16375704111274292	63.199.14.123	63.0.0.0/8
                            0.9486165364123571	0.37693007482657603	0.3326063461140232	63.199.14.123	63.0.0.0/8
                            0.9486165364123571	0.37693007482657603	0.20811293287008847	63.199.14.123	63.0.0.0/8
                            0.983391044295884	0.8164682707049379	0.37012510719993164	67.225.211.20	67.0.0.0/8
                            0.983391044295884	0.8164682707049379	0.579930079916402	67.225.211.20	67.0.0.0/8
                            0.983391044295884	0.8164682707049379	0.9973256211110638	67.225.211.20	67.0.0.0/8
                            0.9406566999388782	0.7527031842126555	0.05996446334622707	1.175.1.249	1.0.0.0/8
                            0.9406566999388782	0.7527031842126555	0.8614735977479013	1.175.1.249	1.0.0.0/8
                            0.9406566999388782	0.7527031842126555	0.680557802449044	1.175.1.249	1.0.0.0/8
                            0.817842783385994	0.5870454520663738	0.8024883386746	131.251.148.11	131.192.0.0/10
                            0.19798975120690754	0.5415040778621206	0.28161213163414156	103.23.54.120	103.0.0.0/10
                            0.414745203151222	0.0949910554198149	0.7095702008326192	73.55.27.171	73.0.0.0/8
                            0.414745203151222	0.0949910554198149	0.1141357892133612	73.55.27.171	73.0.0.0/8
                            0.414745203151222	0.0949910554198149	0.28053790406505974	73.55.27.171	73.0.0.0/8
                            0.8480617356780241	0.4359968934616172	0.671983579113862	194.46.225.183	194.0.0.0/10
                            0.571008510587697	0.13414359249191476	0.06250568098487508	174.191.133.162	174.128.0.0/10
                            0.28830224204911414	0.023021802090365306	0.3520739292115457	106.129.215.134	106.0.0.0/8
                            0.28830224204911414	0.023021802090365306	0.998148150955372	106.129.215.134	106.0.0.0/8
                            0.28830224204911414	0.023021802090365306	0.38336531513189365	106.129.215.134	106.0.0.0/8
                            0.7457049688920759	0.4933359126917871	0.3372177519509181	149.188.233.86	149.0.0.0/8
                            0.7457049688920759	0.4933359126917871	0.12797209488048755	149.188.233.86	149.0.0.0/8
                            0.7457049688920759	0.4933359126917871	0.12994379117198285	149.188.233.86	149.0.0.0/8
                            0.5882880380468102	0.672465013425437	0.1426331629349119	218.248.85.76	218.0.0.0/8
                            0.5882880380468102	0.672465013425437	0.6727222089486165	218.248.85.76	218.0.0.0/8
                            0.5882880380468102	0.672465013425437	0.2778836025870849	218.248.85.76	218.0.0.0/8
                            0.5882880380468102	0.672465013425437	0.7160803712410734	218.248.85.76	218.0.0.0/8
                            0.7052742173839188	0.13986629691625085	0.020930112150893132	191.2.45.21	191.0.0.0/8
                            0.7052742173839188	0.13986629691625085	0.08078637565430047	191.2.45.21	191.0.0.0/8
                            0.15298907061285982	0.4782215723464298	0.458490692099471	90.30.204.0	90.0.0.0/8
                            0.15298907061285982	0.4782215723464298	0.7734684419481714	90.30.204.0	90.0.0.0/8
                            0.6959285807702064	0.8584102518858648	0.9733285028556575	198.150.219.131	198.0.0.0/8
                            0.5584036909528464	0.5365992152879417	0.8036323981337526	251.83.110.112	251.0.0.0/8
                            0.5584036909528464	0.5365992152879417	0.20743538048301713	251.83.110.112	251.0.0.0/8
                            0.5584036909528464	0.5365992152879417	0.0575533974165352	251.83.110.112	251.0.0.0/8
                            0.691696231633519	0.46180290410147695	0.4677219994415258	237.205.181.187	237.0.0.0/8
                            0.691696231633519	0.46180290410147695	0.200858508361334	237.205.181.187	237.0.0.0/8
                            0.691696231633519	0.46180290410147695	0.9534844124580377	237.205.181.187	237.0.0.0/8
                            0.4529942379479097	0.5398931918325383	0.7535126754253635	124.184.91.156	124.0.0.0/8
                            0.6537765482262283	0.6231739521665668	0.7646165585746133	6.202.49.8	6.0.0.0/8
                            0.6790501599943466	0.5509259210693381	0.00377067516496421	234.248.180.18	234.128.0.0/9
                            0.6790501599943466	0.5509259210693381	0.2940437467379643	234.248.180.18	234.128.0.0/9
                            0.6790501599943466	0.5509259210693381	0.24121418835040842	234.248.180.18	234.128.0.0/9
                            0.7108697902640292	0.22253253754106705	0.5537568299882846	146.92.56.170	146.0.0.0/8
                            0.821901761270375	0.8741951249014949	0.8291244749282449	21.33.201.31	21.0.0.0/8
                            0.821901761270375	0.8741951249014949	0.8738285191383893	21.33.201.31	21.0.0.0/8
                            """,
                    "select lon, lat, freq, ip, cidr(cidr) cidr from ips join cidrs on (cidr_contains(cidr, ip)) limit 100",
                    false,
                    true
            );
        });
    }
}
