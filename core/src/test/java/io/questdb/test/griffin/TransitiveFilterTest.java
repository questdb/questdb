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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TransitiveFilterTest extends AbstractCairoTest {
    @Test
    public void testConstantOrientationAndRuntimeRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsAndPlan(
                    query("l.k=1"),
                    "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """
            );
            assertRowsAndPlan(
                    query("1=l.k"),
                    "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: 1=k
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """
            );
            assertRowsAndPlan(
                    query("l.k=CAST(1 AS INT)"),
                    "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async Filter workers: 1
                                          filter: k=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """
            );
            assertRowsAndPlan(
                    query("l.k=2-1"),
                    "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """
            );
            bindVariableService.setInt(0, 1);
            for (String condition : new String[]{"l.k=$1", "$1=l.k", "l.k=CAST($1 AS INT)"}) {
                final String sql = query(condition);
                try (RecordCursorFactory factory = select(sql)) {
                    for (int value : new int[]{1, 2, Numbers.INT_NULL, 1}) {
                        bindVariableService.setInt(0, value);
                        final String rows = value == 1 ? "lid\trid\n1\t11\n"
                                : value == 2 ? "lid\trid\n2\t12\n" : "lid\trid\n4\t14\n";
                        assertRowsOnly(factory, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testMixedTypeJoinKeysTakeMasterKeyFacts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_trans_m(k STRING,ks SYMBOL,ts TIMESTAMP)");
            execute("CREATE TABLE lp_trans_s(k SYMBOL INDEX,ks STRING,tn TIMESTAMP_NS,v LONG)");
            execute("INSERT INTO lp_trans_m VALUES ('hit','hit','2024-01-01T00:00:00.000001Z'),('absent','absent','2024-01-02T00:00:00Z')");
            execute("INSERT INTO lp_trans_s VALUES ('hit','hit','2024-01-01T00:00:00.000001Z',1),('miss','miss','2024-01-03T00:00:00Z',2)");
            assertRowsAndPlan("SELECT b.v FROM lp_trans_m a JOIN lp_trans_s b ON a.k=b.k WHERE a.k='hit'",
                    "v\n1\n",
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: b.k=a.k
                                    Async Filter workers: 1
                                      filter: k='hit'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_m
                                    Hash
                                        DeferredSingleSymbolFilterPageFrame
                                            Index forward scan on: k deferred: true
                                              filter: k='hit'
                                            Frame forward scan on: lp_trans_s
                            """);
            assertRowsAndPlan("SELECT b.v FROM lp_trans_m a JOIN lp_trans_s b ON a.ks=b.ks WHERE a.ks='hit'",
                    "v\n1\n",
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: b.ks=a.ks
                                    Async JIT Filter workers: 1
                                      filter: ks='hit'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_m
                                    Hash
                                        Async Filter workers: 1
                                          filter: ks='hit'
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_s
                            """);
            assertRowsAndPlan("SELECT b.v FROM lp_trans_m a JOIN lp_trans_s b ON a.ts=b.tn WHERE a.ts='2024-01-01T00:00:00.000001Z'",
                    "v\n1\n",
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: b.tn=a.ts
                                    Async JIT Filter workers: 1
                                      filter: 2024-01-01T00:00:00.000001Z=ts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_m
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: 2024-01-01T00:00:00.000001000Z=tn
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_s
                            """);
            execute("CREATE TABLE lp_trans_n(ts TIMESTAMP_NS,v INT)");
            execute("INSERT INTO lp_trans_n VALUES ('2024-01-01T00:00:00.000000001Z',1)");
            execute("CREATE TABLE lp_trans_u(ts TIMESTAMP,v INT)");
            execute("INSERT INTO lp_trans_u VALUES ('2024-01-01T00:00:00Z',2)");
            assertRowsAndPlan("SELECT b.v FROM lp_trans_n a JOIN lp_trans_u b ON a.ts=b.ts WHERE a.ts='2024-01-01T00:00:00.000000001Z'::TIMESTAMP_NS",
                    "v\n",
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: b.ts=a.ts
                                    Async Filter workers: 1
                                      filter: 2024-01-01T00:00:00.000000001Z=ts
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_n
                                    Hash
                                        Empty table
                            """);
        });
    }

    @Test
    public void testOnlyOriginalFactsPropagateAndSlaveFactsDoNotReverse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsAndPlan(query("r.k=1"), "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan(query("l.k=abs(1)"), "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async Filter workers: 1
                                          filter: k=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan(query("l.k=null"), "lid\trid\n4\t14\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan(query("l.k IS NULL"), "lid\trid\n4\t14\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan(query("l.k = NULL"), "lid\trid\n4\t14\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan(query("l.k=null::INT"), "lid\trid\n4\t14\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async Filter workers: 1
                                          filter: k=null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan("SELECT l.id lid,r.id rid FROM (SELECT id,k FROM lp_trans_l WHERE k=1) l "
                            + "JOIN lp_trans_r r ON l.k=r.k ORDER BY lid,rid", "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan("SELECT l.id lid,r.id rid,x.id xid FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k JOIN lp_trans_l x ON r.id=x.id+10 "
                            + "WHERE l.k=1 ORDER BY lid,rid,xid", "lid\trid\txid\n1\t11\t1\n",
                    """
                            Encode sort
                              keys: [lid, rid, xid]
                                SelectedRecord
                                    Filter filter: r.id=x.id+10
                                        Cross Join
                                            Hash Join Light
                                              condition: r.k=l.k
                                                Async JIT Filter workers: 1
                                                  filter: k=1
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_l
                                                Hash
                                                    Async JIT Filter workers: 1
                                                      filter: k=1
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: lp_trans_r
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                            """);
        });
    }

    @Test
    public void testOriginalFactsFollowAlignedJoinKeys() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsAndPlan("SELECT l.id lid,r.id rid,x.id xid FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k JOIN lp_trans_l x ON r.k=x.k "
                            + "WHERE l.k=1 ORDER BY lid,rid,xid", "lid\trid\txid\n1\t11\t1\n",
                    """
                            Encode sort
                              keys: [lid, rid, xid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: x.k=r.k
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_l
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k=1
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_r
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                            """);
            assertRowsOnly("SELECT l.id lid,r.id rid,x.id xid FROM lp_trans_l l "
                    + "JOIN lp_trans_r r ON r.k=x.k JOIN lp_trans_l x ON l.k=x.k "
                    + "WHERE l.k=1 ORDER BY lid,rid,xid", "lid\trid\txid\n1\t11\t1\n");
            assertRowsAndPlan("SELECT l.id lid,r.id rid,x.id xid FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k JOIN lp_trans_l x ON r.k=x.k "
                            + "WHERE r.k=1 ORDER BY lid,rid,xid", "lid\trid\txid\n1\t11\t1\n",
                    """
                            Encode sort
                              keys: [lid, rid, xid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: x.k=r.k
                                        Hash Join Light
                                          condition: r.k=l.k
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k=1
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_r
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_l
                            """);
        });
    }

    @Test
    public void testWhereFactWinsOverOnAndLastFactWinsWithinOrigin() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRowsAndPlan("SELECT l.id lid,r.id rid FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k AND l.k=2 WHERE l.k=1 ORDER BY lid,rid", "lid\trid\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: (k=2 and k=1)
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan("SELECT l.id lid,r.id rid FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k AND l.k=1 AND l.k=2 ORDER BY lid,rid", "lid\trid\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: (k=1 and k=2)
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=2
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan(query("l.k=1 AND l.k=2"), "lid\trid\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: (k=1 and k=2)
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=2
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan("SELECT l.id lid,r.id rid,x.id xid FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k AND l.k=2 JOIN lp_trans_l x ON r.k=x.k "
                            + "WHERE l.k=1 ORDER BY lid,rid,xid", "lid\trid\txid\n",
                    """
                            Encode sort
                              keys: [lid, rid, xid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: x.k=r.k
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async JIT Filter workers: 1
                                              filter: (k=2 and k=1)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_l
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k=1
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_r
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                            """);
        });
    }

    @Test
    public void testRegexFactsAndMixedSignatureEqualityOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (String type : new String[]{"STRING", "SYMBOL"}) {
                execute("CREATE TABLE lp_trans_text_l_" + type + "(id INT,k " + type + ")");
                execute("CREATE TABLE lp_trans_text_r_" + type + "(id INT,k " + type + ")");
                execute("INSERT INTO lp_trans_text_l_" + type + " VALUES (1,'aa'),(2,'bb'),(3,null)");
                execute("INSERT INTO lp_trans_text_r_" + type + " VALUES (11,'aa'),(12,'bb'),(13,null)");
            }
            {
                final String prefix = "SELECT l.id lid,r.id rid FROM lp_trans_text_l_STRING l JOIN lp_trans_text_r_STRING r ON l.k=r.k WHERE ";
                assertRowsAndPlan(
                        prefix + "l.k='aa' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async Filter workers: 1
                                              filter: k='aa'
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_STRING
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k='aa'
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_STRING
                                """
                );
                assertRowsAndPlan(
                        prefix + "'aa'=l.k ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async Filter workers: 1
                                              filter: k='aa'
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_STRING
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k='aa'
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_STRING
                                """
                );
                assertRowsAndPlan(
                        prefix + "l.k~'^a' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async Filter workers: 1
                                              filter: k ~ ^a
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_STRING
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k ~ ^a
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_STRING
                                """
                );
                assertRowsAndPlan(
                        prefix + "l.k='aa' AND l.k~'^a' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async Filter workers: 1
                                              filter: (k='aa' and k ~ ^a)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_STRING
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k ~ ^a
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_STRING
                                """
                );
                assertRowsAndPlan(
                        prefix + "l.k~'^a' AND l.k='aa' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async Filter workers: 1
                                              filter: (k ~ ^a and k='aa')
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_STRING
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k='aa'
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_STRING
                                """
                );
                bindVariableService.setStr(0, "aa");
                assertRowsAndPlan(
                        prefix + "$1=l.k ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                            Async Filter workers: 1
                                              filter: $0::string=k
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_STRING
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k=$0::string
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_STRING
                                """
                );
            }
            {
                final String prefix = "SELECT l.id lid,r.id rid FROM lp_trans_text_l_SYMBOL l JOIN lp_trans_text_r_SYMBOL r ON l.k=r.k WHERE ";
                assertRowsAndPlan(
                        prefix + "l.k='aa' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                          symbolKeyJoin: true
                                            Async JIT Filter workers: 1
                                              filter: k='aa'
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_SYMBOL
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k='aa'
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_SYMBOL
                                """
                );
                assertRowsAndPlan(
                        prefix + "'aa'=l.k ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                          symbolKeyJoin: true
                                            Async JIT Filter workers: 1
                                              filter: k='aa'
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_SYMBOL
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k='aa'
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_SYMBOL
                                """
                );
                assertRowsAndPlan(
                        prefix + "l.k~'^a' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                          symbolKeyJoin: true
                                            Async Filter workers: 1
                                              filter: k ~ ^a
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_SYMBOL
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k ~ ^a
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_SYMBOL
                                """
                );
                assertRowsAndPlan(
                        prefix + "l.k='aa' AND l.k~'^a' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                          symbolKeyJoin: true
                                            Async Filter workers: 1
                                              filter: (k='aa' and k ~ ^a)
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_SYMBOL
                                            Hash
                                                Async Filter workers: 1
                                                  filter: k ~ ^a
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_SYMBOL
                                """
                );
                assertRowsAndPlan(
                        prefix + "l.k~'^a' AND l.k='aa' ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                          symbolKeyJoin: true
                                            Async Filter workers: 1
                                              filter: (k ~ ^a and k='aa')
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_SYMBOL
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k='aa'
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_SYMBOL
                                """
                );
                bindVariableService.setStr(0, "aa");
                assertRowsAndPlan(
                        prefix + "$1=l.k ORDER BY lid,rid",
                        "lid\trid\n1\t11\n",
                        """
                                Encode sort
                                  keys: [lid, rid]
                                    SelectedRecord
                                        Hash Join Light
                                          condition: r.k=l.k
                                          symbolKeyJoin: true
                                            Async JIT Filter workers: 1
                                              filter: k=$0::string
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_text_l_SYMBOL
                                            Hash
                                                Async JIT Filter workers: 1
                                                  filter: k=$0::string
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_text_r_SYMBOL
                                """
                );
            }
        });
    }

    @Test
    public void testPushedOuterEqualityRemapsAliasesWithoutSourceFactRescan() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String prefix = "SELECT lid,rid FROM (SELECT l.id lid,r.id rid,l.k lk FROM lp_trans_l l "
                    + "JOIN lp_trans_r r ON l.k=r.k) q WHERE ";
            assertRowsAndPlan(prefix + "lk=1 ORDER BY lid,rid", "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: k=1
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """);
            bindVariableService.setInt(0, 1);
            assertRowsAndPlan(prefix + "$1=lk ORDER BY lid,rid", "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Hash Join Light
                                      condition: r.k=l.k
                                        Async JIT Filter workers: 1
                                          filter: $0::int=k
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_l
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: k=$0::int
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: lp_trans_r
                            """);
            assertRowsAndPlan("SELECT lid,rid FROM (SELECT l.id lid,r.id rid,l.k lk FROM lp_trans_l l "
                            + "JOIN lp_trans_r r ON l.k=r.k LIMIT 2) q WHERE lk=1 ORDER BY lid,rid", "lid\trid\n1\t11\n",
                    """
                            Encode sort
                              keys: [lid, rid]
                                SelectedRecord
                                    Filter filter: lk=1
                                        Limit value: 2 skip-rows-max: 0 take-rows-max: 2
                                            SelectedRecord
                                                Hash Join Light
                                                  condition: r.k=l.k
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: lp_trans_l
                                                    Hash
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: lp_trans_r
                            """);
        });
    }

    @Test
    public void testOuterFoldedFunctionRetainsKnownExtraDerivedFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT lid,rid FROM (SELECT l.id lid,r.id rid,l.k lk FROM lp_trans_l l "
                    + "JOIN lp_trans_r r ON l.k=r.k) q WHERE lk=abs(1) ORDER BY lid,rid";
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertEquals("lid\trid\n1\t11\n", printFactory(factory));
                Assert.assertEquals(2, filterCount(factory));
            }
        });
    }

    @Test
    public void testSpliceJoinKeepsMasterFilterAboveItAndPassesFactsOnToLaterKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_splice_m(s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE lp_splice_s(d DOUBLE,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE lp_splice_k(s SYMBOL,v INT)");
            execute("INSERT INTO lp_splice_m VALUES ('a','2024-01-01T00:00:01Z'),('b','2024-01-01T00:00:02Z')");
            execute("INSERT INTO lp_splice_s VALUES (7,'2024-01-01T00:00:03Z')");
            execute("INSERT INTO lp_splice_k VALUES ('a',1),('b',2)");
            assertRowsAndPlan("SELECT m.ts,s.d FROM lp_splice_m m SPLICE JOIN lp_splice_s s WHERE m.s='a'",
                    """
                            ts\td
                            2024-01-01T00:00:01.000000Z\tnull
                            """,
                    """
                            SelectedRecord
                                Filter filter: m.s='a'
                                    Splice Join
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_splice_m
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_splice_s
                            """);
            assertRowsAndPlan("SELECT m.ts,s.d,k.v FROM lp_splice_m m SPLICE JOIN lp_splice_s s JOIN lp_splice_k k ON m.s=k.s WHERE m.s='a'",
                    """
                            ts\td\tv
                            2024-01-01T00:00:01.000000Z\tnull\t1
                            """,
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: k.s=m.s
                                  symbolKeyJoin: true
                                    Filter filter: m.s='a'
                                        Splice Join
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_splice_m
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_splice_s
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: s='a'
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_splice_k
                            """);
        });
    }

    @Test
    public void testTemporalJoinSlavesTakeMasterKeyFacts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_trans_t(s SYMBOL,d DOUBLE,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO lp_trans_t VALUES
                    ('a',1,'2024-01-01T00:00:01Z'),('b',2,'2024-01-01T00:00:02Z'),('a',3,'2024-01-01T00:00:03Z'),
                    ('b',4,'2024-01-01T00:00:04Z'),('a',5,'2024-01-01T00:00:05Z')
                    """);
            assertRowsAndPlan("SELECT t.ts,u.d FROM lp_trans_t t LT JOIN lp_trans_t u ON t.s=u.s WHERE t.s='a'",
                    """
                            ts\td
                            2024-01-01T00:00:01.000000Z\tnull
                            2024-01-01T00:00:03.000000Z\t1.0
                            2024-01-01T00:00:05.000000Z\t3.0
                            """,
                    """
                            SelectedRecord
                                Lt Join Light
                                  condition: u.s=t.s
                                    Async JIT Filter workers: 1
                                      filter: s='a'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_t
                                    Async JIT Filter workers: 1
                                      filter: s='a'
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_t
                            """);
            assertRowsAndPlan("SELECT t.ts,u.d,v.d vd FROM lp_trans_t t ASOF JOIN lp_trans_t u ON t.s=u.s "
                            + "JOIN lp_trans_t v ON t.s=v.s WHERE t.s='a' AND v.d<2",
                    """
                            ts\td\tvd
                            2024-01-01T00:00:01.000000Z\t1.0\t1.0
                            2024-01-01T00:00:03.000000Z\t3.0\t1.0
                            2024-01-01T00:00:05.000000Z\t5.0\t1.0
                            """,
                    """
                            SelectedRecord
                                Hash Join Light
                                  condition: v.s=t.s
                                  symbolKeyJoin: true
                                    Filtered AsOf Join Fast
                                      filter: s='a'
                                        Async JIT Filter workers: 1
                                          filter: s='a'
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_t
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: (d<2 and s='a')
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: lp_trans_t
                            """);
            assertQuery("SELECT t.ts,u.d FROM lp_trans_t t SPLICE JOIN lp_trans_t u ON t.s=u.s WHERE t.s='a'")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Filter filter: t.s='a'
                                    Splice Join
                                      condition: u.s=t.s
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_t
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: lp_trans_t
                            """);
        });
    }

    private void assertRowsAndPlan(String sql, String rows, String plan) throws Exception {
        assertQuery(sql).noLeakCheck().withPlan(plan).inferRandomAccess().inferTimestamp().sizeMayVary().returns(rows);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_trans_l(id INT,k INT)");
        execute("CREATE TABLE lp_trans_r(id INT,k INT)");
        execute("INSERT INTO lp_trans_l VALUES (1,1),(2,2),(3,3),(4,null)");
        execute("INSERT INTO lp_trans_r VALUES (11,1),(12,2),(13,3),(14,null)");
    }

    private int filterCount(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        int count = 0;
        for (int i = 1, n = plan.getLineCount(); i <= n; i++) {
            if (plan.getLine(i).toString().contains("Filter")) {
                count++;
            }
        }
        return count;
    }

    private static String query(String condition) {
        return "SELECT l.id lid,r.id rid FROM lp_trans_l l JOIN lp_trans_r r ON l.k=r.k WHERE "
                + condition + " ORDER BY lid,rid";
    }
}
