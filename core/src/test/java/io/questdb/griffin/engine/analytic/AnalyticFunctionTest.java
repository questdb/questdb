/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.analytic;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class AnalyticFunctionTest extends AbstractGriffinTest {

    @Test
    public void testRowNumberWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery("row_number\tprice\tts\n" +
                        "1\t42\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t42\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t42\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t42\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t42\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t42\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t42\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t42\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t42\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t42\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolNoWildcard() throws Exception {
        assertQuery("row_number\n" +
                        "3\n" +
                        "6\n" +
                        "2\n" +
                        "1\n" +
                        "5\n" +
                        "1\n" +
                        "4\n" +
                        "3\n" +
                        "2\n" +
                        "1\n",
                "select row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery("price\tsymbol\tts\trow_number\n" +
                        "0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t3\n" +
                        "0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t6\n" +
                        "0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t2\n" +
                        "0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\n" +
                        "0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t5\n" +
                        "0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t1\n" +
                        "0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t4\n" +
                        "0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t3\n" +
                        "0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t2\n" +
                        "0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t1\n",
                "select *, row_number() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("row_number\tprice\tsymbol\tts\n" +
                        "3\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "6\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "2\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "4\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "3\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "2\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("row_number\tprice\tsymbol\tts\n" +
                        "10\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "7\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "9\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "6\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "8\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "5\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "3\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "2\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderWildcardLast() throws Exception {
        assertQuery("row_number\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select row_number() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberWithNoPartitionAndNoOrderInSubQuery() throws Exception {
        assertQuery("symbol\trn\n" +
                        "CC\t2\n" +
                        "BB\t3\n" +
                        "CC\t4\n" +
                        "AA\t5\n" +
                        "BB\t6\n" +
                        "CC\t7\n" +
                        "BB\t8\n" +
                        "BB\t9\n" +
                        "BB\t10\n" +
                        "BB\t11\n",
                "select symbol, rn + 1 as rn from (select symbol, row_number() over() as rn from trades)",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    public void testRowNumberFailsInNonAnalyticContext() throws Exception {
        assertFailure(
                "select row_number(), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "analytic function called in non-analytic context, make sure to add OVER clause"
        );
    }

    @Test
    public void testRowNumberWithFilter() throws Exception {
        assertQuery("author\tsym\tcommits\trk\n" +
                        "user2\tETH\t3\t2\n" +
                        "user1\tETH\t3\t1\n",
                "with active_devs as (" +
                        "    select author, sym, count() as commits" +
                        "    from dev_stats" +
                        "    where author is not null and author != 'github-actions[bot]'" +
                        "    order by commits desc" +
                        "    limit 100" +
                        "), active_ranked as (" +
                        "    select author, sym, commits, row_number() over (partition by sym order by commits desc) as rk" +
                        "    from active_devs" +
                        ") select * from active_ranked where sym = 'ETH'",
                "create table dev_stats as " +
                        "(" +
                        "select" +
                        " rnd_symbol('ETH','BTC') sym," +
                        " rnd_symbol('user1','user2') author," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionAndOrderByNonSymbol() throws Exception {
        assertQuery("rank\tprice\tts\n" +
                        "1\t42\t1970-01-01T00:00:00.000000Z\n" +
                        "2\t42\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t42\t1970-01-03T07:33:20.000000Z\n" +
                        "4\t42\t1970-01-04T11:20:00.000000Z\n" +
                        "5\t42\t1970-01-05T15:06:40.000000Z\n" +
                        "6\t42\t1970-01-06T18:53:20.000000Z\n" +
                        "7\t42\t1970-01-07T22:40:00.000000Z\n" +
                        "8\t42\t1970-01-09T02:26:40.000000Z\n" +
                        "9\t42\t1970-01-10T06:13:20.000000Z\n" +
                        "10\t42\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by price order by ts), price, ts from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " 42 price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionAndOrderBySymbolNoWildcard() throws Exception {
        assertQuery("rank\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n" +
                        "1\n",
                "select rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionAndOrderBySymbolWildcardFirst() throws Exception {
        assertQuery("price\tsymbol\tts\trank\n" +
                        "0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\t1\n" +
                        "0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\t1\n" +
                        "0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\t1\n" +
                        "0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\t1\n" +
                        "0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\t1\n" +
                        "0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\t1\n" +
                        "0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\t1\n" +
                        "0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\t1\n" +
                        "0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\t1\n" +
                        "0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\t1\n",
                "select *, rank() over (partition by symbol order by symbol) from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionBySymbolAndOrderByPriceWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "2\t0.8043224099968393\tCC\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t0.2845577791213847\tBB\t1970-01-02T03:46:40.000000Z\n" +
                        "3\t0.9344604857394011\tCC\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t0.7905675319675964\tAA\t1970-01-04T11:20:00.000000Z\n" +
                        "6\t0.8899286912289663\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t0.11427984775756228\tCC\t1970-01-06T18:53:20.000000Z\n" +
                        "2\t0.4217768841969397\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "4\t0.7261136209823622\tBB\t1970-01-09T02:26:40.000000Z\n" +
                        "3\t0.6693837147631712\tBB\t1970-01-10T06:13:20.000000Z\n" +
                        "5\t0.8756771741121929\tBB\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionBySymbolAndOrderByIntPriceWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "4\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "4\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionBySymbolAndOrderByIntPriceDescWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "2\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "2\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "2\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "2\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "2\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "2\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by price desc), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionBySymbolAndNoOrderWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "2\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "2\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "4\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "3\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "4\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "2\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithPartitionBySymbolAndMultiOrderWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "4\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "4\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (partition by symbol order by symbol, price), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithNoPartitionByAndOrderBySymbolWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "3\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "7\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "7\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "3\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "3\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "3\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "7\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "7\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (order by symbol), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankWithNoPartitionByAndNoOrderByWildcardLast() throws Exception {
        assertQuery("rank\tprice\tsymbol\tts\n" +
                        "1\t1\tBB\t1970-01-01T00:00:00.000000Z\n" +
                        "1\t2\tCC\t1970-01-02T03:46:40.000000Z\n" +
                        "1\t2\tAA\t1970-01-03T07:33:20.000000Z\n" +
                        "1\t1\tCC\t1970-01-04T11:20:00.000000Z\n" +
                        "1\t2\tBB\t1970-01-05T15:06:40.000000Z\n" +
                        "1\t1\tBB\t1970-01-06T18:53:20.000000Z\n" +
                        "1\t1\tBB\t1970-01-07T22:40:00.000000Z\n" +
                        "1\t1\tCC\t1970-01-09T02:26:40.000000Z\n" +
                        "1\t1\tCC\t1970-01-10T06:13:20.000000Z\n" +
                        "1\t2\tAA\t1970-01-11T10:00:00.000000Z\n",
                "select rank() over (), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true,
                false
        );
    }

    @Test
    @Ignore
    public void testRankFailsInNonAnalyticContext() throws Exception {
        assertFailure(
                "select rank(), * from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_int(1,2,3) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                7,
                "analytic function called in non-analytic context, make sure to add OVER clause"
        );
    }

    @Test
    @Ignore
    public void testAnalyticContextCleanup() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table trades as " +
                    "(" +
                    "select" +
                    " rnd_int(1,2,3) price," +
                    " rnd_symbol('AA','BB','CC') symbol," +
                    " timestamp_sequence(0, 100000000000) ts" +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by day", sqlExecutionContext);

            final String query = "select symbol, price, rank() over (partition by symbol order by price) from trades";
            final String expected = "symbol\tprice\trank\n" +
                    "BB\t1\t1\n" +
                    "CC\t2\t2\n" +
                    "AA\t2\t1\n" +
                    "CC\t1\t1\n" +
                    "BB\t2\t2\n";
            assertSql(query, expected);

            // AnalyticContext should be properly clean up when we try to execute the next query.

            try {
                compile("select rank() from trades", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(7, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "analytic function called in non-analytic context, make sure to add OVER clause");
            }
        });
    }
}
