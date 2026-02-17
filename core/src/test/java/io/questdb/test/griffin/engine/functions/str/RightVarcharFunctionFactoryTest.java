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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class RightVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\n" +
                        "檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\n" +
                        "*i^!{j<9Etl\";&\t*i^!{j<9Etl\";&\n" +
                        "(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\n" +
                        "\t\n" +
                        "mLG -$}(,V\tmLG -$}(,V\n" +
                        "Yc0F?Mn%l-E\"+~M/8\tYc0F?Mn%l-E\"+~M/8\n" +
                        "=&y@kk1CW#k1.xo'=\t=&y@kk1CW#k1.xo'=\n" +
                        "kiM,1DzqxI62D\tkiM,1DzqxI62D\n" +
                        ">)~I_?|?,V\t>)~I_?|?,V\n",
                "select k, right(k,500) from x", // 500 > than max string len
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNeg() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\n" +
                        "檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\n" +
                        "*i^!{j<9Etl\";&\ti^!{j<9Etl\";&\n" +
                        "(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\tOFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\n" +
                        "\t\n" +
                        "mLG -$}(,V\tLG -$}(,V\n" +
                        "Yc0F?Mn%l-E\"+~M/8\tc0F?Mn%l-E\"+~M/8\n" +
                        "=&y@kk1CW#k1.xo'=\t&y@kk1CW#k1.xo'=\n" +
                        "kiM,1DzqxI62D\tiM,1DzqxI62D\n" +
                        ">)~I_?|?,V\t)~I_?|?,V\n",
                "select k, right(k,-1) from x",
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t\n" +
                        "檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t\n" +
                        "*i^!{j<9Etl\";&\t\n" +
                        "(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t\n" +
                        "\t\n" +
                        "mLG -$}(,V\t\n" +
                        "Yc0F?Mn%l-E\"+~M/8\t\n" +
                        "=&y@kk1CW#k1.xo'=\t\n" +
                        "kiM,1DzqxI62D\t\n" +
                        ">)~I_?|?,V\t\n",
                "select k, right(k,-400) from x",
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNull() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t\n" +
                        "檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t\n" +
                        "*i^!{j<9Etl\";&\t\n" +
                        "(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t\n" +
                        "\t\n" +
                        "mLG -$}(,V\t\n" +
                        "Yc0F?Mn%l-E\"+~M/8\t\n" +
                        "=&y@kk1CW#k1.xo'=\t\n" +
                        "kiM,1DzqxI62D\t\n" +
                        ">)~I_?|?,V\t\n",
                "select k, right(k,null) from x",
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t͛Ԉ\n" +
                        "檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t\uDB8D\uDE4Eᯤ\n" +
                        "*i^!{j<9Etl\";&\t;&\n" +
                        "(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\tً\uDAF5\uDE17\n" +
                        "\t\n" +
                        "mLG -$}(,V\t,V\n" +
                        "Yc0F?Mn%l-E\"+~M/8\t/8\n" +
                        "=&y@kk1CW#k1.xo'=\t'=\n" +
                        "kiM,1DzqxI62D\t2D\n" +
                        ">)~I_?|?,V\t,V\n" +
                        "uXK&J\"G~;.3kEC}k$\tk$\n" +
                        "\t\n" +
                        "\uDA43\uDFF0-㔍x钷Mͱ:աf@ץ\t@ץ\n" +
                        "780|'?7t~mPO=I~9\t~9\n" +
                        "g>)5{l5J\\d;f7u\t7u\n" +
                        "bOyf4zhx&.\t&.\n" +
                        "іa\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄BǬ\tBǬ\n" +
                        "v59Q,?/qbOku|U#E\t#E\n" +
                        "\t\n" +
                        ">'nK4P^XG2\"b\t\"b\n",
                "select k, right(k,2) from x",
                "create table x as (select rnd_varchar(10,20,3) k from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVar() throws Exception {
        assertQuery(
                "k\tn\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t16\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\n" +
                        "0\uDA89\uDFA4~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\\t6\t}ѱʜ\uDB8D\uDE4Eᯤ\\\n" +
                        "j䇜\"ŸO(OFг\uDBAE\uDD12ɜ|\\軦۽\tnull\t\n" +
                        "\t19\t\n" +
                        "\"+zMKZ 4xL?49Mqqp\t19\t\"+zMKZ 4xL?49Mqqp\n" +
                        "5+wG`'{h)`qqjbzK.kq\t0\t\n" +
                        "-\uDBED\uDC98\uDA30\uDEE01W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\t5\tX夺\uDA02\uDE66\uDA29\uDE0E⋜\n" +
                        "\t1\t\n" +
                        "곔4칒\uD94E\uDF98\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔b\t14\t4칒\uD94E\uDF98\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔b\n" +
                        "ϫ\uD95D\uDD6FOa\uDA76\uDDD4*\uDB87\uDF60-ă堝\t20\tϫ\uD95D\uDD6FOa\uDA76\uDDD4*\uDB87\uDF60-ă堝\n",
                "select k, n, right(k,n) from x",
                "create table x as (select rnd_varchar(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery(
                "k\tright\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\t\n" +
                        "3TsZs\\ZXzqVx\t\n" +
                        "͛Ԉ龘и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH뤻\t\n" +
                        "\"\uDB87\uDFA35ʜ\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46O\t\n" +
                        "H93rhi\\J)#T\t\n",
                "select k, right(k,0) from x",
                "create table x as (select rnd_varchar(10,12,3) k from long_sequence(5))",
                null,
                true,
                true
        );
    }
}
