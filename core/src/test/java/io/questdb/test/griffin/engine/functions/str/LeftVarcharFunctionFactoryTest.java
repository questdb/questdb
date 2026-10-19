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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LeftVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery("select k, left(k,500) from x") // 500 > than max string len
                .ddl("create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ
                        檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ
                        *i^!{j<9Etl";&\t*i^!{j<9Etl";&
                        (OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17
                        \t
                        mLG -$}(,V\tmLG -$}(,V
                        Yc0F?Mn%l-E"+~M/8\tYc0F?Mn%l-E"+~M/8
                        =&y@kk1CW#k1.xo'=\t=&y@kk1CW#k1.xo'=
                        kiM,1DzqxI62D\tkiM,1DzqxI62D
                        >)~I_?|?,V\t>)~I_?|?,V
                        """);
    }

    @Test
    public void testConstNeg() throws Exception {
        assertQuery("select k, left(k,-1) from x")
                .ddl("create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛
                        檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E
                        *i^!{j<9Etl";&\t*i^!{j<9Etl";
                        (OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t(OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً
                        \t
                        mLG -$}(,V\tmLG -$}(,
                        Yc0F?Mn%l-E"+~M/8\tYc0F?Mn%l-E"+~M/
                        =&y@kk1CW#k1.xo'=\t=&y@kk1CW#k1.xo'
                        kiM,1DzqxI62D\tkiM,1DzqxI62
                        >)~I_?|?,V\t>)~I_?|?,
                        """);
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery("select k, left(k,-400) from x")
                .ddl("create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t
                        檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t
                        *i^!{j<9Etl";&\t
                        (OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t
                        \t
                        mLG -$}(,V\t
                        Yc0F?Mn%l-E"+~M/8\t
                        =&y@kk1CW#k1.xo'=\t
                        kiM,1DzqxI62D\t
                        >)~I_?|?,V\t
                        """);
    }

    @Test
    public void testConstNull() throws Exception {
        assertQuery("select k, left(k,null) from x")
                .ddl("create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tleft
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t
                        檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t
                        *i^!{j<9Etl";&\t
                        (OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t
                        \t
                        mLG -$}(,V\t
                        Yc0F?Mn%l-E"+~M/8\t
                        =&y@kk1CW#k1.xo'=\t
                        kiM,1DzqxI62D\t
                        >)~I_?|?,V\t
                        """);
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("select k, left(k,2) from x")
                .ddl("create table x as (select rnd_varchar(10,20,3) k from long_sequence(20))")
                .expectSize()
                .returns("""
                        k\tleft
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t&\uDA1F\uDE98
                        檲\\~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\t檲\\
                        *i^!{j<9Etl";&\t*i
                        (OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K裷\uD9CC\uDE73+\u0093ً\uDAF5\uDE17\t(O
                        \t
                        mLG -$}(,V\tmL
                        Yc0F?Mn%l-E"+~M/8\tYc
                        =&y@kk1CW#k1.xo'=\t=&
                        kiM,1DzqxI62D\tki
                        >)~I_?|?,V\t>)
                        uXK&J"G~;.3kEC}k$\tuX
                        \t
                        \uDA43\uDFF0-㔍x钷Mͱ:աf@ץ\t\uDA43\uDFF0-
                        780|'?7t~mPO=I~9\t78
                        g>)5{l5J\\d;f7u\tg>
                        bOyf4zhx&.\tbO
                        іa\uDA76\uDDD4*\uDB87\uDF60-ă堝ᢣ΄BǬ\tіa
                        v59Q,?/qbOku|U#E\tv5
                        \t
                        >'nK4P^XG2"b\t>'
                        """);
    }

    @Test
    public void testVar() throws Exception {
        assertQuery("select k, n, left(k,n) from x")
                .ddl("create table x as (select rnd_varchar(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))")
                .expectSize()
                .returns("""
                        k\tn\tleft
                        &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t16\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ
                        0\uDA89\uDFA4~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\\t6\t0\uDA89\uDFA4~2\uDAC6\uDED3ڎ
                        j䇜"ŸO(OFг\uDBAE\uDD12ɜ|\\軦۽\tnull\t
                        \t19\t
                        "+zMKZ 4xL?49Mqqp\t19\t"+zMKZ 4xL?49Mqqp
                        5+wG`'{h)`qqjbzK.kq\t0\t
                        -\uDBED\uDC98\uDA30\uDEE01W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\t5\t-\uDBED\uDC98\uDA30\uDEE01W
                        \t1\t
                        곔4칒\uD94E\uDF98\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔b\t14\t곔4칒\uD94E\uDF98\uD908\uDECBŗ\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔
                        ϫ\uD95D\uDD6FOa\uDA76\uDDD4*\uDB87\uDF60-ă堝\t20\tϫ\uD95D\uDD6FOa\uDA76\uDDD4*\uDB87\uDF60-ă堝
                        """);
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery("select k, left(k,0) from x")
                .ddl("create table x as (select rnd_varchar(3,5,1) k from long_sequence(5))")
                .expectSize()
                .returns("""
                        k\tleft
                        &\uDA1F\uDE98|\t
                        JX1u\t
                        )|1u\t
                        L>gG\t
                        ĕ擉q\t
                        """);
    }
}
