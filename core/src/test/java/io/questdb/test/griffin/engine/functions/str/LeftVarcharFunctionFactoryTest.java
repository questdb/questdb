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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class LeftVarcharFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstLarge() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\n" +
                        "蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E\t蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E\n" +
                        "-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\n" +
                        "\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01\t\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01\n" +
                        "씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@\t씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@\n" +
                        "\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\t\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\n" +
                        "\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95\t\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95\n" +
                        "ǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò墠\tǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò墠\n" +
                        "\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\t\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\n" +
                        "㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!\t㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!\n",
                "select k, left(k,500) from x", // 500 > than max string len
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNeg() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛\n" +
                        "蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E\t蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\n" +
                        "-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽\n" +
                        "\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01\t\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE0\n" +
                        "씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@\t씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf\n" +
                        "\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\t\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9і\n" +
                        "\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95\t\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\n" +
                        "ǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò墠\tǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò\n" +
                        "\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\t\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\n" +
                        "㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!\t㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅\n",
                "select k, left(k,-1) from x",
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNegLarge() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t\n" +
                        "蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E\t\n" +
                        "-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t\n" +
                        "\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01\t\n" +
                        "씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@\t\n" +
                        "\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\t\n" +
                        "\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95\t\n" +
                        "ǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò墠\t\n" +
                        "\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\t\n" +
                        "㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!\t\n",
                "select k, left(k,-400) from x",
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testConstNull() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t\n" +
                        "蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E\t\n" +
                        "-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t\n" +
                        "\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01\t\n" +
                        "씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@\t\n" +
                        "\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\t\n" +
                        "\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95\t\n" +
                        "ǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò墠\t\n" +
                        "\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\t\n" +
                        "㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!\t\n",
                "select k, left(k,null) from x",
                "create table x as (select rnd_varchar(10,20,1) k from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t핕\u05FA\n" +
                        "蝰L➤~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4E\t蝰L\n" +
                        "-\\篸{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\t-\\\n" +
                        "\uD911\uDE23⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01\t\uD911\uDE23⟩\n" +
                        "씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@\t씌䒙\n" +
                        "\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\t\uDB47\uDD9C\uDA96\uDF8F\n" +
                        "\uDBE2\uDEA2\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95\t\uDBE2\uDEA2\uDB4F\uDC7D\n" +
                        "ǜbȶ\u05EC˟'ꋯɟ\uF6BE腠f\uDA7B\uDF85zA'ò墠\tǜb\n" +
                        "\uDBE0\uDDBB\uD8FF\uDEDDʔ_\uDA8B\uDFC4︵Ƀ^DU?$s_*\uDBAE\uDF56\t\uDBE0\uDDBB\uD8FF\uDEDD\n" +
                        "㤞Mۄه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!\t㤞M\n" +
                        "\t\n" +
                        "\uDB05\uDCCDB6\u06025\uD8F0\uDDE47\uDBBB\uDC3A0ؠ\t\uDB05\uDCCDB\n" +
                        "\uDAD6\uDCF6A87俽Y_Bt\uDA4D\uDEE3u\uDA83\uDD58ߡ˪\t\uDAD6\uDCF6A\n" +
                        "\uD8DB\uDF7C}M\uF0FE\uD977\uDE47\u20FCӲ\uD981\uDF09۾芊\t\uD8DB\uDF7C}\n" +
                        "\uD931\uDF48ҽ\uDA01\uDE60E죢魷ꞛ#DQ헙׳G\uDA20\uDDC7犯zG¼Şծ\t\uD931\uDF48ҽ\n" +
                        "骞\uDA1A\uDCB5\uD936\uDF44Uę\uDA65\uDE071(rոҊG\uD9A6\uDD42\uDB48\uDC78{ϸ\uD9F4\uDFB9\uDA0A\uDC7A\uDA76\uDC87\t骞\uDA1A\uDCB5\n" +
                        "\uD8F0\uDF66Ҫb\uDBB1\uDEA3ȃ*H콡H\uDB76\uDD47\uD9D3\uDCEE\t\uD8F0\uDF66Ҫ\n" +
                        "+5ŪD꘥\u061Cܺ̑\uE743\u07B3\uDA8F\uDC319믓˫ᡙ\uDBEC\uDE3B櫑߸\t+5\n" +
                        "\t\n" +
                        ".û\uD9F3\uDFD5a~=V냣俄둄\uDAEF\uDDE2뮣݇8YD\t.û\n",
                "select k, left(k,2) from x",
                "create table x as (select rnd_varchar(10,20,3) k from long_sequence(20))",
                null,
                true,
                true
        );
    }

    @Test
    public void testVar() throws Exception {
        assertQuery(
                "k\tn\tleft\n" +
                        "핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\t16\t핕\u05FA씎鈄۲ӄǈ2Lg\uD95A\uDFD9唶鴙\uDAE2\uDC5E͛Ԉ\n" +
                        "и\uDA89\uDFA4~2\uDAC6\uDED3ڎBH뤻䰭\u008B}ѱʜ\uDB8D\uDE4Eᯤ\\\t6\tи\uDA89\uDFA4~2\uDAC6\uDED3ڎ\n" +
                        "{\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ|\\軦۽㒾\uD99D\uDEA7K\t5\t{\uD9D7\uDFE5\uDAE9\uDF46OF\n" +
                        "\tNaN\t\n" +
                        "9qp-鳓w\uDB63\uDE4B\uDAF5\uDE17qRӽ-\t4\t9qp-\n" +
                        "\uDBED\uDC98\uDA30\uDEE01W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮա\t6\t\uDBED\uDC98\uDA30\uDEE01W씌䒙\n" +
                        "\tNaN\t\n" +
                        "\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9іa\t-1\t\uDB47\uDD9C\uDA96\uDF8F㔸\uD989\uDDFF>\uDAEE\uDC4FƑ䈔bɄh볱9і\n" +
                        "\t9\t\n" +
                        "\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95Qǜb\t19\t\uDB4F\uDC7Dl⤃堝ᢣ΄BǬ\uDB37\uDC95Qǜb\n",
                "select k, n, left(k,n) from x",
                "create table x as (select rnd_varchar(10,20,1) k, rnd_int(-1, 20, 1) n from long_sequence(10))",
                null,
                true,
                true
        );
    }

    @Test
    public void testWhenCountIsZeroThenReturnsEmptyStringOrNull() throws Exception {
        assertQuery(
                "k\tleft\n" +
                        "핕\u05FA씎\t\n" +
                        "đ.ӄǈ\t\n" +
                        "Lg\uD95A\uDFD9唶\t\n" +
                        "q\uDAE2\uDC5E͛Ԉ\t\n" +
                        "蝰L➤\t\n",
                "select k, left(k,0) from x",
                "create table x as (select rnd_varchar(3,5,1) k from long_sequence(5))",
                null,
                true,
                true
        );
    }
}
