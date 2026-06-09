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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class TestRndVarchar extends AbstractCairoTest {
    @Test
    public void testAscii() throws Exception {
        assertQuery("select rnd_varchar(5,10,1) n from long_sequence(130)")
                .returnsOnce("n\n" +
                        "&\uDA1F\uDE98|\uD924\uDE04۲\n" +
                        "\n" +
                        "䶓zV衞͛Ԉ龘и\uDA89\uDFA4~\n" +
                        "5ʜ\uDB8D\uDE4Eᯤ\\篸{\n" +
                        "x\uDB59\uDF3B룒jᷚ\n" +
                        " 4xL?49Mqq\n" +
                        "Ǆ Ԡ阷l싒8쮠\n" +
                        "\"G~;.3kE\n" +
                        "f@ץ;윦\u0382\n" +
                        "\n" +
                        "\n" +
                        "볱9іa\uDA76\uDDD4*\n" +
                        "?/qbOku\n" +
                        "4P^XG2\"b\n" +
                        "naz>A0'wQ\n" +
                        "!o>mw)Y!L\n" +
                        "$bsz_9*Ql\n" +
                        "ه\uD909\uDF4Dٶ嫣\uEB54\uDAF0\uDF8F̅!w雑\n" +
                        "]6O}(2\n" +
                        "\n" +
                        "\n" +
                        "pjo^3&m0n\n" +
                        "ｯؾBt\uDA4D\uDEE3u\n" +
                        "\n" +
                        "[H=lFr\n" +
                        "\n" +
                        "W/t]}\n" +
                        "\n" +
                        "\n" +
                        "IG)Ff9\n" +
                        "ŞծZ骞\uDA1A\uDCB5\n" +
                        "\uD973\uDD38ܮ\uD985\uDD78ҊG\uD9A6\uDD42\n" +
                        "\n" +
                        "פ\uDA2C\uDF7BN\uD92F\uDF1AT *H콡H\n" +
                        "\n" +
                        "ŪD꘥\u061Cܺ̑\uE743\u07B3\n" +
                        "9?~8R&<wJ4\n" +
                        "u9h_\\b 6*)\n" +
                        ":FV8B\n" +
                        "BBo,OX\n" +
                        "b沿z駊:\uDACD\uDD7D\n" +
                        "\n" +
                        "\n" +
                        "ﭙ@\u0602i\uDB00\uDF8AϿ˄礏\n" +
                        "\uDB41\uDD94\uF0CFB\uDB1E\uDDDFڔX\uDBF9\uDE68\n" +
                        "\uD9EA\uDC46ߎ\uDA8B\uDE1Ef絕\uD98E\uDF30-\uE605\n" +
                        "\n" +
                        "[%_P-]g\n" +
                        "XD\uDBDB\uDE52G\uD91B\uDFE0\n" +
                        "l4~E -o\n" +
                        "\n" +
                        "\n" +
                        "6:Hk=\n" +
                        "ÑΑOG\uD930\uDD3A÷\n" +
                        "\uD9A8\uDFFBi⟃2*ص다P\n" +
                        ";ĂڇZmkЍx\uDA22\uDD8AV\n" +
                        "\n" +
                        "\n" +
                        "T\uDBA0\uDD2Fmτ⻱\n" +
                        "\n" +
                        "\n" +
                        "c߷珳0ا暧͏\n" +
                        "\uDB46\uDE223CcrҜ\n" +
                        "~ߚ\uDA41\uDE49E\uD99C\uDD61n\n" +
                        "XNma`v\n" +
                        "\n" +
                        "\uD9FB\uDEE24ޗp迸ӣ\n" +
                        "w՛(Ө*\uDADD\uDD4C2\uD95A\uDC74\uD900\uDF7F\n" +
                        "ꐞ4ˣ!۱ݥ0;\uE373춑\n" +
                        "\uDA23\uDE60̠ጹ!Z\n" +
                        "\n" +
                        "\n" +
                        "n!|Z2EZc\n" +
                        "ĺY}í\uDB18\uDE54ֹf\n" +
                        "ŰӤ0\uD98D\uDEAB?\uDBCB\uDE78ሯ\n" +
                        "0ݓ\uDB9C\uDE77⸴闷\uDAAE\uDD39\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "A\"浾!x\n" +
                        "ͨ\uDA11\uDD0B혟˶O\uDA93\uDE9EݚӉ\n" +
                        "A;<Yo$\\G1\n" +
                        "I5aw1UR?Mx\n" +
                        "VZA=x\n" +
                        ":\uDB79\uDE6B⤯?Ꭱ\n" +
                        "\n" +
                        "\n" +
                        "ImG8qnF0q\n" +
                        "\uDA12\uDCB40ￒvﾱ\uDAED\uDE95 8PP\n" +
                        "MSKMLQ\n" +
                        "\uDA8E\uDF68Єy玉͠ћ\n" +
                        "G˗\uD919\uDFC8렇ʵˣ\uDADB\uDD51\uDB86\uDC87\n" +
                        ";Zpmd\n" +
                        "\n" +
                        "(;˺\u218F出n\n" +
                        "_ȿ̥bږₙЊm䳍x\n" +
                        "s\uDA89\uDC50k㸵jήLÕϾ\uD95E\uDC2D\n" +
                        "\uDAD1\uDF3EҼ9]輪T뉰L\n" +
                        "3o!E@OqYQ\n" +
                        "\uDA57\uDC83g㢓䪄Μۋ\n" +
                        "$\"y`Mx7\n" +
                        "\n" +
                        "\n" +
                        "FA?%x^q4Z&\n" +
                        "z<{|%\\+@FA\n" +
                        "\n" +
                        "t6r#9('|z\n" +
                        "15g.;\n" +
                        "\n" +
                        "_\"al_v}7GL\n" +
                        "G?Q;If=\n" +
                        "GYTY]\n" +
                        "R42>O\n" +
                        "Wzz`t\n" +
                        "Ԅ\uD930\uDDB3N͢\uDB0D\uDF23Oo\n" +
                        " gYNvmu}s\n" +
                        ")J#]h1_L@w\n" +
                        "-][i3\n" +
                        "罶\uD9A1\uDF57ә쮼㿚\uD9BF\uDE6A\n" +
                        "䜉ۧ\u1C4B\uDA3B\uDC8E@(H,\n" +
                        "ˈoo'p%蚐7\n" +
                        "\n" +
                        "o輖NI>ٍ\uD9CA\uDD37Ϫ\uDA9B\uDDF3둪\n" +
                        "\n" +
                        "\uDB4E\uDD52 l˪H\uD9CF\uDC53⢷뤺m~\n" +
                        "\n" +
                        "e**m3g&`r\n" +
                        "\n" +
                        "<T/g\"\n" +
                        "\n"
                );
    }
}
