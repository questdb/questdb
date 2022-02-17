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

package io.questdb.griffin.engine.functions.str;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class StringRndListFunctionTest extends AbstractGriffinTest {

    @Test
    public void testStringRndListFunctionFixedLengthStr() throws Exception {
        testInsertAsSelect("a\tb\n" +
                        "1232884790\tBBTGP\n" +
                        "-2119387831\tSXUXI\n" +
                        "1699553881\tGWFFY\n" +
                        "1253890363\tVTJWC\n" +
                        "-422941535\tRXPEH\n" +
                        "-547127752\tNRXGZ\n" +
                        "-303295973\tRXPEH\n" +
                        "-2132716300\tVTJWC\n" +
                        "-461611463\tUDEYY\n" +
                        "264240638\tVTJWC\n" +
                        "-483853667\tVTJWC\n" +
                        "1890602616\tRXPEH\n" +
                        "68265578\tRXPEH\n" +
                        "-2002373666\tSXUXI\n" +
                        "458818940\tRXPEH\n" +
                        "-2144581835\tUDEYY\n" +
                        "-1418341054\tSXUXI\n" +
                        "2031014705\tUDEYY\n" +
                        "-1575135393\tBBTGP\n" +
                        "936627841\tNRXGZ\n" +
                        "-667031149\tNRXGZ\n" +
                        "-2034804966\tGWFFY\n" +
                        "1637847416\tNRXGZ\n" +
                        "-1819240775\tUDEYY\n" +
                        "-1787109293\tVTJWC\n" +
                        "-1515787781\tSXUXI\n" +
                        "161592763\tRXPEH\n" +
                        "636045524\tVTJWC\n" +
                        "-1538602195\tVTJWC\n" +
                        "-372268574\tBBTGP\n",
                "create table x (a INT, b SYMBOL)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(8,5,5,0)" +
                        " from long_sequence(30)",
                "x");
    }

    @Test
    public void testStringRndListFunctionFixedLengthStrWithNulls() throws Exception {
        testInsertAsSelect("a\tb\n" +
                        "1232884790\tUDEYY\n" +
                        "-212807500\t\n" +
                        "1110979454\tVTJWC\n" +
                        "-422941535\tVTJWC\n" +
                        "-1271909747\tRXPEH\n" +
                        "-2132716300\tUDEYY\n" +
                        "-27395319\tVTJWC\n" +
                        "-483853667\tVTJWC\n" +
                        "-1272693194\tRXPEH\n" +
                        "-2002373666\tSXUXI\n" +
                        "410717394\tUDEYY\n" +
                        "-1418341054\tPSWHY\n" +
                        "-530317703\tBBTGP\n" +
                        "936627841\tBBTGP\n" +
                        "-1870444467\tGWFFY\n" +
                        "1637847416\tUDEYY\n" +
                        "-1533414895\tVTJWC\n" +
                        "-1515787781\tNRXGZ\n" +
                        "1920890138\tVTJWC\n" +
                        "-1538602195\tGWFFY\n" +
                        "-235358133\tRXPEH\n" +
                        "-10505757\t\n" +
                        "1857212401\tGWFFY\n" +
                        "-98924388\t\n" +
                        "-307026682\tPSWHY\n" +
                        "-1201923128\tRXPEH\n" +
                        "584460681\tNRXGZ\n" +
                        "532665695\tGWFFY\n" +
                        "-572338288\tNRXGZ\n" +
                        "-373499303\tNRXGZ\n",
                "create table xyz (a INT, b SYMBOL)",
                "insert into xyz " +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(8,5,5,4)" +
                        " from long_sequence(30)",
                "xyz");
    }

    @Test
    public void testStringRndListFunction() throws Exception {
        testInsertAsSelect("a\tb\n" +
                        "-483853667\tTJWCP\n" +
                        "1890602616\tNRXGZS\n" +
                        "68265578\tNRXGZS\n" +
                        "-2002373666\tGWFFYUDEY\n" +
                        "458818940\tNRXGZS\n" +
                        "-2144581835\tEOUOJ\n" +
                        "-1418341054\tGWFFYUDEY\n" +
                        "2031014705\tEOUOJ\n" +
                        "-1575135393\tQEHBHFOW\n" +
                        "936627841\tUXIBBTG\n" +
                        "-667031149\tUXIBBTG\n" +
                        "-2034804966\tPDXYS\n" +
                        "1637847416\tUXIBBTG\n" +
                        "-1819240775\tEOUOJ\n" +
                        "-1787109293\tTJWCP\n" +
                        "-1515787781\tGWFFYUDEY\n" +
                        "161592763\tNRXGZS\n" +
                        "636045524\tTJWCP\n" +
                        "-1538602195\tTJWCP\n" +
                        "-372268574\tQEHBHFOW\n" +
                        "-1299391311\tNRXGZS\n" +
                        "-10505757\tWHYRXPE\n" +
                        "1857212401\tNRXGZS\n" +
                        "-443320374\tGWFFYUDEY\n" +
                        "1196016669\tNRXGZS\n" +
                        "-1566901076\tWHYRXPE\n" +
                        "-1201923128\tUXIBBTG\n" +
                        "1876812930\tWHYRXPE\n" +
                        "-1582495445\tUXIBBTG\n" +
                        "532665695\tNRXGZS\n",
                "create table x (a INT, b SYMBOL)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(8,5,10,0)" +
                        " from long_sequence(30)",
                "x");
    }

    @Test
    public void testStringRndListFunctionWithNulls() throws Exception {
        testInsertAsSelect("a\tb\n" +
                        "-483853667\t\n" +
                        "1890602616\tNRXGZS\n" +
                        "1036510002\tGWFFYUDEY\n" +
                        "458818940\tUXIBBTG\n" +
                        "1978144263\tGWFFYUDEY\n" +
                        "2031014705\tWHYRXPE\n" +
                        "-296610933\t\n" +
                        "326010667\tUXIBBTG\n" +
                        "-2034804966\t\n" +
                        "1637847416\tEOUOJ\n" +
                        "-1533414895\tTJWCP\n" +
                        "-1515787781\tUXIBBTG\n" +
                        "1920890138\tTJWCP\n" +
                        "-1538602195\tPDXYS\n" +
                        "-235358133\tNRXGZS\n" +
                        "-10505757\tWHYRXPE\n" +
                        "-661194722\tGWFFYUDEY\n" +
                        "1196016669\tGWFFYUDEY\n" +
                        "-1269042121\tUXIBBTG\n" +
                        "1876812930\tQEHBHFOW\n" +
                        "-1424048819\tNRXGZS\n" +
                        "1234796102\tQEHBHFOW\n" +
                        "1775935667\tNRXGZS\n" +
                        "-916132123\tEOUOJ\n" +
                        "215354468\tGWFFYUDEY\n" +
                        "-731466113\t\n" +
                        "-882371473\tEOUOJ\n" +
                        "602954926\tWHYRXPE\n" +
                        "-2075675260\t\n" +
                        "-712702244\tUXIBBTG\n",
                "create table x (a INT, b SYMBOL)",
                "insert into x " +
                        "select" +
                        " rnd_int()," +
                        " rnd_str(8,5,10,3)" +
                        " from long_sequence(30)",
                "x");
    }

    private void testInsertAsSelect(CharSequence expectedData, CharSequence ddl, CharSequence insert, CharSequence select) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(ddl, sqlExecutionContext);
            compiler.compile(insert, sqlExecutionContext);
            assertSql(
                    select,
                    expectedData
            );
        });
    }
}
