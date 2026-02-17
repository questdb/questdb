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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.lt.LtLong256FunctionFactory;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LtLong256FunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testGreaterOrEqThanNull() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede5";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Long256Impl.NULL_LONG256;
        callBySignature(">=(HH)", l1, l1).andAssert(true);
        callBySignature(">=(HH)", l1, l2).andAssert(false);
        callBySignature(">=(HH)", l2, l1).andAssert(false);
        callBySignature(">=(HH)", l2, l2).andAssert(false);
    }

    @Test
    public void testGreaterThan() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede5";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature(">(HH)", l1, l1).andAssert(false);
        callBySignature(">(HH)", l1, l2).andAssert(false);
        callBySignature(">(HH)", l2, l1).andAssert(true);
    }

    @Test
    public void testGreaterThanNull() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede5";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Long256Impl.NULL_LONG256;
        callBySignature(">(HH)", l1, l1).andAssert(false);
        callBySignature(">(HH)", l1, l2).andAssert(false);
        callBySignature(">(HH)", l2, l1).andAssert(false);
        callBySignature(">(HH)", l2, l2).andAssert(false);
    }

    @Test
    public void testGreaterThanOrEqual() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede5";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature(">=(HH)", l1, l1).andAssert(true);
        callBySignature(">=(HH)", l1, l2).andAssert(false);
        callBySignature(">=(HH)", l2, l1).andAssert(true);
    }

    @Test
    public void testLessOrEqThanNull() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede5";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Long256Impl.NULL_LONG256;
        callBySignature("<=(HH)", l1, l1).andAssert(true);
        callBySignature("<=(HH)", l1, l2).andAssert(false);
        callBySignature("<=(HH)", l2, l1).andAssert(false);
        callBySignature("<=(HH)", l2, l2).andAssert(false);
    }

    @Test
    public void testLessThan0() throws SqlException {
        CharSequence tok1 = "0x0a";
        CharSequence tok2 = "0x0b";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("<(HH)", l1, l2).andAssert(true);
    }

    @Test
    public void testLessThan1() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede9";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("<(HH)", l1, l2).andAssert(true);
    }

    @Test
    public void testLessThan2() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        CharSequence tok2 = "0x7ee65ec8b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("<(HH)", l1, l2).andAssert(true);
    }

    @Test
    public void testLessThan3() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a423a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";

        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("<(HH)", l1, l2).andAssert(true);
    }

    @Test
    public void testLessThan4() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199bf5c2aa91ba39c022fa261bdede7";

        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("<(HH)", l1, l2).andAssert(true);
    }

    @Test
    public void testLessThanOrEqual() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede5";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("<=(HH)", l1, l1).andAssert(true);
        callBySignature("<=(HH)", l1, l2).andAssert(true);
        callBySignature("<=(HH)", l2, l1).andAssert(false);
    }

    @Test
    public void testSameRecordDifferentColumns_negativeCase() throws Exception {
        assertQuery("a\tb\n",
                "select * from tab where a < b",
                "create table tab as (select 0x11111111 as a, 0x11111111 as b from long_sequence(1))",
                null, true);
    }

    @Test
    public void testSameRecordDifferentColumns_positiveCase() throws Exception {
        assertQuery("a\tb\n" +
                        "0x11111111\t0x11111112\n",
                "select * from tab where a < b",
                "create table tab as (select 0x11111111 as a, 0x11111112 as b from long_sequence(1))",
                null, true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LtLong256FunctionFactory();
    }
}