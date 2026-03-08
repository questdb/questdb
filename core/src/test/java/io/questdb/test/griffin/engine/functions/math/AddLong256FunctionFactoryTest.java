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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.math.AddLong256FunctionFactory;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Random;

public class AddLong256FunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAddNoCarry() throws SqlException {
        CharSequence tok1 = "0x7fffffffffffffff7fffffffffffffff7fffffffffffffff7fffffffffffffff";
        CharSequence tok2 = "0x7000000000000000800000000000000080000000000000008000000000000000";
        CharSequence tok3 = "0xefffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        Long256 l3 = Numbers.parseLong256(tok3, new Long256Impl());
        callBySignature("+(HH)", l1, l2).andAssertLong256(l3);
    }

    @Test
    public void testAddSimple() throws SqlException {
        CharSequence tok1 = "0x01";
        CharSequence tok2 = "0x02";
        CharSequence tok3 = "0x03";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        Long256 l3 = Numbers.parseLong256(tok3, new Long256Impl());
        callBySignature("+(HH)", l1, l2).andAssertLong256(l3);
    }

    @Test
    public void testAddWithCarry() throws SqlException {
        CharSequence tok1 = "0x7ffffffffffffff7fffffffffffffff7fffffffffffffff7ffffffffffffffff";
        CharSequence tok2 = "0x1000000000000000100000000000000010000000000000001000000000000000";
        CharSequence tok3 = "0x8ffffffffffffff80ffffffffffffff80ffffffffffffff80fffffffffffffff";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        Long256 l3 = Numbers.parseLong256(tok3, new Long256Impl());
        callBySignature("+(HH)", l1, l2).andAssertLong256(l3);
    }

    @Test
    public void testAddWithCarry1() throws SqlException {
        CharSequence tok1 = "fffffffffffffff0fffffffffffffff0fffffffffffffff0ffffffffffffffff";
        CharSequence tok2 = "0000000000000000100000000000000010000000000000001000000000000000";
        BigInteger b1 = new BigInteger(tok1.toString(), 16);
        BigInteger b2 = new BigInteger(tok2.toString(), 16);
        //CharSequence tok2 = "0x01";
        CharSequence tok3 = b1.add(b2).toString(16);
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        Long256 l3 = Numbers.parseLong256(tok3, new Long256Impl());
        callBySignature("+(HH)", l1, l2).andAssertLong256(l3);
    }

    @Test
    public void testAddWithNull() throws SqlException {
        CharSequence tok1 = "0x01";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        callBySignature("+(HH)", l1, Long256Impl.NULL_LONG256).andAssertLong256(Long256Impl.NULL_LONG256);
        callBySignature("+(HH)", Long256Impl.NULL_LONG256, l1).andAssertLong256(Long256Impl.NULL_LONG256);
    }

    @Test
    public void testAddWithOverflow() throws SqlException {
        CharSequence tok1 = "0x7fffffffffffffff7fffffffffffffff7fffffffffffffff7fffffffffffffff";
        CharSequence tok2 = "0x8000000000000000800000000000000080000000000000008000000000000000";
        Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());
        callBySignature("+(HH)", l1, l2).andAssertLong256(Long256Impl.NULL_LONG256);
    }

    @Test
    public void testRandom() throws SqlException {
        Random rnd = new Random();
        for (int i = 0; i < 1000; i++) {
            BigInteger b1 = new BigInteger(256, rnd);
            BigInteger b2 = new BigInteger(256, rnd);
            CharSequence expTok = b1.add(b2).toString(16);
            Long256 expected = Numbers.parseLong256(expTok, new Long256Impl());

            CharSequence tok1 = b1.toString(16);
            CharSequence tok2 = b2.toString(16);
            Long256 l1 = Numbers.parseLong256(tok1, new Long256Impl());
            Long256 l2 = Numbers.parseLong256(tok2, new Long256Impl());

            callBySignature("+(HH)", l1, l2).andAssertLong256(expected);
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new AddLong256FunctionFactory();
    }
}