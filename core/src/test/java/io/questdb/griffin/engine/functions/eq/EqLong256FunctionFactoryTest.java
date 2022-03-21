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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import org.junit.Test;

public class EqLong256FunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testEqual() throws SqlException {
        CharSequence tok1 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, tok1.length(), new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, tok2.length(), new Long256Impl());
        callBySignature("=(HH)", l1, l2).andAssert(true);
        callBySignature("=(HH)", l2, l1).andAssert(true);
    }

    @Test
    public void testNotEqual() throws SqlException {
        CharSequence tok1 = "0x7ae65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        CharSequence tok2 = "0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, tok1.length(), new Long256Impl());
        Long256 l2 = Numbers.parseLong256(tok2, tok2.length(), new Long256Impl());
        callBySignature("=(HH)", l1, l2).andAssert(false);
        callBySignature("=(HH)", l2, l1).andAssert(false);
    }

    @Test
    public void tesEqualNull() throws SqlException {
        CharSequence tok1 = "0x7ae65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7";
        Long256 l1 = Numbers.parseLong256(tok1, tok1.length(), new Long256Impl());
        Long256 l2 = Long256Impl.NULL_LONG256;
        callBySignature("=(HH)", l1, l2).andAssert(false);
        callBySignature("=(HH)", l2, l1).andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqLong256FunctionFactory();
    }
}