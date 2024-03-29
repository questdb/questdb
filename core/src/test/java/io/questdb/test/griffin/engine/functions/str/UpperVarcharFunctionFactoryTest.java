/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.UpperVarcharFunctionFactory;
import io.questdb.std.str.Utf8String;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class UpperVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testSimple() throws SqlException {
        call(new Utf8String("AbC")).andAssert("ABC");
    }

    @Test
    public void testWithAllLowercase() throws SqlException {
        call(new Utf8String("abcdefghijklmnopqrstuvxz")).andAssert("ABCDEFGHIJKLMNOPQRSTUVXZ");
    }

    @Test
    public void testWithAllUppercase() throws SqlException {
        call(new Utf8String("ABCDEFGHIJKLMNOPQRSTUVXZ")).andAssert("ABCDEFGHIJKLMNOPQRSTUVXZ");
    }

    @Test
    public void testWithMixedCases() throws SqlException {
        call(new Utf8String("ABCdefGHIjklMNOpqrSTUvxz")).andAssert("ABCDEFGHIJKLMNOPQRSTUVXZ");
    }

    @Test
    public void testWithNonAsciiCharacters() throws SqlException {
        call(new Utf8String("abcDEFghiJKLm...() { _; } >_[$($())] { <<< (=) \noPQRstuVXZ"))
                .andAssert("ABCDEFGHIJKLM...() { _; } >_[$($())] { <<< (=) \nOPQRSTUVXZ");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new UpperVarcharFunctionFactory();
    }
}
