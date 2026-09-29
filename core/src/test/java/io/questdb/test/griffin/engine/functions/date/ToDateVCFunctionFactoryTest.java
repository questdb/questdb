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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.date.ToDateFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class ToDateVCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNonCompliantDate() throws SqlException {
        call("2015 03/12 abc", "yyyy dd/MM").andAssertDate(Numbers.LONG_NULL);
    }

    @Test
    public void testNullDate() throws SqlException {
        call(null, "yyyy dd/MM").andAssertDate(Numbers.LONG_NULL);
    }

    @Test
    public void testNullPattern() {
        assertFailure(11, "pattern is required", "2015", null);
    }

    @Test
    public void testSimple() throws SqlException {
        call("2015 03/12", "yyyy dd/MM").andAssertDate(1449100800000L);
    }

    @Test
    public void testSingleCharDelimiterAboveU7fff() throws Exception {
        // Korean one-char delimiters (U+B144, U+C6D4, U+C77C) do not fit a sipush operand
        assertQuery("SELECT to_date('2024년 01월 02일', 'yyyy년 MM월 dd일') t")
                .expectSize()
                .returns("""
                        t
                        2024-01-02T00:00:00.000Z
                        """);
        assertQuery("SELECT to_date(x, 'yyyy년MM월dd일') t FROM (SELECT '2024년01월02일'::VARCHAR x FROM long_sequence(1))")
                .expectSize()
                .returns("""
                        t
                        2024-01-02T00:00:00.000Z
                        """);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToDateFunctionFactory();
    }
}