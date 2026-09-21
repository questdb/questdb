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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.MulIntFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class MulIntFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBothNan() throws Exception {
        assertQuery("SELECT null*null").expectSize().returns("column\nnull\n");
    }

    @Test
    public void testIntOverflow() throws Exception {
        // A constant INT*INT that overflows wraps mod 2^32, exactly like the column and bind
        // paths - it is no longer folded to a wider LONG. Every context reads that same wrapped
        // value; see testTimestampIntOverflow and IntWidthWrapTest.
        assertQuery("SELECT 1_720_468_802 * 1_000_000").expectSize().returns("column\n-607497088\n");
    }

    @Test
    public void testLeftNan() throws Exception {
        assertQuery("SELECT null*5").expectSize().returns("column\nnull\n");
    }

    @Test
    public void testMulByZero() throws Exception {
        assertQuery("SELECT 10*0").expectSize().returns("column\n0\n");
    }

    @Test
    public void testRightNan() throws Exception {
        assertQuery("SELECT 123*null").expectSize().returns("column\nnull\n");
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("SELECT 10*81").expectSize().returns("column\n810\n");
    }

    @Test
    public void testTimestamp() throws Exception {
        assertQuery("SELECT to_utc(10*81, 'Europe/Berlin')").expectSize().returns("to_utc\n1969-12-31T23:00:00.000810Z\n");
    }

    @Test
    public void testTimestampIntOverflow() throws Exception {
        // This is the GitHub issue #4752 repro, and it reads the wrapped product on purpose.
        //
        // PR #4824 fixed the issue by giving the INT operators a getLong() that recomputed at 64
        // bits, which made one INT expression carry two values - the wrapped one under getInt() and
        // the full one under getLong() - with nothing in the type to say which a consumer got. The
        // decision recorded here is that INT arithmetic wraps at 32 bits in EVERY context, exactly
        // as LONG arithmetic wraps at 64. To compute at 64 bits, widen an operand.
        //
        // The cost is that this query returns a 1970 date again, silently, and that cost was
        // accepted rather than overlooked. See IntWidthWrapTest for the full rule, its consequences
        // and the workaround the issue itself named.
        assertQuery("SELECT to_utc(1720468802 * 1000000, 'Europe/Berlin')").expectSize()
                .returns("to_utc\n1969-12-31T22:49:52.502912Z\n");
        // widen an operand and the conversion is correct
        assertQuery("SELECT to_utc(1720468802 * 1000000L, 'Europe/Berlin')").expectSize()
                .returns("to_utc\n2024-07-08T18:00:02.000000Z\n");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new MulIntFunctionFactory();
    }
}