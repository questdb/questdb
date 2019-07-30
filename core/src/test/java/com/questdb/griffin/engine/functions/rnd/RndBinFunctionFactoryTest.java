/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import com.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class RndBinFunctionFactoryTest extends AbstractFunctionFactoryTest {
    private static final CairoEngine engine = new CairoEngine(configuration);
    private static final SqlCompiler compiler = new SqlCompiler(engine);

    @Before
    public void setup() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadMinimum() {
        assertFailure(8, "minimum has to be grater than 0", 0L, 10L, 2);
    }

    @Test
    public void testFixedLength() throws SqlException, IOException {
        assertQuery("x\n" +
                        "00000000 ee 41 1d 15 55 8a\n" +
                        "\n" +
                        "00000000 d8 cc 14 ce f1 59\n" +
                        "00000000 c4 91 3b 72 db f3\n" +
                        "00000000 1b c7 88 de a0 79\n" +
                        "00000000 77 15 68 61 26 af\n" +
                        "00000000 c4 95 94 36 53 49\n" +
                        "\n" +
                        "\n" +
                        "00000000 3b 08 a1 1e 38 8d\n",
                "random_cursor(10, 'x', to_char(rnd_bin(6,6,2)))");
    }

    @Test
    public void testFixedLengthNoNulls() throws SqlException, IOException {
        assertQuery("x\n" +
                        "00000000 ee 41 1d 15 55\n" +
                        "00000000 17 fa d8 cc 14\n" +
                        "00000000 f1 59 88 c4 91\n" +
                        "00000000 72 db f3 04 1b\n" +
                        "00000000 88 de a0 79 3c\n" +
                        "00000000 15 68 61 26 af\n" +
                        "00000000 c4 95 94 36 53\n" +
                        "00000000 b4 59 7e 3b 08\n" +
                        "00000000 1e 38 8d 1b 9e\n" +
                        "00000000 c8 39 09 fe d8\n",
                "random_cursor(10, 'x', to_char(rnd_bin(5,5,0)))");
    }

    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 150L, 140L, 3);
    }

    @Test
    public void testNegativeNullRate() {
        assertFailure(14, "invalid null rate", 20L, 30L, -1);
    }

    @Test
    public void testVarLength() throws SqlException, IOException {
        assertQuery("x\n" +
                        "00000000 41 1d 15\n" +
                        "00000000 17 fa d8 cc 14\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "00000000 91 3b 72 db f3\n" +
                        "00000000 c7 88 de a0 79 3c 77 15\n" +
                        "00000000 26 af 19 c4 95 94 36 53\n" +
                        "\n" +
                        "\n",
                "random_cursor(10, 'x', to_char(rnd_bin(3,8,2)))");
    }

    @Test
    public void testVarLengthNoNulls() throws SqlException, IOException {
        assertQuery("x\n" +
                        "00000000 41 1d 15\n" +
                        "00000000 17 fa d8 cc 14\n" +
                        "00000000 59 88 c4 91 3b 72\n" +
                        "00000000 04 1b c7 88 de a0\n" +
                        "00000000 77 15 68\n" +
                        "00000000 af 19 c4 95 94 36 53\n" +
                        "00000000 59 7e 3b 08 a1\n" +
                        "00000000 8d 1b 9e f4 c8 39 09\n" +
                        "00000000 9d 30 78\n" +
                        "00000000 32 de e4\n",
                "random_cursor(10, 'x', to_char(rnd_bin(3,8,0)))");
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndBinFunctionFactory();
    }

    private void assertQuery(CharSequence expected, CharSequence sql) throws SqlException, IOException {
        RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext);
        assertOnce(expected, factory.getCursor(sqlExecutionContext), factory.getMetadata(), true);
    }
}