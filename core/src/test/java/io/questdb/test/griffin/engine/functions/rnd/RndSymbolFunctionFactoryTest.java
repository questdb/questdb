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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.PropertyKey;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndSymbolFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Ignore;
import org.junit.Test;

public class RndSymbolFunctionFactoryTest extends AbstractFunctionFactoryTest {

    // null rate is not used properly in RndSymbolFunctionFactory
    @Ignore
    @Test
    public void testAllNulls() throws Exception {
        testNullRate("testCol\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                1);
    }

    @Test
    public void testCountNegative() {
        assertFailure("[18] invalid symbol count", "select rnd_symbol(-3,15,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testCountZero() {
        assertFailure("[18] invalid symbol count", "select rnd_symbol(0,15,33,6) as testCol from long_sequence(20)");
    }

    // null rate is not used properly in RndSymbolFunctionFactory
    @Ignore
    @Test
    public void testFixedLengthAllNulls() throws Exception {
        testNullRateFixedLength("testCol\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                1);
    }

    @Test
    public void testFixedLengthNoNulls() throws Exception {
        testNullRateFixedLength("testCol\n" +
                        "PDXYSBE\n" +
                        "VTJWCPS\n" +
                        "VTJWCPS\n" +
                        "HNRXGZS\n" +
                        "HNRXGZS\n" +
                        "GPGWFFY\n" +
                        "HNRXGZS\n" +
                        "PDXYSBE\n" +
                        "GPGWFFY\n" +
                        "PDXYSBE\n" +
                        "UDEYYQE\n" +
                        "XUXIBBT\n" +
                        "XUXIBBT\n" +
                        "HBHFOWL\n" +
                        "XUXIBBT\n" +
                        "PDXYSBE\n" +
                        "VTJWCPS\n" +
                        "GPGWFFY\n" +
                        "HNRXGZS\n" +
                        "VTJWCPS\n",
                0);
    }

    @Test
    public void testFixedLengthWithNulls() throws Exception {
        testNullRateFixedLength("testCol\n" +
                        "PDXYSBE\n" +
                        "VTJWCPS\n" +
                        "VTJWCPS\n" +
                        "\n" +
                        "HNRXGZS\n" +
                        "HNRXGZS\n" +
                        "GPGWFFY\n" +
                        "XUXIBBT\n" +
                        "HBHFOWL\n" +
                        "WHYRXPE\n" +
                        "WHYRXPE\n" +
                        "WHYRXPE\n" +
                        "UDEYYQE\n" +
                        "HBHFOWL\n" +
                        "VTJWCPS\n" +
                        "PDXYSBE\n" +
                        "UDEYYQE\n" +
                        "\n" +
                        "\n" +
                        "XUXIBBT\n",
                4);
    }

    @Test
    public void testInvalidRange() {
        assertFailure("[7] invalid range", "select rnd_symbol(5,34,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testLowNegative() {
        assertFailure("[7] invalid range", "select rnd_symbol(50,-1,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testLowZero() {
        assertFailure("[7] invalid range", "select rnd_symbol(50,0,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testNegativeNullRate() {
        assertFailure("[26] null rate must be positive", "select rnd_symbol(5,25,33,-1) as testCol from long_sequence(20)");
    }

    @Test
    public void testNoNulls() throws Exception {
        testNullRate("testCol\n" +
                        "XYSBEOUOJ\n" +
                        "WHYRXPE\n" +
                        "WHYRXPE\n" +
                        "WHYRXPE\n" +
                        "HBHFOWLP\n" +
                        "XYSBEOUOJ\n" +
                        "TJWCP\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "HBHFOWLP\n" +
                        "UXIBBTGP\n" +
                        "WFFYUDEYYQ\n" +
                        "UXIBBTGP\n" +
                        "XYSBEOUOJ\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "WHYRXPE\n" +
                        "XYSBEOUOJ\n" +
                        "HBHFOWLP\n" +
                        "WFFYUDEYYQ\n",
                0);
    }

    @Test
    public void testRndFunctionsMemoryConfiguration() {
        node1.setProperty(PropertyKey.CAIRO_RND_MEMORY_PAGE_SIZE, 1024);
        node1.setProperty(PropertyKey.CAIRO_RND_MEMORY_MAX_PAGES, 32);

        assertFailure("[18] breached memory limit set for rnd_symbol(iiii) [pageSize=1024, maxPages=32, memLimit=32768, requiredMem=78000]",
                "select rnd_symbol(1000,30,33,0) as testCol from long_sequence(20)");
    }

    @Test
    public void testWithNulls() throws Exception {
        testNullRate("testCol\n" +
                        "XYSBEOUOJ\n" +
                        "WHYRXPE\n" +
                        "WHYRXPE\n" +
                        "WHYRXPE\n" +
                        "HBHFOWLP\n" +
                        "XYSBEOUOJ\n" +
                        "TJWCP\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "\n" +
                        "\n" +
                        "UXIBBTGP\n" +
                        "WFFYUDEYYQ\n" +
                        "UXIBBTGP\n" +
                        "XYSBEOUOJ\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "\n" +
                        "\n" +
                        "XYSBEOUOJ\n",
                4);
    }

    private void testNullRate(CharSequence expectedData, int nullRate) throws Exception {
        assertQuery(expectedData, "select rnd_symbol(8,5,10," + nullRate + ") as testCol from long_sequence(20)");
    }

    private void testNullRateFixedLength(CharSequence expectedData, int nullRate) throws Exception {
        assertQuery(expectedData, "select rnd_symbol(8,7,7," + nullRate + ") as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndSymbolFunctionFactory();
    }
}
