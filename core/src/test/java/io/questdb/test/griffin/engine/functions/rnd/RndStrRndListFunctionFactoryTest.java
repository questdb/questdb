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
import io.questdb.griffin.engine.functions.rnd.RndStrRndListFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RndStrRndListFunctionFactoryTest extends AbstractFunctionFactoryTest {

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
        assertFailure("[15] invalid string count", "select rnd_str(-3,15,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testCountZero() {
        assertFailure("[15] invalid string count", "select rnd_str(0,15,33,6) as testCol from long_sequence(20)");
    }

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
                        "PDXYSBE\n" +
                        "HBHFOWL\n" +
                        "VTJWCPS\n" +
                        "XUXIBBT\n" +
                        "VTJWCPS\n" +
                        "VTJWCPS\n" +
                        "HNRXGZS\n" +
                        "HNRXGZS\n" +
                        "HNRXGZS\n" +
                        "HNRXGZS\n" +
                        "GPGWFFY\n" +
                        "GPGWFFY\n" +
                        "HNRXGZS\n" +
                        "XUXIBBT\n" +
                        "PDXYSBE\n" +
                        "HBHFOWL\n" +
                        "GPGWFFY\n" +
                        "WHYRXPE\n" +
                        "PDXYSBE\n",
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
                        "\n" +
                        "\n" +
                        "XUXIBBT\n" +
                        "HBHFOWL\n" +
                        "\n" +
                        "PDXYSBE\n" +
                        "UDEYYQE\n" +
                        "XUXIBBT\n" +
                        "XUXIBBT\n" +
                        "HBHFOWL\n" +
                        "\n" +
                        "PDXYSBE\n" +
                        "UDEYYQE\n" +
                        "\n",
                4);
    }

    @Test
    public void testInvalidRange() {
        assertFailure("[7] invalid range", "select rnd_str(5,34,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testLowNegative() {
        assertFailure("[7] invalid range", "select rnd_str(50,-1,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testLowZero() {
        assertFailure("[7] invalid range", "select rnd_str(50,0,33,6) as testCol from long_sequence(20)");
    }

    @Test
    public void testNegativeNullRate() {
        assertFailure("[23] null rate must be positive", "select rnd_str(5,25,33,-1) as testCol from long_sequence(20)");
    }

    @Test
    public void testNoNulls() throws Exception {
        testNullRate("testCol\n" +
                        "HRUEDRQQUL\n" +
                        "XYSBEOUOJ\n" +
                        "WFFYUDEYYQ\n" +
                        "WHYRXPE\n" +
                        "HRUEDRQQUL\n" +
                        "WHYRXPE\n" +
                        "HBHFOWLP\n" +
                        "WHYRXPE\n" +
                        "UXIBBTGP\n" +
                        "HBHFOWLP\n" +
                        "UXIBBTGP\n" +
                        "XYSBEOUOJ\n" +
                        "XYSBEOUOJ\n" +
                        "TJWCP\n" +
                        "UXIBBTGP\n" +
                        "HRUEDRQQUL\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "TJWCP\n" +
                        "HBHFOWLP\n",
                0);
    }

    @Test
    public void testRndFunctionsMemoryConfiguration() {
        node1.setProperty(PropertyKey.CAIRO_RND_MEMORY_PAGE_SIZE, 1024);
        node1.setProperty(PropertyKey.CAIRO_RND_MEMORY_MAX_PAGES, 16);

        assertFailure("[15] breached memory limit set for rnd_str(iiii) [pageSize=1024, maxPages=16, memLimit=16384, requiredMem=78000]",
                "select rnd_str(1000,30,33,0) as testCol from long_sequence(20)");
    }

    @Test
    public void testWithNulls() throws Exception {
        testNullRate("testCol\n" +
                        "XYSBEOUOJ\n" +
                        "\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "UXIBBTGP\n" +
                        "UXIBBTGP\n" +
                        "XYSBEOUOJ\n" +
                        "\n" +
                        "HRUEDRQQUL\n" +
                        "HBHFOWLP\n" +
                        "\n" +
                        "WFFYUDEYYQ\n" +
                        "NRXGZS\n" +
                        "\n" +
                        "\n" +
                        "TJWCP\n" +
                        "HBHFOWLP\n" +
                        "NRXGZS\n" +
                        "WHYRXPE\n" +
                        "NRXGZS\n",
                4);
    }

    private void testNullRate(CharSequence expectedData, int nullRate) throws Exception {
        assertQuery(expectedData, "select rnd_str(8,5,10," + nullRate + ") as testCol from long_sequence(20)");
    }

    private void testNullRateFixedLength(CharSequence expectedData, int nullRate) throws Exception {
        assertQuery(expectedData, "select rnd_str(8,7,7," + nullRate + ") as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndStrRndListFunctionFactory();
    }
}
