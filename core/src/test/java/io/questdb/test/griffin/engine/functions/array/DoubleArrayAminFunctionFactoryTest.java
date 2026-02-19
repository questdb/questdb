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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DoubleArrayAminFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNullArrays() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "amin\n\n",
                "SELECT amin(null::double[], null::double[])",
                null,
                false,
                true
        ));
    }

    @Test
    public void testDifferentLengths() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "amin\n[3.0,4.0,5.0]\n",
                "SELECT amin(ARRAY[10.0, 20.0], ARRAY[3.0, 4.0, 5.0])",
                null,
                false,
                true
        ));
    }

    @Test
    public void testFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[5.0, 6.0], ARRAY[3.0, 4.0])");
            assertQueryNoLeakCheck(
                    "amin\n[3.0,4.0]\n",
                    "SELECT amin(a, b) FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNanElements() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "amin\n[3.0,2.0]\n",
                "SELECT amin(ARRAY[3.0, null], ARRAY[null, 2.0])",
                null,
                false,
                true
        ));
    }

    @Test
    public void testOneNullArray() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "amin\n[1.0,2.0]\n",
                "SELECT amin(null::double[], ARRAY[1.0, 2.0])",
                null,
                false,
                true
        ));
    }

    @Test
    public void testThreeArraysSameLength() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "amin\n[1.0,1.0]\n",
                "SELECT amin(ARRAY[1.0, 2.0], ARRAY[3.0, 1.0], ARRAY[2.0, 4.0])",
                null,
                false,
                true
        ));
    }

    @Test
    public void testTwoArraysSameLength() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "amin\n[1.0,2.0]\n",
                "SELECT amin(ARRAY[1.0, 5.0], ARRAY[3.0, 2.0])",
                null,
                false,
                true
        ));
    }
}
