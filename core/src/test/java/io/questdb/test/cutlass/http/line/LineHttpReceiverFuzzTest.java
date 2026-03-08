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

package io.questdb.test.cutlass.http.line;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class LineHttpReceiverFuzzTest extends AbstractLineHttpFuzzTest {

    private static final Log LOG = LogFactory.getLog(LineHttpReceiverFuzzTest.class);

    @Test
    public void testAddColumns() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        initLoadParameters(15 + rnd.nextInt(100), 5 + rnd.nextInt(5),
                2 + rnd.nextInt(20), 1 + rnd.nextInt(4),
                rnd.nextInt(75));

        initFuzzParameters(-1, -1, 1, 1 + rnd.nextInt(3), -1, false, true, false, 0.05);
        runTest();
    }

    @Test
    public void testAddColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, false, false, 0.05);
        runTest();
    }

    @Test
    public void testAddConvertColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, true, false, 0.05);
        runTest();
    }

    @Test
    public void testAllMixed() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(3, 4, 5, 10, 5, true, true, false, 0.05);
        runTest();
    }

    @Test
    public void testAllMixedSplitPart() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 1, 50);
        initFuzzParameters(-1, -1, -1, 10, -1, false, true, false, 0.05);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, 0.05);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, false, false, 0.05);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, 0.05);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        runTest();
    }

    @Test
    public void testLoadNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, false, 0.05);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, true, false, 0.05);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAsciiNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, false, false, 0.05);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, true, false, 0.05);
        runTest();
    }

    @Test
    public void testReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, false, false, 0.05);
        runTest();
    }

    @Override
    protected Log getLog() {
        return LOG;
    }
}
