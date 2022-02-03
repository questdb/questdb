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

package io.questdb.cutlass.line.tcp;

import org.junit.Test;

// we have known issues with handling metadata reload in TableReader. Alex is working on a solution. When
// this is ready we will make this test part of the CI
public class LineTcpReceiverFuzzTest extends AbstractLineTcpReceiverFuzzTest {

    // there seem to be an issue with the transactionality of adding new columns
    // when the issue is fixed 'newColumnFactor' can be used and this test should be enabled
    @Test
    public void testAddColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, false, true, false);
        runTest();
    }

    // there seem to be an issue with the transactionality of adding new columns
    // when the issue is fixed 'newColumnFactor' can be used and this test should be enabled
    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, -1, 4, -1, false, true, false, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, 3, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, false, true, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(100, 3, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, 3, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true, true, false, true);
        runTest();
    }

    @Test
    public void testLoadNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, 3, 7, 12, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, false, true, false);
        runTest();
    }

    @Test
    public void testLoadSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, 3, 4, 8, 20);
        initFuzzParameters(-1, -1, -1, -1, -1, false, true, false, true);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, 3, 7, 12, 20);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAsciiNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, 2, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, 3, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true, true, false, false);
        runTest();
    }

    @Test
    public void testReorderingColumnsNoTagsStringsAsSymbol() throws Exception {
        initLoadParameters(100, 3, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, 3, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false, true, false, false);
        runTest();
    }
}
