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

import io.questdb.cutlass.line.tcp.load.LineData;
import org.junit.Ignore;
import org.junit.Test;

public class LineTcpReceiverAddColumnsTest extends LineTcpReceiverDuplicateColumnsTest {
    private final int newColumnFactor = 4;
    private final int newColumnRandomizeFactor = 2;

    public LineTcpReceiverAddColumnsTest() {
        super(10, 10, 10, 10, 50);
    }

    @Override
    void addNewColumn(LineData line) {
            if (random.nextInt(newColumnFactor) == 0) {
                final int extraColIndex = random.nextInt(colNameBases.length);
                final CharSequence colNameNew = generateName(extraColIndex, true);
                final CharSequence colValueNew = generateValue(extraColIndex);
                line.add(colNameNew, colValueNew);
            }
    }

    @Override
    String generateName(int index) {
        return generateName(index, false);
    }

    private String generateName(int index, boolean randomize) {
        final String postfix = randomize ? Integer.toString(random.nextInt(newColumnRandomizeFactor)) : "";
        return super.generateName(index) + postfix;
    }

    // there seem to be issues around the transactionality of adding new columns
    // when this is fixed the test can be enabled
    @Ignore
    @Test
    public void testLoad() throws Exception {
        super.testLoad();
    }
}
