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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.questdb.cairo.ColumnType.*;

public class LineTcpReceiverDuplicateColumnsTest extends LineTcpReceiverLoadTest {
    private final char[] nonAsciiChars = {'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55};

    private final int duplicatesFactor = 4;
    private final int columnReorderingFactor = 4;
    private final int columnSkipFactor = 4;
    private final int maxNumOfSkippedCols = 2;
    private final int nonAsciiValueFactor = 4;

    public LineTcpReceiverDuplicateColumnsTest() {
        super(500, 10, 10, 10, 50);
    }

    protected LineTcpReceiverDuplicateColumnsTest(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis) {
        super(numOfLines, numOfIterations, numOfThreads, numOfTables, waitBetweenIterationsMillis);
    }

    @Override
    void addDuplicateColumn(LineData line, int colIndex, CharSequence colName) {
        if (random.nextInt(duplicatesFactor) == 0) {
            final CharSequence colValueDupe = generateValue(colIndex);
            line.add(colName, colValueDupe);
        }
    }

    @Override
    int[] getColumnIndexes() {
        return skipColumns(generateColumnOrdering());
    }

    private int[] generateColumnOrdering() {
        final int[] columnOrdering = new int[colNameBases.length];
        if (random.nextInt(columnReorderingFactor) == 0) {
            final List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < columnOrdering.length; i++) {
                indexes.add(i);
            }
            Collections.shuffle(indexes);
            for (int i = 0; i < columnOrdering.length; i++) {
                columnOrdering[i] = indexes.get(i);
            }
        } else {
            for (int i = 0; i < columnOrdering.length; i++) {
                columnOrdering[i] = i;
            }
        }
        return columnOrdering;
    }

    private int[] skipColumns(int[] originalColumnIndexes) {
        if (random.nextInt(columnSkipFactor) == 0) {
            // avoid list here and just copy slices of the original array into the new one
            final List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < originalColumnIndexes.length; i++) {
                indexes.add(originalColumnIndexes[i]);
            }
            final int numOfSkippedCols = random.nextInt(maxNumOfSkippedCols) + 1;
            for (int i = 0; i < numOfSkippedCols; i++) {
                final int skipIndex = random.nextInt(indexes.size());
                indexes.remove(skipIndex);
            }
            final int[] columnIndexes = new int[indexes.size()];
            for (int i = 0; i < columnIndexes.length; i++) {
                columnIndexes[i] = indexes.get(i);
            }
            return columnIndexes;
        }
        return originalColumnIndexes;
    }

    @Override
    String generateName(int index) {
        final int caseIndex = random.nextInt(colNameBases[index].length);
        return colNameBases[index][caseIndex];
    }

    @Override
    String generateValue(int index) {
        final String postfix;
        switch (colTypes[index]) {
            case DOUBLE:
                postfix = random.nextInt(9) + ".0";
                break;
            case STRING:
                postfix = Character.toString(random.nextInt(nonAsciiValueFactor) == 0
                        ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                break;
            default:
                postfix = "";
        }
        return colValueBases[index] + postfix;
    }
}
