/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.imp;

import com.nfsdb.column.ColumnType;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.imp.probes.BooleanProbe;
import com.nfsdb.imp.probes.DoubleProbe;
import com.nfsdb.imp.probes.IntProbe;

import java.util.List;

public class FieldTypeDetector implements Csv.Listener {

    // order of probes in array is critical
    private static final TypeProbe probes[] = new TypeProbe[]{new IntProbe(), new DoubleProbe(), new BooleanProbe()};
    private static final int probeLen = probes.length;
    private int fieldCount;
    private int histogram[];
    private int blanks[];
    private boolean header = false;
    private ColumnType types[];

    public ColumnType getTypes(int index) {
        return types[index];
    }

    public boolean isHeader() {
        return header;
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onField(int index, CharSequence value, int line, boolean eol) {
        int offset = index * probeLen;
        if (value.length() == 0) {
            blanks[index]++;
        }
        for (int i = 0; i < probeLen; i++) {
            if (probes[i].probe(value)) {

                histogram[i + offset]++;
            }
        }
    }

    @Override
    public void onFieldCount(int count) {
        this.histogram = new int[(fieldCount = count) * probeLen];
        this.blanks = new int[count];
        this.types = new ColumnType[count];
    }

    @Override
    public void onLineCount(int count) {
        // try calculate types counting all rows
        // if all types come up as strings, reduce count by one and retry
        // if some fields come up as non-string after subtracting row - we have a header
        if (calcTypes(count, true)) {
            header = !calcTypes(count - 1, false);
        }
    }

    @Override
    public void onNames(List<ColumnMetadata> meta) {

    }

    /**
     * Histogram contains counts for every probe that validates field. It is possible for multiple probes to validate same field.
     * It can happen because of two reasons.
     * <p/>
     * probes are compatible, for example INT is compatible wth DOUBLE in a sense that DOUBLE probe will positively
     * validate every INT. If this the case we will use order of probes as priority. First probe wins
     * <p/>
     * it is possible to have mixed types in same column, in which case column has to become string.
     * to establish if we have mixed column we check if probe count + blank values add up to total number of rows.
     */
    private boolean calcTypes(int count, boolean setDefault) {
        boolean allStrings = true;
        for (int i = 0; i < fieldCount; i++) {
            int offset = i * probeLen;
            int blanks = this.blanks[i];

            for (int k = 1; k < probeLen; k++) {
                if (histogram[k + offset] + blanks == count && blanks < count) {
                    types[i] = probes[k].getType();
                    if (allStrings) {
                        allStrings = false;
                    }
                    break;
                }
            }

            if (setDefault && types[i] == null) {
                types[i] = ColumnType.STRING;
            }
        }

        return allStrings;
    }
}
