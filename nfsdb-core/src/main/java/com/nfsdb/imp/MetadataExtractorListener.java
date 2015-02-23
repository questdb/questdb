/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import com.nfsdb.imp.probes.FloatProbe;
import com.nfsdb.imp.probes.IntProbe;

public class MetadataExtractorListener implements Listener {

    // order of probes in array is critical
    private static final TypeProbe probes[] = new TypeProbe[]{new IntProbe(), new FloatProbe(), new DoubleProbe(), new BooleanProbe()};
    private static final int probeLen = probes.length;
    private int fieldCount;
    private int histogram[];
    private int blanks[];
    private ColumnMetadata metadata[];
    private String headers[];
    private boolean header = false;

    public ColumnMetadata[] getMetadata() {
        return metadata;
    }

    public boolean isHeader() {
        return header;
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onField(int line, CharSequence values[], int hi) {
        // keep first line in case its a header
        if (line == 0) {
            stashPossibleHeader(values, hi);
        }

        for (int i = 0; i < hi; i++) {
            if (values[i].length() == 0) {
                blanks[i]++;
            }
            int offset = i * probeLen;
            for (int k = 0; k < probeLen; k++) {
                if (probes[k].probe(values[i])) {
                    histogram[k + offset]++;
                }
            }
        }
    }

    @Override
    public void onFieldCount(int count) {
        this.histogram = new int[(fieldCount = count) * probeLen];
        this.blanks = new int[count];
        this.metadata = new ColumnMetadata[count];
        this.headers = new String[count];
    }

    @Override
    public void onHeader(CharSequence[] values, int hi) {

    }

    @Override
    public void onLineCount(int count) {
        // try calculate types counting all rows
        // if all types come up as strings, reduce count by one and retry
        // if some fields come up as non-string after subtracting row - we have a header
        if (calcTypes(count, true)) {
            if (!calcTypes(count - 1, false)) {
                // copy headers
                for (int i = 0; i < fieldCount; i++) {
                    metadata[i].name = headers[i];
                }
                header = true;
                return;
            }
        }

        for (int i = 0; i < fieldCount; i++) {
            metadata[i].name = "f" + i;
        }

    }

    private void stashPossibleHeader(CharSequence values[], int hi) {
        for (int i = 0; i < hi; i++) {
            headers[i] = values[i].toString();
        }
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

            for (int k = 0; k < probeLen; k++) {
                if (histogram[k + offset] + blanks == count && blanks < count) {
                    metadata[i] = probes[k].getMetadata();
                    if (allStrings) {
                        allStrings = false;
                    }
                    break;
                }
            }

            if (setDefault && metadata[i] == null) {
                ColumnMetadata meta = new ColumnMetadata();
                meta.type = ColumnType.STRING;
                meta.size = meta.avgSize + 4;
                metadata[i] = meta;
            }
        }

        return allStrings;
    }
}
