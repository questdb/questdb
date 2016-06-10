/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.io.parser.listener;

import com.questdb.io.ImportedColumnMetadata;
import com.questdb.io.ImportedColumnType;
import com.questdb.io.Schema;
import com.questdb.io.parser.listener.probe.*;
import com.questdb.io.sink.StringSink;
import com.questdb.std.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"PL_PARALLEL_LISTS", "LII_LIST_INDEXED_ITERATING"})
public class MetadataExtractorListener implements Listener, Mutable {
    // order of probes in array is critical
    private static final TypeProbe probes[] = new TypeProbe[]{
            new IntProbe(),
            new LongProbe(),
            new DoubleProbe(),
            new BooleanProbe(),
            new DateIsoProbe(),
            new DateFmt1Probe(),
            new DateFmt2Probe(),
            new DateFmt3Probe()
    };
    private static final int probeCount = probes.length;
    private final StringSink tempSink = new StringSink();
    private final ObjList<ImportedColumnMetadata> _metadata = new ObjList<>();
    private final ObjList<String> _headers = new ObjList<>();
    private final IntList _blanks = new IntList();
    private final IntList _histogram = new IntList();
    private final CharSequenceObjHashMap<ImportedColumnMetadata> schemaColumns = new CharSequenceObjHashMap<>();
    private final ObjectPool<ImportedColumnMetadata> mPool;
    private int fieldCount;
    private boolean header = false;

    public MetadataExtractorListener(ObjectPool<ImportedColumnMetadata> mPool) {
        this.mPool = mPool;
    }

    @Override
    public void clear() {
        tempSink.clear();
        _headers.clear();
        _blanks.clear();
        _histogram.clear();
        fieldCount = 0;
        header = false;
        _metadata.clear();
        schemaColumns.clear();
    }

    public ObjList<ImportedColumnMetadata> getMetadata() {
        return _metadata;
    }

    public boolean isHeader() {
        return header;
    }

    public void of(Schema schema) {
        clear();
        if (schema != null) {
            ObjList<ImportedColumnMetadata> list = schema.getMetadata();
            for (int i = 0, n = list.size(); i < n; i++) {
                ImportedColumnMetadata m = list.getQuick(i);
                schemaColumns.put(m.name, m);
            }
        }
    }

    @Override
    public void onError(int line) {
    }

    @Override
    public void onFieldCount(int count) {
        this._histogram.setAll((fieldCount = count) * probeCount, 0);
        this._blanks.setAll(count, 0);
        for (int i = 0; i < count; i++) {
            this._metadata.add(mPool.next());
        }
        this._headers.setAll(count, null);
    }

    @Override
    public void onFields(int line, ObjList<DirectByteCharSequence> values, int hi) {
        // keep first line in case its a header
        if (line == 0) {
            stashPossibleHeader(values, hi);
        }

        for (int i = 0; i < hi; i++) {
            DirectByteCharSequence cs = values.getQuick(i);
            if (cs.length() == 0) {
                _blanks.increment(i);
            }
            int offset = i * probeCount;
            for (int k = 0; k < probeCount; k++) {
                if (probes[k].probe(cs)) {
                    _histogram.increment(k + offset);
                }
            }
        }
    }

    @Override
    public void onHeader(ObjList<DirectByteCharSequence> values, int hi) {

    }

    @Override
    public void onLineCount(int count) {
        // try calculate types counting all rows
        // if all types come up as strings, reduce count by one and retry
        // if some fields come up as non-string after subtracting row - we have a header
        if (calcTypes(count, true) && !calcTypes(count - 1, false)) {
            // copy headers
            for (int i = 0; i < fieldCount; i++) {
                _metadata.getQuick(i).name = _headers.getQuick(i);
            }
            header = true;
        }

        // make up field names if there is no header
        if (!header) {
            for (int i = 0; i < fieldCount; i++) {
                tempSink.clear();
                tempSink.put('f').put(i);
                _metadata.getQuick(i).name = tempSink.toString();
            }
        }

        // override calculated types with user-supplied information
        if (schemaColumns.size() > 0) {
            for (int i = 0, k = _metadata.size(); i < k; i++) {
                ImportedColumnMetadata _m = _metadata.getQuick(i);
                ImportedColumnMetadata m = schemaColumns.get(_m.name);
                if (m != null) {
                    _m.type = m.type;
                }
            }
        }
    }

    /**
     * Histogram contains counts for every probe that validates field. It is possible for multiple probes to validate same field.
     * It can happen because of two reasons.
     * <p>
     * probes are compatible, for example INT is compatible wth DOUBLE in a sense that DOUBLE probe will positively
     * validate every INT. If this the case we will use order of probes as priority. First probe wins
     * <p>
     * it is possible to have mixed types in same column, in which case column has to become string.
     * to establish if we have mixed column we check if probe count + blank values add up to total number of rows.
     */
    private boolean calcTypes(int count, boolean setDefault) {
        boolean allStrings = true;
        for (int i = 0; i < fieldCount; i++) {
            int offset = i * probeCount;
            int blanks = _blanks.getQuick(i);
            boolean unprobed = true;
            ImportedColumnMetadata m = _metadata.getQuick(i);

            for (int k = 0; k < probeCount; k++) {
                if (_histogram.getQuick(k + offset) + blanks == count && blanks < count) {
                    unprobed = false;
                    probes[k].getMetadata(m);
                    if (allStrings) {
                        allStrings = false;
                    }
                    break;
                }
            }

            if (setDefault && unprobed) {
                m.type = ImportedColumnType.STRING;
            }
        }

        return allStrings;
    }

    private String normalise(CharSequence seq) {
        boolean capNext = false;
        tempSink.clear();
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
            if (c > 2047) {
                continue;
            }
            switch (c) {
                case ' ':
                case '_':
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case '\0':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                    capNext = true;
                    break;
                default:
                    if (capNext) {
                        tempSink.put(Character.toUpperCase(c));
                        capNext = false;
                    } else {
                        tempSink.put(c);
                    }
                    break;
            }
        }
        return tempSink.toString();
    }

    private void stashPossibleHeader(ObjList<DirectByteCharSequence> values, int hi) {
        for (int i = 0; i < hi; i++) {
            _headers.setQuick(i, normalise(values.getQuick(i)));
        }
    }
}
