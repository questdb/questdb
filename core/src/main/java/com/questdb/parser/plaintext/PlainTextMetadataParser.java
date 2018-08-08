/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.parser.plaintext;

import com.questdb.parser.ImportedColumnMetadata;
import com.questdb.parser.typeprobe.TypeProbe;
import com.questdb.parser.typeprobe.TypeProbeCollection;
import com.questdb.std.*;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;

public class PlainTextMetadataParser implements PlainTextParser, Mutable {
    private final StringSink tempSink = new StringSink();
    private final ObjList<ImportedColumnMetadata> _metadata = new ObjList<>();
    private final ObjList<String> _headers = new ObjList<>();
    private final IntList _blanks = new IntList();
    private final IntList _histogram = new IntList();
    private final CharSequenceObjHashMap<ImportedColumnMetadata> schemaColumns = new CharSequenceObjHashMap<>();
    private final ObjectPool<ImportedColumnMetadata> mPool;
    private final TypeProbeCollection typeProbeCollection;
    private int fieldCount;
    private boolean header = false;
    private boolean forceHeader = false;

    public PlainTextMetadataParser(ObjectPool<ImportedColumnMetadata> mPool, TypeProbeCollection typeProbeCollection) {
        this.mPool = mPool;
        this.typeProbeCollection = typeProbeCollection;
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
        forceHeader = false;
    }

    public ObjList<ImportedColumnMetadata> getMetadata() {
        return _metadata;
    }

    public boolean isHeader() {
        return header;
    }

    public void of(ObjList<ImportedColumnMetadata> schema, boolean forceHeader) {
        clear();
        if (schema != null) {
            for (int i = 0, n = schema.size(); i < n; i++) {
                ImportedColumnMetadata m = schema.getQuick(i);
                schemaColumns.put(m.name, m);
            }
        }
        this.forceHeader = forceHeader;
    }

    @Override
    public void onError(int line) {
    }

    @Override
    public void onFieldCount(int count) {
        this._histogram.setAll((fieldCount = count) * typeProbeCollection.getProbeCount(), 0);
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

        int count = typeProbeCollection.getProbeCount();
        for (int i = 0; i < hi; i++) {
            DirectByteCharSequence cs = values.getQuick(i);
            if (cs.length() == 0) {
                _blanks.increment(i);
            }
            int offset = i * count;
            for (int k = 0; k < count; k++) {
                TypeProbe probe = typeProbeCollection.getProbe(k);
                if (probe.probe(cs)) {
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
        if ((calcTypes(count, true) && !calcTypes(count - 1, false)) || forceHeader) {
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
                    m.copyTo(_m);
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
        int probeCount = typeProbeCollection.getProbeCount();
        for (int i = 0; i < fieldCount; i++) {
            int offset = i * probeCount;
            int blanks = _blanks.getQuick(i);
            boolean unprobed = true;
            ImportedColumnMetadata m = _metadata.getQuick(i);

            for (int k = 0; k < probeCount; k++) {
                if (_histogram.getQuick(k + offset) + blanks == count && blanks < count) {
                    unprobed = false;
                    TypeProbe probe = typeProbeCollection.getProbe(k);
                    m.importedColumnType = probe.getType();
                    m.pattern = probe.getFormat();
                    m.dateFormat = probe.getDateFormat();
                    m.dateLocale = probe.getDateLocale();
                    if (allStrings) {
                        allStrings = false;
                    }
                    break;
                }
            }

            if (setDefault && unprobed) {
                m.importedColumnType = ColumnType.STRING;
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

                    if (i == 0 && Character.isDigit(c)) {
                        tempSink.put('_');
                    }

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
