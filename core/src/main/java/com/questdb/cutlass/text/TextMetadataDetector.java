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

package com.questdb.cutlass.text;

import com.questdb.cutlass.text.typeprobe.TypeProbe;
import com.questdb.cutlass.text.typeprobe.TypeProbeCollection;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.DirectCharSink;
import com.questdb.std.str.StringSink;
import com.questdb.store.ColumnType;

import java.io.Closeable;

import static com.questdb.std.Chars.utf8DecodeMultiByte;

public class TextMetadataDetector implements TextLexer.Listener, Mutable, Closeable {
    private static final Log LOG = LogFactory.getLog(TextMetadataDetector.class);
    private final StringSink tempSink = new StringSink();
    private final ObjList<TextMetadata> _metadata = new ObjList<>();
    private final ObjList<String> _headers = new ObjList<>();
    private final IntList _blanks = new IntList();
    private final IntList _histogram = new IntList();
    private final CharSequenceObjHashMap<TextMetadata> schemaColumns = new CharSequenceObjHashMap<>();
    private final ObjectPool<TextMetadata> mPool = new ObjectPool<>(TextMetadata::new, 256);
    private final TypeProbeCollection typeProbeCollection;
    private final DirectCharSink utf8Sink;
    private int fieldCount;
    private boolean header = false;
    private boolean forceHeader = false;
    private CharSequence tableName;

    public TextMetadataDetector(
            TypeProbeCollection typeProbeCollection,
            TextConfiguration textConfiguration
    ) {
        this.typeProbeCollection = typeProbeCollection;
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkCapacity());
    }

    public static boolean utf8Decode(long lo, long hi, CharSink sink) {
        long p = lo;
        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = utf8DecodeMultiByte(p, hi, b, sink);
                if (n == -1) {
                    // UTF8 error
                    return false;
                }
                p += n;
            } else {
                sink.put((char) b);
                ++p;
            }
        }
        return true;
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
        mPool.clear();
    }

    @Override
    public void close() {
        Misc.free(utf8Sink);
    }

    public void evaluateResults(long lineCount, long errorCount) {
        // try calculate types counting all rows
        // if all types come up as strings, reduce lineCount by one and retry
        // if some fields come up as non-string after subtracting row - we have a header
        if ((calcTypes(lineCount - errorCount, true) && !calcTypes(lineCount - errorCount - 1, false)) || forceHeader) {
            // copy headers
            for (int i = 0; i < fieldCount; i++) {
                _metadata.getQuick(i).name = _headers.getQuick(i);
            }
            header = true;
        } else {
            LOG.info()
                    .$("no header [table=").$(tableName)
                    .$(", lineCount=").$(lineCount)
                    .$(", errorCount=").$(errorCount)
                    .$(", forceHeader=").$(forceHeader)
                    .$(']').$();
        }

        // make up field names if there is no header
        for (int i = 0; i < fieldCount; i++) {
            final TextMetadata metadata = _metadata.getQuick(i);
            if (metadata.name.length() == 0) {
                tempSink.clear();
                tempSink.put('f').put(i);
                metadata.name = tempSink.toString();
            }
        }

        // override calculated types with user-supplied information
        if (schemaColumns.size() > 0) {
            for (int i = 0, k = _metadata.size(); i < k; i++) {
                TextMetadata _m = _metadata.getQuick(i);
                TextMetadata m = schemaColumns.get(_m.name);
                if (m != null) {
                    m.copyTo(_m);
                }
            }
        }
    }

    public ObjList<TextMetadata> getMetadata() {
        return _metadata;
    }

    public boolean isHeader() {
        return header;
    }

    public void of(ObjList<TextMetadata> seedMetadata, boolean forceHeader) {
        clear();
        if (seedMetadata != null) {
            for (int i = 0, n = seedMetadata.size(); i < n; i++) {
                TextMetadata m = seedMetadata.getQuick(i);
                schemaColumns.put(m.name, m);
            }
        }
        this.forceHeader = forceHeader;
    }

    @Override
    public void onFields(long line, ObjList<DirectByteCharSequence> values, int fieldCount) {
        // keep first line in case its a header
        if (line == 0) {
            seedFields(fieldCount);
            stashPossibleHeader(values, fieldCount);
        }

        int count = typeProbeCollection.getProbeCount();
        for (int i = 0; i < fieldCount; i++) {
            DirectByteCharSequence cs = values.getQuick(i);
            if (cs.length() == 0) {
                _blanks.increment(i);
            }
            int offset = i * count;
            for (int k = 0; k < count; k++) {
                final TypeProbe probe = typeProbeCollection.getProbe(k);
                if (probe.probe(cs)) {
                    _histogram.increment(k + offset);
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
    private boolean calcTypes(long count, boolean setDefault) {
        boolean allStrings = true;
        int probeCount = typeProbeCollection.getProbeCount();
        for (int i = 0; i < fieldCount; i++) {
            int offset = i * probeCount;
            int blanks = _blanks.getQuick(i);
            boolean unprobed = true;
            TextMetadata m = _metadata.getQuick(i);

            for (int k = 0; k < probeCount; k++) {
                if (_histogram.getQuick(k + offset) + blanks == count && blanks < count) {
                    unprobed = false;
                    TypeProbe probe = typeProbeCollection.getProbe(k);
                    m.type = probe.getType();
                    m.dateFormat = probe.getDateFormat();
                    m.dateLocale = probe.getDateLocale();
                    if (allStrings) {
                        allStrings = false;
                    }
                    break;
                }
            }

            if (setDefault && unprobed) {
                m.type = ColumnType.STRING;
            }
        }

        return allStrings;
    }

    private String normalise(CharSequence seq) {
        boolean capNext = false;
        tempSink.clear();
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
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

                    if (tempSink.length() == 0 && Character.isDigit(c)) {
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
        return Chars.toString(tempSink);
    }

    private void seedFields(int count) {
        this._histogram.setAll((fieldCount = count) * typeProbeCollection.getProbeCount(), 0);
        this._blanks.setAll(count, 0);
        for (int i = 0; i < count; i++) {
            this._metadata.add(mPool.next());
        }
        this._headers.setAll(count, "");
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
    }

    private void stashPossibleHeader(ObjList<DirectByteCharSequence> values, int hi) {
        for (int i = 0; i < hi; i++) {
            DirectByteCharSequence value = values.getQuick(i);
            utf8Sink.clear();
            if (utf8Decode(value.getLo(), value.getHi(), utf8Sink)) {
                _headers.setQuick(i, normalise(utf8Sink));
            } else {
                LOG.info().$("utf8 error [table=").$(tableName).$(", line=0, col=").$(i).$(']').$();
            }
        }
    }
}
