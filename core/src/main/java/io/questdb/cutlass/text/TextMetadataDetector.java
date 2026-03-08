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

package io.questdb.cutlass.text;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

public class TextMetadataDetector implements CsvTextLexer.Listener, Mutable, Closeable {
    private static final Log LOG = LogFactory.getLog(TextMetadataDetector.class);
    private final IntList _blanks = new IntList();
    private final IntList _histogram = new IntList();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final ObjList<TypeAdapter> columnTypes = new ObjList<>();
    private final int defaultColumnType;
    private final CharSequenceObjHashMap<TypeAdapter> schemaColumns = new CharSequenceObjHashMap<>();
    private final StringSink tempSink = new StringSink();
    private final TypeManager typeManager;
    private final LowerCaseCharSequenceHashSet uniqueColumnNames = new LowerCaseCharSequenceHashSet();
    private final DirectUtf16Sink utf8Sink;
    private int fieldCount;
    private boolean forceHeader = false;
    private boolean header = false;
    private CharSequence tableName;

    public TextMetadataDetector(
            TypeManager typeManager,
            TextConfiguration textConfiguration
    ) {
        this.typeManager = typeManager;
        this.utf8Sink = new DirectUtf16Sink(textConfiguration.getUtf8SinkSize());
        this.defaultColumnType = textConfiguration.isUseLegacyStringDefault() ? ColumnType.STRING : ColumnType.VARCHAR;
    }

    @Override
    public void clear() {
        tempSink.clear();
        columnNames.clear();
        uniqueColumnNames.clear();
        _blanks.clear();
        _histogram.clear();
        fieldCount = 0;
        header = false;
        columnTypes.clear();
        schemaColumns.clear();
        forceHeader = false;
    }

    @Override
    public void close() {
        Misc.free(utf8Sink);
    }

    public void evaluateResults(long lineCount, long errorCount) {
        // try to calculate types counting all rows
        // if all types come up as strings, reduce lineCount by one and retry
        // if some fields come up as non-string after subtracting row - we have a header
        if ((calcTypes(lineCount - errorCount, true) && !calcTypes(lineCount - errorCount - 1, false)) || forceHeader) {
            // copy headers
            header = true;
        } else {
            LOG.info()
                    .$("no header [table=").$safe(tableName)
                    .$(", lineCount=").$(lineCount)
                    .$(", errorCount=").$(errorCount)
                    .$(", forceHeader=").$(forceHeader)
                    .$(']').$();
        }

        for (int i = 0; i < fieldCount; i++) {
            if (!header || columnNames.getQuick(i).length() == 0) {
                tempSink.clear();
                tempSink.put('f').put(i);

                if (header) {
                    for (int attempt = 0; attempt < 20; attempt++) {
                        if (!columnNames.contains(tempSink)) {
                            break;
                        }

                        tempSink.put('_');
                    }

                    if (columnNames.contains(tempSink)) {
                        throw TextException.$("Failed to generate unique name for column [no=").put(i).put("]");
                    }
                }

                columnNames.setQuick(i, tempSink.toString());
            }

            if (!uniqueColumnNames.add(columnNames.getQuick(i))) {
                throw TextException.$("duplicate column name found [no=").put(i).put(",name=").put(columnNames.get(i)).put(']');
            }
        }

        // override calculated types with user-supplied information
        if (schemaColumns.size() > 0) {
            for (int i = 0, k = columnNames.size(); i < k; i++) {
                TypeAdapter type = schemaColumns.get(columnNames.getQuick(i));
                if (type != null) {
                    columnTypes.setQuick(i, type);
                }
            }
        }
    }

    public boolean isHeader() {
        return header;
    }

    public void of(CharSequence tableName, ObjList<CharSequence> names, ObjList<TypeAdapter> types, boolean forceHeader) {
        clear();
        if (names != null && types != null) {
            final int n = names.size();
            assert n == types.size();
            for (int i = 0; i < n; i++) {
                schemaColumns.put(names.getQuick(i), types.getQuick(i));
            }
        }
        this.forceHeader = forceHeader;
        this.tableName = tableName;
    }

    @Override
    public void onFields(long line, ObjList<DirectUtf8String> values, int fieldCount) {
        // keep first line in case it's a header
        if (line == 0) {
            seedFields(fieldCount);
            stashPossibleHeader(values, fieldCount);
        }

        int count = typeManager.getProbeCount();
        for (int i = 0; i < fieldCount; i++) {
            DirectUtf8Sequence cs = values.getQuick(i);
            if (cs.size() == 0) {
                _blanks.increment(i);
            }
            int offset = i * count;
            for (int k = 0; k < count; k++) {
                final TypeAdapter probe = typeManager.getProbe(k);
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
     * Probes are compatible, for example INT is compatible with DOUBLE in a sense that DOUBLE probe will positively
     * validate every INT. If this the case we will use order of probes as priority. First probe wins.
     * <p>
     * It is possible to have mixed types in same column, in which case column has to become string.
     * To establish if we have mixed column we check if probe count + blank values add up to total number of rows.
     */
    private boolean calcTypes(long count, boolean setDefault) {
        boolean allStrings = true;
        int probeCount = typeManager.getProbeCount();
        for (int i = 0; i < fieldCount; i++) {
            int offset = i * probeCount;
            int blanks = _blanks.getQuick(i);
            boolean unprobed = true;

            for (int k = 0; k < probeCount; k++) {
                if (_histogram.getQuick(k + offset) + blanks == count && blanks < count) {
                    unprobed = false;
                    columnTypes.setQuick(i, typeManager.getProbe(k));
                    if (allStrings && typeManager.getProbe(k).getType() != ColumnType.CHAR) {
                        allStrings = false;
                    }
                    break;
                }
            }

            if (setDefault && unprobed) {
                columnTypes.setQuick(i, typeManager.getTypeAdapter(defaultColumnType));
            }
        }

        return allStrings;
    }

    // metadata detector is essentially part of text lexer
    // we can potentially keep a cache of char sequences until the whole
    // system is reset, similar to flyweight char sequence over array of chars
    //NOTE! should be kept consistent with TableUtils.isValidColumnName()
    private String normalise(CharSequence seq) {
        boolean capNext = false;
        tempSink.clear();
        for (int i = 0, l = seq.length(); i < l; i++) {
            char c = seq.charAt(i);
            switch (c) {
                case ' ':
                case '?':
                case '.':
                case ',':
                case '\'':
                case '\"':
                case '\\':
                case '/':
                case ':':
                case ')':
                case '(':
                case '+':
                case '-':
                case '*':
                case '%':
                case '~':
                case '\u0000':
                case '\u0001':
                case '\u0002':
                case '\u0003':
                case '\u0004':
                case '\u0005':
                case '\u0006':
                case '\u0007':
                case '\u0008':
                case '\u0009':
                case '\u000B':
                case '\u000c':
                case '\r':
                case '\n':
                case '\u000e':
                case '\u000f':
                case '\u007f':
                    capNext = true;
                case 0xfeff: // UTF-8 BOM (Byte Order Mark) can appear at the beginning of a character stream
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
        this._histogram.setAll((fieldCount = count) * typeManager.getProbeCount(), 0);
        this._blanks.setAll(count, 0);
        this.columnTypes.extendAndSet(count - 1, null);
        this.columnNames.setAll(count, "");
    }

    private void stashPossibleHeader(ObjList<DirectUtf8String> values, int hi) {
        for (int i = 0; i < hi; i++) {
            DirectUtf8Sequence value = values.getQuick(i);
            utf8Sink.clear();
            if (Utf8s.utf8ToUtf16(value.lo(), value.hi(), utf8Sink)) {
                columnNames.setQuick(i, normalise(utf8Sink));
            } else {
                LOG.info().$("utf8 error [table=").$safe(tableName).$(", line=0, col=").$(i).$(']').$();
                columnNames.setQuick(i, "");
            }
        }
    }

    ObjList<CharSequence> getColumnNames() {
        return columnNames;
    }

    ObjList<TypeAdapter> getColumnTypes() {
        return columnTypes;
    }
}
