/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.text.types;

import com.questdb.cairo.CairoException;
import com.questdb.cairo.ColumnType;
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.cutlass.text.TextConfiguration;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.DirectCharSink;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;

import java.io.IOException;
import java.io.InputStream;

public class TypeManager implements Mutable {
    private static final Log LOG = LogFactory.getLog(TypeManager.class);
    private static final int STATE_EXPECT_TOP = 0;
    private static final int STATE_EXPECT_FIRST_LEVEL_NAME = 1;
    private static final int STATE_EXPECT_DATE_FORMAT_ARRAY = 2;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_ARRAY = 3;
    private static final int STATE_EXPECT_DATE_FORMAT_VALUE = 4;
    private static final int STATE_EXPECT_DATE_LOCALE_VALUE = 5;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_VALUE = 6;
    private static final int STATE_EXPECT_TIMESTAMP_LOCALE_VALUE = 7;
    private static final int STATE_EXPECT_DATE_FORMAT_ENTRY = 8;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY = 9;
    private final ObjList<TypeAdapter> probes = new ObjList<>();
    private final int probeCount;
    private final StringAdapter stringAdapter;
    private final DirectCharSink utf8Sink;
    private final ObjectPool<DateAdapter> dateAdapterPool;
    private final ObjectPool<TimestampAdapter> timestampAdapterPool;
    private final SymbolAdapter symbolAdapter;
    private final JsonLexer jsonLexer;
    private final DateFormatFactory dateFormatFactory;
    private final DateLocaleFactory dateLocaleFactory;
    private final com.questdb.std.microtime.DateFormatFactory timestampFormatFactory;
    private final com.questdb.std.microtime.DateLocaleFactory timestampLocaleFactory;
    private int jsonState = STATE_EXPECT_TOP; // expect start of object
    private DateFormat jsonDateFormat;
    private DateLocale jsonDateLocale;
    private com.questdb.std.microtime.DateFormat jsonTimestampFormat;
    private com.questdb.std.microtime.DateLocale jsonTimestampLocale;

    public TypeManager(TextConfiguration configuration, DirectCharSink utf8Sink, JsonLexer jsonLexer) throws JsonException {
        this.utf8Sink = utf8Sink;
        this.dateAdapterPool = new ObjectPool<>(() -> new DateAdapter(utf8Sink), configuration.getDateAdapterPoolCapacity());
        this.timestampAdapterPool = new ObjectPool<>(() -> new TimestampAdapter(utf8Sink), configuration.getTimestampAdapterPoolCapacity());
        this.stringAdapter = new StringAdapter(utf8Sink);
        this.symbolAdapter = new SymbolAdapter(utf8Sink);
        this.jsonLexer = jsonLexer;
        addDefaultProbes();
        this.dateFormatFactory = new DateFormatFactory();
        this.dateLocaleFactory = DateLocaleFactory.INSTANCE;
        this.timestampFormatFactory = new com.questdb.std.microtime.DateFormatFactory();
        this.timestampLocaleFactory = com.questdb.std.microtime.DateLocaleFactory.INSTANCE;
        parseConfiguration(configuration.getAdapterSetConfigurationFileName());
        this.probeCount = probes.size();
    }

    @Override
    public void clear() {
        dateAdapterPool.clear();
        timestampAdapterPool.clear();
    }

    public TypeAdapter getProbe(int index) {
        return probes.getQuick(index);
    }

    public int getProbeCount() {
        return probeCount;
    }

    public TypeAdapter getTypeAdapter(int columnType) {
        switch (columnType) {
            case ColumnType.BYTE:
                return ByteAdapter.INSTANCE;
            case ColumnType.SHORT:
                return ShortAdapter.INSTANCE;
            case ColumnType.CHAR:
                return CharAdapter.INSTANCE;
            case ColumnType.INT:
                return IntAdapter.INSTANCE;
            case ColumnType.LONG:
                return LongAdapter.INSTANCE;
            case ColumnType.BOOLEAN:
                return BooleanAdapter.INSTANCE;
            case ColumnType.FLOAT:
                return FloatAdapter.INSTANCE;
            case ColumnType.DOUBLE:
                return DoubleAdapter.INSTANCE;
            case ColumnType.STRING:
                return stringAdapter;
            case ColumnType.SYMBOL:
                return symbolAdapter;
            default:
                throw CairoException.instance(0).put("no adapter for type [id=").put(columnType).put(", name=").put(ColumnType.nameOf(columnType)).put(']');
        }
    }

    public DateAdapter nextDateAdapter() {
        return dateAdapterPool.next();
    }

    public TimestampAdapter nextTimestampAdapter() {
        return timestampAdapterPool.next();
    }

    private void addDefaultProbes() {
        probes.add(getTypeAdapter(ColumnType.INT));
        probes.add(getTypeAdapter(ColumnType.LONG));
        probes.add(getTypeAdapter(ColumnType.DOUBLE));
        probes.add(getTypeAdapter(ColumnType.BOOLEAN));
    }

    ObjList<TypeAdapter> getAllAdapters() {
        return probes;
    }

    private void onJsonEvent(int code, CharSequence tag, int position) throws JsonException {
        switch (code) {
            case JsonLexer.EVT_OBJ_START:
                switch (jsonState) {
                    case STATE_EXPECT_TOP:
                        // this is top level object
                        // lets dive in
                        jsonState = STATE_EXPECT_FIRST_LEVEL_NAME;
                        break;
                    case STATE_EXPECT_DATE_FORMAT_VALUE:
                    case STATE_EXPECT_TIMESTAMP_FORMAT_VALUE:
                        throw JsonException.$(position, "format value expected (obj)");
                    case STATE_EXPECT_DATE_LOCALE_VALUE:
                    case STATE_EXPECT_TIMESTAMP_LOCALE_VALUE:
                        throw JsonException.$(position, "locale value expected (obj)");
                    case STATE_EXPECT_DATE_FORMAT_ENTRY:
                        jsonDateFormat = null;
                        jsonDateLocale = null;
                        break;
                    case STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY:
                        jsonTimestampFormat = null;
                        jsonTimestampLocale = null;
                        break;
                    default:
                        throw JsonException.$(position, "array expected (obj)");
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                switch (jsonState) {
                    case STATE_EXPECT_DATE_FORMAT_ENTRY: // we just closed a date object
                        if (jsonDateFormat == null) {
                            throw JsonException.$(position, "date format is missing");
                        }
                        probes.add(
                                new DateAdapter(utf8Sink)
                                        .of(
                                                jsonDateFormat,
                                                jsonDateLocale == null ? DateLocaleFactory.INSTANCE.getDefaultDateLocale() : jsonDateLocale
                                        )
                        );
                        break;
                    case STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY:
                        if (jsonTimestampFormat == null) {
                            throw JsonException.$(position, "timestamp format is missing");
                        }

                        probes.add(
                                new TimestampAdapter(utf8Sink)
                                        .of(
                                                jsonTimestampFormat,
                                                jsonTimestampLocale == null ? com.questdb.std.microtime.DateLocaleFactory.INSTANCE.getDefaultDateLocale() : jsonTimestampLocale
                                        )
                        );
                        break;
                    default:
                        // the only time we get here would be when
                        // main object is closed.
                        // other end_of_object cannot get there unless we
                        // allow to enter these objects in the first place
                        break;
                }
                break;
            case JsonLexer.EVT_ARRAY_END:
                jsonState = STATE_EXPECT_FIRST_LEVEL_NAME;
                break;
            case JsonLexer.EVT_NAME:
                switch (jsonState) {
                    case STATE_EXPECT_FIRST_LEVEL_NAME:
                        if (Chars.equals(tag, "date")) {
                            jsonState = STATE_EXPECT_DATE_FORMAT_ARRAY; // expect array with date formats
                        } else if (Chars.equals(tag, "timestamp")) {
                            jsonState = STATE_EXPECT_TIMESTAMP_FORMAT_ARRAY; // expect array with timestamp formats
                        } else {
                            // unknown tag name?
                            throw JsonException.$(position, "'date' and/or 'timestamp' expected");
                        }
                        break;
                    case STATE_EXPECT_DATE_FORMAT_ENTRY:
                        processEntry(tag, position, STATE_EXPECT_DATE_FORMAT_VALUE, STATE_EXPECT_DATE_LOCALE_VALUE);
                        break;
                    default:
                        processEntry(tag, position, STATE_EXPECT_TIMESTAMP_FORMAT_VALUE, STATE_EXPECT_TIMESTAMP_LOCALE_VALUE);
                        break;
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (jsonState) {
                    case STATE_EXPECT_DATE_FORMAT_VALUE:
                        // date format
                        assert jsonDateFormat == null;
                        if (Chars.equals("null", tag)) {
                            throw JsonException.$(position, "null format");
                        }
                        jsonDateFormat = dateFormatFactory.get(tag);
                        jsonState = STATE_EXPECT_DATE_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_DATE_LOCALE_VALUE: // date locale
                        assert jsonDateLocale == null;
                        jsonDateLocale = dateLocaleFactory.getDateLocale(tag);
                        if (jsonDateLocale == null) {
                            throw JsonException.$(position, "invalid [locale=").put(tag).put(']');
                        }
                        jsonState = STATE_EXPECT_DATE_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_TIMESTAMP_FORMAT_VALUE: // timestamp format
                        assert jsonTimestampFormat == null;
                        if (Chars.equals("null", tag)) {
                            throw JsonException.$(position, "null format");
                        }
                        jsonTimestampFormat = timestampFormatFactory.get(tag);
                        jsonState = STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_TIMESTAMP_LOCALE_VALUE:
                        assert jsonTimestampLocale == null;
                        jsonTimestampLocale = timestampLocaleFactory.getDateLocale(tag);
                        if (jsonTimestampLocale == null) {
                            throw JsonException.$(position, "invalid [locale=").put(tag).put(']');
                        }
                        jsonState = STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY;
                        break;
                    default:
                        // we are picking up values from attributes we don't expect
                        throw JsonException.$(position, "array expected (value)");
                }
                break;
            case JsonLexer.EVT_ARRAY_START:
                switch (jsonState) {
                    case STATE_EXPECT_DATE_FORMAT_ARRAY: // we are working on dates
                        jsonState = STATE_EXPECT_DATE_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_TIMESTAMP_FORMAT_ARRAY: // we are working on timestamps
                        jsonState = STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_DATE_FORMAT_VALUE:
                    case STATE_EXPECT_TIMESTAMP_FORMAT_VALUE:
                        throw JsonException.$(position, "format value expected (array)");
                    default:
                        throw JsonException.$(position, "locale value expected (array)");
                }
                break;
            default:
                break;
        }
    }

    private void parseConfiguration(String adapterSetConfigurationFileName) throws JsonException {
        LOG.info().$("loading [from=").$(adapterSetConfigurationFileName).$(']').$();
        try (InputStream stream = this.getClass().getResourceAsStream(adapterSetConfigurationFileName)) {
            if (stream == null) {
                throw JsonException.$(0, "could not find [resource=").put(adapterSetConfigurationFileName).put(']');
            }
            // here is where using direct memory is very disadvantageous
            // we will copy buffer twice to parse json, but luckily contents should be small
            // and we should be parsing this only once on startup
            byte[] heapBuffer = new byte[4096];
            long memBuffer = Unsafe.malloc(heapBuffer.length);
            try {
                int len;
                while ((len = stream.read(heapBuffer)) > 0) {
                    // copy to mem buffer
                    for (int i = 0; i < len; i++) {
                        Unsafe.getUnsafe().putByte(memBuffer + i, heapBuffer[i]);
                    }
                    jsonLexer.parse(memBuffer, len, this::onJsonEvent);
                }
                jsonLexer.clear();
            } finally {
                Unsafe.free(memBuffer, heapBuffer.length);
            }
        } catch (IOException e) {
            throw JsonException.$(0, "could not read [resource=").put(adapterSetConfigurationFileName).put(']');
        }
    }

    private void processEntry(CharSequence tag, int position, int stateExpectFormatValue, int stateExpectLocaleValue) throws JsonException {
        if (Chars.equals(tag, "format")) {
            jsonState = stateExpectFormatValue; // expect date format
        } else if (Chars.equals(tag, "locale")) {
            jsonState = stateExpectLocaleValue;
        } else {
            // unknown tag name?
            throw JsonException.$(position, "unknown [tag=").put(tag).put(']');
        }
    }
}