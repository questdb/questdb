/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.text.types;

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.TimestampFormatFactory;
import io.questdb.std.datetime.millitime.DateFormatFactory;

import java.io.IOException;
import java.io.InputStream;

public class InputFormatConfiguration {
    private final static Log LOG = LogFactory.getLog(InputFormatConfiguration.class);
    private static final int STATE_EXPECT_TOP = 0;
    private static final int STATE_EXPECT_FIRST_LEVEL_NAME = 1;
    private static final int STATE_EXPECT_DATE_FORMAT_ARRAY = 2;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_ARRAY = 3;
    private static final int STATE_EXPECT_DATE_FORMAT_VALUE = 4;
    private static final int STATE_EXPECT_DATE_LOCALE_VALUE = 5;
    private static final int STATE_EXPECT_DATE_UTF8_VALUE = 10;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_VALUE = 6;
    private static final int STATE_EXPECT_TIMESTAMP_LOCALE_VALUE = 7;
    private static final int STATE_EXPECT_DATE_FORMAT_ENTRY = 8;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY = 9;
    private static final int STATE_EXPECT_TIMESTAMP_UTF8_VALUE = 11;
    private final ObjList<DateFormat> dateFormats = new ObjList<>();
    private final ObjList<DateLocale> dateLocales = new ObjList<>();
    private final IntList dateUtf8Flags = new IntList();
    private final ObjList<DateFormat> timestampFormats = new ObjList<>();
    private final ObjList<DateLocale> timestampLocales = new ObjList<>();
    private final IntList timestampUtf8Flags = new IntList();
    private final DateFormatFactory dateFormatFactory;
    private final DateLocaleFactory dateLocaleFactory;
    private final TimestampFormatFactory timestampFormatFactory;
    private int jsonState = STATE_EXPECT_TOP; // expect start of object
    private DateFormat jsonDateFormat;
    private DateLocale jsonDateLocale;
    private boolean jsonDateUtf8;
    private DateFormat jsonTimestampFormat;
    private DateLocale jsonTimestampLocale;
    private boolean jsonTimestampUtf8;
    private final DateLocale dateLocale;

    public InputFormatConfiguration(
            DateFormatFactory dateFormatFactory,
            DateLocaleFactory dateLocaleFactory,
            TimestampFormatFactory timestampFormatFactory,
            DateLocale dateLocale
    ) {
        this.dateFormatFactory = dateFormatFactory;
        this.dateLocaleFactory = dateLocaleFactory;
        this.timestampFormatFactory = timestampFormatFactory;
        this.dateLocale = dateLocale;
    }

    public void clear() {
        dateFormats.clear();
        dateLocales.clear();
        dateUtf8Flags.clear();
        timestampFormats.clear();
        timestampLocales.clear();
        timestampUtf8Flags.clear();
        jsonState = STATE_EXPECT_TOP;
        jsonDateFormat = null;
        jsonDateLocale = null;
        jsonDateUtf8 = false;
        jsonTimestampFormat = null;
        jsonTimestampLocale = null;
        jsonTimestampUtf8 = false;
    }

    public DateFormatFactory getDateFormatFactory() {
        return dateFormatFactory;
    }

    public ObjList<DateFormat> getDateFormats() {
        return dateFormats;
    }

    public DateLocaleFactory getDateLocaleFactory() {
        return dateLocaleFactory;
    }

    public ObjList<DateLocale> getDateLocales() {
        return dateLocales;
    }

    public IntList getDateUtf8Flags() {
        return dateUtf8Flags;
    }

    public TimestampFormatFactory getTimestampFormatFactory() {
        return timestampFormatFactory;
    }

    public ObjList<DateFormat> getTimestampFormats() {
        return timestampFormats;
    }

    public ObjList<DateLocale> getTimestampLocales() {
        return timestampLocales;
    }

    public IntList getTimestampUtf8Flags() {
        return timestampUtf8Flags;
    }

    public void parseConfiguration(JsonLexer jsonLexer, String adapterSetConfigurationFileName) throws JsonException {

        this.clear();
        jsonLexer.clear();

        final JsonParser parser = this::onJsonEvent;

        LOG.info().$("loading [from=").$(adapterSetConfigurationFileName).$(']').$();
        try (InputStream stream = this.getClass().getResourceAsStream(adapterSetConfigurationFileName)) {
            if (stream == null) {
                throw JsonException.$(0, "could not find [resource=").put(adapterSetConfigurationFileName).put(']');
            }
            // here is where using direct memory is very disadvantageous
            // we will copy buffer twice to parse json, but luckily contents should be small
            // and we should be parsing this only once on startup
            byte[] heapBuffer = new byte[4096];
            long memBuffer = Unsafe.malloc(heapBuffer.length, MemoryTag.NATIVE_DEFAULT);
            try {
                int len;
                while ((len = stream.read(heapBuffer)) > 0) {
                    // copy to mem buffer
                    for (int i = 0; i < len; i++) {
                        Unsafe.getUnsafe().putByte(memBuffer + i, heapBuffer[i]);
                    }
                    jsonLexer.parse(memBuffer, memBuffer + len, parser);
                }
                jsonLexer.clear();
            } finally {
                Unsafe.free(memBuffer, heapBuffer.length, MemoryTag.NATIVE_DEFAULT);
            }
        } catch (IOException e) {
            throw JsonException.$(0, "could not read [resource=").put(adapterSetConfigurationFileName).put(']');
        }
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
                        dateFormats.add(jsonDateFormat);
                        dateLocales.add(jsonDateLocale == null ? dateLocale : jsonDateLocale);
                        dateUtf8Flags.add(jsonDateUtf8 ? 1 : 0);
                        break;
                    case STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY:
                        if (jsonTimestampFormat == null) {
                            throw JsonException.$(position, "timestamp format is missing");
                        }

                        timestampFormats.add(jsonTimestampFormat);
                        timestampLocales.add(jsonTimestampLocale == null ? dateLocale : jsonTimestampLocale);
                        timestampUtf8Flags.add(jsonTimestampUtf8 ? 1 : 0);
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
                        processEntry(tag, position, STATE_EXPECT_DATE_FORMAT_VALUE, STATE_EXPECT_DATE_LOCALE_VALUE, STATE_EXPECT_DATE_UTF8_VALUE);
                        break;
                    default:
                        processEntry(tag, position, STATE_EXPECT_TIMESTAMP_FORMAT_VALUE, STATE_EXPECT_TIMESTAMP_LOCALE_VALUE, STATE_EXPECT_TIMESTAMP_UTF8_VALUE);
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
                        jsonDateLocale = dateLocaleFactory.getLocale(tag);
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
                        jsonTimestampLocale = dateLocaleFactory.getLocale(tag);
                        if (jsonTimestampLocale == null) {
                            throw JsonException.$(position, "invalid [locale=").put(tag).put(']');
                        }
                        jsonState = STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_TIMESTAMP_UTF8_VALUE:
                        jsonTimestampUtf8 = Chars.equalsLowerCaseAscii(tag, "true");
                        jsonState = STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY;
                        break;
                    case STATE_EXPECT_DATE_UTF8_VALUE:
                        jsonDateUtf8 = Chars.equalsLowerCaseAscii(tag, "true");
                        jsonState = STATE_EXPECT_DATE_FORMAT_ENTRY;
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

    private void processEntry(CharSequence tag, int position, int stateExpectFormatValue, int stateExpectLocaleValue, int stateExpectUtf8Value) throws JsonException {
        if (Chars.equals(tag, "format")) {
            jsonState = stateExpectFormatValue; // expect date format
        } else if (Chars.equals(tag, "locale")) {
            jsonState = stateExpectLocaleValue;
        } else if (Chars.equals(tag, "utf8")) {
            jsonState = stateExpectUtf8Value;
        } else {
            // unknown tag name?
            throw JsonException.$(position, "unknown [tag=").put(tag).put(']');
        }
    }
}
