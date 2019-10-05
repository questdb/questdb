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

package io.questdb.cutlass.text.types;

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.time.DateFormat;
import io.questdb.std.time.DateFormatFactory;
import io.questdb.std.time.DateLocale;
import io.questdb.std.time.DateLocaleFactory;

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
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_VALUE = 6;
    private static final int STATE_EXPECT_TIMESTAMP_LOCALE_VALUE = 7;
    private static final int STATE_EXPECT_DATE_FORMAT_ENTRY = 8;
    private static final int STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY = 9;
    private final ObjList<DateFormat> dateFormats = new ObjList<>();
    private final ObjList<DateLocale> dateLocales = new ObjList<>();
    private final ObjList<io.questdb.std.microtime.DateFormat> timestampFormats = new ObjList<>();
    private final ObjList<io.questdb.std.microtime.DateLocale> timestampLocales = new ObjList<>();
    private final DateFormatFactory dateFormatFactory;
    private final DateLocaleFactory dateLocaleFactory;
    private final io.questdb.std.microtime.DateFormatFactory timestampFormatFactory;
    private final io.questdb.std.microtime.DateLocaleFactory timestampLocaleFactory;
    private int jsonState = STATE_EXPECT_TOP; // expect start of object
    private DateFormat jsonDateFormat;
    private DateLocale jsonDateLocale;
    private io.questdb.std.microtime.DateFormat jsonTimestampFormat;
    private io.questdb.std.microtime.DateLocale jsonTimestampLocale;

    public InputFormatConfiguration(
            DateFormatFactory dateFormatFactory,
            DateLocaleFactory dateLocaleFactory,
            io.questdb.std.microtime.DateFormatFactory timestampFormatFactory,
            io.questdb.std.microtime.DateLocaleFactory timestampLocaleFactory
    ) {
        this.dateFormatFactory = dateFormatFactory;
        this.dateLocaleFactory = dateLocaleFactory;
        this.timestampFormatFactory = timestampFormatFactory;
        this.timestampLocaleFactory = timestampLocaleFactory;
    }

    public void clear() {
        dateFormats.clear();
        dateLocales.clear();
        timestampFormats.clear();
        timestampLocales.clear();
        jsonState = STATE_EXPECT_TOP;
        jsonDateFormat = null;
        jsonDateLocale = null;
        jsonTimestampFormat = null;
        jsonTimestampLocale = null;
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

    public io.questdb.std.microtime.DateFormatFactory getTimestampFormatFactory() {
        return timestampFormatFactory;
    }

    public ObjList<io.questdb.std.microtime.DateFormat> getTimestampFormats() {
        return timestampFormats;
    }

    public io.questdb.std.microtime.DateLocaleFactory getTimestampLocaleFactory() {
        return timestampLocaleFactory;
    }

    public ObjList<io.questdb.std.microtime.DateLocale> getTimestampLocales() {
        return timestampLocales;
    }

    public void parseConfiguration(JsonLexer jsonLexer, String adapterSetConfigurationFileName) throws JsonException {

        this.clear();
        jsonLexer.clear();

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
                    jsonLexer.parse(memBuffer, memBuffer + len, this::onJsonEvent);
                }
                jsonLexer.clear();
            } finally {
                Unsafe.free(memBuffer, heapBuffer.length);
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
                        dateLocales.add(jsonDateLocale == null ? DateLocaleFactory.INSTANCE.getDefaultDateLocale() : jsonDateLocale);
                        break;
                    case STATE_EXPECT_TIMESTAMP_FORMAT_ENTRY:
                        if (jsonTimestampFormat == null) {
                            throw JsonException.$(position, "timestamp format is missing");
                        }

                        timestampFormats.add(jsonTimestampFormat);
                        timestampLocales.add(jsonTimestampLocale == null ? io.questdb.std.microtime.DateLocaleFactory.INSTANCE.getDefaultDateLocale() : jsonTimestampLocale);
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
