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

package io.questdb.log;

import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

import java.util.Enumeration;
import java.util.Properties;

public class DollarExprResolver implements Sinkable {

    private static final String DATE_FORMAT_KEY = "date:";
    private static final int NIL = -1;

    public static CharSequenceObjHashMap<CharSequence> adaptProperties(Properties props) {
        CharSequenceObjHashMap<CharSequence> properties = new CharSequenceObjHashMap<>(props.size());
        for (Enumeration<?> keys = props.propertyNames(); keys.hasMoreElements(); ) {
            String key = keys.nextElement().toString();
            properties.put(key, props.getProperty(key));
        }
        return properties;
    }

    private final TimestampFormatCompiler dateCompiler = new TimestampFormatCompiler();
    private final ObjList<Sinkable> locationComponents = new ObjList<>();
    private final StringSink resolveSink = new StringSink();
    private CharSequenceObjHashMap<CharSequence> properties;
    private CharSequence originalTxt;
    private long dateValue;


    public void setDateValue(long dateValue) {
        this.dateValue = dateValue;
    }

    public DollarExprResolver resolve(final CharSequence location, final long fileTimestamp) {
        return resolve(location, fileTimestamp, adaptProperties(System.getProperties()));
    }

    public DollarExprResolver resolve(CharSequence originalTxt, long dateValue, Properties properties) {
        return resolve(originalTxt, dateValue, adaptProperties(properties));
    }

    public DollarExprResolver resolve(
            CharSequence originalTxt,
            long dateValue,
            CharSequenceObjHashMap<CharSequence> properties
    ) {

        // if we find a $ then:
        //
        // 1.- it is the start of an expression: ${<DATE_FORMAT_KEY> <FORMAT>}
        //     where <DATE_FORMAT_KEY> is "date:",
        //           <FORMAT> can be whatever is supported by TimestampFormatCompiler
        //           for example:
        //             - dd/MM/y
        //             - yyyy-MM-dd HH:mm:ss
        //             - yyyy-MM-ddTHH:mm:ss.SSSz
        //             - MM/dd/y
        //             - yyyy-MM-ddTHH:mm:ss.SSSUUUz
        //             - etc
        //     the purpose of this is to have the LogWriter replace the expression
        //     with the DateFormatted string equivalent of fileTimestamp
        //     (fileTimestamp can be changed dynamically, which is in fact what
        //     LogRollingFileWriter does).
        // 2.- it is the start of an expression: $name, or ${name}

        this.originalTxt = originalTxt;
        this.dateValue = dateValue;
        this.properties = properties;
        locationComponents.clear();
        int dollarStart = NIL; // points at the $
        int dateStart = NIL; // points at the first char after {
        int lastExprEnd = 0; // points to the char right after the expression
        final int locationLen = this.originalTxt.length();
        for (int i = 0; i < locationLen; i++) {
            char c = this.originalTxt.charAt(i);
            switch (c) {
                case '$':
                    if (dollarStart != NIL) { // already found a $
                        if (i - dollarStart > 1) {
                            locationComponents.add(new EnvSinkable(resolveSysProp(dollarStart, i)));
                            lastExprEnd = i + 1;
                        } else {
                            throw new LogError("Unexpected '$' at position " + i);
                        }
                    } else {
                        if (i - lastExprEnd > 0) {
                            locationComponents.add(new SubStrSinkable(lastExprEnd, i));
                            lastExprEnd = i + 1;
                        }
                    }
                    dollarStart = i;
                    break;
                case '{':
                    if (dollarStart == NIL) {
                        continue;
                    }
                    dateStart = i + 1;
                    break;
                case '}':
                    if (dollarStart == NIL) {
                        continue;
                    }
                    if (dateStart == NIL) {
                        throw new LogError("Unexpected '}' at position " + i);
                    }
                    int keyLen = DATE_FORMAT_KEY.length();
                    int exprLen = i - dateStart;
                    if (exprLen == 0) {
                        throw new LogError("Missing expression at position " + dateStart);
                    }
                    int dateFormatStart = dateStart + keyLen;
                    if (exprLen >= keyLen && Chars.startsWith(this.originalTxt, dateStart, dateFormatStart, DATE_FORMAT_KEY)) {
                        DateFormat dateFormat;
                        String dateFormatStr = this.originalTxt.subSequence(dateFormatStart, i).toString().trim();
                        if (dateFormatStr.isEmpty()) {
                            throw new LogError("Missing expression at position " + dateFormatStart);
                        }
                        // TODO: unfortunately compilation will not throw any exception in the presence of a bad format
                        dateFormat = dateCompiler.compile(this.originalTxt, dateFormatStart, i, false);
                        locationComponents.add(new DateSinkable(dateFormat));
                    } else {
                        locationComponents.add(new EnvSinkable(resolveSysProp(dollarStart + 1, i)));
                    }
                    dateStart = NIL;
                    dollarStart = NIL;
                    lastExprEnd = i + 1;
                    break;
            }
        }
        if (dollarStart == NIL) {
            if (locationLen - lastExprEnd > 0) {
                locationComponents.add(new SubStrSinkable(lastExprEnd, locationLen));
            }
        } else {
            if (dateStart != NIL) {
                throw new LogError("Missing '}' at position " + locationLen);
            }
            if (locationLen - dollarStart > 1) {
                locationComponents.add(new EnvSinkable(resolveSysProp(dollarStart, locationLen)));
            } else {
                throw new LogError("Unexpected '$' at position " + dollarStart);
            }
        }
        return this;
    }

    @Override
    public String toString() {
        resolveSink.clear();
        toSink(resolveSink);
        return resolveSink.toString();
    }

    // for test purposes
    ObjList<Sinkable> getLocationComponents() {
        return locationComponents;
    }

    private CharSequence resolveSysProp(int start, int end) {
        String envKey = originalTxt.subSequence(start + 1, end).toString().trim();
        CharSequence envValue = properties.get(envKey);
        if (envValue == null) {
            throw new LogError("Undefined property: " + envKey);
        }
        return envValue;
    }

    @Override
    public void toSink(CharSink sink) {
        for (int i = 0, n = locationComponents.size(); i < n; i++) {
            locationComponents.getQuick(i).toSink(sink);
        }
    }

    private class SubStrSinkable implements Sinkable {
        protected final int start;
        protected final int end;

        public SubStrSinkable(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void toSink(CharSink sink) {
            sink.put(originalTxt, start, end);
        }
    }

    private class EnvSinkable implements Sinkable {
        private final CharSequence envValue;

        public EnvSinkable(CharSequence envValue) {
            this.envValue = envValue;
        }

        @Override
        public void toSink(CharSink sink) {
            sink.put(envValue);
        }
    }

    private class DateSinkable implements Sinkable {
        private final DateFormat format;

        public DateSinkable(DateFormat format) {
            this.format = format;
        }

        @Override
        public void toSink(CharSink sink) {
            format.format(
                    dateValue,
                    TimestampFormatUtils.enLocale,
                    null,
                    sink
            );
        }
    }
}
