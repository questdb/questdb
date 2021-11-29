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

import java.util.Map;

public class TemplateParser implements Sinkable {

    public static CharSequenceObjHashMap<CharSequence> adaptMap(Map<String, String> props) {
        CharSequenceObjHashMap<CharSequence> properties = new CharSequenceObjHashMap<>(props.size());
        for (String key : props.keySet()) {
            properties.put(key, props.get(key));
        }
        return properties;
    }

    private static final String DATE_FORMAT_KEY = "date:";
    private static final int NIL = -1;

    private final TimestampFormatCompiler dateCompiler = new TimestampFormatCompiler();
    private final StringSink resolveSink = new StringSink();
    private final ObjList<Sinkable> txtComponents = new ObjList<>();
    private final CharSequenceIntHashMap keyStartIdxs = new CharSequenceIntHashMap();
    private CharSequenceObjHashMap<CharSequence> props;
    private CharSequence originalTxt;
    private long dateValue;


    public TemplateParser parseEnv(CharSequence txt, long dateValue) {
        return parse(txt, dateValue, adaptMap(System.getenv()));
    }

    public TemplateParser parse(CharSequence txt, long dateValue, Map<String, String> props) {
        return parse(txt, dateValue, adaptMap(props));
    }

    public TemplateParser parse(CharSequence txt, long dateValue, CharSequenceObjHashMap<CharSequence> props) {
        originalTxt = txt;
        this.dateValue = dateValue;
        this.props = props;
        txtComponents.clear();
        keyStartIdxs.clear();
        int dollarStart = NIL; // points at $
        int keyStart = NIL;   // points at the first char after {
        int lastExprEnd = 0;   // points to the char right after the expression
        int curlyBraces = 0;
        final int locationLen = originalTxt.length();
        for (int i = 0; i < locationLen; i++) {
            char c = originalTxt.charAt(i);
            switch (c) {
                case '$':
                    if (dollarStart != NIL) { // already found a $
                        if (i - dollarStart > 1) {
                            txtComponents.add(resolveEnv(dollarStart, dollarStart + 1, i));
                            lastExprEnd = i + 1;
                        } else {
                            throw new LogError("Unexpected '$' at position " + i);
                        }
                    } else {
                        if (i - lastExprEnd > 0) {
                            txtComponents.add(new SSubStr(lastExprEnd, i));
                            lastExprEnd = i + 1;
                        }
                    }
                    dollarStart = i;
                    break;
                case '{':
                    curlyBraces++;
                    if (dollarStart == NIL) {
                        continue;
                    }
                    keyStart = i + 1;
                    break;
                case '}':
                    curlyBraces--;
                    if (dollarStart == NIL) {
                        continue;
                    }
                    if (keyStart == NIL) {
                        txtComponents.add(resolveEnv(dollarStart, dollarStart + 1, i));
                        lastExprEnd = i;
                    } else {
                        int exprLen = i - keyStart;
                        if (exprLen == 0) {
                            throw new LogError("Missing expression at position " + keyStart);
                        }
                        int formatStart = keyStart + DATE_FORMAT_KEY.length();
                        if (Chars.startsWith(originalTxt, keyStart, formatStart, DATE_FORMAT_KEY)) {
                            txtComponents.add(new SDate(formatStart, i));
                        } else {
                            txtComponents.add(resolveEnv(dollarStart, dollarStart + 2, i));
                        }
                        lastExprEnd = i + 1;
                    }
                    keyStart = NIL;
                    dollarStart = NIL;
            }
        }
        if (dollarStart == NIL) {
            if (curlyBraces != 0) {
                throw new LogError("Mismatched '{}' at position " + lastExprEnd);
            }
            if (locationLen - lastExprEnd > 0) {
                txtComponents.add(new SSubStr(lastExprEnd, locationLen));
            }
        } else {
            if (keyStart != NIL) {
                throw new LogError("Missing '}' at position " + locationLen);
            }
            if (locationLen - dollarStart > 1) {
                txtComponents.add(resolveEnv(dollarStart, dollarStart + 1, locationLen));
            } else {
                throw new LogError("Unexpected '$' at position " + dollarStart);
            }
        }
        return this;
    }

    public void setDateValue(long dateValue) {
        this.dateValue = dateValue;
    }

    public int getKeyOffset(CharSequence key) {
        return keyStartIdxs.get(key); // relative to originalTxt
    }

    public ObjList<Sinkable> getLocationComponents() {
        return txtComponents;
    }

    @Override
    public void toSink(CharSink sink) {
        for (int i = 0, n = txtComponents.size(); i < n; i++) {
            sink.put(txtComponents.getQuick(i));
        }
    }

    @Override
    public String toString() {
        resolveSink.clear();
        for (int i = 0, n = txtComponents.size(); i < n; i++) {
            resolveSink.put(txtComponents.getQuick(i));
        }
        return resolveSink.toString();
    }

    private Sinkable resolveEnv(int dollarOffset, int envStart, int envEnd) {
        CharSequence envKey = originalTxt.subSequence(envStart, envEnd);
        final CharSequence envValue = props.get(envKey);
        if (envValue == null) {
            throw new LogError("Undefined property: " + envKey);
        }
        keyStartIdxs.put(envKey, dollarOffset);
        return sink -> sink.put(envValue);
    }

    private class SDate implements Sinkable {

        private final DateFormat dateFormat;

        SDate(int start, int end) {
            if (end - start < 1) {
                throw new LogError("Missing expression at position " + start);
            }
            int actualStart = start;
            int actualEnd = end;
            while (originalTxt.charAt(actualStart) == ' ' && actualStart < actualEnd) {
                actualStart++;
            }
            while (originalTxt.charAt(actualEnd - 1) == ' ' && actualEnd > actualStart) {
                actualEnd--;
            }
            if (actualEnd - actualStart < 1) {
                throw new LogError("Missing expression at position " + actualStart);
            }
            dateFormat = dateCompiler.compile(originalTxt, actualStart, actualEnd, false);
        }

        @Override
        public void toSink(CharSink sink) {
            dateFormat.format(dateValue, TimestampFormatUtils.enLocale, null, sink);
        }
    }

    private class SSubStr implements Sinkable {
        protected final int start;
        protected final int end;

        SSubStr(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void toSink(CharSink sink) {
            sink.put(originalTxt, start, end);
        }
    }
}
