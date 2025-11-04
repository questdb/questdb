/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.MicrosFormatCompiler;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TemplateParser implements Sinkable {

    private static final String DATE_FORMAT_KEY = "date:";
    private static final int NIL = -1;
    private final MicrosFormatCompiler dateCompiler = new MicrosFormatCompiler();
    private final AtomicLong dateValue = new AtomicLong();
    private final CharSequenceIntHashMap envStartIdxs = new CharSequenceIntHashMap();
    private final Utf8StringSink resolveSink = new Utf8StringSink();
    private final ObjList<TemplateNode> templateNodes = new ObjList<>();
    private CharSequence originalTxt;
    private CharSequenceObjHashMap<CharSequence> props;

    public static CharSequenceObjHashMap<CharSequence> adaptMap(Map<String, String> props) {
        CharSequenceObjHashMap<CharSequence> properties = new CharSequenceObjHashMap<>(props.size());
        for (String key : props.keySet()) {
            properties.put(key, props.get(key));
        }
        return properties;
    }

    public int getKeyOffset(CharSequence key) {
        return envStartIdxs.get(key); // relative to originalTxt
    }

    public ObjList<TemplateNode> getTemplateNodes() {
        return templateNodes;
    }

    public TemplateParser parse(CharSequence txt, long dateValue, CharSequenceObjHashMap<CharSequence> props) {
        return parse(txt, dateValue, props, true);
    }

    public TemplateParser parse(CharSequence txt, long dateValue, Map<String, String> props) {
        return parse(txt, dateValue, adaptMap(props), true);
    }

    public TemplateParser parseEnv(CharSequence txt, long dateValue) {
        return parse(txt, dateValue, adaptMap(System.getenv()), true);
    }

    public TemplateParser parseUtf8(CharSequence txt, long dateValue, CharSequenceObjHashMap<CharSequence> props) {
        return parse(txt, dateValue, props, false);
    }

    public void setDateValue(long dateValue) {
        this.dateValue.set(dateValue);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        for (int i = 0, n = templateNodes.size(); i < n; i++) {
            sink.put(templateNodes.getQuick(i));
        }
    }

    @Override
    public String toString() {
        resolveSink.clear();
        toSink(resolveSink);
        return resolveSink.toString();
    }

    private void addDateTemplateNode(int start, int end) {
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
        final DateFormat dateFormat = dateCompiler.compile(originalTxt, actualStart, actualEnd, false);
        templateNodes.add(new TemplateNode(TemplateNode.TYPE_DATE, DATE_FORMAT_KEY) {
            @Override
            public void toSink(@NotNull CharSink<?> sink) {
                dateFormat.format(dateValue.get(), DateLocaleFactory.EN_LOCALE, null, sink);
            }
        });
    }

    private void addEnvTemplateNode(int dollarOffset, int envStart, int envEnd) {
        final String envKey = originalTxt.subSequence(envStart, envEnd).toString();
        CharSequence envVal = props.get(envKey);
        if (envVal == null) {
            if (Chars.equals(envKey, "log.dir")) {
                envVal = props.get("QDB_LOG_LOG_DIR");
                if (envVal == null) {
                    throw CairoException.nonCritical().put("could not find property `log.dir`. Did you pass `QDB_LOG_LOG_DIR` as an environment variable?");
                }
            } else {
                throw new LogError("Undefined property: " + envKey);
            }
        }
        envStartIdxs.put(envKey, dollarOffset);
        CharSequence finalEnvVal = envVal;
        templateNodes.add(new TemplateNode(TemplateNode.TYPE_ENV, envKey) {
            @Override
            public void toSink(@NotNull CharSink<?> sink) {
                sink.put(finalEnvVal);
            }
        });
    }

    private void addStaticTemplateNode(int start, int end, boolean needsUtf8Encoding) {
        templateNodes.add(new TemplateNode(TemplateNode.TYPE_STATIC, null) {
            @Override
            public void toSink(@NotNull CharSink<?> sink) {
                if (needsUtf8Encoding) {
                    sink.put(originalTxt, start, end);
                } else {
                    sink.putAscii(originalTxt, start, end);
                }
            }
        });
    }

    private TemplateParser parse(
            CharSequence txt,
            long dateValue,
            CharSequenceObjHashMap<CharSequence> props,
            boolean needsUtf8Encoding
    ) {
        originalTxt = txt;
        this.dateValue.set(dateValue);
        this.props = props;
        templateNodes.clear();
        envStartIdxs.clear();
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
                            addEnvTemplateNode(dollarStart, dollarStart + 1, i);
                            lastExprEnd = i + 1;
                        } else {
                            throw new LogError("Unexpected '$' at position " + i);
                        }
                    } else {
                        if (i - lastExprEnd > 0) {
                            addStaticTemplateNode(lastExprEnd, i, needsUtf8Encoding);
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
                        addEnvTemplateNode(dollarStart, dollarStart + 1, i);
                        lastExprEnd = i;
                    } else {
                        int exprLen = i - keyStart;
                        if (exprLen == 0) {
                            throw new LogError("Missing expression at position " + keyStart);
                        }
                        int formatStart = keyStart + DATE_FORMAT_KEY.length();
                        if (Chars.startsWith(originalTxt, keyStart, formatStart, DATE_FORMAT_KEY)) {
                            addDateTemplateNode(formatStart, i);
                        } else {
                            addEnvTemplateNode(dollarStart, dollarStart + 2, i);
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
                addStaticTemplateNode(lastExprEnd, locationLen, needsUtf8Encoding);
            }
        } else {
            if (keyStart != NIL) {
                throw new LogError("Missing '}' at position " + locationLen);
            }
            if (locationLen - dollarStart > 1) {
                addEnvTemplateNode(dollarStart, dollarStart + 1, locationLen);
            } else {
                throw new LogError("Unexpected '$' at position " + dollarStart);
            }
        }
        return this;
    }

    public static abstract class TemplateNode implements Sinkable {
        private final static int TYPE_DATE = 2;
        private final static int TYPE_ENV = 1;
        private final static int TYPE_STATIC = 0;
        private final CharSequence key;
        private final int type;

        private TemplateNode(int type, CharSequence key) {
            this.type = type;
            this.key = key;
        }

        public boolean isEnv(CharSequence key) {
            return type == TYPE_ENV && Chars.equals(this.key, key);
        }
    }
}
