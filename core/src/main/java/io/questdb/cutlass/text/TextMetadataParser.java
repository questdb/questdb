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

package io.questdb.cutlass.text;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.griffin.SqlKeywords;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.str.AbstractCharSequence;

import java.io.Closeable;

public class TextMetadataParser implements JsonParser, Mutable, Closeable {
    private static final Log LOG = LogFactory.getLog(TextMetadataParser.class);
    private static final int P_INDEX = 6;
    private static final int P_LOCALE = 4;
    private static final int P_NAME = 1;
    private static final int P_PATTERN = 3;
    private static final int P_TYPE = 2;
    private static final int P_UTF8 = 5;
    private static final int S_NEED_ARRAY = 1;
    private static final int S_NEED_OBJECT = 2;
    private static final int S_NEED_PROPERTY = 3;
    private static final CharSequenceIntHashMap propertyNameMap = new CharSequenceIntHashMap();
    private final ObjList<CharSequence> columnNames;
    private final ObjList<TypeAdapter> columnTypes;
    private final ObjectPool<FloatingCharSequence> csPool;
    private final DateFormatFactory dateFormatFactory;
    private final DateLocale dateLocale;
    private final DateLocaleFactory dateLocaleFactory;
    private final TypeManager typeManager;
    private long buf;
    private long bufCapacity = 0;
    private int bufSize = 0;
    private boolean index = false;
    private CharSequence locale;
    private int localePosition;
    private CharSequence name;
    private CharSequence pattern;
    private int propertyIndex;
    private int state = S_NEED_ARRAY;
    private CharSequence tableName;
    private int type = -1;
    private boolean utf8 = false;

    public TextMetadataParser(TextConfiguration textConfiguration, TypeManager typeManager) {
        this.columnNames = new ObjList<>();
        this.columnTypes = new ObjList<>();
        this.csPool = new ObjectPool<>(FloatingCharSequence::new, textConfiguration.getMetadataStringPoolCapacity());
        this.dateLocaleFactory = typeManager.getInputFormatConfiguration().getDateLocaleFactory();
        this.dateFormatFactory = typeManager.getInputFormatConfiguration().getDateFormatFactory();
        this.typeManager = typeManager;
        this.dateLocale = textConfiguration.getDefaultDateLocale();
    }

    @Override
    public void clear() {
        bufSize = 0;
        state = S_NEED_ARRAY;
        columnNames.clear();
        columnTypes.clear();
        csPool.clear();
        clearStage();
    }

    @Override
    public void close() {
        clear();
        if (bufCapacity > 0) {
            Unsafe.free(buf, bufCapacity, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            bufCapacity = 0;
        }
    }

    public ObjList<CharSequence> getColumnNames() {
        return columnNames;
    }

    public ObjList<TypeAdapter> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public void onEvent(int code, CharSequence tag, int position) throws JsonException {
        switch (code) {
            case JsonLexer.EVT_ARRAY_START:
                if (state != S_NEED_ARRAY) {
                    throw JsonException.$(position, "Unexpected array");
                }
                state = S_NEED_OBJECT;
                break;
            case JsonLexer.EVT_OBJ_START:
                if (state != S_NEED_OBJECT) {
                    throw JsonException.$(position, "Unexpected object");
                }
                state = S_NEED_PROPERTY;
                break;
            case JsonLexer.EVT_NAME:
                this.propertyIndex = propertyNameMap.get(tag);
                if (this.propertyIndex == -1) {
                    LOG.info().$("unknown [table=").$safe(tableName).$(", tag=").$safe(tag).$(']').$();
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (propertyIndex) {
                    case P_NAME:
                        name = copy(tag);
                        break;
                    case P_TYPE:
                        type = ColumnType.typeOf(tag);
                        if (type == -1) {
                            throw JsonException.$(position, "Invalid type");
                        }
                        break;
                    case P_PATTERN:
                        pattern = copy(tag);
                        break;
                    case P_LOCALE:
                        locale = copy(tag);
                        localePosition = position;
                        break;
                    case P_UTF8:
                        utf8 = SqlKeywords.isTrueKeyword(tag);
                        break;
                    case P_INDEX:
                        index = SqlKeywords.isTrueKeyword(tag);
                        break;
                    default:
                        LOG.info().$("ignoring [table=").$safe(tableName).$(", value=").$safe(tag).$(']').$();
                        break;
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                state = S_NEED_OBJECT;
                createImportedType(position);
                break;
            case JsonLexer.EVT_ARRAY_VALUE:
                throw JsonException.$(position, "Must be an object");
            default:
                break;
        }
    }

    private static void checkInputs(int position, CharSequence name, int type) throws JsonException {
        if (name == null) {
            throw JsonException.$(position, "Missing 'name' property");
        }

        if (type == -1) {
            throw JsonException.$(position, "Missing 'type' property");
        }
    }

    private static void strcpyw(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + ((long) i << 1), value.charAt(i));
        }
    }

    private void clearStage() {
        name = null;
        type = -1;
        pattern = null;
        locale = null;
        localePosition = 0;
        utf8 = false;
        index = false;
    }

    private CharSequence copy(CharSequence tag) {
        final int l = tag.length() * 2;
        final long n = bufSize + l;
        if (n > bufCapacity) {
            long ptr = Unsafe.malloc(n * 2, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            Vect.memcpy(ptr, buf, bufSize);
            if (bufCapacity > 0) {
                Unsafe.free(buf, bufCapacity, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            }
            buf = ptr;
            bufCapacity = n * 2;
        }

        strcpyw(tag, l / 2, buf + bufSize);
        CharSequence cs = csPool.next().of(bufSize, l / 2);
        bufSize += l;
        return cs;
    }

    private void createImportedType(int position) throws JsonException {
        checkInputs(position, name, type);

        columnNames.add(name);

        switch (ColumnType.tagOf(type)) {
            case ColumnType.DATE:
                DateLocale dateLocale = locale == null ? this.dateLocale : dateLocaleFactory.getLocale(locale);

                if (dateLocale == null) {
                    throw JsonException.$(localePosition, "Invalid date locale");
                }

                // date pattern is required
                if (pattern == null) {
                    throw JsonException.$(0, "DATE format pattern is required");
                }
                columnTypes.add(typeManager.nextDateAdapter().of(dateFormatFactory.get(pattern), dateLocale));
                break;
            case ColumnType.TIMESTAMP:
                DateLocale timestampLocale =
                        locale == null ?
                                this.dateLocale
                                : dateLocaleFactory.getLocale(locale);
                if (timestampLocale == null) {
                    throw JsonException.$(localePosition, "Invalid timestamp locale");
                }

                // timestamp pattern is required
                if (pattern == null) {
                    throw JsonException.$(0, "TIMESTAMP format pattern is required");
                }
                columnTypes.add(typeManager.nextTimestampAdapter(utf8, ColumnType.getTimestampDriver(type).getTimestampDateFormatFactory().get(pattern), timestampLocale, pattern.toString()));
                break;
            case ColumnType.SYMBOL:
                columnTypes.add(typeManager.nextSymbolAdapter(index));
                break;
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256:
                columnTypes.add(typeManager.nextDecimalAdapter(type));
                break;
            default:
                columnTypes.add(typeManager.getTypeAdapter(type));
                break;
        }
        // prepare for next iteration
        clearStage();
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
    }

    private class FloatingCharSequence extends AbstractCharSequence implements Mutable {

        private int len;
        private int offset;

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(buf + offset + index * 2L);
        }

        @Override
        public void clear() {
        }

        @Override
        public int length() {
            return len;
        }

        CharSequence of(int lo, int len) {
            this.offset = lo;
            this.len = len;
            return this;
        }
    }

    static {
        propertyNameMap.put("name", P_NAME);
        propertyNameMap.put("type", P_TYPE);
        propertyNameMap.put("pattern", P_PATTERN);
        propertyNameMap.put("locale", P_LOCALE);
        propertyNameMap.put("utf8", P_UTF8);
        propertyNameMap.put("index", P_INDEX);
    }
}
