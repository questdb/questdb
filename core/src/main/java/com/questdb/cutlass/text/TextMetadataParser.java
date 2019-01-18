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

import com.questdb.cairo.ColumnType;
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.cutlass.json.JsonParser;
import com.questdb.cutlass.text.types.TypeAdapter;
import com.questdb.cutlass.text.types.TypeManager;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.*;
import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;

import java.io.Closeable;

public class TextMetadataParser implements JsonParser, Mutable, Closeable {
    private static final Log LOG = LogFactory.getLog(TextMetadataParser.class);

    private static final int S_NEED_ARRAY = 1;
    private static final int S_NEED_OBJECT = 2;
    private static final int S_NEED_PROPERTY = 3;
    private static final int P_NAME = 1;
    private static final int P_TYPE = 2;
    private static final int P_PATTERN = 3;
    private static final int P_LOCALE = 4;
    private static final CharSequenceIntHashMap propertyNameMap = new CharSequenceIntHashMap();
    private final DateLocaleFactory dateLocaleFactory;
    private final ObjectPool<FloatingCharSequence> csPool;
    private final DateFormatFactory dateFormatFactory;
    private final ObjList<CharSequence> columnNames;
    private final ObjList<TypeAdapter> columnTypes;
    private final TypeManager typeManager;
    private int state = S_NEED_ARRAY;
    private CharSequence name;
    private int type = -1;
    private CharSequence pattern;
    private CharSequence locale;
    private int propertyIndex;
    private long buf;
    private long bufCapacity = 0;
    private int bufSize = 0;
    private CharSequence tableName;
    private int localePosition;

    public TextMetadataParser(
            TextConfiguration textConfiguration,
            DateLocaleFactory dateLocaleFactory,
            DateFormatFactory dateFormatFactory,
            TypeManager typeManager
    ) {
        this.columnNames = new ObjList<>();
        this.columnTypes = new ObjList<>();
        this.csPool = new ObjectPool<>(FloatingCharSequence::new, textConfiguration.getMetadataStringPoolSize());
        this.dateLocaleFactory = dateLocaleFactory;
        this.dateFormatFactory = dateFormatFactory;
        this.typeManager = typeManager;
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
            Unsafe.free(buf, bufCapacity);
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
                    LOG.info().$("unknown [table=").$(tableName).$(", tag=").$(tag).$(']').$();
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (propertyIndex) {
                    case P_NAME:
                        name = copy(tag);
                        break;
                    case P_TYPE:
                        type = ColumnType.columnTypeOf(tag);
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
                    default:
                        LOG.info().$("ignoring [table=").$(tableName).$(", value=").$(tag).$(']').$();
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

    private void clearStage() {
        name = null;
        type = -1;
        pattern = null;
        locale = null;
        localePosition = 0;
    }

    private CharSequence copy(CharSequence tag) {
        final int l = tag.length() * 2;
        final long n = bufSize + l;
        if (n > bufCapacity) {
            long ptr = Unsafe.malloc(n * 2);
            Unsafe.getUnsafe().copyMemory(buf, ptr, bufSize);
            if (bufCapacity > 0) {
                Unsafe.free(buf, bufCapacity);
            }
            buf = ptr;
            bufCapacity = n * 2;
        }

        Chars.strcpyw(tag, l / 2, buf + bufSize);
        CharSequence cs = csPool.next().of(bufSize, l / 2);
        bufSize += l;
        return cs;
    }

    private void createImportedType(int position) throws JsonException {
        if (name == null) {
            throw JsonException.$(position, "Missing 'name' property");
        }

        if (type == -1) {
            throw JsonException.$(position, "Missing 'type' property");
        }

        columnNames.add(name);

        switch (type) {
            case ColumnType.DATE:
                DateLocale dateLocale = locale == null ? dateLocaleFactory.getDefaultDateLocale() : dateLocaleFactory.getDateLocale(locale);
                if (dateLocale == null) {
                    throw JsonException.$(localePosition, "Invalid date locale");
                }
                columnTypes.add(typeManager.nextDateAdapter().of(dateFormatFactory.get(pattern), dateLocale));
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

        private int offset;
        private int len;

        @Override
        public void clear() {
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(buf + offset + index * 2);
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
    }
}
