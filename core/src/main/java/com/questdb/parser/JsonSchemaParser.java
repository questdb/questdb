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

package com.questdb.parser;

import com.questdb.BootstrapEnv;
import com.questdb.parser.json.JsonException;
import com.questdb.parser.json.JsonLexer;
import com.questdb.parser.json.JsonParser;
import com.questdb.std.*;
import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.store.ColumnType;

public class JsonSchemaParser implements JsonParser, Mutable {
    private static final int S_NEED_ARRAY = 1;
    private static final int S_NEED_OBJECT = 2;
    private static final int S_NEED_PROPERTY = 3;
    private static final int P_NAME = 1;
    private static final int P_TYPE = 2;
    private static final int P_PATTERN = 3;
    private static final int P_LOCALE = 4;
    private static final CharSequenceIntHashMap propertyNameMap = new CharSequenceIntHashMap();
    private final ObjectPool<ImportedColumnMetadata> mPool = new ObjectPool<>(ImportedColumnMetadata::new, 64);
    private final DateLocaleFactory dateLocaleFactory;
    private final ObjList<ImportedColumnMetadata> metadata = new ObjList<>();
    private final ObjectPool<FloatingCharSequence> csPool = new ObjectPool<>(FloatingCharSequence::new, 64);
    private final DateFormatFactory dateFormatFactory;
    private int state = S_NEED_ARRAY;
    private CharSequence name;
    private int type = -1;
    private CharSequence pattern;
    private DateFormat dateFormat;
    private DateLocale dateLocale;
    private int propertyIndex;
    private long buf;
    private long bufCapacity = 0;
    private int bufSize = 0;

    public JsonSchemaParser(BootstrapEnv env) {
        this.dateLocaleFactory = env.dateLocaleFactory;
        this.dateFormatFactory = env.dateFormatFactory;
    }

    @Override
    public void clear() {
        bufSize = 0;
        state = S_NEED_ARRAY;
        metadata.clear();
        csPool.clear();
        mPool.clear();
        clearStage();
    }

    public ObjList<ImportedColumnMetadata> getMetadata() {
        return metadata;
    }

    @Override
    public void onEvent(int code, CharSequence tag, int position) throws JsonException {
        switch (code) {
            case JsonLexer.EVT_ARRAY_START:
                if (state != S_NEED_ARRAY) {
                    throw JsonException.with("Unexpected array", position);
                }
                state = S_NEED_OBJECT;
                break;
            case JsonLexer.EVT_OBJ_START:
                if (state != S_NEED_OBJECT) {
                    throw JsonException.with("Unexpected object", position);
                }
                state = S_NEED_PROPERTY;
                break;
            case JsonLexer.EVT_NAME:
                this.propertyIndex = propertyNameMap.get(tag);
                break;
            case JsonLexer.EVT_VALUE:
                switch (propertyIndex) {
                    case P_NAME:
                        name = copy(tag);
                        break;
                    case P_TYPE:
                        type = ColumnType.columnTypeOf(tag);
                        if (type == -1) {
                            throw JsonException.with("Invalid type", position);
                        }
                        break;
                    case P_PATTERN:
                        dateFormat = dateFormatFactory.get(tag);
                        pattern = copy(tag);
                        break;
                    case P_LOCALE:
                        dateLocale = dateLocaleFactory.getDateLocale(tag);
                        if (dateLocale == null) {
                            throw JsonException.with("Invalid date locale", position);
                        }
                        break;
                    default:
                        break;
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                state = S_NEED_OBJECT;
                createImportedType(position);
                break;
            case JsonLexer.EVT_ARRAY_VALUE:
                throw JsonException.with("Must be an object", position);
            default:
                break;
        }
    }

    private void clearStage() {
        name = null;
        pattern = null;
        type = -1;
        dateLocale = null;
        dateFormat = null;
    }

    private CharSequence copy(CharSequence tag) {
        int l = tag.length();
        long n = bufSize + l;
        if (n >= bufCapacity) {
            long ptr = Unsafe.malloc(n * 2);
            Unsafe.getUnsafe().copyMemory(buf, ptr, bufSize);
            Unsafe.free(buf, bufCapacity);
            buf = ptr;
            bufCapacity = n * 2;
        }

        Chars.strcpy(tag, l, buf + bufSize);
        CharSequence cs = csPool.next().of(bufSize, bufSize + l);
        bufSize += l;
        return cs;
    }

    private void createImportedType(int position) throws JsonException {
        if (name == null) {
            throw JsonException.with("Missing 'name' property", position);
        }

        if (type == -1) {
            throw JsonException.with("Missing 'type' property", position);
        }

        ImportedColumnMetadata m = mPool.next();
        m.name = name;
        m.importedColumnType = type;
        m.pattern = pattern;
        m.dateFormat = dateFormat;
        m.dateLocale = dateLocale == null && type == ColumnType.DATE ? dateLocaleFactory.getDefaultDateLocale() : dateLocale;
        metadata.add(m);

        // prepare for next iteration
        clearStage();
    }

    private class FloatingCharSequence extends AbstractCharSequence implements Mutable {

        int lo;
        int hi;

        @Override
        public int length() {
            return hi - lo;
        }

        @Override
        public char charAt(int index) {
            return (char) Unsafe.getUnsafe().getByte(buf + lo + index);
        }

        CharSequence of(int lo, int hi) {
            this.lo = lo;
            this.hi = hi;
            return this;
        }

        @Override
        public void clear() {
        }
    }

    static {
        propertyNameMap.put("name", P_NAME);
        propertyNameMap.put("type", P_TYPE);
        propertyNameMap.put("pattern", P_PATTERN);
        propertyNameMap.put("locale", P_LOCALE);
    }
}
