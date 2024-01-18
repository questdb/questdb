/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.text.schema2;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.*;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.TimestampFormatFactory;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.str.AbstractCharSequence;

import java.io.Closeable;

public class SchemaV2Parser implements JsonParser, Mutable, Closeable {

    private static final int LVL_1_COLUMNS = 1;
    private static final int LVL_1_FORMATS = 2;
    private static final int LVL_1_FORMATS_ACTION = 3;
    private static final int LVL_2_COLUMN_TYPE = 3;
    private static final int LVL_2_FILE_COLUMN_IGNORE = 5;
    private static final int LVL_2_FILE_COLUMN_INDEX = 2;
    private static final int LVL_2_FILE_COLUMN_NAME = 1;
    private static final int LVL_2_FORMATS = 6;
    private static final int LVL_2_TABLE_COLUMN_NAME = 4;
    private static final int LVL_3_LOCALE = 2;
    private static final int LVL_3_PATTERN = 1;
    private static final int LVL_3_UTF8 = 3;
    private static final int S_NEED_ARRAY = 1;
    private static final int S_NEED_OBJECT = 2;
    private static final int S_NEED_PROPERTY_NAME = 3;
    private static final int S_NEED_PROPERTY_VALUE = 4;
    private static final CharSequenceIntHashMap lvl1Branches = new CharSequenceIntHashMap();
    private static final LowerCaseCharSequenceIntHashMap lvl1FormatsActionValues = new LowerCaseCharSequenceIntHashMap();
    private static final CharSequenceIntHashMap lvl2Branches = new CharSequenceIntHashMap();
    private static final CharSequenceIntHashMap lvl3Branches = new CharSequenceIntHashMap();
    private final ObjectPool<FloatingCharSequence> csPool;
    private final DateFormatFactory dateFormatFactory;
    private final DateLocale dateLocale;
    private final DateLocaleFactory dateLocaleFactory;
    private final ObjList<TypeAdapter> formatsInFlight = new ObjList<>();
    private final IntHashSet seenFormatsColumnTypes = new IntHashSet();
    private final TimestampFormatFactory timestampFormatFactory;
    private final TypeManager typeManager;
    private long buf;
    private long bufCapacity = 0;
    private int bufSize = 0;
    private int columnType = -1;
    private boolean columnsDefined = false;
    private boolean fileColumnIgnore;
    private int fileColumnIndex = -1;
    private CharSequence fileColumnName;
    private CharSequence formatLocale;
    private int formatLocalePosition;
    private String formatPattern;
    private int formatPatternPosition;
    private boolean formatUtf8 = false;
    private int formatsAction = SchemaV2.FORMATS_ACTION_ADD;
    private boolean formatsActionDefined = false;
    private boolean formatsDefined = false;
    private int level;
    private int lvl1Index;
    private int lvl2Index;
    private int lvl3Index;
    private SchemaV2 schema;
    private int state = S_NEED_OBJECT;
    private CharSequence tableColumnName;

    public SchemaV2Parser(TextConfiguration textConfiguration, TypeManager typeManager) {
        this.csPool = new ObjectPool<>(FloatingCharSequence::new, textConfiguration.getMetadataStringPoolCapacity());
        this.dateLocaleFactory = typeManager.getInputFormatConfiguration().getDateLocaleFactory();
        this.dateFormatFactory = typeManager.getInputFormatConfiguration().getDateFormatFactory();
        this.timestampFormatFactory = typeManager.getInputFormatConfiguration().getTimestampFormatFactory();
        this.typeManager = typeManager;
        this.dateLocale = textConfiguration.getDefaultDateLocale();
    }

    @Override
    public void clear() {
        columnsDefined = false;
        formatsDefined = false;
        formatsActionDefined = false;
        formatsAction = SchemaV2.FORMATS_ACTION_ADD;
        level = 0;
        bufSize = 0;
        state = S_NEED_OBJECT;
        csPool.clear();
        clearColumnStage();
        clearFormatStage();
        seenFormatsColumnTypes.clear();
    }

    @Override
    public void close() {
        clear();
        if (bufCapacity > 0) {
            Unsafe.free(buf, bufCapacity, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            bufCapacity = 0;
        }
    }

    @Override
    public void onEvent(int code, CharSequence tag, int position) throws JsonException {
        switch (code) {
            case JsonLexer.EVT_ARRAY_START:
                switch (state) {
                    case S_NEED_ARRAY:
                        state = S_NEED_OBJECT;
                        break;
                    case S_NEED_OBJECT:
                        throw JsonException.$(position, "object expected");
                    case S_NEED_PROPERTY_VALUE:
                        throw JsonException.$(position, "scalar value expected");
                    default:
                        throw JsonException.$(position, "unexpected array");
                }
                break;
            case JsonLexer.EVT_ARRAY_END:
                if (level == 2 && lvl1Index == LVL_1_FORMATS) {
                    schema.addFormats(columnType, formatsInFlight);
                    formatsInFlight.clear();
                }
                state = S_NEED_PROPERTY_NAME;
                break;
            case JsonLexer.EVT_OBJ_START:
                switch (state) {
                    case S_NEED_ARRAY:
                        throw JsonException.$(position, "array expected");
                    case S_NEED_PROPERTY_VALUE:
                        throw JsonException.$(position, "scalar value expected");
                    case S_NEED_OBJECT:
                        state = S_NEED_PROPERTY_NAME;
                        level++;
                        break;
                    default:
                }
                break;
            case JsonLexer.EVT_NAME:
                switch (level) {
                    case 1:
                        this.lvl1Index = lvl1Branches.get(tag);
                        switch (lvl1Index) {
                            case LVL_1_COLUMNS:
                                if (columnsDefined) {
                                    throw JsonException.$(position, "columns are already defined");
                                }
                                columnsDefined = true;
                                state = S_NEED_ARRAY;
                                break;
                            case LVL_1_FORMATS:
                                if (formatsDefined) {
                                    throw JsonException.$(position, "formats are already defined");
                                }
                                formatsDefined = true;
                                state = S_NEED_OBJECT;
                                break;
                            case LVL_1_FORMATS_ACTION:
                                if (formatsActionDefined) {
                                    throw JsonException.$(position, "formats_action is already defined");
                                }
                                formatsActionDefined = true;
                                state = S_NEED_PROPERTY_VALUE;
                                break;
                            default:
                                throw JsonException.$(position, "unexpected property [tag=").put(tag).put(']');
                        }
                        break;
                    case 2:
                        switch (lvl1Index) {
                            case LVL_1_COLUMNS:
                                this.lvl2Index = lvl2Branches.get(tag);
                                switch (lvl2Index) {
                                    case LVL_2_FILE_COLUMN_NAME:
                                    case LVL_2_FILE_COLUMN_INDEX:
                                    case LVL_2_FILE_COLUMN_IGNORE:
                                    case LVL_2_COLUMN_TYPE:
                                    case LVL_2_TABLE_COLUMN_NAME:
                                        state = S_NEED_PROPERTY_VALUE;
                                        break;
                                    case LVL_2_FORMATS:
                                        state = S_NEED_ARRAY;
                                        break;
                                    default:
                                        throw JsonException.$(position, "unexpected property [tag=").put(tag).put(']');
                                }
                                break;
                            case LVL_1_FORMATS:
                                // column type goes here
                                columnType = ColumnType.typeOf(tag);
                                if (columnType == -1) {
                                    throw JsonException.$(position, "Invalid column type [tag=").put(tag).put(']');
                                }
                                if (!seenFormatsColumnTypes.add(columnType)) {
                                    throw JsonException.$(position, "duplicate formats column type [tag=").put(tag).put(']');
                                }
                                state = S_NEED_ARRAY;
                                break;
                        }
                        break;
                    case 3:
                        this.lvl3Index = lvl3Branches.get(tag);
                        if (lvl3Index == -1) {
                            throw JsonException.$(position, "unexpected property [tag=").put(tag).put(']');
                        } else {
                            state = S_NEED_PROPERTY_VALUE;
                        }
                        break;
                }
                break;
            case JsonLexer.EVT_VALUE:
                switch (state) {
                    case S_NEED_ARRAY:
                        throw JsonException.$(position, "array expected");
                    case S_NEED_OBJECT:
                        throw JsonException.$(position, "object expected");
                    case S_NEED_PROPERTY_VALUE:
                        switch (level) {
                            case 1:
                                if (lvl1Index == LVL_1_FORMATS_ACTION) {
                                    formatsAction = lvl1FormatsActionValues.get(tag);
                                    if (formatsAction == -1) {
                                        throw JsonException.$(position, "'formats_action' valid values are  'ADD or 'REPLACE' [tag=").put(tag).put(']');
                                    }
                                    schema.setFormatsAction(formatsAction);
                                }
                                break;
                            case 2:
                                switch (lvl2Index) {
                                    case LVL_2_FILE_COLUMN_NAME:
                                        fileColumnName = copy(tag);
                                        break;
                                    case LVL_2_FILE_COLUMN_INDEX:
                                        if (tag != null) {
                                            try {
                                                fileColumnIndex = Numbers.parseInt(tag);
                                            } catch (NumericException e) {
                                                throw JsonException.$(position, "Invalid file column index [tag=").put(tag).put(']');
                                            }
                                        }
                                        break;
                                    case LVL_2_FILE_COLUMN_IGNORE:
                                        fileColumnIgnore = Chars.equalsIgnoreCaseNc("true", tag);
                                        break;
                                    case LVL_2_COLUMN_TYPE:
                                        columnType = ColumnType.typeOf(tag);
                                        if (columnType == -1) {
                                            throw JsonException.$(position, "Invalid column type [tag=").put(tag).put(']');
                                        }
                                        break;
                                    case LVL_2_TABLE_COLUMN_NAME:
                                        tableColumnName = copy(tag);
                                        break;
                                    case LVL_2_FORMATS:
                                        // expect array of formats
                                        break;
                                }
                                break;
                            case 3:
                                switch (lvl3Index) {
                                    case LVL_3_LOCALE:
                                        if (tag != null) {
                                            formatLocale = copy(tag);
                                            formatLocalePosition = position;
                                        }
                                        break;
                                    case LVL_3_UTF8:
                                        if (tag != null) {
                                            formatUtf8 = SqlKeywords.isTrueKeyword(tag);
                                            if (!formatUtf8 && !SqlKeywords.isFalseKeyword(tag)) {
                                                throw JsonException.$(position, "boolean value expected [tag=").put(tag).put(']');
                                            }
                                        }
                                        break;
                                    case LVL_3_PATTERN:
                                        formatPatternPosition = position;
                                        if (tag != null) {
                                            formatPattern = Chars.toString(tag);
                                        }
                                        break;
                                    default:
                                        throw JsonException.$(position, "Invalid column type [tag=").put(tag).put(']');
                                }

                        }
                        break;
                    default:
                        throw JsonException.$(position, "unexpected value");
                }
                break;
            case JsonLexer.EVT_OBJ_END:
                state = S_NEED_OBJECT;
                switch (level) {
                    case 2:
                        switch (lvl1Index) {
                            case LVL_1_COLUMNS:
                                schema.addColumn(
                                        fileColumnName,
                                        fileColumnIndex,
                                        fileColumnIgnore,
                                        columnType,
                                        tableColumnName,
                                        formatsInFlight
                                );
                                formatsInFlight.clear();
                                clearColumnStage();
                                break;
                            case LVL_1_FORMATS:
                                // nothing to do
                                break;
                            default:
                                assert false;
                        }
                        break;
                    case 3:
                        addFormat();
                        clearFormatStage();
                        break;
                }
                level--;
                break;
            case JsonLexer.EVT_ARRAY_VALUE:
                throw JsonException.$(position, "Must be an object");
            default:
                break;
        }
    }

    public void withSchema(SchemaV2 schema) {
        this.schema = schema;
    }

    private static void strcpyw(final CharSequence value, final int len, final long address) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + ((long) i << 1), value.charAt(i));
        }
    }

    private void addFormat() throws JsonException {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DATE:
                DateLocale dateLocale = formatLocale == null ? this.dateLocale : dateLocaleFactory.getLocale(formatLocale);

                if (dateLocale == null) {
                    throw JsonException.$(formatLocalePosition, "Invalid date locale");
                }

                // date pattern is required
                if (formatPattern == null) {
                    throw JsonException.$(0, "DATE format pattern is required");
                }
                formatsInFlight.add(typeManager.nextDateAdapter().of(formatPattern, dateFormatFactory.get(formatPattern), dateLocale));
                break;
            case ColumnType.TIMESTAMP:
                DateLocale timestampLocale =
                        formatLocale == null ?
                                this.dateLocale
                                : dateLocaleFactory.getLocale(formatLocale);
                if (timestampLocale == null) {
                    throw JsonException.$(formatLocalePosition, "Invalid timestamp locale [tag=").put(formatLocale).put(']');
                }

                // timestamp pattern is required
                if (formatPattern == null) {
                    throw JsonException.$(formatPatternPosition, "TIMESTAMP format pattern is required");
                }
                formatsInFlight.add(typeManager.nextTimestampAdapter(formatPattern, formatUtf8, timestampFormatFactory.get(formatPattern), timestampLocale));
                break;
            default:
                formatsInFlight.add(typeManager.getTypeAdapter(columnType));
                break;
        }
    }

    private void clearColumnStage() {
        fileColumnName = null;
        fileColumnIndex = -1;
        fileColumnIgnore = false;
        tableColumnName = null;
        columnType = -1;
    }

    private void clearFormatStage() {
        formatPattern = null;
        formatLocale = null;
        formatLocalePosition = 0;
        formatUtf8 = false;
        formatPatternPosition = 0;
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
        lvl1Branches.put("columns", LVL_1_COLUMNS);
        lvl1Branches.put("formats", LVL_1_FORMATS);
        lvl1Branches.put("formats_action", LVL_1_FORMATS_ACTION);

        lvl2Branches.put("file_column_name", LVL_2_FILE_COLUMN_NAME);
        lvl2Branches.put("file_column_index", LVL_2_FILE_COLUMN_INDEX);
        lvl2Branches.put("file_column_ignore", LVL_2_FILE_COLUMN_IGNORE);
        lvl2Branches.put("column_type", LVL_2_COLUMN_TYPE);
        lvl2Branches.put("table_column_name", LVL_2_TABLE_COLUMN_NAME);
        lvl2Branches.put("formats", LVL_2_FORMATS);

        lvl3Branches.put("pattern", LVL_3_PATTERN);
        lvl3Branches.put("locale", LVL_3_LOCALE);
        lvl3Branches.put("utf8", LVL_3_UTF8);

        lvl1FormatsActionValues.put("add", SchemaV2.FORMATS_ACTION_ADD);
        lvl1FormatsActionValues.put("replace", SchemaV2.FORMATS_ACTION_REPLACE);
    }
}
