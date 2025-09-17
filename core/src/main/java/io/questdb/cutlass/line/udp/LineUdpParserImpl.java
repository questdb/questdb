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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TABLE_DOES_NOT_EXIST;
import static io.questdb.cairo.TableUtils.TABLE_EXISTS;

public class LineUdpParserImpl implements LineUdpParser, Closeable {
    private final static Log LOG = LogFactory.getLog(LineUdpParserImpl.class);
    private static final FieldNameParser NOOP_FIELD_NAME = name -> {
    };
    private static final FieldValueParser NOOP_FIELD_VALUE = (value, cache) -> {
    };
    private static final LineEndParser NOOP_LINE_END = cache -> {
    };
    private static final String WRITER_LOCK_REASON = "ilpUdp";
    private final boolean autoCreateNewColumns;
    private final boolean autoCreateNewTables;
    private final Clock clock;
    private final LongList columnIndexAndType = new LongList();
    private final LongList columnNameType = new LongList();
    private final LongList columnValues = new LongList();
    private final CharSequenceObjHashMap<TableWriter> commitList = new CharSequenceObjHashMap<>();
    private final CairoConfiguration configuration;
    private final MemoryMARW ddlMem = Vm.getCMARWInstance();
    private final short defaultFloatColumnType;
    private final short defaultIntegerColumnType;
    private final CairoEngine engine;
    private final IntList geoHashBitsSizeByColIdx = new IntList(); // 0 if not a GeoHash, else bits precision
    private final FieldValueParser MY_NEW_TAG_VALUE = this::parseTagValueNewTable;
    private final Path path = new Path();
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final byte timestampUnit;
    private final LineUdpReceiverConfiguration udpConfiguration;
    private final boolean useLegacyStringDefault;
    private final CharSequenceObjHashMap<CacheEntry> writerCache = new CharSequenceObjHashMap<>();
    // state
    // cache entry index is always a negative value
    private int cacheEntryIndex = 0;
    private int columnIndex;
    private long columnName;
    private int columnType;
    private RecordMetadata metadata;
    private final FieldNameParser MY_FIELD_NAME = this::parseFieldName;
    private FieldNameParser onFieldName;
    private FieldValueParser onFieldValue;
    private LineEndParser onLineEnd;
    private FieldValueParser onTagValue;
    private final FieldNameParser MY_NEW_FIELD_NAME = this::parseFieldNameNewTable;
    private final FieldValueParser MY_NEW_FIELD_VALUE = this::parseFieldValueNewTable;
    private long tableName;
    private TableToken tableToken;
    private TimestampDriver timestampDriver;
    private TableWriter writer;
    private final LineEndParser MY_LINE_END = this::appendRow;
    private final LineEndParser MY_NEW_LINE_END = this::createTableAndAppendRow;
    private final FieldValueParser MY_TAG_VALUE = this::parseTagValue;
    private final FieldValueParser MY_FIELD_VALUE = this::parseFieldValue;

    public LineUdpParserImpl(
            CairoEngine engine,
            LineUdpReceiverConfiguration udpConfiguration
    ) {
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.engine = engine;
        this.udpConfiguration = udpConfiguration;
        this.timestampUnit = udpConfiguration.getTimestampUnit();

        this.defaultFloatColumnType = udpConfiguration.getDefaultColumnTypeForFloat();
        this.defaultIntegerColumnType = udpConfiguration.getDefaultColumnTypeForInteger();
        this.useLegacyStringDefault = udpConfiguration.isUseLegacyStringDefault();

        this.autoCreateNewTables = udpConfiguration.getAutoCreateNewTables();
        this.autoCreateNewColumns = udpConfiguration.getAutoCreateNewColumns();
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(ddlMem);
        for (int i = 0, n = writerCache.size(); i < n; i++) {
            Misc.free(writerCache.valueQuick(i).writer);
        }
    }

    public void commitAll() {
        if (writer != null) {
            writer.commit();
        }
        for (int i = 0, n = commitList.size(); i < n; i++) {
            //noinspection resource
            commitList.valueQuick(i).commit();
        }
        commitList.clear();
    }

    @Override
    public void onError(int position, int state, int code) {
        clearState();
    }

    @Override
    public void onEvent(CachedCharSequence token, int eventType, CharSequenceCache cache) {
        switch (eventType) {
            case EVT_MEASUREMENT:
                int wrtIndex = writerCache.keyIndex(token);
                // this condition relies on the fact that this.cacheEntryIndex is always negative
                // which indicates that entry is in cache
                if (wrtIndex == this.cacheEntryIndex) {
                    // same table as from last line?
                    // make sure we append it in case it was in "create" mode
                    if (writer != null) {
                        switchModeToAppend();
                    } else {
                        initCacheEntry(token, writerCache.valueAtQuick(wrtIndex));
                    }
                } else {
                    switchTable(token, wrtIndex);
                }
                break;
            case EVT_FIELD_NAME:
            case EVT_TAG_NAME:
                onFieldName.parse(token);
                break;
            case EVT_TAG_VALUE:
                onTagValue.parse(token, cache);
                break;
            case EVT_FIELD_VALUE:
                onFieldValue.parse(token, cache);
                break;
            case EVT_TIMESTAMP:
                columnValues.add(token.getCacheAddress());
                geoHashBitsSizeByColIdx.add(0);
                break;
            default:
                break;
        }
    }

    @Override
    public void onLineEnd(CharSequenceCache cache) {
        try {
            onLineEnd.parse(cache);
        } catch (CairoException e) {
            LOG.error().$((Sinkable) e).$();
        }
        clearState();
    }

    private void appendFirstRowAndCacheWriter(CharSequenceCache cache) {
        TableWriter writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
        this.writer = writer;
        this.timestampDriver = ColumnType.getTimestampDriver(writer.getTimestampType());
        this.metadata = writer.getMetadata();
        writerCache.valueAtQuick(cacheEntryIndex).writer = writer;

        final int columnCount = columnNameType.size() / 2;
        final TableWriter.Row row = createNewRow(cache, columnCount);
        if (row == null) {
            return;
        }

        for (int i = 0; i < columnCount; i++) {
            LineUdpParserSupport.putValue(
                    row,
                    (int) columnNameType.getQuick(i * 2 + 1),
                    geoHashBitsSizeByColIdx.getQuick(i),
                    i,
                    cache.get(columnValues.getQuick(i))
            );
        }
        row.append();
    }

    private void appendRow(CharSequenceCache cache) {
        final int columnCount = columnIndexAndType.size();
        final TableWriter.Row row = createNewRow(cache, columnCount);
        if (row == null) {
            return;
        }

        for (int i = 0; i < columnCount; i++) {
            final long value = columnIndexAndType.getQuick(i);
            LineUdpParserSupport.putValue(
                    row,
                    Numbers.decodeHighInt(value),
                    geoHashBitsSizeByColIdx.getQuick(i),
                    Numbers.decodeLowInt(value),
                    cache.get(columnValues.getQuick(i))
            );
        }
        row.append();
    }

    private void cacheWriter(CacheEntry entry, CachedCharSequence tableName, TableToken tableToken) {
        try {
            entry.writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
            this.tableToken = tableToken;
            this.tableName = tableName.getCacheAddress();
            createState(entry);
            LOG.info().$("cached writer [name=").$safe(tableName).$(']').$();
        } catch (CairoException ex) {
            LOG.error().$((Sinkable) ex).$();
            switchModeToSkipLine();
        }
    }

    private void clearState() {
        columnNameType.clear();
        columnIndexAndType.clear();
        geoHashBitsSizeByColIdx.clear();
        columnValues.clear();
    }

    private TableWriter.Row createNewRow(CharSequenceCache cache, int columnCount) {
        final int valueCount = columnValues.size();
        if (columnCount == valueCount) {
            return writer.newRow(clock.getTicks());
        } else {
            try {
                return writer.newRow(timestampDriver.from(Numbers.parseLong(cache.get(columnValues.getQuick(valueCount - 1))), timestampUnit));
            } catch (NumericException e) {
                LOG.error().$("invalid timestamp: ").$safe(cache.get(columnValues.getQuick(valueCount - 1))).$();
                return null;
            }
        }
    }

    private void createState(CacheEntry entry) {
        writer = entry.writer;
        timestampDriver = ColumnType.getTimestampDriver(writer.getTimestampType());
        metadata = writer.getMetadata();
        switchModeToAppend();
    }

    private void createTableAndAppendRow(CharSequenceCache cache) {
        tableToken = engine.createTable(
                AllowAllSecurityContext.INSTANCE,
                ddlMem,
                path,
                true,
                tableStructureAdapter.of(cache),
                false
        );
        appendFirstRowAndCacheWriter(cache);
    }

    private void initCacheEntry(CachedCharSequence token, CacheEntry entry) {
        TableToken tableToken = engine.getTableTokenIfExists(token);
        switch (entry.state) {
            case 0:
                int exists = engine.getTableStatus(path, tableToken);
                switch (exists) {
                    case TABLE_EXISTS:
                        if (tableToken != null && tableToken.isMatView()) {
                            throw CairoException.nonCritical()
                                    .put("cannot modify materialized view [view=")
                                    .put(tableToken.getTableName())
                                    .put(']');
                        }
                        entry.state = 1;
                        cacheWriter(entry, token, tableToken);
                        break;
                    case TABLE_DOES_NOT_EXIST:
                        if (!autoCreateNewTables) {
                            throw CairoException.nonCritical()
                                    .put("table does not exist, creating new tables is disabled [table=").put(token)
                                    .put(']');
                        }
                        if (!autoCreateNewColumns) {
                            throw CairoException.nonCritical()
                                    .put("table does not exist, cannot create table, creating new columns is disabled [table=").put(token)
                                    .put(']');
                        }
                        tableName = token.getCacheAddress();
                        if (onLineEnd != MY_NEW_LINE_END) {
                            onLineEnd = MY_NEW_LINE_END;
                            onFieldName = MY_NEW_FIELD_NAME;
                            onFieldValue = MY_NEW_FIELD_VALUE;
                            onTagValue = MY_NEW_TAG_VALUE;
                        }
                        break;
                    default:
                        entry.state = 3;
                        switchModeToSkipLine();
                        break;
                }
                break;
            case 1:
                cacheWriter(entry, token, tableToken);
                break;
            default:
                switchModeToSkipLine();
                break;
        }
    }

    private void parseFieldName(CachedCharSequence token) {
        columnIndex = metadata.getColumnIndexQuiet(token);
        if (columnIndex > -1) {
            columnType = metadata.getColumnType(columnIndex);
        }

        if (columnIndex < 0 || columnType < 0) {
            prepareNewColumn(token);
        }
    }

    private void parseFieldNameNewTable(CachedCharSequence token) {
        if (!TableUtils.isValidColumnName(token, udpConfiguration.getMaxFileNameLength())) {
            LOG.error().$("invalid column name [columnName=").$safe(token).I$();
            switchModeToSkipLine();
            return;
        }
        columnNameType.add(token.getCacheAddress());
    }

    private void parseFieldValue(CachedCharSequence value, CharSequenceCache cache) {
        int valueType = LineUdpParserSupport.getValueType(value, defaultFloatColumnType, defaultIntegerColumnType, useLegacyStringDefault);
        if (valueType == ColumnType.UNDEFINED) {
            switchModeToSkipLine();
        } else {
            parseValue(value, valueType, cache, true);
        }
    }

    private void parseFieldValueNewTable(CachedCharSequence value, CharSequenceCache cache) {
        int valueType = LineUdpParserSupport.getValueType(value, defaultFloatColumnType, defaultIntegerColumnType, useLegacyStringDefault);
        if (valueType == ColumnType.UNDEFINED || valueType == ColumnType.NULL) { // cannot create a col of type null
            switchModeToSkipLine();
        } else {
            parseValueNewTable(value, valueType);
        }
    }

    private void parseTagValue(CachedCharSequence value, CharSequenceCache cache) {
        parseValue(value, ColumnType.SYMBOL, cache, false);
    }

    private void parseTagValueNewTable(CachedCharSequence value, CharSequenceCache cache) {
        parseValueNewTable(value, ColumnType.SYMBOL);
    }

    private void parseValue(CachedCharSequence value, int valueType, CharSequenceCache cache, boolean isForField) {
        assert valueType > ColumnType.UNDEFINED;
        if (columnType > ColumnType.UNDEFINED) {
            boolean valid;
            int geoHashBits = 0;
            if (valueType != ColumnType.NULL) {
                final int valueTypeTag = ColumnType.tagOf(valueType);
                final int columnTypeTag = ColumnType.tagOf(columnType);
                switch (valueTypeTag) {
                    case ColumnType.LONG:
                        valid = columnTypeTag == ColumnType.LONG
                                || columnTypeTag == ColumnType.INT
                                || columnTypeTag == ColumnType.SHORT
                                || columnTypeTag == ColumnType.BYTE
                                || columnTypeTag == ColumnType.TIMESTAMP
                                || columnTypeTag == ColumnType.DATE;
                        break;
                    case ColumnType.INT:
                        valid = columnTypeTag == ColumnType.INT
                                || columnTypeTag == ColumnType.SHORT
                                || columnTypeTag == ColumnType.BYTE;
                        break;
                    case ColumnType.SHORT:
                        valid = columnTypeTag == ColumnType.SHORT
                                || columnTypeTag == ColumnType.BYTE;
                        break;
                    case ColumnType.BYTE:
                        valid = columnTypeTag == ColumnType.BYTE;
                        break;
                    case ColumnType.BOOLEAN:
                        valid = columnTypeTag == ColumnType.BOOLEAN;
                        break;
                    case ColumnType.STRING:
                    case ColumnType.VARCHAR:
                        valid = columnTypeTag == ColumnType.STRING ||
                                columnTypeTag == ColumnType.VARCHAR ||
                                columnTypeTag == ColumnType.CHAR ||
                                columnTypeTag == ColumnType.IPv4 ||
                                isForField &&
                                        (geoHashBits = ColumnType.getGeoHashBits(columnType)) != 0;
                        break;
                    case ColumnType.DOUBLE:
                        valid = columnTypeTag == ColumnType.DOUBLE || columnTypeTag == ColumnType.FLOAT;
                        break;
                    case ColumnType.FLOAT:
                        valid = columnTypeTag == ColumnType.FLOAT;
                        break;
                    case ColumnType.SYMBOL:
                        valid = columnTypeTag == ColumnType.SYMBOL;
                        break;
                    case ColumnType.LONG256:
                        valid = columnTypeTag == ColumnType.LONG256;
                        break;
                    case ColumnType.TIMESTAMP:
                        valid = columnTypeTag == ColumnType.TIMESTAMP;
                        break;
                    default:
                        valid = false;
                }
            } else {
                valid = true; // null is valid, the storage value is assigned later
            }
            if (valid) {
                columnIndexAndType.add(Numbers.encodeLowHighInts(columnIndex, columnType));
                columnValues.add(value.getCacheAddress());
                geoHashBitsSizeByColIdx.add(geoHashBits);
            } else {
                LOG.error().$("mismatched column and value types [table=").$(writer.getTableToken())
                        .$(", column=").$safe(metadata.getColumnName(columnIndex))
                        .$(", columnType=").$(ColumnType.nameOf(columnType))
                        .$(", valueType=").$(ColumnType.nameOf(valueType))
                        .$(']').$();
                switchModeToSkipLine();
            }
        } else {
            CharSequence colNameAsChars = cache.get(columnName);
            if (autoCreateNewColumns && TableUtils.isValidColumnName(colNameAsChars, udpConfiguration.getMaxFileNameLength())) {
                writer.addColumn(colNameAsChars, valueType);
                // Writer index can be different from column count, it keeps deleted columns in metadata
                int columnIndex = writer.getColumnIndex(colNameAsChars);
                columnIndexAndType.add(Numbers.encodeLowHighInts(columnIndex, valueType));
                columnValues.add(value.getCacheAddress());
                geoHashBitsSizeByColIdx.add(0);
            } else if (!autoCreateNewColumns) {
                throw CairoException.nonCritical()
                        .put("column does not exist, creating new columns is disabled [table=").put(writer.getTableToken().getTableName())
                        .put(", columnName=").put(colNameAsChars)
                        .put(']');
            } else {
                LOG.error().$("invalid column name [table=").$(writer.getTableToken())
                        .$(", columnName=").$safe(colNameAsChars)
                        .$(']').$();
                switchModeToSkipLine();
            }
        }
    }

    private void parseValueNewTable(CachedCharSequence value, int valueType) {
        columnNameType.add(valueType);
        columnValues.add(value.getCacheAddress());
        geoHashBitsSizeByColIdx.add(0); // not a GeoHash, no constant literal
        // that can be recognised yet
    }

    private void prepareNewColumn(CachedCharSequence token) {
        columnName = token.getCacheAddress();
        columnType = ColumnType.UNDEFINED;
    }

    private void switchModeToAppend() {
        if (onLineEnd != MY_LINE_END) {
            onLineEnd = MY_LINE_END;
            onFieldName = MY_FIELD_NAME;
            onFieldValue = MY_FIELD_VALUE;
            onTagValue = MY_TAG_VALUE;
        }
    }

    private void switchModeToSkipLine() {
        if (onFieldValue != NOOP_FIELD_VALUE) {
            onFieldValue = NOOP_FIELD_VALUE;
            onFieldName = NOOP_FIELD_NAME;
            onTagValue = NOOP_FIELD_VALUE;
            onLineEnd = NOOP_LINE_END;
        }
    }

    private void switchTable(CachedCharSequence tableName, int entryIndex) {
        if (this.cacheEntryIndex != 0) {
            // add previous writer to commit list
            CacheEntry e = writerCache.valueAtQuick(cacheEntryIndex);
            if (e.writer != null) {
                if (Chars.equals(tableName, e.writer.getTableToken().getTableName())) {
                    commitList.put(e.writer.getTableToken().getTableName(), e.writer);
                } else {
                    // Cannot happen except with WAL table rename and out of date TableWriter.tableToken.
                    commitList.put(Chars.toString(tableName), e.writer);
                }
            }
        }

        CacheEntry entry;
        if (entryIndex < 0) {
            entry = writerCache.valueAtQuick(entryIndex);
        } else {
            entry = new CacheEntry();
            writerCache.putAt(entryIndex, Chars.toString(tableName), entry);
            // adjust writer map index to negative, which indicates that entry exists
            entryIndex = -entryIndex - 1;
        }

        this.cacheEntryIndex = entryIndex;

        if (entry.writer == null) {
            initCacheEntry(tableName, entry);
        } else {
            createState(entry);
        }
    }

    @FunctionalInterface
    private interface FieldNameParser {
        void parse(CachedCharSequence name);
    }

    @FunctionalInterface
    private interface FieldValueParser {
        void parse(CachedCharSequence value, CharSequenceCache cache);
    }

    @FunctionalInterface
    private interface LineEndParser {
        void parse(CharSequenceCache cache);
    }

    private static class CacheEntry {
        private int state = 0;
        private TableWriter writer;
    }

    private class TableStructureAdapter implements TableStructure {
        private CharSequenceCache cache;
        private int columnCount;
        private int timestampIndex;

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            if (columnIndex == getTimestampIndex()) {
                return "timestamp";
            }
            CharSequence colName = cache.get(columnNameType.getQuick(columnIndex * 2));
            if (TableUtils.isValidColumnName(colName, configuration.getMaxFileNameLength())) {
                return colName;
            }
            throw CairoException.nonCritical().put("column name contains invalid characters [colName=").put(colName).put(']');
        }

        @Override
        public int getColumnType(int columnIndex) {
            if (columnIndex == getTimestampIndex()) {
                return ColumnType.TIMESTAMP_MICRO;
            }
            return (int) columnNameType.getQuick(columnIndex * 2 + 1);
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return configuration.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            return udpConfiguration.getDefaultPartitionBy();
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return configuration.getDefaultSymbolCacheFlag();
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return configuration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return cache.get(tableName);
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            return configuration.getWalEnabledDefault() && PartitionBy.isPartitioned(getPartitionBy());
        }

        TableStructureAdapter of(CharSequenceCache cache) {
            this.cache = cache;
            this.timestampIndex = columnNameType.size() / 2;
            this.columnCount = timestampIndex + 1;
            return this;
        }
    }
}
