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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.*;
import io.questdb.cutlass.line.udp.LineUdpParserSupport.BadCastException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TABLE_DOES_NOT_EXIST;
import static io.questdb.cairo.TableUtils.TABLE_EXISTS;

public class LineUdpParserImpl implements LineUdpParser, Closeable {
    private final static Log LOG = LogFactory.getLog(LineUdpParserImpl.class);
    private static final String WRITER_LOCK_REASON = "ilpUdp";
    private static final LineEndParser NOOP_LINE_END = cache -> {
    };
    private static final FieldValueParser NOOP_FIELD_VALUE = (value, cache) -> {
    };
    private static final FieldNameParser NOOP_FIELD_NAME = name -> {
    };

    private final CairoEngine engine;
    private final CharSequenceObjHashMap<CacheEntry> writerCache = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<TableWriter> commitList = new CharSequenceObjHashMap<>();
    private final Path path = new Path();
    private final CairoConfiguration configuration;
    private final LongList columnNameType = new LongList();
    private final LongList columnIndexAndType = new LongList();
    private final IntList geoHashBitsSizeByColIdx = new IntList(); // 0 if not a GeoHash, else bits precision
    private final LongList columnValues = new LongList();
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final MicrosecondClock clock;
    private final FieldNameParser MY_NEW_FIELD_NAME = this::parseFieldNameNewTable;
    private final FieldValueParser MY_NEW_TAG_VALUE = this::parseTagValueNewTable;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final CairoSecurityContext cairoSecurityContext;
    private final LineProtoTimestampAdapter timestampAdapter;
    private final LineUdpReceiverConfiguration udpConfiguration;
    private final short defaultFloatColumnType;
    private final short defaultIntegerColumnType;
    // state
    // cache entry index is always a negative value
    private int cacheEntryIndex = 0;
    private TableWriter writer;
    private final LineEndParser MY_LINE_END = this::appendRow;
    private RecordMetadata metadata;
    private int columnCount;
    private int columnIndex;
    private long columnName;
    private int columnType;
    private final FieldNameParser MY_FIELD_NAME = this::parseFieldName;
    private long tableName;
    private final LineEndParser MY_NEW_LINE_END = this::createTableAndAppendRow;
    private LineEndParser onLineEnd;
    private FieldNameParser onFieldName;
    private FieldValueParser onFieldValue;
    private FieldValueParser onTagValue;
    private final FieldValueParser MY_TAG_VALUE = this::parseTagValue;
    private final FieldValueParser MY_FIELD_VALUE = this::parseFieldValue;
    private final FieldValueParser MY_NEW_FIELD_VALUE = this::parseFieldValueNewTable;

    public LineUdpParserImpl(
            CairoEngine engine,
            LineUdpReceiverConfiguration udpConfiguration
    ) {
        this.configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.engine = engine;
        this.udpConfiguration = udpConfiguration;
        this.cairoSecurityContext = udpConfiguration.getCairoSecurityContext();
        this.timestampAdapter = udpConfiguration.getTimestampAdapter();

        defaultFloatColumnType = udpConfiguration.getDefaultColumnTypeForFloat();
        defaultIntegerColumnType = udpConfiguration.getDefaultColumnTypeForInteger();
    }

    @Override
    public void close() {
        Misc.free(path);
        Misc.free(ddlMem);
        for (int i = 0, n = writerCache.size(); i < n; i++) {
            Misc.free(writerCache.valueQuick(i).writer);
        }
    }

    public void commitAll(int commitMode) {
        if (writer != null) {
            writer.commit(commitMode);
        }
        for (int i = 0, n = commitList.size(); i < n; i++) {
            commitList.valueQuick(i).commit(commitMode);
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
        TableWriter writer = engine.getWriter(cairoSecurityContext, cache.get(tableName), WRITER_LOCK_REASON);
        this.writer = writer;
        this.metadata = writer.getMetadata();
        this.columnCount = metadata.getColumnCount();
        writerCache.valueAtQuick(cacheEntryIndex).writer = writer;

        final int columnCount = columnNameType.size() / 2;
        final TableWriter.Row row = createNewRow(cache, columnCount);
        if (row == null) {
            return;
        }

        try {
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
        } catch (BadCastException ignore) {
            row.cancel();
        }
    }

    private void appendRow(CharSequenceCache cache) {
        final int columnCount = columnIndexAndType.size();
        final TableWriter.Row row = createNewRow(cache, columnCount);
        if (row == null) {
            return;
        }

        try {
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
        } catch (BadCastException ignore) {
            row.cancel();
        }
    }

    private void cacheWriter(CacheEntry entry, CachedCharSequence tableName) {
        try {
            entry.writer = engine.getWriter(cairoSecurityContext, tableName, WRITER_LOCK_REASON);
            this.tableName = tableName.getCacheAddress();
            createState(entry);
            LOG.info().$("cached writer [name=").$(tableName).$(']').$();
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
                return writer.newRow(timestampAdapter.getMicros(cache.get(columnValues.getQuick(valueCount - 1))));
            } catch (NumericException e) {
                LOG.error().$("invalid timestamp: ").$(cache.get(columnValues.getQuick(valueCount - 1))).$();
                return null;
            }
        }
    }

    private void createState(CacheEntry entry) {
        writer = entry.writer;
        metadata = writer.getMetadata();
        columnCount = metadata.getColumnCount();
        switchModeToAppend();
    }

    private void createTableAndAppendRow(CharSequenceCache cache) {
        engine.createTable(
                cairoSecurityContext,
                ddlMem,
                path,
                tableStructureAdapter.of(cache)
        );
        appendFirstRowAndCacheWriter(cache);
    }

    private void initCacheEntry(CachedCharSequence token, CacheEntry entry) {
        switch (entry.state) {
            case 0:
                int exists = engine.getStatus(cairoSecurityContext, path, token);
                switch (exists) {
                    case TABLE_EXISTS:
                        entry.state = 1;
                        cacheWriter(entry, token);
                        break;
                    case TABLE_DOES_NOT_EXIST:
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
                cacheWriter(entry, token);
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
        } else {
            prepareNewColumn(token);
        }
    }

    private void parseFieldNameNewTable(CachedCharSequence token) {
        columnNameType.add(token.getCacheAddress());
    }

    private void parseFieldValue(CachedCharSequence value, CharSequenceCache cache) {
        int valueType = LineUdpParserSupport.getValueType(value, defaultFloatColumnType, defaultIntegerColumnType);
        if (valueType == ColumnType.UNDEFINED) {
            switchModeToSkipLine();
        } else {
            parseValue(value, valueType, cache, true);
        }
    }

    @SuppressWarnings("unused")
    private void parseFieldValueNewTable(CachedCharSequence value, CharSequenceCache cache) {
        int valueType = LineUdpParserSupport.getValueType(value, defaultFloatColumnType, defaultIntegerColumnType);
        if (valueType == ColumnType.UNDEFINED || valueType == ColumnType.NULL) { // cannot create a col of type null
            switchModeToSkipLine();
        } else {
            parseValueNewTable(value, valueType);
        }
    }

    private void parseTagValue(CachedCharSequence value, CharSequenceCache cache) {
        parseValue(value, ColumnType.SYMBOL, cache, false);
    }

    @SuppressWarnings("unused")
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
                        valid = columnTypeTag == ColumnType.STRING ||
                                columnTypeTag == ColumnType.CHAR ||
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
                LOG.error().$("mismatched column and value types [table=").$(writer.getTableName())
                        .$(", column=").$(metadata.getColumnName(columnIndex))
                        .$(", columnType=").$(ColumnType.nameOf(columnType))
                        .$(", valueType=").$(ColumnType.nameOf(valueType))
                        .$(']').$();
                switchModeToSkipLine();
            }
        } else {
            CharSequence colNameAsChars = cache.get(columnName);
            if (TableUtils.isValidColumnName(colNameAsChars)) {
                writer.addColumn(colNameAsChars, valueType);
                columnIndexAndType.add(Numbers.encodeLowHighInts(columnCount++, valueType));
                columnValues.add(value.getCacheAddress());
                geoHashBitsSizeByColIdx.add(0);
            } else {
                LOG.error().$("invalid column name [table=").$(writer.getTableName())
                        .$(", columnName=").$(colNameAsChars)
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
                commitList.put(e.writer.getTableName(), e.writer);
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
    private interface LineEndParser {
        void parse(CharSequenceCache cache);
    }

    @FunctionalInterface
    private interface FieldNameParser {
        void parse(CachedCharSequence name);
    }

    @FunctionalInterface
    private interface FieldValueParser {
        void parse(CachedCharSequence value, CharSequenceCache cache);
    }

    private static class CacheEntry {
        private TableWriter writer;
        private int state = 0;
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
            if (TableUtils.isValidColumnName(colName)) {
                return colName;
            }
            throw CairoException.instance(0).put("column name contains invalid characters [colName=").put(colName).put(']');
        }

        @Override
        public int getColumnType(int columnIndex) {
            if (columnIndex == getTimestampIndex()) {
                return ColumnType.TIMESTAMP;
            }
            return (int) columnNameType.getQuick(columnIndex * 2 + 1);
        }

        @Override
        public long getColumnHash(int columnIndex) {
            return configuration.getRandom().nextLong();
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return false;
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
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getCommitLag() {
            return configuration.getCommitLag();
        }

        TableStructureAdapter of(CharSequenceCache cache) {
            this.cache = cache;
            this.timestampIndex = columnNameType.size() / 2;
            this.columnCount = timestampIndex + 1;
            return this;
        }
    }
}
