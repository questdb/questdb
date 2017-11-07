package com.questdb.parser.lp;

import com.questdb.cairo.*;
import com.questdb.cairo.pool.ResourcePool;
import com.questdb.ex.NumericException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Chars;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.LongList;
import com.questdb.std.Sinkable;
import com.questdb.std.clock.Clock;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.ImmutableCharSequence;
import com.questdb.std.str.Path;
import com.questdb.store.ColumnType;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.RecordMetadata;

import java.io.Closeable;
import java.io.IOException;

import static com.questdb.cairo.TableUtils.TABLE_DOES_NOT_EXIST;
import static com.questdb.cairo.TableUtils.TABLE_EXISTS;

public class CairoLineProtoParser implements LineProtoParser, Closeable {
    private final static Log LOG = LogFactory.getLog(CairoLineProtoParser.class);
    private static final LineEndParser NOOP_LINE_END = cache -> {
    };
    private static final FieldValueParser NOOP_FIELD_VALUE = (value, cache) -> {
    };
    private static final FieldNameParser NOOP_FIELD_NAME = name -> {
    };
    private final ResourcePool<TableWriter> pool;
    private final CharSequenceObjHashMap<CacheEntry> writerCache = new CharSequenceObjHashMap<>();
    private final CompositePath path = new CompositePath();
    private final CairoConfiguration configuration;
    private final LongList columnNameType = new LongList();
    private final LongList columnValues = new LongList();
    private final AppendMemory appendMemory = new AppendMemory();
    private final Clock clock;
    private final FieldNameParser MY_NEW_FIELD_NAME = this::parseFieldNameNewTable;
    private final FieldValueParser MY_NEW_TAG_VALUE = this::parseTagValueNewTable;
    // state
    private TableWriter writer;
    private final LineEndParser MY_LINE_END = this::appendRow;
    private RecordMetadata metadata;
    private int columnCount;
    private int columnIndex;
    private long columnName;
    private int columnType;
    private final FieldNameParser MY_FIELD_NAME = this::parseFieldName;
    private long tableName;
    private CacheEntry entry;
    private final LineEndParser MY_NEW_LINE_END = this::createTableAndAppendRow;
    private LineEndParser onLineEnd;
    private FieldNameParser onFieldName;
    private FieldValueParser onFieldValue;
    private FieldValueParser onTagValue;
    private final FieldValueParser MY_FIELD_VALUE = this::parseFieldValue;
    private final FieldValueParser MY_NEW_FIELD_VALUE = this::parseFieldValueNewTable;
    private final FieldValueParser MY_TAG_VALUE = this::parseTagValue;

    public CairoLineProtoParser(CairoConfiguration configuration, ResourcePool<TableWriter> pool) {
        this.configuration = configuration;
        this.clock = configuration.getClock();
        this.pool = pool;
    }

    @Override
    public void close() throws IOException {
        Misc.free(path);
        Misc.free(appendMemory);
        for (int i = 0, n = writerCache.size(); i < n; i++) {
            Misc.free(writerCache.valueQuick(i).writer);
        }
    }

    public void commitAll() {
        for (int i = 0, n = writerCache.size(); i < n; i++) {
            CacheEntry e = writerCache.valueQuick(i);
            if (e.writer != null) {
                writer.commit();
            }
        }
    }

    @Override
    public void onError(int position, int state, int code) {
        clearState();
    }

    @Override
    public void onEvent(CachedCharSequence token, int eventType, CharSequenceCache cache) {

        switch (eventType) {
            case EVT_MEASUREMENT:
                CacheEntry entry = writerCache.get(token);
                if (entry == null) {
                    writerCache.put(ImmutableCharSequence.of(token), entry = new CacheEntry());
                }

                if (entry.writer == null) {
                    initCacheEntry(token, entry);
                } else {
                    createState(entry);
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
        TableWriter writer = entry.writer = pool.get(cache.get(tableName));
        int columnCount = columnNameType.size() / 2;
        int valueCount = columnValues.size();

        TableWriter.Row row;

        if (columnCount == valueCount) {
            row = writer.newRow(clock.getTicks());
        } else {
            try {
                row = writer.newRow(Numbers.parseLong(cache.get(columnValues.getQuick(valueCount - 1))));
            } catch (NumericException e) {
                LOG.error().$("invalid timestamp: ").$(cache.get(columnValues.getQuick(valueCount - 1))).$();
                return;
            }
        }

        try {
            for (int i = 0; i < columnCount; i++) {
                putValue(row
                        , i
                        , (int) columnNameType.getQuick(i * 2 + 1)
                        , cache.get(columnValues.getQuick(i)));
            }
            row.append();
            writer.commit();
        } catch (BadCastException ignore) {
            row.cancel();
        }
    }

    private void appendRow(CharSequenceCache cache) {
        int columnCount = columnNameType.size() / 2;
        int valueCount = columnValues.size();

        TableWriter.Row row;

        if (columnCount == valueCount) {
            row = writer.newRow(clock.getTicks());
        } else {
            try {
                row = writer.newRow(Numbers.parseLong(cache.get(columnValues.getQuick(valueCount - 1))));
            } catch (NumericException e) {
                LOG.error().$("invalid timestamp: ").$(cache.get(columnValues.getQuick(valueCount - 1))).$();
                return;
            }
        }

        try {
            for (int i = 0; i < columnCount; i++) {
                putValue(row
                        , (int) columnNameType.getQuick(i * 2)
                        , (int) columnNameType.getQuick(i * 2 + 1), cache.get(columnValues.getQuick(i))
                );
            }
            row.append();
            writer.commit();
        } catch (BadCastException ignore) {
            row.cancel();
        }
    }

    private void cacheWriter(CacheEntry entry, CachedCharSequence tableName) {
        try {
            entry.writer = pool.get(tableName);
            this.tableName = tableName.getCacheAddress();
            createState(entry);
        } catch (CairoException ex) {
            LOG.error().$((Sinkable) ex).$();
            skipAll();
        }
    }

    private void clearState() {
        writer = null;
        columnNameType.clear();
        columnValues.clear();
    }

    private void createState(CacheEntry entry) {
        writer = entry.writer;
        metadata = writer.getMetadata();
        columnCount = metadata.getColumnCount();
        if (onLineEnd != MY_LINE_END) {
            onLineEnd = MY_LINE_END;
            onFieldName = MY_FIELD_NAME;
            onFieldValue = MY_FIELD_VALUE;
            onTagValue = MY_TAG_VALUE;
        }
    }

    private void createTable(CharSequenceCache cache) {
        path.of(configuration.getRoot()).concat(cache.get(tableName));
        final int rootLen = path.length();
        if (configuration.getFilesFacade().mkdirs(path.put(Path.SEPARATOR).$(), configuration.getMkDirMode()) == -1) {
            throw CairoException.instance(0).put("cannot create directory: ").put(path);
        }

        try (AppendMemory mem = appendMemory) {
            mem.of(configuration.getFilesFacade(), path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$(), configuration.getFilesFacade().getPageSize());

            int count = columnNameType.size() / 2;
            mem.putInt(count + 1);       // number of columns gathered + timestamp
            mem.putInt(PartitionBy.NONE);     // not available on protocol
            mem.putInt(count);                // timestamp is always last
            for (int i = 0; i < count; i++) {
                // type is second value in pair
                mem.putInt((int) columnNameType.getQuick(i * 2 + 1));
            }
            mem.putInt(ColumnType.DATE);

            for (int i = 0; i < count; i++) {
                mem.putStr(cache.get(columnNameType.getQuick(i * 2)));
            }
            mem.putStr("timestamp");

            mem.of(configuration.getFilesFacade(), path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$(), configuration.getFilesFacade().getPageSize());
            TableUtils.resetTxn(mem);
        }
    }

    private void createTableAndAppendRow(CharSequenceCache cache) {
        createTable(cache);
        appendFirstRowAndCacheWriter(cache);
    }

    private int getValueType(CharSequence token) {
        int len = token.length();
        char c = token.charAt(len - 1);
        switch (c) {
            case 'i':
                return ColumnType.INT;
            case 'e':
                // tru(e)
                // fals(e)
                return ColumnType.BOOLEAN;
            case '"':
                if (len < 2 || token.charAt(0) != '\"') {
                    LOG.error().$("incorrectly quoted string: ").$(token).$();
                    return -1;
                }
                return ColumnType.STRING;
            default:
                return ColumnType.DOUBLE;

        }
    }

    private void initCacheEntry(CachedCharSequence token, CacheEntry entry) {
        if (entry.state == 0) {
            int exists = TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), token);
            switch (exists) {
                case TABLE_EXISTS:
                    entry.state = 1;
                    cacheWriter(entry, token);
                    break;
                case TABLE_DOES_NOT_EXIST:
                    this.entry = entry;
                    tableName = token.getCacheAddress();
                    this.entry = entry;
                    if (onLineEnd != MY_NEW_LINE_END) {
                        onLineEnd = MY_NEW_LINE_END;
                        onFieldName = MY_NEW_FIELD_NAME;
                        onFieldValue = MY_NEW_FIELD_VALUE;
                        onTagValue = MY_NEW_TAG_VALUE;
                    }
                    break;
                default:
                    entry.state = 3;
                    skipAll();
                    break;
            }
        } else if (entry.state == 1) {
            // have to retry pool
            cacheWriter(entry, token);
        }
    }

    private void parseFieldName(CachedCharSequence token) {
        columnIndex = metadata.getColumnIndexQuiet(token);
        if (columnIndex == -1) {
            columnName = token.getCacheAddress();
        } else {
            columnType = metadata.getColumnQuick(columnIndex).getType();
        }
    }

    private void parseFieldNameNewTable(CachedCharSequence token) {
        columnNameType.add(token.getCacheAddress());
    }

    private void parseFieldValue(CachedCharSequence value, CharSequenceCache cache) {
        int valueType = getValueType(value);
        if (valueType == -1) {
            skipAll();
        } else {
            parseValue(value, valueType, cache);
        }
    }

    @SuppressWarnings("unused")
    private void parseFieldValueNewTable(CachedCharSequence value, CharSequenceCache cache) {
        int valueType = getValueType(value);
        if (valueType == -1) {
            skipAll();
        } else {
            parseValueNewTable(value, valueType);
        }
    }

    private void parseTagValue(CachedCharSequence value, CharSequenceCache cache) {
        parseValue(value, ColumnType.SYMBOL, cache);
    }

    @SuppressWarnings("unused")
    private void parseTagValueNewTable(CachedCharSequence value, CharSequenceCache cache) {
        parseValueNewTable(value, ColumnType.SYMBOL);
    }

    private void parseValue(CachedCharSequence value, int valueType, CharSequenceCache cache) {
        if (columnIndex == -1) {
            columnNameType.add(columnCount++);
            columnNameType.add(valueType);
            writer.addColumn(cache.get(columnName), valueType);
        } else {
            if (columnType != valueType) {
                LOG.error().$("mismatched column and value types [table=").$(writer.getName())
                        .$(", column=").$(metadata.getColumnQuick(columnIndex).getName())
                        .$(", columnType=").$(ColumnType.nameOf(columnType))
                        .$(", valueType=").$(ColumnType.nameOf(valueType))
                        .$(']').$();
                skipAll();
            } else {
                columnNameType.add(columnIndex);
                columnNameType.add(valueType);
            }
        }
        columnValues.add(value.getCacheAddress());
    }

    private void parseValueNewTable(CachedCharSequence value, int valueType) {
        columnNameType.add(valueType);
        columnValues.add(value.getCacheAddress());
    }

    /**
     * Writes column value to table row. CharSequence value is interpreted depending on
     * column type and written to column, identified by columnIndex. If value cannot be
     * cast to column type, #BadCastException is thrown.
     *
     * @param row        table row
     * @param index      index of column to write value to
     * @param columnType column type value will be cast to
     * @param value      value characters
     */
    private void putValue(TableWriter.Row row, int index, int columnType, CharSequence value) throws BadCastException {
        switch (columnType) {
            case ColumnType.INT:
                try {
                    row.putInt(index, Numbers.parseInt(value, 0, value.length() - 1));
                } catch (NumericException e) {
                    LOG.error().$("not an INT: ").$(value).$();
                    throw BadCastException.INSTANCE;
                }
                break;
            case ColumnType.DOUBLE:
                try {
                    row.putDouble(index, Numbers.parseDouble(value));
                } catch (NumericException e) {
                    LOG.error().$("not a DOUBLE: ").$(value).$();
                    throw BadCastException.INSTANCE;
                }
                break;
            case ColumnType.BOOLEAN:
                row.putBool(index, Chars.equals(value, "true"));
                break;
            case ColumnType.STRING:
                row.putStr(index, value, 1, value.length() - 2);
                break;
            case ColumnType.SYMBOL:
                row.putStr(index, value);
            default:
                break;

        }
    }

    private void skipAll() {
        if (onFieldValue != NOOP_FIELD_VALUE) {
            onFieldValue = NOOP_FIELD_VALUE;
            onFieldName = NOOP_FIELD_NAME;
            onTagValue = NOOP_FIELD_VALUE;
            onLineEnd = NOOP_LINE_END;
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

    private static class BadCastException extends Exception {
        private static final BadCastException INSTANCE = new BadCastException();
    }

    private class CacheEntry {
        private TableWriter writer;
        private int state = 0;
    }
}
