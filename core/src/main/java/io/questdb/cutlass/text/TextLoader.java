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

package io.questdb.cutlass.text;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.schema2.SchemaV2;
import io.questdb.cutlass.text.types.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TextLoader implements Closeable, Mutable {

    public static final int ANALYZE_STRUCTURE = 1;
    public static final int LOAD_DATA = 2;
    public static final int LOAD_JSON_METADATA = 0;
    public static final int NO_INDEX = -1;
    private static final Log LOG = LogFactory.getLog(TextLoader.class);
    private static final String WRITER_LOCK_REASON = "textLoader";
    private final CairoConfiguration cairoConfiguration;
    private final LongList columnErrorCounts = new LongList();
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final CairoEngine engine;
    private final JsonLexer jsonLexer;
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);
    private final ObjList<ParserMethod> parseMethods = new ObjList<>();
    private final Path path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
    private final IntList remapIndex = new IntList();
    private final SchemaV2 schema = new SchemaV2();
    private final SchemaV1Parser schemaV1Parser;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final int textAnalysisMaxLines;
    private final TextConfiguration textConfiguration;
    private final TextDelimiterScanner textDelimiterScanner;
    private final TextMetadataDetector textMetadataDetector;
    private final TextLexerWrapper tlw;
    private final TypeManager typeManager;
    private final DirectCharSink utf8Sink;
    private int atomicity;
    private byte columnDelimiter = -1;
    private CharSequence designatedTimestampColumnName;
    private int designatedTimestampIndex;
    private ObjList<TypeAdapter> detectedColumnTypes;
    private boolean forceHeaders = false;
    private CharSequence importedTimestampColumnName;
    private AbstractTextLexer lexer;
    private int maxUncommittedRows = -1;
    private RecordMetadata metadata;
    private long o3MaxLag = -1;
    private boolean overwrite;
    private int partitionBy;
    private boolean skipLinesWithExtraValues = true;
    private int state;
    private CharSequence tableName;
    private TimestampAdapter timestampAdapter;
    private CharSequence timestampColumnName;
    private int timestampIndex = NO_INDEX;
    private boolean walTable;
    private int warnings;
    private TableWriterAPI writer;
    private int writtenLineCount;
    private final CsvTextLexer.Listener partitionedListener = this::onFieldsPartitioned;
    private final CsvTextLexer.Listener nonPartitionedListener = this::onFieldsNonPartitioned;

    public TextLoader(CairoEngine engine) {
        this.engine = engine;
        this.tlw = new TextLexerWrapper(engine.getConfiguration().getTextConfiguration());
        this.textConfiguration = engine.getConfiguration().getTextConfiguration();
        this.cairoConfiguration = engine.getConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        jsonLexer = new JsonLexer(textConfiguration.getJsonCacheSize(), textConfiguration.getJsonCacheLimit());
        textMetadataDetector = new TextMetadataDetector(typeManager, textConfiguration, schema);
        schemaV1Parser = new SchemaV1Parser(textConfiguration, typeManager);
        textAnalysisMaxLines = textConfiguration.getTextAnalysisMaxLines();
        textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
        parseMethods.extendAndSet(LOAD_JSON_METADATA, this::parseJsonMetadata);
        parseMethods.extendAndSet(ANALYZE_STRUCTURE, this::parseStructure);
        parseMethods.extendAndSet(LOAD_DATA, this::parseData);
    }

    @Override
    public void clear() {
        otherToTimestampAdapterPool.clear();
        writer = Misc.free(writer);
        metadata = null;
        columnErrorCounts.clear();
        timestampAdapter = null;
        writtenLineCount = 0;
        warnings = TextLoadWarning.NONE;
        designatedTimestampColumnName = null;
        designatedTimestampIndex = NO_INDEX;
        timestampIndex = NO_INDEX;
        importedTimestampColumnName = null;
        maxUncommittedRows = -1;
        o3MaxLag = -1;
        remapIndex.clear();

        if (lexer != null) {
            lexer.clear();
            lexer = null;
        }
        schemaV1Parser.clear();
        textMetadataDetector.clear();
        jsonLexer.clear();
        forceHeaders = false;
        columnDelimiter = -1;
        typeManager.clear();
        timestampAdapter = null;
        skipLinesWithExtraValues = true;
    }

    @Override
    public void close() {
        this.writer = Misc.free(writer);
        Misc.free(ddlMem);
        Misc.free(tlw);
        Misc.free(textMetadataDetector);
        Misc.free(schemaV1Parser);
        Misc.free(jsonLexer);
        Misc.free(path);
        Misc.free(textDelimiterScanner);
        Misc.free(utf8Sink);
    }

    public void closeWriter() {
        writer = Misc.free(writer);
        metadata = null;
    }

    public void commit() {
        if (writer != null) {
            writer.commit();
        }
    }

    public void configureColumnDelimiter(byte columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
        assert this.columnDelimiter > 0;
    }

    public void configureDestination(
            CharSequence tableName,
            boolean walTable,
            boolean overwrite,
            int atomicity,
            int partitionBy,
            CharSequence timestampColumn,
            CharSequence timestampFormat
    ) {
        this.tableName = tableName;
        this.walTable = walTable;
        this.overwrite = overwrite;
        this.atomicity = atomicity;
        this.partitionBy = partitionBy;
        this.importedTimestampColumnName = timestampColumn;
        this.textDelimiterScanner.setTableName(tableName);
        this.schemaV1Parser.setTableName(tableName);
        this.timestampColumnName = timestampColumn;
        this.schema.clear();
        if (timestampFormat != null) {
            DateFormat dateFormat = typeManager.getInputFormatConfiguration().getTimestampFormatFactory().get(timestampFormat);
            this.timestampAdapter = (TimestampAdapter) typeManager.nextTimestampAdapter(
                    Chars.toString(timestampFormat),
                    false,
                    dateFormat,
                    textConfiguration.getDefaultDateLocale()
            );
        }

        LOG.info()
                .$("configured [table=`").$(tableName)
                .$("`, overwrite=").$(overwrite)
                .$(", atomicity=").$(atomicity)
                .$(", partitionBy=").$(PartitionBy.toString(partitionBy))
                .$(", timestamp=").$(timestampColumn)
                .$(", timestampFormat=").$(timestampFormat)
                .$(']').$();
    }

    public byte getColumnDelimiter() {
        return columnDelimiter;
    }

    public LongList getColumnErrorCounts() {
        return columnErrorCounts;
    }

    public long getErrorLineCount() {
        return lexer != null ? lexer.getErrorCount() : 0;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getO3MaxLag() {
        return o3MaxLag;
    }

    public long getParsedLineCount() {
        return lexer != null ? lexer.getLineCount() : 0;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getState() {
        return state;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public CsvTextLexer.Listener getTextListener() {
        return timestampAdapter != null ? partitionedListener : nonPartitionedListener;
    }

    public CharSequence getTimestampCol() {
        return designatedTimestampColumnName;
    }

    public int getWarnings() {
        return warnings;
    }

    public long getWrittenLineCount() {
        return writtenLineCount;
    }

    public boolean isForceHeaders() {
        return forceHeaders;
    }

    public boolean isHeaderDetected() {
        return textMetadataDetector.isHeader();
    }

    public void parse(long lo, long hi, int lineCountLimit, CsvTextLexer.Listener textLexerListener) {
        lexer.parse(lo, hi, lineCountLimit, textLexerListener);
    }

    public void parse(long lo, long hi, int lineCountLimit) {
        lexer.parse(lo, hi, lineCountLimit, getTextListener());
    }

    public void parse(long lo, long hi, SecurityContext securityContext) throws TextException {
        parseMethods.getQuick(state).parse(lo, hi, securityContext);
    }

    public final void restart(boolean header) {
        lexer.restart(header);
    }

    public void setDelimiter(byte delimiter) {
        this.lexer = tlw.getLexer(delimiter);
        this.lexer.setTableName(tableName);
        this.lexer.setSkipLinesWithExtraValues(skipLinesWithExtraValues);
    }

    public void setForceHeaders(boolean forceHeaders) {
        this.forceHeaders = forceHeaders;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    public void setSkipLinesWithExtraValues(boolean skipLinesWithExtraValues) {
        this.skipLinesWithExtraValues = skipLinesWithExtraValues;
    }

    public void setState(int state) {
        LOG.debug().$("state change [old=").$(this.state).$(", new=").$(state).$(']').$();
        this.state = state;
        jsonLexer.clear();
    }

    public void wrapUp() throws TextException {
        switch (state) {
            case LOAD_JSON_METADATA:
                try {
                    jsonLexer.parseLast();
                } catch (JsonException e) {
                    throw TextException.$(e.getFlyweightMessage());
                }
                break;
            case ANALYZE_STRUCTURE:
            case LOAD_DATA:
                // when file is empty lexer could be null because we
                // didn't get to find out the delimiter
                if (lexer != null) {
                    lexer.parseLast();
                }
                commit();
                break;
            default:
                break;
        }
    }

    private void checkUncommittedRowCount() {
        if (writer != null && maxUncommittedRows > 0 && writer.getUncommittedRowCount() >= maxUncommittedRows) {
            writer.ic(o3MaxLag);
        }
    }

    private TableToken createTable(
            ObjList<CharSequence> columnNames,
            ObjList<TypeAdapter> detectedColumnTypes,
            SecurityContext securityContext,
            Path path
    ) throws TextException {
        TableToken tableToken = engine.createTable(
                securityContext,
                ddlMem,
                path,
                false,
                tableStructureAdapter.of(columnNames, detectedColumnTypes),
                false
        );
        this.detectedColumnTypes = detectedColumnTypes;
        return tableToken;
    }

    private CharSequence getDesignatedTimestampColumnName(RecordMetadata metadata) {
        return metadata.getTimestampIndex() > -1 ? metadata.getColumnName(metadata.getTimestampIndex()) : null;
    }

    private void initWriterAndOverrideImportTypes(
            TableToken tableToken,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            TypeManager typeManager
    ) {
        final TableWriterAPI writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
        final RecordMetadata metadata = GenericRecordMetadata.copyDense(writer.getMetadata());

        // Now, compare column count. Cannot continue if different.

        if (metadata.getColumnCount() < detectedTypes.size()) {
            writer.close();
            throw CairoException.nonCritical()
                    .put("column count mismatch [textColumnCount=").put(detectedTypes.size())
                    .put(", tableColumnCount=").put(metadata.getColumnCount())
                    .put(", table=").put(tableName)
                    .put(']');
        }

        this.detectedColumnTypes = detectedTypes;

        // Overwrite detected types with actual table column types.
        remapIndex.ensureCapacity(detectedColumnTypes.size());
        for (int i = 0, n = detectedColumnTypes.size(); i < n; i++) {
            final int columnIndex = metadata.getColumnIndexQuiet(names.getQuick(i));
            final int idx = columnIndex > -1 ? columnIndex : i; // check for strict match ?
            remapIndex.set(i, metadata.getWriterIndex(idx));

            final int columnType = metadata.getColumnType(idx);
            final TypeAdapter detectedAdapter = detectedColumnTypes.getQuick(i);
            final int detectedType = detectedAdapter.getType();
            if (detectedType != columnType) {
                // when DATE type is mis-detected as STRING we
                // would not have either date format nor locale to
                // use when populating this field
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.DATE:
                        logTypeError(i);
                        this.detectedColumnTypes.setQuick(i, BadDateAdapter.INSTANCE);
                        break;
                    case ColumnType.TIMESTAMP:
                        if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                            this.detectedColumnTypes.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter));
                        } else {
                            logTypeError(i);
                            this.detectedColumnTypes.setQuick(i, BadTimestampAdapter.INSTANCE);
                        }
                        break;
                    case ColumnType.BINARY:
                        writer.close();
                        throw CairoException.nonCritical().put("cannot import text into BINARY column [index=").put(i).put(']');
                    default:
                        this.detectedColumnTypes.setQuick(i, typeManager.getTypeAdapter(columnType));
                        break;
                }
            }
        }

        this.writer = writer;
        this.metadata = metadata;
    }

    private void logError(long line, int i, DirectByteCharSequence dbcs) {
        LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(detectedColumnTypes.getQuick(i).getType())).$("]\n\t");
        logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dbcs).$();
        columnErrorCounts.increment(i);
    }

    private void logTypeError(int i) {
        LOG.info()
                .$("mis-detected [table=").$(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(detectedColumnTypes.getQuick(i).getType()))
                .$(']').$();
    }

    private boolean onField(long line, DirectByteCharSequence dbcs, TableWriter.Row w, int i) {
        try {
            final int tableIndex = remapIndex.size() > 0 ? remapIndex.get(i) : i;
            detectedColumnTypes.getQuick(i).write(w, tableIndex, dbcs);
        } catch (Exception ignore) {
            logError(line, i, dbcs);
            switch (atomicity) {
                case Atomicity.SKIP_ALL:
                    writer.rollback();
                    throw CairoException.nonCritical().put("bad syntax [line=").put(line).put(", col=").put(i).put(']');
                case Atomicity.SKIP_ROW:
                    w.cancel();
                    return true;
                default:
                    // SKIP column
                    break;
            }
        }
        return false;
    }

    private void onFieldsNonPartitioned(long line, ObjList<DirectByteCharSequence> values, int valuesLength) {
        final TableWriter.Row w = writer.newRow();
        for (int i = 0; i < valuesLength; i++) {
            final DirectByteCharSequence dbcs = values.getQuick(i);
            if (dbcs.length() == 0) {
                continue;
            }
            if (onField(line, dbcs, w, i)) return;
        }
        w.append();
        writtenLineCount++;
    }

    private void onFieldsPartitioned(long line, ObjList<DirectByteCharSequence> values, int valuesLength) {
        final int timestampIndex = this.timestampIndex;
        DirectByteCharSequence dbcs = values.getQuick(timestampIndex);
        try {
            final TableWriter.Row w = writer.newRow(timestampAdapter.getTimestamp(dbcs));
            for (int i = 0; i < valuesLength; i++) {
                dbcs = values.getQuick(i);
                if (i == timestampIndex || dbcs.length() == 0) {
                    continue;
                }
                if (onField(line, dbcs, w, i)) {
                    return;
                }
            }
            w.append();
            writtenLineCount++;
            checkUncommittedRowCount();
        } catch (Exception e) {
            logError(line, timestampIndex, dbcs);
        }
    }

    private void parseData(long lo, long hi, SecurityContext securityContext) {
        parse(lo, hi, Integer.MAX_VALUE);
    }

    private void parseJsonMetadata(long lo, long hi, SecurityContext securityContext) throws TextException {
        System.out.println("PARSING META");
        try {
            jsonLexer.parse(lo, hi, schemaV1Parser);
        } catch (JsonException e) {
            throw TextException.$(e.getFlyweightMessage());
        }
    }

    private void parseStructure(long lo, long hi, SecurityContext securityContext) throws TextException {
        if (columnDelimiter > 0) {
            setDelimiter(columnDelimiter);
        } else {
            setDelimiter(textDelimiterScanner.scan(lo, hi));
        }

        if (timestampColumnName != null && timestampAdapter != null) {
            schema.addColumn(timestampColumnName, ColumnType.TIMESTAMP, timestampAdapter);
        }

        final ObjList<CharSequence> columnNames = schemaV1Parser.getColumnNames();
        final ObjList<TypeAdapter> columnTypes = schemaV1Parser.getColumnTypes();

        final int n = columnNames.size();
        assert n == columnTypes.size();
        for (int i = 0; i < n; i++) {
            TypeAdapter columnType = columnTypes.getQuick(i);
            schema.addColumn(columnNames.getQuick(i), columnType.getType(), columnType);
        }

        textMetadataDetector.of(getTableName(), forceHeaders);

        System.out.println("STRUCTURE ???" + Thread.currentThread().getName());
        new Exception().printStackTrace();

        parse(lo, hi, textAnalysisMaxLines, textMetadataDetector);
        System.out.println("STRUCTURE DONE");
        textMetadataDetector.evaluateResults(getParsedLineCount(), getErrorLineCount());
        restart(textMetadataDetector.isHeader());

        // todo: use schema
        ObjList<CharSequence> names = textMetadataDetector.getColumnNames();
        ObjList<TypeAdapter> types = textMetadataDetector.getColumnTypes();

        assert writer == null;

        if (types.size() == 0) {
            throw CairoException.nonCritical().put("cannot determine text structure");
        }

        TableToken tableToken = engine.getTableTokenIfExists(tableName);

        boolean tableReCreated = false;
        switch (engine.getTableStatus(path, tableToken)) {
            case TableUtils.TABLE_DOES_NOT_EXIST:
                tableToken = createTable(names, types, securityContext, path);
                tableReCreated = true;
                writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                designatedTimestampColumnName = getDesignatedTimestampColumnName(metadata);
                designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                break;
            case TableUtils.TABLE_EXISTS:
                if (overwrite) {
                    securityContext.authorizeTableDrop(tableToken);
                    engine.drop(path, tableToken);
                    tableToken = createTable(names, types, securityContext, path);
                    tableReCreated = true;
                    writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                } else {
                    initWriterAndOverrideImportTypes(tableToken, names, types, typeManager);
                    designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    designatedTimestampColumnName = getDesignatedTimestampColumnName(writer.getMetadata());
                    if (importedTimestampColumnName != null &&
                            !Chars.equalsNc(importedTimestampColumnName, designatedTimestampColumnName)) {
                        warnings |= TextLoadWarning.TIMESTAMP_MISMATCH;
                    }
                    int tablePartitionBy = TableUtils.getPartitionBy(writer.getMetadata(), engine);
                    if (PartitionBy.isPartitioned(partitionBy) && partitionBy != tablePartitionBy) {
                        warnings |= TextLoadWarning.PARTITION_TYPE_MISMATCH;
                    }
                    partitionBy = tablePartitionBy;
                    tableStructureAdapter.of(names, types);
                    securityContext.authorizeInsert(tableToken);
                }
                break;
            default:
                throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
        }
        if (!tableReCreated && (o3MaxLag > -1 || maxUncommittedRows > -1)) {
            LOG.info().$("cannot update metadata attributes o3MaxLag and maxUncommittedRows when the table exists and parameter overwrite is false").$();
        }
        if (PartitionBy.isPartitioned(partitionBy)) {
            // We want to limit memory consumption during the import, so make sure
            // to use table's maxUncommittedRows and o3MaxLag if they're not set.
            if (o3MaxLag == -1 && !writer.getMetadata().isWalEnabled()) {
                o3MaxLag = TableUtils.getO3MaxLag(writer.getMetadata(), engine);
                LOG.info().$("using table's o3MaxLag ").$(o3MaxLag).$(", table=").utf8(tableName).$();
            }
            if (maxUncommittedRows == -1) {
                maxUncommittedRows = TableUtils.getMaxUncommittedRows(writer.getMetadata(), engine);
                LOG.info().$("using table's maxUncommittedRows ").$(maxUncommittedRows).$(", table=").utf8(tableName).$();
            }
        }
        columnErrorCounts.seed(writer.getMetadata().getColumnCount(), 0);

        if (
                timestampIndex != NO_INDEX
                        && timestampAdapter == null
                        && ColumnType.isTimestamp(this.detectedColumnTypes.getQuick(timestampIndex).getType())
        ) {
            this.timestampAdapter = (TimestampAdapter) this.detectedColumnTypes.getQuick(timestampIndex);
        }

        parse(lo, hi, Integer.MAX_VALUE);
        state = LOAD_DATA;
    }

    @FunctionalInterface
    protected interface ParserMethod {
        void parse(long lo, long hi, SecurityContext securityContext) throws TextException;
    }

    private class TableStructureAdapter implements TableStructure {
        private ObjList<CharSequence> columnNames;
        private ObjList<TypeAdapter> columnTypes;

        @Override
        public int getColumnCount() {
            return columnTypes.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return columnNames.getQuick(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return columnTypes.getQuick(columnIndex).getType();
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return cairoConfiguration.getIndexValueBlockSize();
        }

        @Override
        public int getMaxUncommittedRows() {
            return maxUncommittedRows > -1 && PartitionBy.isPartitioned(partitionBy) ? maxUncommittedRows : cairoConfiguration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return o3MaxLag > -1 && PartitionBy.isPartitioned(partitionBy) ? o3MaxLag : cairoConfiguration.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            return partitionBy;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return cairoConfiguration.getDefaultSymbolCacheFlag();
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return cairoConfiguration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
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
            return columnTypes.getQuick(columnIndex).isIndexed();
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            return walTable && PartitionBy.isPartitioned(partitionBy) && !Chars.isBlank(timestampColumnName);
        }

        TableStructureAdapter of(ObjList<CharSequence> columnNames, ObjList<TypeAdapter> columnTypes) throws TextException {
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;

            if (importedTimestampColumnName == null && designatedTimestampColumnName == null) {
                timestampIndex = NO_INDEX;
            } else if (importedTimestampColumnName != null) {
                timestampIndex = columnNames.indexOf(importedTimestampColumnName);
                if (timestampIndex == NO_INDEX) {
                    throw TextException.$("invalid timestamp column '").put(importedTimestampColumnName).put('\'');
                }
            } else {
                timestampIndex = columnNames.indexOf(designatedTimestampColumnName);
                if (timestampIndex == NO_INDEX) {
                    // columns in the imported file may not have headers, then use writer timestamp index
                    timestampIndex = designatedTimestampIndex;
                }
            }

            if (timestampIndex != NO_INDEX) {
                final TypeAdapter timestampAdapter = columnTypes.getQuick(timestampIndex);
                final int typeTag = ColumnType.tagOf(timestampAdapter.getType());
                if ((typeTag != ColumnType.LONG && typeTag != ColumnType.TIMESTAMP) || timestampAdapter == BadTimestampAdapter.INSTANCE) {
                    throw TextException.$("not a timestamp '").put(importedTimestampColumnName).put('\'');
                }
            }
            return this;
        }
    }
}
