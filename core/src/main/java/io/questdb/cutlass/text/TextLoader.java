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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.schema2.Column;
import io.questdb.cutlass.text.schema2.SchemaV2;
import io.questdb.cutlass.text.schema2.SchemaV2Parser;
import io.questdb.cutlass.text.types.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TextLoader implements Closeable, Mutable {

    public static final int ANALYZE_STRUCTURE = 1;
    public static final int LOAD_DATA = 2;
    public static final int LOAD_JSON_METADATA = 0;
    public static final int NO_INDEX = -1;
    private static final int CHECK_COLUMN_COUNT_DIVERGED = 1;
    private static final int CHECK_COLUMN_TYPE_MISMATCH = 2;
    private static final int CHECK_OK = 0;
    private static final Log LOG = LogFactory.getLog(TextLoader.class);
    private static final String WRITER_LOCK_REASON = "textLoader";
    private final CairoConfiguration cairoConfiguration;
    private final LongList columnErrorCounts = new LongList();
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final LowerCaseCharSequenceHashSet duplicateTableColumnNames = new LowerCaseCharSequenceHashSet();
    private final CairoEngine engine;
    private final JsonLexer jsonLexer;
    private final ObjList<CsvColumnMapping> mappingColumns = new ObjList<>();
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);
    private final ObjList<ParserMethod> parseMethods = new ObjList<>();
    private final Path path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
    /* maps csv columns to table writer column indexes (these may not be the same as column indexes) */
    private final IntList remapIndex = new IntList();
    private final SchemaV2 schema = new SchemaV2();
    private final SchemaV1Parser schemaV1Parser;
    private final SchemaV2Parser schemaV2Parser;
    private final LowerCaseCharSequenceHashSet tableColumnNamesSet = new LowerCaseCharSequenceHashSet();
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final int textAnalysisMaxLines;
    private final TextConfiguration textConfiguration;
    private final TextDelimiterScanner textDelimiterScanner;
    private final TextStructureAnalyser textStructureAnalyser;
    private final TextLexerWrapper tlw;
    private final TypeManager typeManager;
    private final DirectUtf16Sink utf8Sink;
    // indexes of csv columns, which types did not match table column types
    private final IntList typeMismatchCsvColumnIndexes = new IntList();
    // indexes of CSV columns that could not have been mapped to the table columns
    private final IntList unmappedCsvColumnIndexes = new IntList();
    private final DirectCharSink utf8Sink;
    // indexes of columns that do not match a csv file column (file_column_name or file_column_index is wrong)
    private final IntList wrongSourceSchemaColumnIndexes = new IntList();
    // We are looking for CSV columns that are not mapped at all, or
    // mapped to non-existing table columns. This list will contain
    // column indexes in the CSV schema that we could not map to the
    // table column names
    private final IntHashSet wrongTargetSchemaColumnIndexes = new IntHashSet();
    private int atomicity;
    private byte columnDelimiter = -1;
    private CharSequence designatedTimestampColumnName;
    private int designatedTimestampIndex;
    private boolean forceHeaders = false;
    // flag marking when table writer acquisition failed, used to skip repeating prior actions on retry to avoid e.g. duplicating columns
    private boolean getWriterFailed = false;
    private boolean ignoreEverything = false;
    private AbstractTextLexer lexer;
    private int maxUncommittedRows = -1;
    private RecordMetadata metadata;
    private long o3MaxLag = -1;
    private boolean overwriteTable;
    private int partitionBy;
    private CharSequence schemaTimestampColumnName;
    private int schemaVersion;
    private long skipLines;
    private boolean skipLinesWithExtraValues = true;
    private int state;
    private CharSequence tableName;
    private CharSequence tableTimestampColumnName;
    private ObjList<TypeAdapter> tableTypeAdapters;
    private TimestampAdapter timestampAdapter;
    // index of designated timestamp column in the csv
    private int timestampIndex = NO_INDEX;
    private boolean truncateTable;
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
        this.utf8Sink = new DirectUtf16Sink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        jsonLexer = new JsonLexer(textConfiguration.getJsonCacheSize(), textConfiguration.getJsonCacheLimit());
        textStructureAnalyser = new TextStructureAnalyser(typeManager, textConfiguration, schema);
        schemaV1Parser = new SchemaV1Parser(textConfiguration, typeManager);
        schemaV2Parser = new SchemaV2Parser(textConfiguration, typeManager);
        schemaV2Parser.withSchema(schema);
        textAnalysisMaxLines = textConfiguration.getTextAnalysisMaxLines();
        textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
        parseMethods.extendAndSet(LOAD_JSON_METADATA, this::parseJsonMetadata);
        parseMethods.extendAndSet(ANALYZE_STRUCTURE, this::analyseTextStructure);
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
        schemaTimestampColumnName = null;
        maxUncommittedRows = -1;
        o3MaxLag = -1;
        remapIndex.clear();

        if (lexer != null) {
            lexer.clear();
            lexer = null;
        }
        schemaV1Parser.clear();
        schemaV2Parser.clear();
        textStructureAnalyser.clear();
        jsonLexer.clear();
        forceHeaders = false;
        columnDelimiter = -1;
        typeManager.clear();
        timestampAdapter = null;
        skipLinesWithExtraValues = true;
        ignoreEverything = false;

        wrongTargetSchemaColumnIndexes.clear();
        wrongSourceSchemaColumnIndexes.clear();
        unmappedCsvColumnIndexes.clear();
        schema.clear();
        typeMismatchCsvColumnIndexes.clear();
        mappingColumns.clear();
        schemaVersion = 1;
        getWriterFailed = false;
        duplicateTableColumnNames.clear();
        tableColumnNamesSet.clear();
        skipLines = 0;
    }

    public void clearSchemas() {
        schemaV1Parser.clear();
        schema.clear();
    }

    @Override
    public void close() {
        this.writer = Misc.free(writer);
        Misc.free(ddlMem);
        Misc.free(tlw);
        Misc.free(textStructureAnalyser);
        Misc.free(schemaV1Parser);
        Misc.free(schemaV2Parser);
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
            @NotNull Utf8Sequence tableName,
            boolean walTable,
            boolean overwrite,
            int atomicity,
            int partitionBy,
            @Nullable Utf8Sequence timestampColumnName,
            @Nullable Utf8Sequence timestampFormat,
            boolean truncate
    ) {
        final String tableNameUtf16 = Utf8s.toString(tableName);
        final String timestampColumnUtf16 = Utf8s.toString(timestampColumnName);
        final String timestampFormatUtf16 = Utf8s.toString(timestampFormat);

        this.tableName = tableNameUtf16;
        this.walTable = walTable;
        this.overwriteTable = overwrite;
        this.truncateTable = !overwrite && truncate;
        this.atomicity = atomicity;
        this.partitionBy = partitionBy;
        this.schemaTimestampColumnName = timestampColumnUtf16;
        this.textDelimiterScanner.setTableName(tableNameUtf16);
        this.schemaV1Parser.setTableName(tableNameUtf16);
        this.tableTimestampColumnName = timestampColumnUtf16;

        if (timestampFormat != null) {
            DateFormat dateFormat = typeManager.getInputFormatConfiguration().getTimestampFormatFactory().get(timestampFormatUtf16);
            this.timestampAdapter = (TimestampAdapter) typeManager.nextTimestampAdapter(
                    Chars.toString(timestampFormatUtf16),
                    false,
                    dateFormat,
                    textConfiguration.getDefaultDateLocale()
            );
        }

        LOG.info()
                .$("configured [table=`").$(tableName)
                .$("`, overwrite=").$(overwrite)
                .$("`, truncate=").$(truncate)
                .$(", atomicity=").$(atomicity)
                .$(", partitionBy=").$(PartitionBy.toString(partitionBy))
                .$(", timestamp=").$(timestampColumnName)
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

    public ObjList<CsvColumnMapping> getMappingColumns() {
        return mappingColumns;
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
        return lexer != null ? Math.max(lexer.getLineCount() - lexer.getSkipLines(), 0) : 0;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public SchemaV2 getSchema() {
        return schema;
    }

    public int getState() {
        return state;
    }

    public boolean getCreate() {
        return textWriter.getCreate();
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
        return textStructureAnalyser.hasHeader();
    }

    public void parse(long lo, long hi, int lineCountLimit, CsvTextLexer.Listener textLexerListener) {
        lexer.parse(lo, hi, lineCountLimit, textLexerListener);
    }

    public void parse(long lo, long hi, int lineCountLimit) {
        lexer.parse(lo, hi, lineCountLimit, getTextListener());
    }

    public void parse(long lo, long hi, SecurityContext securityContext) throws TextException {
        if (ignoreEverything) {
            return;
        }
        parseMethods.getQuick(state).parse(lo, hi, securityContext);
    }

    public final void restart(boolean header) {
        lexer.restart(header);
    }

    public void setDelimiter(byte delimiter) {
        this.lexer = tlw.getLexer(delimiter);
        this.lexer.setTableName(tableName);
        this.lexer.setSkipLinesWithExtraValues(skipLinesWithExtraValues);
        this.lexer.setSkipLines(skipLines);
    }

    public void setForceHeaders(boolean forceHeaders) {
        this.forceHeaders = forceHeaders;
    }

    public void setCreate(boolean create) {
        textWriter.setCreate(create);
    }

    public void setIgnoreEverything(boolean ignoreEverything) {
        this.ignoreEverything = ignoreEverything;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    public void setSchemaVersion(int version) {
        this.schemaVersion = version;
    }

    public void setSkipLines(long skipLines) {
        this.skipLines = skipLines;
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

    private static int getColumnCount(TableRecordMetadata metadata) {
        int count = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (!metadata.getColumnMetadata(i).isDeleted()) {
                count++;
            }
        }
        return count;
    }

    private void analyseTextStructure(long lo, long hi, SecurityContext securityContext) throws TextException {
        if (columnDelimiter > 0) {
            setDelimiter(columnDelimiter);
        } else {
            try {
                setDelimiter(textDelimiterScanner.scan(lo, hi));
            } catch (TextException e) {
                throw TextException.$("Text delimiter can't be detected automatically. Please set it manually.");
            }
        }

        if (tableTimestampColumnName != null &&
                timestampAdapter != null &&
                !getWriterFailed) {
            schema.addColumn(tableTimestampColumnName, ColumnType.TIMESTAMP, timestampAdapter);
        }

        final ObjList<CharSequence> columnNames = schemaV1Parser.getColumnNames();
        final ObjList<TypeAdapter> columnTypes = schemaV1Parser.getColumnTypes();

        // add data from legacy schema v1 if client sent it
        final int columnNamesSize = columnNames.size();
        assert columnNamesSize == columnTypes.size();

        // don't add old schema columns to schema if writer acquisition failed and import is being re-tried
        if (schemaVersion != 2 && !getWriterFailed) {
            for (int i = 0; i < columnNamesSize; i++) {
                TypeAdapter columnType = columnTypes.getQuick(i);
                schema.addColumn(columnNames.getQuick(i), columnType.getType(), columnType);
            }
        }

        TableToken tableToken = engine.getTableTokenIfExists(tableName);
        final int tableStatus = engine.getTableStatus(path, tableToken);

        if (tableStatus == TableUtils.TABLE_EXISTS && !overwriteTable) {
            securityContext.authorizeInsert(tableToken);
            try {
                writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
            } catch (Throwable t) {
                getWriterFailed = true;
                throw t;
            }
            designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
            designatedTimestampColumnName = getDesignatedTimestampColumnName(writer.getMetadata());

            if (schemaTimestampColumnName != null &&
                    !Chars.equalsNc(schemaTimestampColumnName, designatedTimestampColumnName)) {
                warnings |= TextLoadWarning.TIMESTAMP_MISMATCH;
            }
            int tablePartitionBy = TableUtils.getPartitionBy(writer.getMetadata(), engine);
            if (PartitionBy.isPartitioned(partitionBy) && partitionBy != tablePartitionBy) {
                warnings |= TextLoadWarning.PARTITION_TYPE_MISMATCH;
            }
            partitionBy = tablePartitionBy;
            checkO3Options(false);

            final TableRecordMetadata existingTableMetadata = writer.getMetadata();

            // validate schema against table metadata
            // schema column list may not define CSV columns fully, it could:
            // 1. define all columns
            // 2. define fewer number of columns that CSV has
            // 3. define greater number of columns that CSV has
            // at this point we don't have csv header names yet and can't validate with file column index
            final ObjList<Column> schemaColumnList = schema.getColumnList();
            for (int i = 0, m = schemaColumnList.size(); i < m; i++) {
                Column column = schemaColumnList.get(i);
                CharSequence fileColumnName = column.getFileColumnName();
                CharSequence tableColumnName = column.getTableColumnName();
                int tableColumnIndex;

                if (tableColumnName != null) {
                    tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                } else if (fileColumnName != null) {
                    tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(fileColumnName);
                } else {
                    continue;
                }

                if (tableColumnIndex == -1 ||
                        existingTableMetadata.getColumnMetadata(tableColumnIndex).isDeleted()) {
                    wrongTargetSchemaColumnIndexes.add(i);
                }
            }

            // We cannot trust the schema to have correct order of columns as found in the CSV file.
            // Instead, we are going to read header names from the file and see
            // if we can alter unmapped column list (map some more columns) and set the required column types

            final ArrayColumnTypes requiredColumnTypes = new ArrayColumnTypes();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            boolean csvHeaderPresent = textStructureAnalyser.hasHeader();
            restart(csvHeaderPresent);

            final ObjList<CharSequence> tableColumnNames = new ObjList<>();

            // Technically we cannot import data when we find ambiguously mapped columns. However, to provide
            // better diagnostics we will analyse the CSV header regardless
            final int csvColumnCount;

            if (csvHeaderPresent) {
                // use combination of schema mappings and CSV headers to create list of required types for
                // which we need to establish the formatters
                final ObjList<CharSequence> csvHeaderNames = textStructureAnalyser.getColumnNames();

                csvColumnCount = csvHeaderNames.size();
                remapIndex.ensureCapacity(csvColumnCount);

                for (int i = 0, n = schemaColumnList.size(); i < n; i++) {
                    wrongSourceSchemaColumnIndexes.add(i);
                }

                for (int i = 0; i < csvColumnCount; i++) {
                    final CharSequence headerName = csvHeaderNames.getQuick(i);
                    CharSequence tableColumnName;
                    final int mappingColumnType;

                    Column column = schema.getFileColumnNameToColumnMap().get(headerName);
                    if (column != null) {
                        tableColumnName = column.getTableOrFileColumnName();
                        mappingColumnType = column.getColumnType();
                        wrongSourceSchemaColumnIndexes.remove(schemaColumnList.indexOf(column));
                    } else {
                        column = schema.getFileColumnIndexToColumnMap().get(i);
                        if (column != null) {
                            tableColumnName = column.getTableColumnName();
                            mappingColumnType = column.getColumnType();
                            wrongSourceSchemaColumnIndexes.remove(schemaColumnList.indexOf(column));
                        } else {
                            tableColumnName = headerName;
                            mappingColumnType = -1;
                        }
                    }

                    // two csv columns can map to same table column if they have the same header or
                    // csv column specified by index is mapped by file name
                    if (tableColumnNamesSet.contains(tableColumnName)) {
                        duplicateTableColumnNames.add(tableColumnName);
                    }

                    tableColumnNames.add(tableColumnName);
                    tableColumnNamesSet.add(tableColumnName);

                    if (column != null && column.isFileColumnIgnore()) {
                        // ignore column during mapping
                        requiredColumnTypes.add(-1);
                        remapIndex.setQuick(i, -1);// NoopTypeAdapter will ignore remap index
                        continue;
                    }

                    if (tableColumnName != null) {
                        int tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                        if (tableColumnIndex != -1 &&
                                !existingTableMetadata.getColumnMetadata(tableColumnIndex).isDeleted()) {
                            int tableColumnType = existingTableMetadata.getColumnType(tableColumnIndex);
                            requiredColumnTypes.add(tableColumnType);
                            remapIndex.setQuick(i, existingTableMetadata.getWriterIndex(tableColumnIndex));
                        } else {
                            // column doesn't exist or is deleted
                            tableColumnName = null;
                        }
                    }

                    if (tableColumnName == null) {
                        // User did not map this column to a table column, or table column doesn't exist.
                        // We will determine column type on the server even though we're not importing this file.
                        // We are helping user to map the column.
                        // If user specified the type we try to validate it against the data.
                        if (column != null) {
                            wrongTargetSchemaColumnIndexes.add(schemaColumnList.indexOf(column));
                        } else {
                            unmappedCsvColumnIndexes.add(i);
                        }
                        requiredColumnTypes.add(mappingColumnType > -1 ? mappingColumnType : ColumnType.TYPES_SIZE);
                        remapIndex.add(-1);
                    }
                }
            } else {
                // when CSV header is absent the required types are a subset of the
                // columns that have been mapped via the schema. The rest of CSV columns
                // are using auto-detected types to assist the user.

                csvColumnCount = textStructureAnalyser.getColumnTypes().size();
                remapIndex.ensureCapacity(csvColumnCount);

                for (int i = 0; i < csvColumnCount; i++) {
                    //Note: we don't check via synthetic column names, e.g. f0, f1, etc.
                    Column column = schema.getFileColumnIndexToColumnMap().get(i);
                    if (column == null) {
                        unmappedCsvColumnIndexes.add(i);
                        // auto-detect the type
                        requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        tableColumnNames.add(null);
                        continue;
                    }

                    CharSequence tableColumnName = column.getTableOrFileColumnName();
                    tableColumnNames.add(tableColumnName);
                    int tableColumnIndex = -1;
                    if (tableColumnName != null) {
                        tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                    }

                    if (column.isFileColumnIgnore()) {
                        // ignore detection and insert default value
                        requiredColumnTypes.add(-1);
                        remapIndex.setQuick(i, -1);
                        if (tableColumnIndex == -1) {
                            unmappedCsvColumnIndexes.add(i);
                        }
                    } else if (tableColumnIndex == -1) {
                        // validate against default adapters or that supplied by user
                        requiredColumnTypes.add(column.getColumnType() > -1 ? column.getColumnType() : ColumnType.TYPES_SIZE);
                        unmappedCsvColumnIndexes.add(i);
                    } else {
                        requiredColumnTypes.add(existingTableMetadata.getColumnType(tableColumnIndex));
                        remapIndex.setQuick(i, existingTableMetadata.getWriterIndex(tableColumnIndex));
                    }
                }
            }

            // we need to evaluate column formats for types we already know and also
            // types we do not know but that are present in the CSV file
            lexer.clear();
            lexer.setSkipLines(skipLines);
            textStructureAnalyser.clear();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            restart(csvHeaderPresent);

            if (wrongTargetSchemaColumnIndexes.size() > 0
                    || wrongSourceSchemaColumnIndexes.size() > 0
                    || unmappedCsvColumnIndexes.size() > 0
                    || duplicateTableColumnNames.size() > 0
            ) {
                updateMapping(csvHeaderPresent, tableColumnNames, existingTableMetadata);

                CairoException exception = CairoException.nonCritical().put("column mapping is invalid [");
                boolean isFirst = true;

                if (wrongTargetSchemaColumnIndexes.size() > 0) {
                    isFirst = false;
                    exception.put("unmapped_schema_column_indexes=").put(wrongTargetSchemaColumnIndexes);
                }

                if (unmappedCsvColumnIndexes.size() > 0) {
                    if (!isFirst) {
                        exception.put(",");
                    }
                    isFirst = false;
                    exception.put("unmapped_csv_column_indexes=").put(unmappedCsvColumnIndexes);
                }

                if (duplicateTableColumnNames.size() > 0) {
                    if (!isFirst) {
                        exception.put(",");
                    }
                    exception.put("ambiguous_table_column_names=").put(duplicateTableColumnNames);
                }

                exception.put(']');
                throw exception;
            }

            // This is where we are going to attempt file import. Our mapping has to be
            // perfect and unambiguous. We are using structure analyser's types only to
            // collect formatters. Otherwise, column types must match exactly.
            ObjList<TypeAdapter> csvColumnTypes = textStructureAnalyser.getColumnTypes();

            // csv column count on the final pass, it should be the same value as csvColumnCount unless there is a bug
            int finalPassCsvColumnCount = csvColumnTypes.size();

            // There is small chance that we could not find formatter for the give type
            // or perhaps due to a bug, structure analyser deviated from the prescribed
            // column types. We must validate the types we're going to use for the import
            int checkStatus = CHECK_OK;

            if (finalPassCsvColumnCount != csvColumnCount) {
                checkStatus = CHECK_COLUMN_COUNT_DIVERGED;
            }

            if (checkStatus == CHECK_OK) {
                for (int i = 0; i < csvColumnCount; i++) {
                    final TypeAdapter typeAdapter = csvColumnTypes.getQuick(i);
                    final int requiredColumnType = requiredColumnTypes.getColumnType(i);

                    if (requiredColumnType != ColumnType.TYPES_SIZE && typeAdapter.getType() != requiredColumnType) {
                        // accept mismatch caused by limitations of type analysis
                        if (requiredColumnType == ColumnType.SYMBOL && typeAdapter.getType() == ColumnType.STRING) {
                            boolean indexed = false;
                            CharSequence tableColumnName = tableColumnNames.get(i);
                            if (tableColumnName != null) {
                                int tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                                if (tableColumnIndex != -1) {
                                    indexed = existingTableMetadata.isColumnIndexed(tableColumnIndex);
                                }
                            }

                            csvColumnTypes.setQuick(i, typeManager.nextSymbolAdapter(indexed));
                            continue;
                        } else if (requiredColumnType == ColumnType.BINARY) {
                            throw CairoException.nonCritical().put("cannot import text into BINARY column [index=").put(i).put(']');
                        }

                        checkStatus = CHECK_COLUMN_TYPE_MISMATCH;
                        typeMismatchCsvColumnIndexes.add(i);
                    }
                }
            }

            if (checkStatus != CHECK_OK) {
                updateMapping(csvHeaderPresent, tableColumnNames, existingTableMetadata);

                if (checkStatus == CHECK_COLUMN_COUNT_DIVERGED) {
                    throw CairoException.nonCritical().put("column number mismatch");
                } else {
                    throw CairoException.nonCritical().put("column type mismatch");
                }
            }

            tableTypeAdapters = csvColumnTypes;
            int tableTimestampIndex = existingTableMetadata.getTimestampIndex();
            if (tableTimestampIndex == -1) {
                timestampIndex = NO_INDEX;
            } else {
                timestampIndex = remapIndex.indexOf(tableTimestampIndex);
                if (timestampIndex < 0) {
                    timestampIndex = NO_INDEX;
                }
            }

            if (tableTimestampIndex > -1 && timestampIndex == -1) {
                throw CairoException.nonCritical().put("designated timestamp column missing");
            }

            if (timestampIndex != NO_INDEX
                    && timestampAdapter == null
                    && ColumnType.isTimestamp(this.tableTypeAdapters.getQuick(timestampIndex).getType())
            ) {
                this.timestampAdapter = (TimestampAdapter) this.tableTypeAdapters.getQuick(timestampIndex);
            }

            updateMapping(csvHeaderPresent, tableColumnNames, existingTableMetadata);

            if (truncateTable) {
                if (writer.getMetadata().isWalEnabled()) {
                    writer.truncateSoft();
                } else {
                    if (engine.lockReaders(tableToken)) {
                        try {
                            writer.truncate();
                        } finally {
                            engine.unlockReaders(tableToken);
                        }
                    } else {
                        throw CairoException.nonCritical().put("can't lock readers to perform table truncate on '").put(tableToken).put("'");
                    }
                }
            }

            // when metadata is set, text output displays import stats
            metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
            parse(lo, hi, Integer.MAX_VALUE);
            state = LOAD_DATA;
        } else {
            // we are importing into a new table, there are no required column types
            // we're merging auto-detected column names and types with those provided in the schema

            ArrayColumnTypes requiredColumnTypes = new ArrayColumnTypes();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());

            boolean csvHeaderPresent = textStructureAnalyser.hasHeader();
            restart(csvHeaderPresent);

            final ObjList<CharSequence> tableColumnNames = new ObjList<>();
            final ObjList<CharSequence> csvHeaderNames = textStructureAnalyser.getColumnNames();
            final int csvColumnCount = csvHeaderNames.size();

            if (csvHeaderPresent) {
                // When CSV header present we could merge schema using column names
                // lookup column names and types from the schema
                for (int i = 0; i < csvColumnCount; i++) {
                    CharSequence headerName = csvHeaderNames.getQuick(i);

                    Column column = schema.getFileColumnNameToColumnMap().get(headerName);
                    if (column != null) {
                        // when column is present in the schema and table column name is present
                        // it will be used as new table column name
                        if (column.getTableColumnName() != null) {
                            tableColumnNames.add(column.getTableColumnName());
                            requiredColumnTypes.add(column.getColumnType() != -1 ? column.getColumnType() : ColumnType.TYPES_SIZE);
                        } else {
                            tableColumnNames.add(headerName);
                            requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        }
                    } else {
                        // we could not look up schema column by header name
                        // we should try to look it up by column index
                        column = schema.getFileColumnIndexToColumnMap().get(i);
                        if (column != null && column.getTableColumnName() != null) {
                            tableColumnNames.add(column.getFileColumnName());
                            requiredColumnTypes.add(column.getColumnType() != -1 ? column.getColumnType() : ColumnType.TYPES_SIZE);
                        } else {
                            tableColumnNames.add(headerName);
                            requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        }
                    }
                }
            } else {
                // when header is not present we can look up column names by csv column indexes
                for (int i = 0; i < csvColumnCount; i++) {
                    Column column = schema.getFileColumnIndexToColumnMap().get(i);
                    if (column != null) {
                        if (column.getTableColumnName() != null) {
                            tableColumnNames.add(column.getTableColumnName());
                        } else {
                            tableColumnNames.add(csvHeaderNames.getQuick(i));
                        }

                        requiredColumnTypes.add(column.getColumnType() != -1 ? column.getColumnType() : ColumnType.TYPES_SIZE);
                    } else {
                        tableColumnNames.add(csvHeaderNames.getQuick(i));
                        requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                    }
                }
            }

            // re-run analysis using the required column types, specified in the schema
            lexer.clear();
            lexer.setSkipLines(skipLines);
            textStructureAnalyser.clear();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            restart(csvHeaderPresent);

            ObjList<TypeAdapter> csvColumnTypes = textStructureAnalyser.getColumnTypes();
            int finalPassCsvColumnCount = csvColumnTypes.size();

            // There is small chance that we could not find formatter for the give type
            // or perhaps due to a bug, structure analyser deviated from the prescribed
            // column types. We must validate the types we're going to use for the import
            int checkStatus = CHECK_OK;
            if (finalPassCsvColumnCount != csvColumnCount) {
                checkStatus = CHECK_COLUMN_COUNT_DIVERGED;
            }

            if (checkStatus == CHECK_OK) {
                for (int i = 0; i < csvColumnCount; i++) {
                    final TypeAdapter typeAdapter = csvColumnTypes.getQuick(i);
                    final int requiredType = requiredColumnTypes.getColumnType(i);

                    // accept mismatches caused by limitations of type analysis
                    if (requiredType == ColumnType.SYMBOL && typeAdapter.getType() == ColumnType.STRING) {
                        csvColumnTypes.setQuick(i, typeManager.nextSymbolAdapter(false));
                        continue;
                    }

                    if (requiredType != ColumnType.TYPES_SIZE && typeAdapter.getType() != requiredType) {
                        checkStatus = CHECK_COLUMN_TYPE_MISMATCH;
                        typeMismatchCsvColumnIndexes.add(i);
                    }
                }
            }

            if (csvColumnCount == 0) {
                throw CairoException.nonCritical().put("cannot determine text structure");
            }

            if (checkStatus != CHECK_OK) {
                updateMapping(csvHeaderPresent, tableColumnNames, null);

                if (checkStatus == CHECK_COLUMN_COUNT_DIVERGED) {
                    throw CairoException.nonCritical().put("column number mismatch");
                } else {
                    throw CairoException.nonCritical().put("column type mismatch");
                }
            }

            boolean tableRecreated = false;
            // we can create table now and try importing
            switch (tableStatus) {
                case TableUtils.TABLE_DOES_NOT_EXIST:
                    tableToken = createTable(tableColumnNames, csvColumnTypes, securityContext, path);
                    writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                    designatedTimestampColumnName = getDesignatedTimestampColumnName(metadata);
                    designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    break;
                case TableUtils.TABLE_EXISTS:
                    securityContext.authorizeTableDrop(tableToken);
                    engine.drop(path, tableToken);
                    tableToken = createTable(tableColumnNames, csvColumnTypes, securityContext, path);
                    writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                    tableRecreated = true;
                    break;
                default:
                    throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
            }

            checkO3Options(tableRecreated);
            updateMapping(csvHeaderPresent, tableColumnNames, writer.getMetadata());

            columnErrorCounts.seed(getColumnCount(writer.getMetadata()), 0);

            if (
                    timestampIndex != NO_INDEX
                            && timestampAdapter == null
                            && ColumnType.isTimestamp(this.tableTypeAdapters.getQuick(timestampIndex).getType())
            ) {
                this.timestampAdapter = (TimestampAdapter) this.tableTypeAdapters.getQuick(timestampIndex);
            }

            parse(lo, hi, Integer.MAX_VALUE);
            state = LOAD_DATA;
        }

/*

            textStructureAnalyser.of(getTableName(), forceHeaders);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            restart(textStructureAnalyser.hasHeader());

            ObjList<CharSequence> names = textStructureAnalyser.getColumnNames();
            ObjList<TypeAdapter> types = textStructureAnalyser.getColumnTypes();

            assert writer == null;

            if (types.size() == 0) {
                throw CairoException.nonCritical().put("cannot determine text structure");
            }


            boolean tableReCreated = false;
            switch (tableStatus) {
                case TableUtils.TABLE_DOES_NOT_EXIST:
                    tableToken = createTable(names, types, securityContext, path);
                    tableReCreated = true;
                    writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                    designatedTimestampColumnName = getDesignatedTimestampColumnName(metadata);
                    designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    break;
                case TableUtils.TABLE_EXISTS:
                    if (overwriteTable) {
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
                        if (schemaTimestampColumnName != null &&
                                !Chars.equalsNc(schemaTimestampColumnName, designatedTimestampColumnName)) {
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
                            && ColumnType.isTimestamp(this.tableTypeAdapters.getQuick(timestampIndex).getType())
            ) {
                this.timestampAdapter = (TimestampAdapter) this.tableTypeAdapters.getQuick(timestampIndex);
            }

            parse(lo, hi, Integer.MAX_VALUE);
            state = LOAD_DATA;
*/
    }

    private void checkO3Options(boolean tableRecreated) {
        if (!tableRecreated && (o3MaxLag > -1 || maxUncommittedRows > -1)) {
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
        this.tableTypeAdapters = detectedColumnTypes;
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

        this.tableTypeAdapters = detectedTypes;

        // Overwrite detected types with actual table column types.
        remapIndex.ensureCapacity(tableTypeAdapters.size());
        for (int i = 0, n = tableTypeAdapters.size(); i < n; i++) {
            final int columnIndex = metadata.getColumnIndexQuiet(names.getQuick(i));
            final int idx = columnIndex > -1 ? columnIndex : i; // check for strict match ?
            remapIndex.set(i, metadata.getWriterIndex(idx));

            final int columnType = metadata.getColumnType(idx);
            final TypeAdapter detectedAdapter = tableTypeAdapters.getQuick(i);
            final int detectedType = detectedAdapter.getType();
            if (detectedType != columnType) {
                // when DATE type is mis-detected as STRING we
                // would not have either date format nor locale to
                // use when populating this field
                switch (ColumnType.tagOf(columnType)) {
                    case ColumnType.DATE:
                        logTypeError(i);
                        this.tableTypeAdapters.setQuick(i, BadDateAdapter.INSTANCE);
                        break;
                    case ColumnType.TIMESTAMP:
                        if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                            this.tableTypeAdapters.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter));
                        } else {
                            logTypeError(i);
                            this.tableTypeAdapters.setQuick(i, BadTimestampAdapter.INSTANCE);
                        }
                        break;
                    case ColumnType.BINARY:
                        writer.close();
                        throw CairoException.nonCritical().put("cannot import text into BINARY column [index=").put(i).put(']');
                    default:
                        this.tableTypeAdapters.setQuick(i, typeManager.getTypeAdapter(columnType));
                        break;
                }
            }
        }

        this.writer = writer;
        this.metadata = metadata;
    }

    private void logError(long line, int i, DirectUtf8Sequence dus) {
        LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(tableTypeAdapters.getQuick(i).getType())).$("]\n\t");
        logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dus).$();
        columnErrorCounts.increment(i);
    }

    private void logTypeError(int i) {
        LOG.info()
                .$("mis-detected [table=").$(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(tableTypeAdapters.getQuick(i).getType()))
                .$(']').$();
    }

    private boolean onField(long line, DirectUtf8Sequence dus, TableWriter.Row row, int i) {
        try {
            final int tableIndex = remapIndex.size() > 0 ? remapIndex.get(i) : i;
            tableTypeAdapters.getQuick(i).write(row, tableIndex, dus);
        } catch (Exception ignore) {
            logError(line, i, dus);
            switch (atomicity) {
                case Atomicity.SKIP_ALL:
                    writer.rollback();
                    throw CairoException.nonCritical().put("bad syntax [line=").put(line).put(", col=").put(i).put(']');
                case Atomicity.SKIP_ROW:
                    row.cancel();
                    return true;
                default:
                    // SKIP column
                    break;
            }
        }
        return false;
    }

    private void onFieldsNonPartitioned(long line, ObjList<DirectUtf8String> values, int valuesLength) {
        final TableWriter.Row w = writer.newRow();
        for (int i = 0; i < valuesLength; i++) {
            final DirectUtf8String dus = values.getQuick(i);
            if (dus.size() == 0) {
                continue;
            }
            if (onField(line, dus, w, i)) return;
        }
        w.append();
        writtenLineCount++;
    }

    private void onFieldsPartitioned(long line, ObjList<DirectUtf8String> values, int valuesLength) {
        final int timestampIndex = this.timestampIndex;
        DirectUtf8String dus = values.getQuick(timestampIndex);
        try {
            final TableWriter.Row w = writer.newRow(timestampAdapter.getTimestamp(dus));
            for (int i = 0; i < valuesLength; i++) {
                dus = values.getQuick(i);
                if (i == timestampIndex || dus.size() == 0) {
                    continue;
                }
                if (onField(line, dus, w, i)) {
                    return;
                }
            }
            w.append();
            writtenLineCount++;
            checkUncommittedRowCount();
        } catch (Exception e) {
            logError(line, timestampIndex, dus);
        }
    }

    private void parseData(long lo, long hi, SecurityContext securityContext) {
        parse(lo, hi, Integer.MAX_VALUE);
    }

    private void parseJsonMetadata(long lo, long hi, SecurityContext securityContext) throws TextException {
        try {
            if (schemaVersion == 2) {
                jsonLexer.parse(lo, hi, schemaV2Parser);
            } else {
                jsonLexer.parse(lo, hi, schemaV1Parser);
            }
        } catch (JsonException e1) {
            throw TextException.$("[").put(e1.getPosition()).put("] ").put(e1.getFlyweightMessage());
        }
    }

    // change existing mapping to make it as compatible with source file and target table as possible
    private void updateMapping(boolean csvHeaderPresent, ObjList<CharSequence> tableColumnNames, TableRecordMetadata tableMetadata) {
        // if header is missing input columns receive synthetic f_idx names so the number of headers should match that in file
        final ObjList<CharSequence> csvHeaderNames = textStructureAnalyser.getColumnNames();
        ObjList<TypeAdapter> csvColumnTypes = textStructureAnalyser.getColumnTypes();

        // prepare column mapping primarily based on csv file content
        mappingColumns.clear();
        for (int i = 0, n = csvHeaderNames.size(); i < n; i++) {
            CsvColumnMapping column = new CsvColumnMapping();
            column.csvColumnIndex = i;
            column.csvColumnName = csvHeaderPresent ? csvHeaderNames.getQuick(i) : null;
            column.csvColumnAdapter = csvColumnTypes.getQuick(i);

            if (!unmappedCsvColumnIndexes.contains(i)) {
                column.tableColumnName = tableColumnNames.get(i);
                if (tableMetadata != null) {
                    int tableColumnIndex = tableMetadata.getColumnIndexQuiet(column.tableColumnName);
                    if (tableColumnIndex > -1) {
                        column.tableColumnType = tableMetadata.getColumnType(tableColumnIndex);
                    }
                }
                column.errors = 0;
                if (duplicateTableColumnNames.contains(column.tableColumnName)) {
                    column.errors |= CsvColumnMapping.STATUS_DUPLICATE_TARGET;
                }
                // tableColumnType defaults to -1
            } else {
                column.tableColumnName = null;
                column.tableColumnType = -1;
                column.errors |= CsvColumnMapping.STATUS_UNMAPPED;
            }

            column.errors |= typeMismatchCsvColumnIndexes.contains(i) ? CsvColumnMapping.STATUS_MISTYPED : 0;

            Column schemaColumn = null;
            if (csvHeaderPresent && column.csvColumnName != null) {
                schemaColumn = schema.getFileColumnNameToColumnMap().get(column.csvColumnName);
            }
            if (schemaColumn == null && column.csvColumnIndex > -1) {
                schemaColumn = schema.getFileColumnIndexToColumnMap().get(column.csvColumnIndex);
            }

            if (schemaColumn != null) {
                if (schemaColumn.isFileColumnIgnore()) {
                    column.csvColumnIgnore = true;
                } else {
                    int schemaColumnIndex = schema.getColumnList().indexOf(schemaColumn);
                    if (wrongTargetSchemaColumnIndexes.contains(schemaColumnIndex)) {
                        column.errors |= CsvColumnMapping.STATUS_BAD_TARGET;
                    }
                }
            }

            mappingColumns.add(column);
        }

        if (wrongSourceSchemaColumnIndexes.size() > 0) {
            for (int i = 0, n = wrongSourceSchemaColumnIndexes.size(); i < n; i++) {
                int schemaColumnIndex = wrongSourceSchemaColumnIndexes.getQuick(i);
                Column column = schema.getColumn(schemaColumnIndex);
                if (column != null) {
                    CsvColumnMapping csvColumn = new CsvColumnMapping();
                    csvColumn.csvColumnIndex = column.getFileColumnIndex();
                    csvColumn.csvColumnName = column.getFileColumnName();
                    csvColumn.tableColumnName = column.getTableColumnName();
                    csvColumn.tableColumnType = column.getColumnType();
                    csvColumn.csvColumnAdapter = column.getColumnType() > -1 ? typeManager.getTypeAdapter(column.getColumnType()) : null;
                    csvColumn.errors = CsvColumnMapping.STATUS_BAD_SOURCE;

                    if (wrongTargetSchemaColumnIndexes.contains(schemaColumnIndex)) {
                        csvColumn.errors |= CsvColumnMapping.STATUS_BAD_TARGET;
                    }

                    mappingColumns.add(csvColumn);
                }
            }
        }

        LOG.debug().$("mapping ").$(mappingColumns).$();
    }

    @FunctionalInterface
    protected interface ParserMethod {
        void parse(long lo, long hi, SecurityContext securityContext) throws TextException;
    }

    // TODO: merge with schema column
    public static class CsvColumnMapping implements Sinkable {

        static final int STATUS_BAD_SOURCE = 4;
        static final int STATUS_BAD_TARGET = 8;
        static final int STATUS_DUPLICATE_TARGET = 16;
        static final int STATUS_MAX_EXP = 4;
        static final int STATUS_MISTYPED = 2;
        static final int STATUS_UNMAPPED = 1;
        private static final IntObjHashMap<String> typeMessageMap = new IntObjHashMap<>();
        private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
        private TypeAdapter csvColumnAdapter;
        private boolean csvColumnIgnore;
        private int csvColumnIndex;
        private CharSequence csvColumnName;
        private int errors;
        private CharSequence tableColumnName;
        private int tableColumnType = -1;

        public void errorsToSink(@NotNull CharSinkBase<?> sink) {
            if (errors != 0) {
                boolean isFirst = true;
                for (int exp = 0; exp <= STATUS_MAX_EXP; exp++) {
                    int error = 1 << exp;
                    if ((errors & error) != 0) {
                        if (!isFirst) {
                            sink.put(',');
                        }
                        isFirst = false;

                        sink.put(nameOf(error));
                    }
                }
            } else {
                sink.put("OK");
            }
        }

        public TypeAdapter getCsvColumnAdapter() {
            return csvColumnAdapter;
        }

        public int getCsvColumnIndex() {
            return csvColumnIndex;
        }

        public CharSequence getCsvColumnName() {
            return csvColumnName;
        }

        public int getErrors() {
            return errors;
        }

        public CharSequence getTableColumnName() {
            return tableColumnName;
        }

        public int getTableColumnType() {
            return tableColumnType;
        }

        public void toJson(CharSinkBase<?> sink) {
            sink.putAscii('{');
            sink.putAscii("\"file_column_name\":\"").put(csvColumnName).putAscii("\",");
            sink.putAscii("\"file_column_index\":").put(csvColumnIndex).putAscii(',');
            sink.putAscii("\"file_column_ignore\":").put(csvColumnIgnore).putAscii(',');
            sink.putAscii("\"column_type\":\"").putAscii(csvColumnAdapter != null ? ColumnType.nameOf(csvColumnAdapter.getType()) : "").putAscii("\",");
            sink.putAscii("\"table_column_name\":\"").put(tableColumnName).putAscii("\",");
            sink.putAscii("\"formats\":").putAscii('[');
            if (csvColumnAdapter != null) {
                csvColumnAdapter.toSink(sink);
            }
            if (errors != 0) {
                sink.putAscii(",\"errors\": [");
                boolean isFirst = true;
                for (int exp = 0; exp <= STATUS_MAX_EXP; exp++) {
                    int error = 1 << exp;
                    if ((errors & error) != 0) {
                        if (!isFirst) {
                            sink.put(',');
                        }
                        isFirst = false;

                        sink.putQuoted(messageOf(error));
                    }
                }
                sink.putAscii(']');
            }
            sink.putAscii(']');
            sink.putAscii('}');
        }

        @Override
        public void toSink(@NotNull CharSinkBase<?> sink) {
            boolean isFirst = true;
            sink.putAscii('[');
            if (csvColumnIndex > -1) {
                sink.putAscii("csvIndex=").put(csvColumnIndex);
                isFirst = false;
            }

            if (csvColumnName != null) {
                if (!isFirst) {
                    sink.putAscii(',');
                }
                isFirst = false;
                sink.putAscii("csvColumnName=").put(csvColumnName);
            }

            if (csvColumnAdapter != null) {
                if (!isFirst) {
                    sink.putAscii(',');
                }
                isFirst = false;
                sink.putAscii("csvColumnType=").putAscii(ColumnType.nameOf(csvColumnAdapter.getType()));
            }

            if (tableColumnName != null) {
                if (!isFirst) {
                    sink.putAscii(',');
                }
                isFirst = false;
                sink.putAscii(",tableColumnName=").put(tableColumnName);
            }

            if (tableColumnType > -1) {
                if (!isFirst) {
                    sink.putAscii(',');
                }
                isFirst = false;
                sink.putAscii(",tableColumnType=").putAscii(ColumnType.nameOf(tableColumnType));
            }

            if (errors != 0) {
                if (!isFirst) {
                    sink.putAscii(',');
                }
                sink.putAscii("errors=");
                errorsToSink(sink);
            }

            sink.put(']');
        }

        static CharSequence messageOf(int errorType) {
            final int index = typeMessageMap.keyIndex(errorType);
            if (index > -1) {
                return "UNKNOWN";
            }
            return typeMessageMap.valueAtQuick(index);
        }

        static CharSequence nameOf(int errorType) {
            final int index = typeNameMap.keyIndex(errorType);
            if (index > -1) {
                return "UNKNOWN";
            }
            return typeNameMap.valueAtQuick(index);
        }

        static {
            typeNameMap.put(STATUS_MISTYPED, "MISTYPED");
            typeNameMap.put(STATUS_UNMAPPED, "UNMAPPED");
            typeNameMap.put(STATUS_BAD_SOURCE, "BAD CSV REF");
            typeNameMap.put(STATUS_BAD_TARGET, "BAD TABLE REF");
            typeNameMap.put(STATUS_DUPLICATE_TARGET, "DUP TABLE REF");

            typeMessageMap.put(STATUS_MISTYPED, "Type mismatch");
            typeMessageMap.put(STATUS_UNMAPPED, "Unmapped column");
            typeMessageMap.put(STATUS_BAD_SOURCE, "Bad csv column reference");
            typeMessageMap.put(STATUS_BAD_TARGET, "Bad table column reference");
            typeMessageMap.put(STATUS_DUPLICATE_TARGET, "Duplicate table column reference");
        }
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
            return walTable && PartitionBy.isPartitioned(partitionBy) && !Chars.isBlank(tableTimestampColumnName);
        }

        TableStructureAdapter of(ObjList<CharSequence> columnNames, ObjList<TypeAdapter> columnTypes) throws TextException {
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;

            if (schemaTimestampColumnName == null && designatedTimestampColumnName == null) {
                timestampIndex = NO_INDEX;
            } else if (schemaTimestampColumnName != null) {
                timestampIndex = columnNames.indexOf(schemaTimestampColumnName);
                if (timestampIndex == NO_INDEX) {
                    throw TextException.$("invalid timestamp column '").put(schemaTimestampColumnName).put('\'');
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
                    throw TextException.$("not a timestamp '").put(schemaTimestampColumnName).put('\'');
                }
            }
            return this;
        }
    }
}
