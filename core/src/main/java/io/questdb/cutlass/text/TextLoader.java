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
    private final TextStructureAnalyser textStructureAnalyser;
    private final TextLexerWrapper tlw;
    private final TypeManager typeManager;
    private final DirectCharSink utf8Sink;
    private int atomicity;
    private byte columnDelimiter = -1;
    private CharSequence designatedTimestampColumnName;
    private int designatedTimestampIndex;
    private boolean forceHeaders = false;
    private boolean ignoreEverything = false;
    private AbstractTextLexer lexer;
    private int maxUncommittedRows = -1;
    private RecordMetadata metadata;
    private long o3MaxLag = -1;
    private boolean overwriteTable;
    private int partitionBy;
    private CharSequence schemaTimestampColumnName;
    private boolean skipLinesWithExtraValues = true;
    private int state;
    private CharSequence tableName;
    private CharSequence tableTimestampColumnName;
    private ObjList<TypeAdapter> tableTypeAdapters;
    private TimestampAdapter timestampAdapter;
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
        textStructureAnalyser = new TextStructureAnalyser(typeManager, textConfiguration, schema);
        schemaV1Parser = new SchemaV1Parser(textConfiguration, typeManager);
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
        textStructureAnalyser.clear();
        jsonLexer.clear();
        forceHeaders = false;
        columnDelimiter = -1;
        typeManager.clear();
        timestampAdapter = null;
        skipLinesWithExtraValues = true;
        ignoreEverything = false;
    }

    @Override
    public void close() {
        this.writer = Misc.free(writer);
        Misc.free(ddlMem);
        Misc.free(tlw);
        Misc.free(textStructureAnalyser);
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
            CharSequence timestampColumnName,
            CharSequence timestampFormat
    ) {
        this.tableName = tableName;
        this.walTable = walTable;
        this.overwriteTable = overwrite;
        this.atomicity = atomicity;
        this.partitionBy = partitionBy;
        this.schemaTimestampColumnName = timestampColumnName;
        this.textDelimiterScanner.setTableName(tableName);
        this.schemaV1Parser.setTableName(tableName);
        this.tableTimestampColumnName = timestampColumnName;
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
    }

    public void setForceHeaders(boolean forceHeaders) {
        this.forceHeaders = forceHeaders;
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

    private void analyseTextStructure(long lo, long hi, SecurityContext securityContext) throws TextException {
        if (columnDelimiter > 0) {
            setDelimiter(columnDelimiter);
        } else {
            setDelimiter(textDelimiterScanner.scan(lo, hi));
        }

        if (tableTimestampColumnName != null && timestampAdapter != null) {
            schema.addColumn(tableTimestampColumnName, ColumnType.TIMESTAMP, timestampAdapter);
        }

        final ObjList<CharSequence> columnNames = schemaV1Parser.getColumnNames();
        final ObjList<TypeAdapter> columnTypes = schemaV1Parser.getColumnTypes();

        final int n = columnNames.size();
        assert n == columnTypes.size();
        for (int i = 0; i < n; i++) {
            TypeAdapter columnType = columnTypes.getQuick(i);
            schema.addColumn(columnNames.getQuick(i), columnType.getType(), columnType);
        }

        TableToken tableToken = engine.getTableTokenIfExists(tableName);
        final int tableStatus = engine.getTableStatus(path, tableToken);

        // csv column count on the final pass, it should be the same value as csvColumnCount unless
        // there is a bug
        int finalPassCsvColumnCount;
        // indexes of csv columns, which types did not match table column types
        final IntList typeMismatchCsvColumnIndexes = new IntList();

        if (tableStatus == TableUtils.TABLE_EXISTS && !overwriteTable) {
            securityContext.authorizeInsert(tableToken);
            writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);

            // validate table schema JSON that is supplied to us
            // to ensure we have no ambiguities as to mapping columns from CSV to the
            // table and also their types
            ObjList<CharSequence> tableColumnNamesNotInSchema = new ObjList<>();
            LowerCaseCharSequenceHashSet tableColumnNamesNotInSchemaSet = new LowerCaseCharSequenceHashSet();
            ObjList<CharSequence> tableColumnNamesAmbiguouslyReferencedFromSchema = new ObjList<>();

            final TableRecordMetadata existingTableMetadata = writer.getMetadata();
            final int tableColumnCount = existingTableMetadata.getColumnCount();
            // we are looking for unmapped or ambiguously mapped columns in the
            // target table
            for (int i = 0; i < tableColumnCount; i++) {
                String tableColumnName = existingTableMetadata.getColumnName(i);

                Column column = schema.getFileColumnNameToColumnMap().get(tableColumnName);
                if (column == null) {
                    column = schema.getTableColumnNameToColumnMap().get(tableColumnName);
                    if (column == null) {
                        tableColumnNamesNotInSchema.add(tableColumnName);
                        tableColumnNamesNotInSchemaSet.add(tableColumnName);
                    }
                } else {
                    Column otherColumn = schema.getTableColumnNameToColumnMap().get(tableColumnName);
                    if (otherColumn != null && otherColumn != column) {
                        tableColumnNamesAmbiguouslyReferencedFromSchema.add(tableColumnName);
                    }
                }
            }

            // We are looking for CSV columns that are not mapped at all, or
            // mapped to non-existing table columns. This list will contain
            // column indexes in the CSV schema that we could not map to the
            // table column names
            IntList unmappedSchemaColumnIndexes = new IntList();

            // indexes of CSV columns that could not have been mapped to the table columns
            IntList unmappedCsvColumnIndexes = new IntList();

            // indexes of CSV columns that are explicitly typed and their types do not match
            // types of table column names
            IntList mistypedCsvColumnIndexes = new IntList();

            // schema column list may not define CSV columns fully, it could:
            // 1. define all columns
            // 2. define fewer number of columns that CSV has
            // 3. define greater number of columns that CSV has
            final ObjList<Column> csvColumnList = schema.getColumnList();
            for (int i = 0, m = csvColumnList.size(); i < m; i++) {
                Column column = csvColumnList.get(i);
                CharSequence tableColumnName = column.getFileColumnName();
                int tableColumnIndex = -1;
                // this is CSV header column name, it may match table column name exactly

                if (tableColumnName != null) {
                    tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                    if (tableColumnIndex == -1) {
                        // zero out column name that did not match anything in the table
                        // assuming user is providing explicit mapping
                        tableColumnName = null;
                    }
                }

                if (tableColumnName == null) {
                    // csv could be missing the header, in which case
                    // csv column must be explicitly mapped to table column
                    tableColumnName = column.getTableColumnName();

                    if (tableColumnName == null) {
                        unmappedSchemaColumnIndexes.add(i);
                        continue;
                    }


                    tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                    if (tableColumnIndex == -1) {
                        unmappedSchemaColumnIndexes.add(i);
                    }
                }
            }

            // We cannot trust the schema to have correct order of columns as
            // found in the CSV file. Instead, we are going to read header names from the file and see
            // if we can alter unmapped column list (map some more columns) and set the
            // required column types

            // at this stage required column types is empty
            final ArrayColumnTypes requiredColumnTypes = new ArrayColumnTypes();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            boolean csvHeaderPresent = textStructureAnalyser.hasHeader();
            restart(csvHeaderPresent);

            // Technically we cannot import data when we find ambiguously mapped columns. However, to provide
            // better diagnostics we will analyse the CSV header regardless
            final int csvColumnCount;
            if (csvHeaderPresent) {
                // use combination of schema mappings and CSV headers to create list of required types for
                // which we need to establish the formatters
                final ObjList<CharSequence> csvHeaderNames = textStructureAnalyser.getColumnNames();

                csvColumnCount = csvHeaderNames.size();
                remapIndex.ensureCapacity(csvColumnCount);

                for (int i = 0; i < csvColumnCount; i++) {
                    final CharSequence headerName = csvHeaderNames.getQuick(i);
                    final CharSequence tableColumnName;
                    final int csvColumnType;

                    Column column = schema.getFileColumnNameToColumnMap().get(headerName);
                    if (column != null) {
                        tableColumnName = column.getTableOrFileColumnName();
                        csvColumnType = column.getColumnType();
                    } else {
                        column = schema.getFileColumnIndexToColumnMap().get(i);
                        if (column != null) {
                            tableColumnName = column.getTableOrFileColumnName();
                            csvColumnType = column.getColumnType();
                        } else {
                            tableColumnName = headerName;
                            // column type is undefined
                            csvColumnType = -1;
                        }
                    }

                    if (tableColumnName == null) {
                        unmappedCsvColumnIndexes.add(i);
                        // user did not map this colum, we will determine column type on the server
                        // even though we're not importing this file. We are helping user to map
                        // the column.
                        requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                    } else {
                        int tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(headerName);
                        if (tableColumnIndex != -1) {
                            int tableColumnType = existingTableMetadata.getColumnType(tableColumnIndex);
                            if (csvColumnType != -1 && csvColumnType != tableColumnType) {
                                mistypedCsvColumnIndexes.add(i);
                            }
                            requiredColumnTypes.add(tableColumnType);
                            remapIndex.setQuick(i, existingTableMetadata.getWriterIndex(tableColumnIndex));
                        } else {
                            unmappedCsvColumnIndexes.add(i);
                            requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        }
                    }
                }
            } else {
                // when CSV header is absent the required types are a subset of the
                // columns that have been mapped via the schema. The rest of CSV columns
                // are using auto-detected types to assist the user.

                csvColumnCount = textStructureAnalyser.getColumnTypes().size();
                remapIndex.ensureCapacity(csvColumnCount);

                for (int i = 0; i < csvColumnCount; i++) {
                    Column column = schema.getFileColumnIndexToColumnMap().get(i);
                    if (column == null) {
                        // auto-detect the type
                        requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        unmappedCsvColumnIndexes.add(i);
                    } else {
                        CharSequence tableColumnName = column.getTableOrFileColumnName();
                        int tableColumnIndex = existingTableMetadata.getColumnIndexQuiet(tableColumnName);
                        if (tableColumnIndex == -1) {
                            requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                            unmappedCsvColumnIndexes.add(i);
                        } else {
                            requiredColumnTypes.add(existingTableMetadata.getColumnType(tableColumnIndex));
                            remapIndex.setQuick(i, existingTableMetadata.getWriterIndex(tableColumnIndex));
                        }
                    }
                }
            }

            // we need to evaluate column formats for types we already know and also
            // types we do not know but that are present in the CSV file
            lexer.clear();
            textStructureAnalyser.clear();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            restart(csvHeaderPresent);

            if (
                    unmappedSchemaColumnIndexes.size() == 0
                            && tableColumnNamesAmbiguouslyReferencedFromSchema.size() == 0
                            && unmappedCsvColumnIndexes.size() == 0
                            && mistypedCsvColumnIndexes.size() == 0
                            && csvColumnCount == tableColumnCount
            ) {
                // This is where we are going to attempt file import. Our mapping has to be
                // perfect and unambiguous. We are using structure analyser's types only to
                // collect formatters. Otherwise, column types must match exactly\

                ObjList<TypeAdapter> csvColumnTypes = textStructureAnalyser.getColumnTypes();
                finalPassCsvColumnCount = csvColumnTypes.size();
                // There is small chance that we could not find formatter for the give type
                // or perhaps due to a bug, structure analyser deviated from the prescribed
                // column types. We must validate the types we're going to use for the import

                int checkStatus = 0; // CHECK_OK

                if (finalPassCsvColumnCount != csvColumnCount) {
                    checkStatus = 1; // CHECK_COLUMN_COUNT_DIVERGED
                }

                if (checkStatus == 0) {
                    for (int i = 0; i < csvColumnCount; i++) {
                        final TypeAdapter typeAdapter = csvColumnTypes.getQuick(i);
                        final int requiredColumnType = requiredColumnTypes.getColumnType(i);
                        if (requiredColumnType != ColumnType.TYPES_SIZE && typeAdapter.getType() != requiredColumnType) {
                            checkStatus = 2;
                            typeMismatchCsvColumnIndexes.add(i);
                        }
                    }
                }

                // todo: if CSV has fewer columns than the table, the schema must have explicit "insert NULL" attribute for those columns
                // todo: if CSV has greater number of columns than the table, the schema must have "ignore" attribute

                if (checkStatus == 0) {
                    tableTypeAdapters = csvColumnTypes;
                    timestampIndex = existingTableMetadata.getTimestampIndex();
                    if (
                            timestampIndex != NO_INDEX
                                    && timestampAdapter == null
                                    && ColumnType.isTimestamp(this.tableTypeAdapters.getQuick(timestampIndex).getType())
                    ) {
                        this.timestampAdapter = (TimestampAdapter) this.tableTypeAdapters.getQuick(timestampIndex);
                    }

                    parse(lo, hi, Integer.MAX_VALUE);
                    state = LOAD_DATA;
                } else {
                    // todo: produce JSON error
                }
            } else {
                // there are unmapped columns
                // todo: produce response json
                throw CairoException.nonCritical()
                        .put("bonzaiii2 [textColumnCount=").put(0)
                        .put(", tableColumnCount=").put(existingTableMetadata.getColumnCount())
                        .put(", table=").put(tableName)
                        .put(']');
            }
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
                            if (column.getColumnType() != -1) {
                                requiredColumnTypes.add(column.getColumnType());
                            } else {
                                requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                            }
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
                            if (column.getColumnType() != -1) {
                                requiredColumnTypes.add(column.getColumnType());
                            } else {
                                requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                            }
                        } else {
                            tableColumnNames.add(headerName);
                            requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        }
                    }
                }
            } else {
                // when header is not present we can look up column names by
                // csv column indexes
                for (int i = 0; i < csvColumnCount; i++) {
                    Column column = schema.getFileColumnIndexToColumnMap().get(i);
                    if (column != null && column.getTableColumnName() != null) {
                        tableColumnNames.add(column.getFileColumnName());
                        if (column.getColumnType() != -1) {
                            requiredColumnTypes.add(column.getColumnType());
                        } else {
                            requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                        }
                    } else {
                        tableColumnNames.add(csvHeaderNames.getQuick(i));
                        requiredColumnTypes.add(ColumnType.TYPES_SIZE);
                    }
                }

            }

            // re-run analysis using the required column types, specified in the schema
            lexer.clear();
            textStructureAnalyser.clear();
            textStructureAnalyser.of(getTableName(), forceHeaders, requiredColumnTypes);
            parse(lo, hi, textAnalysisMaxLines, textStructureAnalyser);
            textStructureAnalyser.evaluateResults(getParsedLineCount(), getErrorLineCount());
            restart(csvHeaderPresent);

            ObjList<TypeAdapter> csvColumnTypes = textStructureAnalyser.getColumnTypes();
            finalPassCsvColumnCount = csvColumnTypes.size();
            // There is small chance that we could not find formatter for the give type
            // or perhaps due to a bug, structure analyser deviated from the prescribed
            // column types. We must validate the types we're going to use for the import

            int checkStatus = 0; // CHECK_OK

            if (finalPassCsvColumnCount != csvColumnCount) {
                checkStatus = 1; // CHECK_COLUMN_COUNT_DIVERGED
            }

            if (checkStatus == 0) {
                for (int i = 0; i < csvColumnCount; i++) {
                    final TypeAdapter typeAdapter = csvColumnTypes.getQuick(i);
                    final int requiredType = requiredColumnTypes.getColumnType(i);
                    if (requiredType != ColumnType.TYPES_SIZE && typeAdapter.getType() != requiredType) {
                        checkStatus = 2;
                        typeMismatchCsvColumnIndexes.add(i);
                    }
                }
            }

            if (checkStatus == 0) {
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
                        break;
                    default:
                        throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
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
            }
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

    private void logError(long line, int i, DirectByteCharSequence dbcs) {
        LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(tableTypeAdapters.getQuick(i).getType())).$("]\n\t");
        logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dbcs).$();
        columnErrorCounts.increment(i);
    }

    private void logTypeError(int i) {
        LOG.info()
                .$("mis-detected [table=").$(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(tableTypeAdapters.getQuick(i).getType()))
                .$(']').$();
    }

    private boolean onField(long line, DirectByteCharSequence dbcs, TableWriter.Row w, int i) {
        try {
            final int tableIndex = remapIndex.size() > 0 ? remapIndex.get(i) : i;
            tableTypeAdapters.getQuick(i).write(w, tableIndex, dbcs);
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
        try {
            jsonLexer.parse(lo, hi, schemaV1Parser);
        } catch (JsonException e) {
            throw TextException.$(e.getFlyweightMessage());
        }
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
