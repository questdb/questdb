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

package io.questdb.cutlass.text;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TextLoader implements Closeable, Mutable {

    public static final int LOAD_JSON_METADATA = 0;
    public static final int ANALYZE_STRUCTURE = 1;
    public static final int LOAD_DATA = 2;
    private static final Log LOG = LogFactory.getLog(TextLoader.class);
    private final TextConfiguration textConfiguration;
    private final TextMetadataDetector textMetadataDetector;
    private final TextMetadataParser textMetadataParser;
    private final JsonLexer jsonLexer;
    private final Path path = new Path();
    private final int textAnalysisMaxLines;
    private final TextDelimiterScanner textDelimiterScanner;
    private final DirectCharSink utf8Sink;
    private final TypeManager typeManager;
    private final CairoTextWriter textWriter;
    private final TextLexerWrapper tlw;
    private final ObjList<ParserMethod> parseMethods = new ObjList<>();
    private AbstractTextLexer lexer;
    private TimestampAdapter timestampAdapter;
    private int state;
    private boolean forceHeaders = false;
    private byte columnDelimiter = -1;
    private CharSequence timestampColumn;
    private CharSequence tableName;
    private boolean skipLinesWithExtraValues = true;

    public TextLoader(CairoEngine engine) {
        this.tlw = new TextLexerWrapper(engine.getConfiguration().getTextConfiguration());
        this.textWriter = new CairoTextWriter(engine);
        this.textConfiguration = engine.getConfiguration().getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        jsonLexer = new JsonLexer(textConfiguration.getJsonCacheSize(), textConfiguration.getJsonCacheLimit());

        textMetadataDetector = new TextMetadataDetector(typeManager, textConfiguration);
        textMetadataParser = new TextMetadataParser(textConfiguration, typeManager);
        textAnalysisMaxLines = textConfiguration.getTextAnalysisMaxLines();
        textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
        parseMethods.extendAndSet(LOAD_JSON_METADATA, this::parseJsonMetadata);
        parseMethods.extendAndSet(ANALYZE_STRUCTURE, this::parseStructure);
        parseMethods.extendAndSet(LOAD_DATA, this::parseData);
    }

    @Override
    public void clear() {
        textWriter.clear();
        if (lexer != null) {
            lexer.clear();
            lexer = null;
        }
        textMetadataParser.clear();
        textMetadataDetector.clear();
        jsonLexer.clear();
        forceHeaders = false;
        columnDelimiter = -1;
        typeManager.clear();
        timestampAdapter = null;
        skipLinesWithExtraValues = true;
        tableName = null;
    }

    @Override
    public void close() {
        Misc.free(textWriter);
        Misc.free(tlw);
        Misc.free(textMetadataDetector);
        Misc.free(textMetadataParser);
        Misc.free(jsonLexer);
        Misc.free(path);
        Misc.free(textDelimiterScanner);
        Misc.free(utf8Sink);
    }

    public void closeWriter() {
        textWriter.closeWriter();
    }

    public void configureColumnDelimiter(byte columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
        assert this.columnDelimiter > 0;
    }

    public void configureDestination(
            CharSequence tableName,
            boolean overwrite,
            boolean durable,
            int atomicity,
            int partitionBy,
            CharSequence timestampColumn,
            CharSequence timestampFormat
    ) {
        configureDestination(tableName, overwrite, durable, atomicity, partitionBy, timestampColumn);
        this.textDelimiterScanner.setTableName(tableName);
        this.textMetadataParser.setTableName(tableName);
        this.timestampColumn = timestampColumn;
        if (timestampFormat != null) {
            DateFormat dateFormat = typeManager.getInputFormatConfiguration().getTimestampFormatFactory().get(timestampFormat);
            this.timestampAdapter = (TimestampAdapter) typeManager.nextTimestampAdapter(false, dateFormat,
                    textConfiguration.getDefaultDateLocale());
        }

        LOG.info()
                .$("configured [table=`").$(tableName)
                .$("`, overwrite=").$(overwrite)
                .$(", durable=").$(durable)
                .$(", atomicity=").$(atomicity)
                .$(", partitionBy=").$(PartitionBy.toString(partitionBy))
                .$(", timestamp=").$(timestampColumn)
                .$(", timestampFormat=").$(timestampFormat)
                .$(']').$();
    }

    public void configureDestination(
            CharSequence tableName,
            boolean overwrite,
            boolean durable,
            int atomicity,
            int partitionBy,
            CharSequence timestampColumn
    ) {
        textWriter.of(tableName, overwrite, durable, atomicity, partitionBy, timestampColumn);
        this.tableName = tableName;
    }

    public byte getColumnDelimiter() {
        return columnDelimiter;
    }

    public LongList getColumnErrorCounts() {
        return textWriter.getColumnErrorCounts();
    }

    public long getErrorLineCount() {
        return lexer != null ? lexer.getErrorCount() : 0;
    }

    public RecordMetadata getMetadata() {
        return textWriter.getMetadata();
    }

    public long getParsedLineCount() {
        return lexer != null ? lexer.getLineCount() : 0;
    }

    public int getPartitionBy() {
        return textWriter.getPartitionBy();
    }

    public CharSequence getTableName() {
        return textWriter.getTableName();
    }

    public CharSequence getTimestampCol() {
        return textWriter.getTimestampCol();
    }

    public int getWarnings() {
        return textWriter.getWarnings();
    }

    public long getWrittenLineCount() {
        return textWriter.getWrittenLineCount();
    }

    public boolean isForceHeaders() {
        return forceHeaders;
    }

    public void setForceHeaders(boolean forceHeaders) {
        this.forceHeaders = forceHeaders;
    }

    public void parse(long lo, long hi, int lineCountLimit, CsvTextLexer.Listener textLexerListener) {
        lexer.parse(lo, hi, lineCountLimit, textLexerListener);
    }

    public void parse(long lo, long hi, int lineCountLimit) {
        lexer.parse(lo, hi, lineCountLimit, textWriter.getTextListener());
    }

    public void parse(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException {
        parseMethods.getQuick(state).parse(lo, hi, cairoSecurityContext);
    }

    public void prepareTable(
            CairoSecurityContext ctx,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types,
            Path path,
            TypeManager typeManager,
            TimestampAdapter timestampAdapter
    ) throws TextException {
        textWriter.prepareTable(ctx, names, types, path, typeManager, timestampAdapter);
    }

    public final void restart(boolean header) {
        lexer.restart(header);
    }

    public void setCommitLag(long commitLag) {
        textWriter.setCommitLag(commitLag);
    }

    public void setDelimiter(byte delimiter) {
        this.lexer = tlw.getLexer(delimiter);
        this.lexer.setTableName(tableName);
        this.lexer.setSkipLinesWithExtraValues(skipLinesWithExtraValues);
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        textWriter.setMaxUncommittedRows(maxUncommittedRows);
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
                textWriter.commit();
                break;
            default:
                break;
        }
    }

    private void parseData(long lo, long hi, CairoSecurityContext cairoSecurityContext) {
        parse(lo, hi, Integer.MAX_VALUE);
    }

    private void parseJsonMetadata(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException {
        try {
            jsonLexer.parse(lo, hi, textMetadataParser);
        } catch (JsonException e) {
            throw TextException.$(e.getFlyweightMessage());
        }
    }

    private void parseStructure(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException {
        if (columnDelimiter > 0) {
            setDelimiter(columnDelimiter);
        } else {
            setDelimiter(textDelimiterScanner.scan(lo, hi));
        }

        if (timestampColumn != null && timestampAdapter != null) {
            textMetadataParser.getColumnNames().add(timestampColumn);
            textMetadataParser.getColumnTypes().add(timestampAdapter);
        }

        textMetadataDetector.of(
                getTableName(),
                textMetadataParser.getColumnNames(),
                textMetadataParser.getColumnTypes(),
                forceHeaders
        );
        parse(lo, hi, textAnalysisMaxLines, textMetadataDetector);
        textMetadataDetector.evaluateResults(getParsedLineCount(), getErrorLineCount());
        restart(textMetadataDetector.isHeader());

        prepareTable(
                cairoSecurityContext,
                textMetadataDetector.getColumnNames(),
                textMetadataDetector.getColumnTypes(),
                path,
                typeManager,
                timestampAdapter
        );
        parse(lo, hi, Integer.MAX_VALUE);
        state = LOAD_DATA;
    }

    @FunctionalInterface
    protected interface ParserMethod {
        void parse(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException;
    }
}
