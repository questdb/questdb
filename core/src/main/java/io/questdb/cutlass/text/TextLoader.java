/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TextLoader implements Closeable, Mutable {
    public static final int LOAD_JSON_METADATA = 0;
    public static final int ANALYZE_STRUCTURE = 1;
    public static final int LOAD_DATA = 2;
    private static final Log LOG = LogFactory.getLog(TextLoader.class);
    private final CairoTextWriter textWriter;
    private final TextMetadataParser textMetadataParser;
    private final TextLexer textLexer;
    private final JsonLexer jsonLexer;
    private final Path path = new Path();
    private final int textAnalysisMaxLines;
    private final TextDelimiterScanner textDelimiterScanner;
    private final DirectCharSink utf8Sink;
    private final TypeManager typeManager;
    private final ObjList<ParserMethod> parseMethods = new ObjList<>();
    private int state;
    private boolean forceHeaders = false;
    private byte columnDelimiter = -1;

    public TextLoader(CairoEngine engine) {
        final TextConfiguration textConfiguration = engine.getConfiguration().getTextConfiguration();
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        jsonLexer = new JsonLexer(
                textConfiguration.getJsonCacheSize(),
                textConfiguration.getJsonCacheLimit()
        );
        this.typeManager = new TypeManager(textConfiguration, utf8Sink);
        textLexer = new TextLexer(textConfiguration, typeManager);
        textWriter = new CairoTextWriter(engine, path, typeManager);
        textMetadataParser = new TextMetadataParser(textConfiguration, typeManager);
        textAnalysisMaxLines = textConfiguration.getTextAnalysisMaxLines();
        textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
        parseMethods.extendAndSet(LOAD_JSON_METADATA, this::parseJsonMetadata);
        parseMethods.extendAndSet(ANALYZE_STRUCTURE, this::parseStructure);
        parseMethods.extendAndSet(LOAD_DATA, this::parseData);
        textLexer.setSkipLinesWithExtraValues(true);
    }

    @Override
    public void clear() {
        textWriter.clear();
        textLexer.clear();
        textMetadataParser.clear();
        jsonLexer.clear();
        forceHeaders = false;
        columnDelimiter = -1;
        typeManager.clear();
    }

    @Override
    public void close() {
        Misc.free(textWriter);
        Misc.free(textLexer);
        Misc.free(textMetadataParser);
        Misc.free(jsonLexer);
        Misc.free(path);
        Misc.free(textDelimiterScanner);
        Misc.free(utf8Sink);
    }

    public void configureColumnDelimiter(byte columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
        assert this.columnDelimiter > 0;
    }

    public void configureDestination(CharSequence tableName, boolean overwrite, boolean durable, int atomicity, int partitionBy, CharSequence timestampIndexCol) {
        textWriter.of(tableName, overwrite, durable, atomicity, partitionBy, timestampIndexCol);
        textDelimiterScanner.setTableName(tableName);
        textMetadataParser.setTableName(tableName);
        textLexer.setTableName(tableName);

        LOG.info()
                .$("configured [table=`").$(tableName)
                .$("`, overwrite=").$(overwrite)
                .$(", durable=").$(durable)
                .$(", atomicity=").$(atomicity)
                .$(']').$();
    }

    public byte getColumnDelimiter() {
        return columnDelimiter;
    }

    public LongList getColumnErrorCounts() {
        return textWriter.getColumnErrorCounts();
    }

    public RecordMetadata getMetadata() {
        return textWriter.getMetadata();
    }

    public long getParsedLineCount() {
        return textLexer.getLineCount();
    }

    public long getErrorLineCount() {
        return textLexer.getErrorCount();
    }

    public int getPartitionBy() {
        return textWriter.getPartitionBy();
    }

    public CharSequence getTableName() {
        return textWriter.getTableName();
    }

    public long getWrittenLineCount() {
        return textWriter.getWrittenLineCount();
    }

    public boolean hasHeader() {
        return textLexer.isHeaderDetected();
    }

    public boolean isForceHeaders() {
        return forceHeaders;
    }

    public void setForceHeaders(boolean forceHeaders) {
        this.forceHeaders = forceHeaders;
    }

    public void setSkipRowsWithExtraValues(boolean skipRowsWithExtraValues) {
        this.textLexer.setSkipLinesWithExtraValues(skipRowsWithExtraValues);
    }

    public void parse(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException {
        parseMethods.getQuick(state).parse(lo, hi, cairoSecurityContext);
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
                textLexer.parseLast();
                textWriter.commit();
                break;
            default:
                break;
        }
    }

    private void parseData(long lo, long hi, CairoSecurityContext cairoSecurityContext) {
        textLexer.parse(lo, hi, Integer.MAX_VALUE, textWriter.getTextListener());
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
            textLexer.of(columnDelimiter);
        } else {
            textLexer.of(textDelimiterScanner.scan(lo, hi));
        }
        textLexer.analyseStructure(
                lo,
                hi,
                textAnalysisMaxLines,
                forceHeaders,
                textMetadataParser.getColumnNames(),
                textMetadataParser.getColumnTypes()
        );
        textWriter.prepareTable(cairoSecurityContext, textLexer.getColumnNames(), textLexer.getColumnTypes());
        textLexer.parse(lo, hi, Integer.MAX_VALUE, textWriter.getTextListener());
        state = LOAD_DATA;
    }

    @FunctionalInterface
    private interface ParserMethod {
        void parse(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException;
    }
}
