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

package com.questdb.cutlass.text;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.cutlass.text.typeprobe.TypeProbeCollection;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.LongList;
import com.questdb.std.Mutable;
import com.questdb.std.str.DirectCharSink;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;

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
    private int state;
    private boolean forceHeaders = false;
    private byte columnDelimiter = -1;
    private final TypeProbeCollection typeProbeCollection;

    public TextLoader(
            CairoConfiguration configuration,
            TextConfiguration textConfiguration,
            CairoEngine engine,
            DateLocaleFactory dateLocaleFactory,
            DateFormatFactory dateFormatFactory
    ) {
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkCapacity());
        this.typeProbeCollection = new TypeProbeCollection(utf8Sink);
        textLexer = new TextLexer(
                textConfiguration,
                typeProbeCollection,
                textConfiguration.getRollBufferSize(),
                textConfiguration.getRollBufferLimit()
        );
        jsonLexer = new JsonLexer(
                textConfiguration.getJsonCacheSize(),
                textConfiguration.getJsonCacheLimit()
        );
        textWriter = new CairoTextWriter(configuration, engine, path, textConfiguration, typeProbeCollection);
        textMetadataParser = new TextMetadataParser(textConfiguration, dateLocaleFactory, dateFormatFactory, utf8Sink, typeProbeCollection);
        textAnalysisMaxLines = textConfiguration.getTextAnalysisMaxLines();
        textDelimiterScanner = new TextDelimiterScanner(textConfiguration);
    }

    @Override
    public void clear() {
        textWriter.clear();
        textLexer.clear();
        textMetadataParser.clear();
        jsonLexer.clear();
        forceHeaders = false;
        columnDelimiter = -1;
        typeProbeCollection.clear();
    }

    @Override
    public void close() {
        textWriter.close();
        textLexer.close();
        textMetadataParser.close();
        jsonLexer.close();
        path.close();
        textDelimiterScanner.close();
        utf8Sink.close();
    }

    public void configureColumnDelimiter(byte columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
        assert this.columnDelimiter > 0;
    }

    public void configureDestination(String tableName, boolean overwrite, boolean durable, int atomicity) {
        textWriter.of(tableName, overwrite, durable, atomicity);
        textDelimiterScanner.setTableName(tableName);
        textMetadataParser.setTableName(tableName);
        textLexer.setTableName(tableName);

        LOG.info()
                .$("configured [table=").$(tableName)
                .$(", overwrite=").$(overwrite)
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

    public long getWrittenLineCount() {
        return textWriter.getWrittenLineCount();
    }

    public boolean isForceHeaders() {
        return forceHeaders;
    }

    public void setForceHeaders(boolean forceHeaders) {
        this.forceHeaders = forceHeaders;
    }

    public void parse(long address, int len) throws JsonException {

        switch (state) {
            case LOAD_JSON_METADATA:
                jsonLexer.parse(address, len, textMetadataParser);
                break;
            case ANALYZE_STRUCTURE:
                if (columnDelimiter > 0) {
                    textLexer.of(columnDelimiter);
                } else {
                    textLexer.of(textDelimiterScanner.scan(address, len));
                }
                textLexer.analyseStructure(
                        address,
                        len,
                        textAnalysisMaxLines,
                        forceHeaders,
                        textMetadataParser.getColumnNames(),
                        textMetadataParser.getColumnTypes()
                );
                textWriter.prepareTable(textLexer.getColumnNames(), textLexer.getColumnTypes());
                textLexer.parse(address, len, Integer.MAX_VALUE, textWriter);
                break;
            case LOAD_DATA:
                textLexer.parse(address, len, Integer.MAX_VALUE, textWriter);
                break;
            default:
                break;
        }
    }

    public void setState(int state) {
        this.state = state;
    }

    public void wrapUp() throws JsonException {
        switch (state) {
            case LOAD_JSON_METADATA:
                jsonLexer.parseLast();
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
}
