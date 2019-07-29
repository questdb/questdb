/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.CairoSecurityContext;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.cutlass.text.types.TypeManager;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.LongList;
import com.questdb.std.Misc;
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
    private final TypeManager typeManager;
    private int state;
    private boolean forceHeaders = false;
    private byte columnDelimiter = -1;

    public TextLoader(
            TextConfiguration textConfiguration,
            CairoEngine engine,
            DateLocaleFactory dateLocaleFactory,
            DateFormatFactory dateFormatFactory,
            com.questdb.std.microtime.DateLocaleFactory timestampLocaleFactory,
            com.questdb.std.microtime.DateFormatFactory timestampFormatFactory

    ) throws JsonException {
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        jsonLexer = new JsonLexer(
                textConfiguration.getJsonCacheSize(),
                textConfiguration.getJsonCacheLimit()
        );
        this.typeManager = new TypeManager(textConfiguration, utf8Sink, jsonLexer);
        textLexer = new TextLexer(textConfiguration, typeManager);
        textWriter = new CairoTextWriter(engine, path, textConfiguration, typeManager);
        textMetadataParser = new TextMetadataParser(
                textConfiguration,
                dateLocaleFactory,
                dateFormatFactory,
                timestampLocaleFactory,
                timestampFormatFactory,
                typeManager
        );
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

    public void configureDestination(CharSequence tableName, boolean overwrite, boolean durable, int atomicity) {
        textWriter.of(tableName, overwrite, durable, atomicity);
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

    public int getPartitionBy() {
        return textWriter.getPartitionBy();
    }

    public long getParsedLineCount() {
        return textLexer.getLineCount();
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

    public void parse(long address, int len, CairoSecurityContext cairoSecurityContext) throws JsonException {

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
                textWriter.prepareTable(cairoSecurityContext, textLexer.getColumnNames(), textLexer.getColumnTypes());
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
        LOG.debug().$("state change [old=").$(this.state).$(", new=").$(state).$(']').$();
        this.state = state;
        jsonLexer.clear();
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
