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
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.cutlass.text.typeprobe.TypeProbeCollection;
import com.questdb.std.Mutable;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;

import java.io.Closeable;

public class TextLoader implements Closeable, Mutable {
    public static final int LOAD_JSON_METADATA = 0;
    public static final int ANALYZE_STRUCTURE = 1;
    public static final int LOAD_DATA = 2;
    private final CairoTextWriter textWriter;
    private final TextMetadataParser textMetadataParser;
    private final TextLexer textLexer;
    private final JsonLexer jsonLexer;
    private final Path path = new Path();
    private int state;
    private boolean forceHeaders = false;
    private int statsLineCountLimit = Integer.MAX_VALUE;

    public TextLoader(
            CairoConfiguration configuration,
            TextConfiguration textConfiguration,
            CairoEngine engine,
            DateLocaleFactory dateLocaleFactory,
            DateFormatFactory dateFormatFactory
    ) {
        TypeProbeCollection typeProbeCollection = new TypeProbeCollection();
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
        textWriter = new CairoTextWriter(configuration, engine, path, textConfiguration);
        textMetadataParser = new TextMetadataParser(textConfiguration, dateLocaleFactory, dateFormatFactory);
    }

    @Override
    public void clear() {
        textWriter.clear();
        textLexer.clear();
        textMetadataParser.clear();
        jsonLexer.clear();
        forceHeaders = false;
    }

    @Override
    public void close() {
        textWriter.close();
        textLexer.close();
        textMetadataParser.close();
        jsonLexer.close();
        path.close();
    }

    public void configureDestination(String tableName, boolean overwrite, boolean durable, int atomicity) {
        textWriter.of(tableName, overwrite, durable, atomicity);
    }

    public void configureSeparator(char columnSeparator) {
        textLexer.of(columnSeparator);
    }

    public long getLineCount() {
        return textLexer.getLineCount();
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
                textLexer.analyseStructure(address, len, statsLineCountLimit, forceHeaders, textMetadataParser.getTextMetadata());
                textWriter.prepareTable(textLexer.getDetectedMetadata());
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

    public void setStatsLineCountLimit(int statsLineCountLimit) {
        this.statsLineCountLimit = statsLineCountLimit;
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
