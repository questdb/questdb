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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class TextLoaderBase implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(TextLoaderBase.class);
    private final CairoTextWriter textWriter;
    private final TextLexer textLexer;

    public TextLoaderBase(CairoEngine engine) {
        textLexer = new TextLexer(engine.getConfiguration().getTextConfiguration());
        textWriter = new CairoTextWriter(engine);
        textLexer.setSkipLinesWithExtraValues(true);
    }

    @Override
    public void clear() {
        textWriter.clear();
        textLexer.clear();
    }

    @Override
    public void close() {
        Misc.free(textWriter);
        Misc.free(textLexer);
    }

    public void closeWriter() {
        textWriter.closeWriter();
    }

    public void configureDestination(CharSequence tableName, boolean overwrite, boolean durable, int atomicity, int partitionBy, CharSequence timestampIndexCol) {
        textWriter.of(tableName, overwrite, durable, atomicity, partitionBy, timestampIndexCol);
        textLexer.setTableName(tableName);
    }

    public LongList getColumnErrorCounts() {
        return textWriter.getColumnErrorCounts();
    }

    public long getErrorLineCount() {
        return textLexer.getErrorCount();
    }

    public RecordMetadata getMetadata() {
        return textWriter.getMetadata();
    }

    public long getParsedLineCount() {
        return textLexer.getLineCount();
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

    public void parse(long lo, long hi, int lineCountLimit, TextLexer.Listener textLexerListener) {
        textLexer.parse(lo, hi, lineCountLimit, textLexerListener);
    }

    public void parse(long lo, long hi, int lineCountLimit) {
        textLexer.parse(lo, hi, lineCountLimit, textWriter.getTextListener());
    }

    public void prepareTable(CairoSecurityContext ctx, ObjList<CharSequence> names, ObjList<TypeAdapter> types, Path path, TypeManager typeManager) throws TextException {
        textWriter.prepareTable(ctx, names, types, path, typeManager);
    }

    public final void restart(boolean header) {
        textLexer.restart(header);
    }

    public void setCommitLag(long commitLag) {
        textWriter.setCommitLag(commitLag);
    }

    public void setDelimiter(byte delimiter) {
        textLexer.of(delimiter);
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        textWriter.setMaxUncommittedRows(maxUncommittedRows);
    }

    public void setSkipRowsWithExtraValues(boolean skipRowsWithExtraValues) {
        this.textLexer.setSkipLinesWithExtraValues(skipRowsWithExtraValues);
    }

    public void wrapUp() throws TextException {
        textLexer.parseLast();
        textWriter.commit();
    }

    public TimestampAdapter getTimestampAdapter() {
        return textWriter.getTimestampAdapter();
    }

    @FunctionalInterface
    protected interface ParserMethod {
        void parse(long lo, long hi, CairoSecurityContext cairoSecurityContext) throws TextException;
    }
}
