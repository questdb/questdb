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

package com.questdb.store.util;

import com.questdb.BootstrapEnv;
import com.questdb.ServerConfiguration;
import com.questdb.parser.ImportedColumnMetadata;
import com.questdb.parser.JsonSchemaParser;
import com.questdb.parser.json.JsonException;
import com.questdb.parser.json.JsonLexer;
import com.questdb.parser.plaintext.MetadataAwareTextParser;
import com.questdb.parser.plaintext.PlainTextLexer;
import com.questdb.parser.plaintext.PlainTextParser;
import com.questdb.parser.plaintext.PlainTextStoringParser;
import com.questdb.parser.typeprobe.TypeProbeCollection;
import com.questdb.std.ByteBuffers;
import com.questdb.std.Chars;
import com.questdb.std.ObjList;
import com.questdb.std.Unsafe;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.TimeZoneRuleFactory;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfiguration;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class ImportManager {
    private static final int SAMPLE_SIZE = 100;

    private ImportManager() {
    }

    /**
     * Imports journal from delimited values text file. At present three types of delimited value files are supported:
     * <ul>
     * <li>CSV
     * <li>TAB
     * <li>PIPE
     * </ul>
     * <p>
     * Both Unix and DOS line endings are automatically detected and parsed.
     * <p>
     * Parser will attempt to determine types of fields in the input by probing first 100 lines. It does a good job if
     * 100 lines are representative of the rest of file. In case 100 is too low, there is another method that takes
     * sample size as a parameter.
     * <p>
     * Once types are auto-detected it is possible to override them by supplying Import Schema. Import Schema is a
     * CSV file with required three columns:
     * <pre>
     *     column#,type,delimiter
     * </pre>
     * Where:
     * <ul>
     * <li>column# - is column number between 0 and count-1, where count is number of columns in input
     * <li>type - is a value of ColumnType enum
     * <li>delimiter - is mainly to disambiguate dates. Format selects date parser for column. There are only three
     * possible values that are currently supported: YYYY-MM-DDThh:mm:ss, YYYY-MM-DD hh:mm:ss and MM/DD/YYYY
     * </ul>
     * Import Schema does not have to describe all columns in input. It is there only to correct auto-detection mistakes.
     * So specify only columns where auto-detection gets it wrong.
     * <p>
     * To import data efficiently parser can use up to 2GB of physical memory, or 1/4 of your total physical memory,
     * whichever is lower.
     * <p>
     * Parser will always attempt to infer journal structure from input, even of journal already exists. If input
     * structure does not match structure of journal - an exception is thrown.
     *
     * @param factory     journal factory
     * @param fileName    name of input file
     * @param delimiter   inout delimiter
     * @param schema      optional instance of ImportSchema
     * @param forceHeader forces to treat first row as a header. When header is not forced algorithm will make best effort to detect it.
     * @throws IOException in case imported file cannot be read
     */
    public static void importFile(Factory factory, String fileName, char delimiter, @Nullable CharSequence schema, boolean forceHeader) throws IOException {
        importFile(factory, fileName, delimiter, schema, SAMPLE_SIZE, forceHeader);
    }

    public static void importFile(Factory factory, String fileName, char delimiter, CharSequence schema, int sampleSize, boolean forceHeader) throws IOException {

        BootstrapEnv env = new BootstrapEnv();
        env.typeProbeCollection = new TypeProbeCollection();
        env.configuration = new ServerConfiguration();
        env.factory = factory;

        try (PlainTextLexer parser = new PlainTextLexer(env).of(delimiter)) {
            File file = new File(fileName);
            String location = file.getName();

            switch (factory.getConfiguration().exists(location)) {
                case JournalConfiguration.EXISTS_FOREIGN:
                    throw new JournalRuntimeException("A foreign file/directory already exists: " + (new File(factory.getConfiguration().getJournalBase(), location)));
                default:
                    try (PlainTextStoringParser l = new PlainTextStoringParser(env).of(location, false, false, PlainTextStoringParser.ATOMICITY_RELAXED)) {
                        analyzeAndParse(file, parser, l, schema, sampleSize, forceHeader);
                    }
                    break;
            }
        }
    }

    public static void parse(File file, PlainTextLexer parser, final long bufSize, boolean header, PlainTextParser plainTextParser) throws IOException {
        parser.clear();
        parser.setHeader(header);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                long bufSz = bufSize == -1 ? ByteBuffers.getMaxMappedBufferSize(size) : bufSize;
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSz ? size - p : bufSz);
                    try {
                        p += buf.remaining();
                        parser.parse(ByteBuffers.getAddress(buf), buf.remaining(), Integer.MAX_VALUE, plainTextParser);
                    } finally {
                        ByteBuffers.release(buf);
                    }
                }
                parser.parseLast();
                plainTextParser.onLineCount(parser.getLineCount());
            }
        }
    }

    private static void analyzeAndParse(
            File file,
            PlainTextLexer parser,
            MetadataAwareTextParser listener,
            CharSequence schema,
            int sampleSize,
            boolean forceHeader) throws IOException {
        parser.clear();

        ObjList<ImportedColumnMetadata> metadata = null;
        if (schema != null) {
            BootstrapEnv env = new BootstrapEnv();
            env.dateLocaleFactory = new DateLocaleFactory(new TimeZoneRuleFactory());
            env.dateFormatFactory = new DateFormatFactory();
            JsonSchemaParser jsonSchemaParser = new JsonSchemaParser(env);
            int len = schema.length();
            long addr = Unsafe.malloc(len);
            try {
                Chars.strcpy(schema, len, addr);
                try (JsonLexer lexer = new JsonLexer(1024, 4096)) {
                    lexer.parse(addr, len, jsonSchemaParser);
                    lexer.parseLast();
                }
                metadata = jsonSchemaParser.getMetadata();

            } catch (JsonException e) {
                throw new IOException(e);
            } finally {
                Unsafe.free(addr, len);
            }
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                long bufSize = ByteBuffers.getMaxMappedBufferSize(size);
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSize ? size - p : bufSize);
                    try {
                        if (p == 0) {
                            parser.analyseStructure(ByteBuffers.getAddress(buf), buf.remaining(), sampleSize, listener, forceHeader, metadata);
                        }
                        p += buf.remaining();
                        parser.parse(ByteBuffers.getAddress(buf), buf.remaining(), Integer.MAX_VALUE, listener);
                    } finally {
                        ByteBuffers.release(buf);
                    }
                }
                parser.parseLast();
            }
        }
    }
}
