/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.io;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.InputAnalysisListener;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.io.parser.listener.MetadataExtractorListener;
import com.nfsdb.misc.ByteBuffers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class ImportManager {
    private static final int SAMPLE_SIZE = 100;

    private ImportManager() {
    }

    /**
     * Imports journal from delimited values text file. At present three types of delimited value files are supported:
     * <el>
     * <li>CSV</li>
     * <li>TAB</li>
     * <li>PIPE</li>
     * </el>
     * <p/>
     * Both Unix and DOS line endings are automatically detected and parsed.
     * <p/>
     * Parser will attempt to determine types of fields in the input by probing first 100 lines. It does a good job if
     * 100 lines are representative of the rest of file. In case 100 is too low, there is another method that takes
     * sample size as a parameter.
     * <p/>
     * Once types are auto-detected it is possible to override them by supplying Import Schema. Import Schema is a
     * CSV file with required three columns:
     * <pre>
     *     column#,type,format
     * </pre>
     * Where:
     * <el>
     * <li>column# - is column number between 0 and count-1, where count is number of columns in input</li>
     * <li>type - is a value of ColumnType enum</li>
     * <li>format - is mainly to disambiguate dates. Format selects date parser for column. There are only three
     * possible values that are currently supported: YYYY-MM-DDThh:mm:ss, YYYY-MM-DD hh:mm:ss and MM/DD/YYYY</li>
     * </el>
     * Import Schema does not have to describe all columns in input. It is there only to correct auto-detection mistakes.
     * So specify only columns where auto-detection gets it wrong.
     * <p/>
     * To import data efficiently parser can use up to 2GB of physical memory, or 1/4 of your total physical memory,
     * whichever is lower.
     * </p>
     * Parser will always attempt to infer journal structure from input, even of journal already exists. If input
     * structure does not match structure of journal - an exception is thrown.
     *
     * @param factory      journal factory
     * @param fileName     name of input file
     * @param format       inout format
     * @param importSchema optional instance of ImportSchema
     * @throws IOException
     */
    public static void importFile(JournalWriterFactory factory, String fileName, TextFileFormat format, @Nullable ImportSchema importSchema) throws IOException {
        importFile(factory, fileName, format, importSchema, SAMPLE_SIZE);
    }

    @SuppressFBWarnings({"PATH_TRAVERSAL_IN"})
    public static void importFile(JournalWriterFactory factory, String fileName, TextFileFormat format, ImportSchema importSchema, int sampleSize) throws IOException {

        try (TextParser parser = format.newParser()) {
            File file = new File(fileName);
            String location = file.getName();

            switch (factory.getConfiguration().exists(location)) {
                case EXISTS_FOREIGN:
                    throw new JournalRuntimeException("A foreign file/directory already exists: " + (new File(factory.getConfiguration().getJournalBase(), location)));
                default:
                    try (JournalImportListener l = new JournalImportListener(factory, location)) {
                        analyzeAndParse(file, parser, l, importSchema, sampleSize);
                    }
            }
        }
    }

    public static void parse(File file, TextParser parser, long bufSize, boolean header, Listener listener) throws IOException {
        parser.clear();
        parser.setHeader(header);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                if (bufSize == -1) {
                    bufSize = ByteBuffers.getMaxMappedBufferSize(size);
                }
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSize ? size - p : bufSize);
                    try {
                        p += buf.remaining();
                        parser.parse(ByteBuffers.getAddress(buf), buf.remaining(), Integer.MAX_VALUE, listener);
                    } finally {
                        ByteBuffers.release(buf);
                    }
                }
                parser.parseLast();
                listener.onLineCount(parser.getLineCount());
            }
        }
    }

    private static void analyzeAndParse(File file, TextParser parser, InputAnalysisListener listener, ImportSchema importSchema, int sampleSize) throws IOException {
        parser.clear();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                long bufSize = ByteBuffers.getMaxMappedBufferSize(size);
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSize ? size - p : bufSize);
                    try {
                        if (p == 0) {
                            analyze(parser, buf, listener, importSchema, sampleSize);
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

    private static void analyze(TextParser parser, ByteBuffer buf, InputAnalysisListener listener, ImportSchema importSchema, int sampleSize) {
        // use field detector listener to process first 100 lines of input
        MetadataExtractorListener lsnr = new MetadataExtractorListener().of(importSchema);
        parser.parse(ByteBuffers.getAddress(buf), buf.remaining(), sampleSize, lsnr);
        lsnr.onLineCount(parser.getLineCount());
        buf.clear();
        listener.onMetadata(lsnr.getMetadata());
        parser.setHeader(lsnr.isHeader());
        parser.restart();
    }
}
