/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.io;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.InputAnalysisListener;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.misc.ByteBuffers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
     *     column#,type,format
     * </pre>
     * Where:
     * <ul>
     * <li>column# - is column number between 0 and count-1, where count is number of columns in input
     * <li>type - is a value of ColumnType enum
     * <li>format - is mainly to disambiguate dates. Format selects date parser for column. There are only three
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
     * @param factory  journal factory
     * @param fileName name of input file
     * @param format   inout format
     * @param schema   optional instance of ImportSchema
     * @throws IOException in case imported file cannot be read
     */
    public static void importFile(JournalWriterFactory factory, String fileName, TextFileFormat format, @Nullable CharSequence schema) throws IOException {
        importFile(factory, fileName, format, schema, SAMPLE_SIZE);
    }

    @SuppressFBWarnings({"PATH_TRAVERSAL_IN"})
    public static void importFile(JournalWriterFactory factory, String fileName, TextFileFormat format, CharSequence schema, int sampleSize) throws IOException {

        try (TextParser parser = format.newParser()) {
            File file = new File(fileName);
            String location = file.getName();

            switch (factory.getConfiguration().exists(location)) {
                case EXISTS_FOREIGN:
                    throw new JournalRuntimeException("A foreign file/directory already exists: " + (new File(factory.getConfiguration().getJournalBase(), location)));
                default:
                    try (JournalImportListener l = new JournalImportListener(factory).of(location, false)) {
                        analyzeAndParse(file, parser, l, schema, sampleSize);
                    }
                    break;
            }
        }
    }

    public static void parse(File file, TextParser parser, final long bufSize, boolean header, Listener listener) throws IOException {
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

    private static void analyzeAndParse(File file, TextParser parser, InputAnalysisListener listener, CharSequence schema, int sampleSize) throws IOException {
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
                            parser.putSchema(schema);
                            parser.analyseStructure(ByteBuffers.getAddress(buf), buf.remaining(), sampleSize, listener);
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
