/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.imp;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.imp.listener.InputAnalysisListener;
import com.nfsdb.imp.listener.JournalImportListener;
import com.nfsdb.imp.listener.Listener;
import com.nfsdb.imp.listener.MetadataExtractorListener;
import com.nfsdb.imp.parser.CsvParser;
import com.nfsdb.imp.parser.PipeParser;
import com.nfsdb.imp.parser.TabParser;
import com.nfsdb.imp.parser.TextParser;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Os;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class ImportManager {
    public static final int SAMPLE_SIZE = 100;

    public static void importCsvFile(JournalWriterFactory factory, String fileName) throws IOException {
        importFile(factory, fileName, new CsvParser());
    }

    public static void importPipeFile(JournalWriterFactory factory, String fileName) throws IOException {
        importFile(factory, fileName, new PipeParser());
    }

    public static void importTabFile(JournalWriterFactory factory, String fileName) throws IOException {
        importFile(factory, fileName, new TabParser());
    }

    public static void parse(File file, TextParser parser, long bufSize, boolean header, Listener listener) throws IOException {
        parser.reset();
        parser.setHeader(header);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                if (bufSize == -1) {
                    bufSize = calcBufferSize(size);
                }
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSize ? size - p : bufSize);
                    try {
                        p += buf.remaining();
                        parser.parse(((DirectBuffer) buf).address(), buf.remaining(), Integer.MAX_VALUE, listener);
                    } finally {
                        ByteBuffers.release(buf);
                    }
                }
                listener.onLineCount(parser.getLineCount());
            }
        }
    }

    private static void importFile(JournalWriterFactory factory, String fileName, TextParser parser) throws IOException {
        try {
            File file = new File(fileName);
            String location = file.getName();

            switch (factory.exists(location)) {
                case EXISTS_FOREIGN:
                    throw new JournalRuntimeException("A foreign file/directory already exists: " + (new File(factory.getConfiguration().getJournalBase(), location)));
                default:
                    try (JournalImportListener l = new JournalImportListener(factory, location)) {
                        analyzeAndParse(file, parser, l);
                    }
            }
        } finally {
            parser.close();
        }
    }

    private static long calcBufferSize(long channelSize) {
        long max = Os.getSystemMemory() / 4;
        max = max > Integer.MAX_VALUE ? Integer.MAX_VALUE : max;
        return channelSize > max ? max : channelSize;
    }

    private static void analyzeAndParse(File file, TextParser parser, InputAnalysisListener listener) throws IOException {
        parser.reset();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                long bufSize = calcBufferSize(size);
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSize ? size - p : bufSize);
                    try {
                        if (p == 0) {
                            analyze(parser, buf, listener);
                        }
                        p += buf.remaining();
                        parser.parse(((DirectBuffer) buf).address(), buf.remaining(), Integer.MAX_VALUE, listener);
                    } finally {
                        ByteBuffers.release(buf);
                    }
                }
            }
        }
    }

    private static void analyze(TextParser parser, ByteBuffer buf, InputAnalysisListener listener) {
        // use field detector listener to process first 100 lines of input
        try (MetadataExtractorListener lsnr = new MetadataExtractorListener()) {
            parser.parse(((DirectBuffer) buf).address(), buf.remaining(), SAMPLE_SIZE, lsnr);
            lsnr.onLineCount(parser.getLineCount());
            buf.clear();
            listener.onMetadata(lsnr.getMetadata());
            parser.setHeader(lsnr.isHeader());
            parser.restart();
        }
    }
}
