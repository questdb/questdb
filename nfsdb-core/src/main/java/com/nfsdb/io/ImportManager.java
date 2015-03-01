/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.io;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.InputAnalysisListener;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.io.parser.listener.MetadataExtractorListener;
import com.nfsdb.utils.ByteBuffers;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class ImportManager {
    public static final int SAMPLE_SIZE = 100;

    private ImportManager() {
    }

    public static void importFile(JournalWriterFactory factory, String fileName, TextFileFormat format, ImportSchema importSchema) throws IOException {
        importFile(factory, fileName, format, importSchema, SAMPLE_SIZE);
    }

    public static void importFile(JournalWriterFactory factory, String fileName, TextFileFormat format, ImportSchema importSchema, int sampleSize) throws IOException {

        try (TextParser parser = format.newParser()) {
            File file = new File(fileName);
            String location = file.getName();

            switch (factory.exists(location)) {
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
        parser.reset();
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
                        parser.parse(((DirectBuffer) buf).address(), buf.remaining(), Integer.MAX_VALUE, listener);
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
        parser.reset();
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
                        parser.parse(((DirectBuffer) buf).address(), buf.remaining(), Integer.MAX_VALUE, listener);
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
        try (MetadataExtractorListener lsnr = new MetadataExtractorListener(importSchema, sampleSize)) {
            parser.parse(((DirectBuffer) buf).address(), buf.remaining(), sampleSize, lsnr);
            lsnr.onLineCount(parser.getLineCount());
            buf.clear();
            listener.onMetadata(lsnr.getMetadata());
            parser.setHeader(lsnr.isHeader());
            parser.restart();
        }
    }
}
