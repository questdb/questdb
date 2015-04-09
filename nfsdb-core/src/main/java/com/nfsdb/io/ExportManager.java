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

package com.nfsdb.io;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.io.sink.FileSink;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;

import java.io.File;
import java.io.IOException;

public final class ExportManager {

    private ExportManager() {
    }

    public static void export(JournalReaderFactory factory, String from, File to, TextFileFormat format) throws JournalException, IOException {
        try (Journal r = factory.reader(from)) {
            export(r.rows(), to, format);
        }
    }

    public static void export(RecordSource<? extends Record> from, File to, TextFileFormat format) throws JournalException, IOException {
        if (to.isDirectory()) {
            throw new JournalException(to + "cannot be a directory");
        }
        try (FileSink sink = new FileSink(to)) {
            RecordSourcePrinter printer = new RecordSourcePrinter(sink, format.getDelimiter());
            printer.print(from);
        }
    }

}
