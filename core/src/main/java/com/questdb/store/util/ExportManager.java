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

import com.questdb.ql.RecordSource;
import com.questdb.ql.RecordSourcePrinter;
import com.questdb.std.ex.JournalException;
import com.questdb.store.factory.ReaderFactory;

import java.io.File;
import java.io.IOException;

public final class ExportManager {

    private ExportManager() {
    }

    public static void export(RecordSource from, ReaderFactory factory, File to, char delimiter) throws JournalException, IOException {
        if (to.isDirectory()) {
            throw new JournalException(to + "cannot be a directory");
        }
        try (FileSink sink = new FileSink(to)) {
            RecordSourcePrinter printer = new RecordSourcePrinter(sink, delimiter);
            printer.print(from, factory, true);
        }
    }

}
