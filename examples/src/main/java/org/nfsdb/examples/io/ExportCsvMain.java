/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2016 Appsicle
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

package org.nfsdb.examples.io;

import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.factory.JournalFactory;
import com.questdb.io.ExportManager;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.parser.QueryCompiler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;

public class ExportCsvMain {
    @SuppressFBWarnings({"PATH_TRAVERSAL_IN"})
    public static void main(String[] args) throws JournalException, IOException, ParserException {

        JournalFactory factory = new JournalFactory(args[0]);
        QueryCompiler compiler = new QueryCompiler(new ServerConfiguration());
        String from = args[1];
        String toDir = args[2];
        char delimiter = args[3].charAt(0);

        // exports "from" journal to a delimited delimiter file written to "toDir" directory. Name of the file is
        // the same as name of exported journal.
        // Delimiter is selected via TextFormatEnum

        ExportManager.export(compiler.compileSource(factory, from), factory, new File(toDir, from), delimiter);
    }
}
