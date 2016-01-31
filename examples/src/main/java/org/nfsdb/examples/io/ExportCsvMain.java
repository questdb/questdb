/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package org.nfsdb.examples.io;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.io.ExportManager;
import com.nfsdb.io.TextFileFormat;
import com.nfsdb.ql.parser.QueryCompiler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;

public class ExportCsvMain {
    @SuppressFBWarnings({"PATH_TRAVERSAL_IN"})
    public static void main(String[] args) throws JournalException, IOException, ParserException {

        JournalFactory factory = new JournalFactory(args[0]);
        QueryCompiler compiler = new QueryCompiler(factory);
        String from = args[1];
        String toDir = args[2];
        TextFileFormat format = TextFileFormat.valueOf(args[3]);

        // exports "from" journal to a delimited format file written to "toDir" directory. Name of the file is
        // the same as name of exported journal.
        // Delimiter is selected via TextFormatEnum

        ExportManager.export(compiler.compile(from), new File(toDir, from), format);
    }
}
