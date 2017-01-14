/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2017 Appsicle
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

package org.questdb.examples;

import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.factory.Factory;
import com.questdb.ql.RecordSource;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.txt.ExportManager;
import com.questdb.txt.ImportManager;

import java.io.File;
import java.io.IOException;

public class SQLExportToTextFile {
    public static void main(String[] args) throws JournalException, IOException, ParserException {

        if (args.length < 1) {
            System.out.println("Usage: SQLExportToTextFile <path>");
            System.exit(1);
        }

        try (Factory factory = new Factory(args[0], 1000, 1)) {

            // import movies data to query
            ImportManager.importFile(factory, SQLExamples.class.getResource("/movies.csv").getFile(), ',', null);

            // Create SQL engine instance.
            QueryCompiler compiler = new QueryCompiler();

            // Query whole table (same as 'select * from movies')
            // RecordSource is equivalent to prepared statement, which is compiled version of your query.
            // Once compiled, query can be executed many times, returning up to date data on every execution.
            // RecordSource instance may have allocated resources, so closing one is essential.

            try (RecordSource rs = compiler.compile(factory, "select year, count() count from (select title, match('\\(([0-9]*?)\\)', title) year from 'movies.csv' order by year desc)")) {
                ExportManager.export(rs, factory, new File(args[0], "export.csv"), ',');
            }
        }
    }
}
