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

package org.questdb.examples;

import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.factory.JournalFactory;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.ql.parser.QueryError;
import com.questdb.txt.ImportManager;

import java.io.IOException;

public class SQLErrorHandling {

    private static final Log LOG = LogFactory.getLog(SQLErrorHandling.class);

    public static void main(String[] args) throws JournalException, ParserException, IOException {

        if (args.length < 1) {
            System.out.println("Usage: SQLErrorHandling <path>");
            System.exit(1);
        }

        try (JournalFactory factory = new JournalFactory(args[0])) {

            // import movies data to query
            ImportManager.importFile(factory, SQLErrorHandling.class.getResource("/movies.csv").getFile(), ',', null);

            // Create SQL engine instance.
            QueryCompiler compiler = new QueryCompiler();

            // Query whole table (same as 'select * from movies')
            // RecordSource is equivalent to prepared statement, which is compiled version of your query.
            // Once compiled, query can be executed many times, returning up to date data on every execution.
            // RecordSource instance may have allocated resources, so closing one is essential.

            try {
                // in case of exception all allocated resources are freed automatically
                compiler.compile(factory, "'movies.csv' where movieIds = :id");
            } catch (ParserException e) {
                LOG.error().$("At (").$(QueryError.getPosition()).$(") : ").$(QueryError.getMessage()).$();
            }
        }
    }
}
