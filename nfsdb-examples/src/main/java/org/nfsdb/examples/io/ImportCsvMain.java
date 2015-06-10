/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package org.nfsdb.examples.io;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.io.ImportManager;
import com.nfsdb.io.ImportSchema;
import com.nfsdb.io.TextFileFormat;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;

public class ImportCsvMain {
    @SuppressFBWarnings({"PATH_TRAVERSAL_IN", "PATH_TRAVERSAL_IN"})
    public static void main(String[] args) throws IOException, JournalException {
        final String databaseLocation = args[0];
        final String from = args[1];
        final TextFileFormat format = TextFileFormat.valueOf(args[2].toUpperCase());
        final ImportSchema importSchema = args.length > 3 ? new ImportSchema(new File(args[3])) : null;


        JournalFactory factory = new JournalFactory(databaseLocation);
        long t = System.currentTimeMillis();

        ImportManager.importFile(factory, from, format, importSchema);

        Journal r = factory.reader(new File(from).getName());

        System.out.println(r.getMetadata());
        System.out.println("Loaded " + r.size() + " rows in " + (System.currentTimeMillis() - t) + "ms");
    }
}
