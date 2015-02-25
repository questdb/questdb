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

package org.nfsdb.examples.imports;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.imp.ImportManager;

import java.io.File;
import java.io.IOException;

public class ImportCsvMain {
    public static void main(String[] args) throws IOException, JournalException {
        final String databaseLocation = args[0];
        final String csv = args[1];

        JournalFactory factory = new JournalFactory(databaseLocation);
        long t = System.currentTimeMillis();
        ImportManager.importCsvFile(factory, csv);

        Journal r = factory.reader(new File(csv).getName());
        System.out.println(r.getMetadata());
        System.out.println("Loaded " + r.size() + " rows in " + (System.currentTimeMillis() - t) + "ms");
    }
}
