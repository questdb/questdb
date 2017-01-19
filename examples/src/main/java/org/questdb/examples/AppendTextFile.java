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
import com.questdb.txt.ImportManager;

import java.io.IOException;

public class AppendTextFile {

    public static void main(String[] args) throws JournalException, ParserException, IOException {

        if (args.length < 1) {
            System.out.println("Usage: AppendRawUnordered <path>");
            System.exit(1);
        }

        try (Factory factory = new Factory(args[0], 1000, 1, 0)) {
            // import manager will determine file structure automatically
            ImportManager.importFile(factory, AppendTextFile.class.getResource("/movies.csv").getFile(), ',', null);
        }
    }
}
