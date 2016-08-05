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

package org.questdb.examples.query;

import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.factory.JournalFactory;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.ql.parser.QueryError;

import static org.questdb.examples.query.GenericAppend.createCustomers;
import static org.questdb.examples.query.GenericAppend.deleteCustomers;

public class HandlingQueryErrors {
    public static void main(String[] args) throws JournalException {
        if (args.length < 1) {
            System.out.println("Usage: GenericAppend <path>");
            System.exit(1);
        }

        final String location = args[0];

        deleteCustomers(location);

        // factory can be reused in application and must be explicitly closed when no longer needed.
        try (JournalFactory factory = new JournalFactory(location)) {
            createCustomers(factory);

            QueryCompiler compiler = new QueryCompiler();

            // compiler throws marker exception if it encounters syntax error
            // marker exception is singleton and doesn't have exception details
            // instead, exception details can be retrieved using QueryError static methods.
            try {
                compiler.compile(factory, "customers where x = 10");
            } catch (ParserException e) {
                System.out.println("Position: " + QueryError.getPosition());
                System.out.println("Message: " + QueryError.getMessage());
            }
        }
    }
}
