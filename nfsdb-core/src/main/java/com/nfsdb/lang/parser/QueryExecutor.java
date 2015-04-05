/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.lang.parser;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.lang.ast.QueryModel;
import com.nfsdb.lang.ast.Statement;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.lang.cst.impl.jsrc.JournalSourceImpl;
import com.nfsdb.lang.cst.impl.psrc.JournalDescPartitionSource;
import com.nfsdb.lang.cst.impl.rsrc.AllRowSource;
import com.nfsdb.lang.cst.impl.virt.SelectedColumnsRecordSource;

public class QueryExecutor {
    private final JournalFactory factory;

    public QueryExecutor(JournalFactory factory) {
        this.factory = factory;
    }

    public RecordSource<? extends Record> execute(Statement statement) throws JournalException {
        switch (statement.getType()) {
            case CREATE_JOURNAL:
                factory.writer(statement.getStructure()).close();
                return null;
            default:
                QueryModel model = statement.getQueryModel();
                Journal r = factory.reader(model.getJournalName());
                return new SelectedColumnsRecordSource(
                        new JournalSourceImpl(
                                new JournalDescPartitionSource(r, false)
                                , new AllRowSource()
                        ),
                        model.getColumnNames()
                );
        }
    }
}
