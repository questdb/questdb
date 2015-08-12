/*
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
 */

package com.nfsdb.ql.parser;

import com.nfsdb.collections.AssociativeCache;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.model.QueryModel;

public class Compiler {

    private final QueryParser parser = new QueryParser();
    private final QueryCompiler builder = new QueryCompiler();
    private final JournalReaderFactory factory;
    private final AssociativeCache<RecordSource<? extends Record>> cache = new AssociativeCache<>(8, 1024);

    public Compiler(JournalReaderFactory factory) {
        this.factory = factory;
    }

    public RecordCursor<? extends Record> compile(CharSequence query) throws ParserException, JournalException {
        return compileSource(query).prepareCursor(factory);
    }

    public <T> RecordCursor<? extends Record> compile(Class<T> clazz) throws JournalException, ParserException {
        return compile(clazz.getName());
    }

    public RecordSource<? extends Record> compileSource(CharSequence query) throws ParserException, JournalException {
        RecordSource<? extends Record> rs = cache.get(query);
        if (rs == null) {
            rs = builder.resetAndCompile(parser.parse(query).getQueryModel(), factory);
            cache.put(query, rs);
        } else {
            rs.reset();
        }
        return rs;
    }

    public CharSequence plan(CharSequence query) throws ParserException, JournalException {
        QueryModel model = parser.parse(query).getQueryModel();
        builder.resetAndOptimise(model, factory);
        return model.plan();
    }
}
