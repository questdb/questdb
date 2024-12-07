/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.pgwire.modern;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;

/**
 * Unlike other TypesAnd* classes, this one doesn't self-return to a pool. That's because
 * it's used for multithreaded calls to {@link io.questdb.std.ConcurrentAssociativeCache}.
 */
public class TypesAndSelectModern implements QuietCloseable, TypeContainer {
    // The client parameter types as they sent it to us when SQL was cached
    // this could be 0 or more parameter types. These types are used
    // to validate cache entries against client requests. For example, when
    // cache entry was created with parameter type INT and next client
    // request attempts to execute the cache entry with parameter type DOUBLE - we have to
    // recompile the SQL for the new parameter type, which we will do after
    // reconciling these types.
    private final IntList inPgParameterTypeOIDs = new IntList();
    // The QuestDB bind variable types (see ColumnType) as scraped from the
    // BindVariableService after SQL compilation.
    //
    // pgParameterTypes and bindVariableTypes are related. Before we compile the SQL,
    // we define BindVariableService indexed entries from the pgParameterTypes. So there is
    // one-to-one map between them. The pgParameterTypes uses PostgreSQL type identifiers
    // and bindVariableTypes uses ours. bindVariableTypes may have more values, in case
    // the client did not define types any times or did not define enough.
    private final IntList outPgParameterTypeOIDs = new IntList();
    // sqlTag is the value we will be returning back to the client
    private final String sqlTag;
    // sqlType is the value determined by the SQL Compiler
    private final short sqlType;
    private RecordCursorFactory factory;

    public TypesAndSelectModern(
            RecordCursorFactory factory,
            short sqlType,
            String sqlTag,
            @Transient IntList inPgParameterTypeOIDs,
            @Transient IntList outPgParameterTypeOIDs
    ) {
        this.factory = factory;
        this.sqlType = sqlType;
        this.sqlTag = sqlTag;
        this.inPgParameterTypeOIDs.addAll(inPgParameterTypeOIDs);
        this.outPgParameterTypeOIDs.addAll(outPgParameterTypeOIDs);
    }

    @Override
    public void close() {
        factory = Misc.free(factory);
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }

    @Override
    public IntList getPgInParameterTypeOIDs() {
        return inPgParameterTypeOIDs;
    }

    public IntList getPgOutParameterTypeOIDs() {
        return outPgParameterTypeOIDs;
    }

    public String getSqlTag() {
        return sqlTag;
    }

    public short getSqlType() {
        return sqlType;
    }
}
