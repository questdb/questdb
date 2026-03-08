/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;

/**
 * Unlike other TypesAnd* classes, this one doesn't self-return to a pool. That's because
 * it's used for multithreaded calls to {@link io.questdb.std.ConcurrentAssociativeCache}.
 */
public class TypesAndSelect implements QuietCloseable, TypeContainer {
    // The client parameter types as they sent it to us when SQL was cached
    // this could be 0 or more parameter types. These types are used
    // to validate cache entries against client requests. For example, when
    // cache entry was created with parameter type INT and next client
    // request attempts to execute the cache entry with parameter type DOUBLE - we have to
    // recompile the SQL for the new parameter type, which we will do after
    // reconciling these types.
    private final IntList inPgParameterTypeOIDs = new IntList();
    // Bind variable types. Each entry combines:
    // 1. Lower 32 bits: QuestDB native types scrapped from BindingService after the SQL text is parsed.
    // 2. Upper 32 bits: PostgresSQL OIDs in BigEndian. This combines types a client sent us in a PARSE message with the
    //                   types SQL Compiled derived from the SQL. Type from the PARSE message have a priority.
    private final LongList outPgParameterTypes = new LongList();
    // sqlTag is the value we will be returning back to the client
    private final String sqlTag;
    // sqlType is the value determined by the SQL Compiler
    private final short sqlType;
    private RecordCursorFactory factory;

    public TypesAndSelect(
            RecordCursorFactory factory,
            short sqlType,
            String sqlTag,
            @Transient IntList inPgParameterTypeOIDs,
            @Transient LongList outPgParameterTypes
    ) {
        this.factory = factory;
        this.sqlType = sqlType;
        this.sqlTag = sqlTag;
        this.inPgParameterTypeOIDs.addAll(inPgParameterTypeOIDs);
        this.outPgParameterTypes.addAll(outPgParameterTypes);
    }

    @Override
    public void close() {
        factory = Misc.free(factory);
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }

    public LongList getOutPgParameterTypes() {
        return outPgParameterTypes;
    }

    @Override
    public IntList getPgInParameterTypeOIDs() {
        return inPgParameterTypeOIDs;
    }

    public String getSqlTag() {
        return sqlTag;
    }

    public short getSqlType() {
        return sqlType;
    }
}
