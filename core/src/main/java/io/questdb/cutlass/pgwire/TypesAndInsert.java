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

import io.questdb.cairo.sql.InsertOperation;
import io.questdb.std.AbstractSelfReturningObject;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.WeakSelfReturningObjectPool;

public class TypesAndInsert extends AbstractSelfReturningObject<TypesAndInsert> implements TypeContainer {
    // Parameter types as received via "P" message. The client is liable to send
    // arbitrary number of parameters, which does not have to match the number of actual
    // bind variable used in the INSERT SQL. These are PostgresSQL OIDs in BigEndian.
    private final IntList pgInParameterTypeOIDs = new IntList();
    // Bind variable types. Each entry combines:
    // 1. Lower 32 bits: QuestDB native types scrapped from BindingService after the SQL text is parsed.
    // 2. Upper 32 bits: PostgresSQL OIDs in BigEndian. This combines types a client sent us in a PARSE message with the
    //                   types SQL Compiled derived from the SQL. Type from the PARSE message have a priority.
    private final LongList pgOutParameterTypes = new LongList();
    private boolean closing;
    private InsertOperation insert;
    private String sqlTag;
    private short sqlType;

    public TypesAndInsert(WeakSelfReturningObjectPool<TypesAndInsert> parentPool) {
        super(parentPool);
    }

    @Override
    public void close() {
        if (!closing) {
            closing = true;
            super.close();
            insert = Misc.free(insert);
            pgInParameterTypeOIDs.clear();
            pgOutParameterTypes.clear();
            Misc.free(insert);
            closing = false;
        }
    }

    public InsertOperation getInsert() {
        return insert;
    }

    @Override
    public IntList getPgInParameterTypeOIDs() {
        return pgInParameterTypeOIDs;
    }

    public LongList getPgOutParameterTypes() {
        return pgOutParameterTypes;
    }

    public String getSqlTag() {
        return sqlTag;
    }

    public short getSqlType() {
        return sqlType;
    }

    public void of(
            InsertOperation insert,
            short sqlType,
            String sqlTag,
            @Transient IntList pgInParameterTypeOIDs,
            @Transient LongList pgOutParameterTypes
    ) {
        this.insert = insert;
        this.sqlType = sqlType;
        this.sqlTag = sqlTag;
        this.pgInParameterTypeOIDs.addAll(pgInParameterTypeOIDs);
        this.pgOutParameterTypes.addAll(pgOutParameterTypes);
    }
}
