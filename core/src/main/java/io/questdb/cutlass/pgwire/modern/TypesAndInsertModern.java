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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.std.AbstractSelfReturningObject;
import io.questdb.std.IntList;
import io.questdb.std.Transient;
import io.questdb.std.WeakSelfReturningObjectPool;

public class TypesAndInsertModern extends AbstractSelfReturningObject<TypesAndInsertModern> implements TypeContainer {
    // Bind variable columns types, typically scraped from BindVariableService after SQL is parsed. These are
    // our column types and are LittleEndian.
    private final IntList bindVariableColumnTypes = new IntList();
    // Parameter types as received via "P" message. The client is liable to send
    // arbitrary number of parameters, which does not have to match the number of actual
    // bind variable used in the INSERT SQL. These are PostgresSQL OIDs in BigEndian.
    private final IntList pgParameterTypeOIDs = new IntList();
    private boolean closing;
    private boolean hasBindVariables;
    private InsertOperation insert;
    private String sqlTag;
    private short sqlType;

    public TypesAndInsertModern(WeakSelfReturningObjectPool<TypesAndInsertModern> parentPool) {
        super(parentPool);
    }

    @Override
    public void close() {
        if (!closing) {
            closing = true;
            super.close();
            pgParameterTypeOIDs.clear();
            bindVariableColumnTypes.clear();
            closing = false;
        }
    }

    public IntList getBindVariableColumnTypes() {
        return bindVariableColumnTypes;
    }

    public InsertOperation getInsert() {
        return insert;
    }

    @Override
    public IntList getPgParameterTypeOIDs() {
        return pgParameterTypeOIDs;
    }

    public String getSqlTag() {
        return sqlTag;
    }

    public short getSqlType() {
        return sqlType;
    }

    public boolean hasBindVariables() {
        return hasBindVariables;
    }

    public void of(
            InsertOperation insert,
            short sqlType,
            String sqlTag,
            @Transient BindVariableService bindVariableService,
            @Transient IntList pgParameterTypeOIDs
    ) {
        this.insert = insert;
        this.sqlType = sqlType;
        this.sqlTag = sqlTag;
        final int n = bindVariableService.getIndexedVariableCount();
        this.hasBindVariables = n > 0;
        for (int i = 0; i < n; i++) {
            Function func = bindVariableService.getFunction(i);
            // For bind variable find in vararg parameters functions are not
            // created upfront. This is due to the type being unknown. On PG
            // wire bind variable type and value are provided *after* the compilation.
            if (func != null) {
                this.bindVariableColumnTypes.add(func.getType());
            } else {
                this.bindVariableColumnTypes.add(ColumnType.UNDEFINED);
            }
        }

        for (int i = 0, m = pgParameterTypeOIDs.size(); i < m; i++) {
            this.pgParameterTypeOIDs.add(pgParameterTypeOIDs.getQuick(i));
        }
    }
}
