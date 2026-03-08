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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.GenericRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class PgProcFunctionFactory implements FunctionFactory {
    private static final RecordMetadata METADATA;

    @Override
    public String getSignature() {
        return "pg_proc()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(
                new GenericRecordCursorFactory(
                        METADATA,
                        new PgProcCatalogueCursor(),
                        false
                )
        );
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("oid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("proname", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("pronamespace", ColumnType.INT));
        metadata.add(new TableColumnMetadata("proowner", ColumnType.INT));
        metadata.add(new TableColumnMetadata("prolang", ColumnType.INT));
        metadata.add(new TableColumnMetadata("procost", ColumnType.FLOAT));
        metadata.add(new TableColumnMetadata("prorows", ColumnType.FLOAT));
        metadata.add(new TableColumnMetadata("provariadic", ColumnType.INT));
        metadata.add(new TableColumnMetadata("prosupport", ColumnType.INT));
        metadata.add(new TableColumnMetadata("prokind", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("prosecdef", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("proleakproof", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("proisstrict", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("proretset", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("provolatile", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("proparallel", ColumnType.CHAR));
        metadata.add(new TableColumnMetadata("pronargs", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("pronargdefaults", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("prorettype", ColumnType.INT));

        // proargtypes - skipping, oidvector
        // proallargtypes - skipping, oid[]
        // proargmodes - skipping, char[]
        // proargnames  - skipping, text[]
        // proargdefaults - skipping, pg_node_tree
        // protrftypes - skipping, oid[]

        metadata.add(new TableColumnMetadata("prosrc", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("probin", ColumnType.STRING));

        // prosqlbody - skipping, pg_node_tree
        // proconfig - skippping, text[]
        // proacl - skipping, aclitem[]

        METADATA = metadata;
    }
}
