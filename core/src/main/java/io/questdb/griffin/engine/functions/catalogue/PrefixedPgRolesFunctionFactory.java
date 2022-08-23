/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;

public class PrefixedPgRolesFunctionFactory extends AbstractEmptyCatalogueFunctionFactory {
    private final static RecordMetadata METADATA;

    public PrefixedPgRolesFunctionFactory() {
        super("pg_catalog.pg_roles()", METADATA);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("rolname", 1, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("rolsuper", 2, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolinherit", 3, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolcreaterole", 4, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolcreatedb", 5, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolcanlogin", 6, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolreplication", 7, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolconnlimit", 8, ColumnType.INT));
        metadata.add(new TableColumnMetadata("rolpassword", 9, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("rolvaliduntil", 10, ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("rolbypassrls", 11, ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("rolconfig", 12, ColumnType.STRING));
        metadata.add(new TableColumnMetadata("oid", 13, ColumnType.INT));
        METADATA = metadata;
    }
}
