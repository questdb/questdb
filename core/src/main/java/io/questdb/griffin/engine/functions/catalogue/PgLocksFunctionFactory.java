/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

public class PgLocksFunctionFactory extends AbstractEmptyCatalogueFunctionFactory {
    private final static RecordMetadata METADATA;

    public PgLocksFunctionFactory() {
        this("pg_locks()");
    }

    protected PgLocksFunctionFactory(String signature) {
        super(signature, METADATA);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("locktype", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("database", ColumnType.INT));
        metadata.add(new TableColumnMetadata("relation", ColumnType.INT));
        metadata.add(new TableColumnMetadata("page", ColumnType.INT));
        metadata.add(new TableColumnMetadata("tuple", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("virtualxid", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("transactionid", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("classid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("objid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("objsubid", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("virtualtransaction", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("pid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("mode", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("granted", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("fastpath", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("waitstart", ColumnType.TIMESTAMP));
        METADATA = metadata;
    }
}
