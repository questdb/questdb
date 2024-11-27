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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;

public class TableConstraintsFunctionFactory extends AbstractEmptyCatalogueFunctionFactory {
    private static final RecordMetadata METADATA;

    public TableConstraintsFunctionFactory() {
        this("information_schema.table_constraints()");
    }

    protected TableConstraintsFunctionFactory(String signature) {
        super(signature, METADATA);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }


    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("constraint_catalog", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("constraint_schema", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("constraint_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("table_catalog", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("table_schema", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("table_name", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("constraint_type", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("is_deferrable", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("initially_deferred", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("enforced", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("nulls_distinct", ColumnType.STRING));
        METADATA = metadata;
    }
}
