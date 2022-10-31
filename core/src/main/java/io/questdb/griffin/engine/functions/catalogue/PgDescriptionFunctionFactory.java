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

public class PgDescriptionFunctionFactory extends AbstractEmptyCatalogueFunctionFactory {
    private static final RecordMetadata METADATA = GenericRecordMetadata.of(
        new TableColumnMetadata("objoid", 1, ColumnType.INT),
        new TableColumnMetadata("classoid", 2, ColumnType.INT),
        //TODO the below column was downgraded to short. We need to support type downgrading of compatible types when joining
        new TableColumnMetadata("objsubid", 3, ColumnType.SHORT),
        new TableColumnMetadata("description", 4, ColumnType.STRING)
    );


    public PgDescriptionFunctionFactory() {
        super("pg_description()", METADATA);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }
}
