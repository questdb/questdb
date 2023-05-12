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

public class PgIndexFunctionFactory extends AbstractEmptyCatalogueFunctionFactory {
    private final static RecordMetadata METADATA;

    public PgIndexFunctionFactory() {
        this("pg_index()");
    }

    protected PgIndexFunctionFactory(String signature) {
        super(signature, METADATA);
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("indexrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("indrelid", ColumnType.INT));
        metadata.add(new TableColumnMetadata("indnatts", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("indnkeyatts", ColumnType.SHORT));
        metadata.add(new TableColumnMetadata("indisunique", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indnullsnotdistinct", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indisprimary", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indisexclusion", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indimmediate", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indisclustered", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indisvalid", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indcheckxmin", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indisready", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indislive", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indisreplident", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indkey", ColumnType.INT));
        metadata.add(new TableColumnMetadata("indcollation", ColumnType.INT));
        metadata.add(new TableColumnMetadata("indclass", ColumnType.INT));
        metadata.add(new TableColumnMetadata("indoption", ColumnType.INT));
        metadata.add(new TableColumnMetadata("indexprs", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("indpred", ColumnType.STRING));
        METADATA = metadata;
    }
}
