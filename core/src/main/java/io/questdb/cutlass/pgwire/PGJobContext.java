/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.std.*;

import java.io.Closeable;

public class PGJobContext implements Closeable {
    public static final int PG_VARCHAR = 1043;
    public static final int PG_TIMESTAMP = 1114;
    public static final int PG_TIMESTAMPZ = 1184;
    public static final int PG_FLOAT8 = 701;
    public static final int PG_FLOAT4 = 700;
    public static final int PG_INT4 = 23;
    public static final int PG_INT2 = 21;
    public static final int PG_INT8 = 20;
    public static final int PG_NUMERIC = 1700;
    public static final int PG_BOOL = 16;
    public static final int PG_CHAR = 18;
    public static final int PG_DATE = 1082;
    public static final int PG_BYTEA = 17;
    public static final int PG_UNSPECIFIED = 0;
    public static final IntList TYPE_OIDS = new IntList();
    public static final IntHashSet PG_TYPE_OIDS = new IntHashSet();
    private final SqlCompiler compiler;
    private final AssociativeCache<Object> factoryCache;
    private final CharSequenceObjHashMap<PGConnectionContext.NamedStatementWrapper> namedStatementMap;
    private final ObjList<BindVariableSetter> bindVariableSetters = new ObjList<>();

    public PGJobContext(PGWireConfiguration configuration, CairoEngine engine, MessageBus messageBus, FunctionFactoryCache functionFactoryCache) {
        this.compiler = new SqlCompiler(engine, messageBus, functionFactoryCache);
        this.factoryCache = new AssociativeCache<>(
                configuration.getFactoryCacheColumnCount(),
                configuration.getFactoryCacheRowCount()
        );
        this.namedStatementMap = new CharSequenceObjHashMap<>();
    }

    @Override
    public void close() {
        Misc.free(compiler);
        Misc.free(factoryCache);
    }

    public void handleClientOperation(PGConnectionContext context)
            throws PeerIsSlowToWriteException,
            PeerIsSlowToReadException,
            PeerDisconnectedException,
            BadProtocolException {
        context.handleClientOperation(compiler, factoryCache, namedStatementMap, bindVariableSetters);
    }

    static {
        TYPE_OIDS.extendAndSet(ColumnType.STRING, PG_VARCHAR); // VARCHAR
        TYPE_OIDS.extendAndSet(ColumnType.TIMESTAMP, PG_TIMESTAMP); // TIMESTAMP
        TYPE_OIDS.extendAndSet(ColumnType.DOUBLE, PG_FLOAT8); // FLOAT8
        TYPE_OIDS.extendAndSet(ColumnType.FLOAT, PG_FLOAT4); // FLOAT4
        TYPE_OIDS.extendAndSet(ColumnType.INT, PG_INT4); // INT4
        TYPE_OIDS.extendAndSet(ColumnType.SHORT, PG_INT2); // INT2
        TYPE_OIDS.extendAndSet(ColumnType.CHAR, PG_CHAR);
        TYPE_OIDS.extendAndSet(ColumnType.SYMBOL, PG_VARCHAR); // NAME
        TYPE_OIDS.extendAndSet(ColumnType.LONG, PG_INT8); // INT8
        TYPE_OIDS.extendAndSet(ColumnType.BYTE, PG_INT2); // INT2
        TYPE_OIDS.extendAndSet(ColumnType.BOOLEAN, PG_BOOL); // BOOL
        TYPE_OIDS.extendAndSet(ColumnType.DATE, PG_TIMESTAMP); // DATE
        TYPE_OIDS.extendAndSet(ColumnType.BINARY, PG_BYTEA); // BYTEA
        TYPE_OIDS.extendAndSet(ColumnType.LONG256, PG_NUMERIC); // NUMERIC
        for (int i = 0, n = TYPE_OIDS.size(); i < n; i++) {
            PG_TYPE_OIDS.add(TYPE_OIDS.getQuick(i));
        }
    }
}
