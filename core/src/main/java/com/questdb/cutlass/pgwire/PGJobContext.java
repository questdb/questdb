/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.pgwire;

import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.SqlCompiler;
import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
import com.questdb.network.PeerIsSlowToWriteException;
import com.questdb.std.AssociativeCache;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;

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
    public static final int PG_BOOL = 16;
    public static final int PG_CHAR = 18;
    public static final int PG_DATE = 1082;
    public static final int PG_BYTEA = 17;
    public static final int PG_UNSPECIFIED = 0;
    private final SqlCompiler compiler;
    private final AssociativeCache<RecordCursorFactory> factoryCache;
    private final ObjList<BindVariableSetter> bindVariableSetters = new ObjList<>();

    public PGJobContext(PGWireConfiguration configuration, CairoEngine engine) {
        this.compiler = new SqlCompiler(engine);
        this.factoryCache = new AssociativeCache<>(
                configuration.getFactoryCacheColumnCount(),
                configuration.getFactoryCacheRowCount()
        );
    }

    @Override
    public void close() {
        Misc.free(compiler);
    }

    public void handleClientOperation(PGConnectionContext context)
            throws PeerIsSlowToWriteException,
            PeerIsSlowToReadException,
            PeerDisconnectedException,
            BadProtocolException {
        context.handleClientOperation(compiler, factoryCache, bindVariableSetters);
    }
}
