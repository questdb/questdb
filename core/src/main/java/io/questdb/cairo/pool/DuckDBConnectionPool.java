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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.duckdb.*;
import io.questdb.std.str.DirectUtf8Sequence;

public class DuckDBConnectionPool extends AbstractMultiTenantPool<DuckDBConnectionPool.Connection> {
    private static final TableToken duckDBTableToken = new TableToken("duckdb", "/duckdb/", 0, false, false, false);
    private final long db;

    public DuckDBConnectionPool(CairoConfiguration configuration) {
        super(configuration, configuration.getWalWriterPoolMaxSegments(), configuration.getInactiveWalWriterTTL());
        db = DuckDB.databaseOpen(0, 0); // TODO: add config and/or path
        if (db == 0) {
            throw CairoException.critical(DuckDB.errorType());
        }
        DuckDB.registerQuestDBScanFunction(db);
    }

    @Override
    protected void closePool() {
        super.closePool();
        DuckDB.databaseClose(db);
    }

    @Override
    protected byte getListenerSrc() {
        return 0;
    }

    public Connection get() {
        // TODO: add sharding support
        return super.get(duckDBTableToken);
    }

    @Override
    protected Connection newTenant(TableToken tableName, Entry<Connection> entry, int index) {
        long connection = DuckDB.databaseConnect(db);
        // TODO: throw exception if connection is 0
        return new Connection(
                connection,
                index,
                tableName,
                this,
                entry
        );
    }

    public static class Connection implements PoolTenant {
        private final long connection;
        private final int index;
        private Entry<Connection> entry;
        private AbstractMultiTenantPool<Connection> pool;
        private TableToken tableToken;

        public Connection(
                long connection,
                int index,
                TableToken tableToken,
                AbstractMultiTenantPool<Connection> pool,
                Entry<Connection> entry
        ) {
            assert connection != 0;
            this.connection = connection;
            this.index = index;
            this.tableToken = tableToken;
            this.pool = pool;
            this.entry = entry;
        }

        public long getConnection() {
            return connection;
        }

        @Override
        public void close() {
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            DuckDB.connectionDisconnect(connection);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Entry<Connection> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public void goodbye() {
            this.pool = null;
            this.entry = null;
        }

        @Override
        public void refresh() {

        }

        @Override
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }

        public long query(DirectUtf8Sequence query) {
            return DuckDB.connectionQuery(connection, query.ptr(), query.size());
        }

        public long prepare(DirectUtf8Sequence stmt) {
            return DuckDB.connectionPrepare(connection, stmt.ptr(), stmt.size());
        }

        public void interrupt() {
            DuckDB.connectionInterrupt(connection);
        }
    }
}
