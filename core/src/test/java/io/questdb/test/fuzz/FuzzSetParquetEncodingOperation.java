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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

public class FuzzSetParquetEncodingOperation implements FuzzTransactionOperation {

    private final String columnName;
    private final int compression;
    private final int compressionLevel;
    private final int encoding;

    public FuzzSetParquetEncodingOperation(String columnName, int encoding, int compression, int compressionLevel) {
        this.columnName = columnName;
        this.encoding = encoding;
        this.compression = compression;
        this.compressionLevel = compressionLevel;
    }

    @Override
    public boolean apply(Rnd tempRnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
             SqlCompiler sqlCompiler = engine.getSqlCompiler()
        ) {
            context.with(AllowAllSecurityContext.INSTANCE);
            StringBuilder sb = new StringBuilder();
            sb.append("ALTER TABLE ").append(wApi.getTableToken().getTableName())
                    .append(" ALTER COLUMN ").append(columnName).append(" SET PARQUET");

            if (encoding > 0) {
                sb.append(" ENCODING ").append(ParquetEncoding.getEncodingName(encoding));
            }
            if (compression >= 0) {
                sb.append(" COMPRESSION ").append(ParquetCompression.getCompressionName(compression));
                if (compressionLevel > 0) {
                    sb.append(' ').append(compressionLevel);
                }
            }

            String sql = sb.toString();

            try {
                CompiledQuery query = sqlCompiler.compile(sql, context);
                AlterOperation alterOp = query.getAlterOperation();
                alterOp.withSqlStatement(sql);
                alterOp.withContext(context);
                wApi.apply(alterOp, false);
                return true;
            } catch (CairoException e) {
                if (Chars.contains(e.getFlyweightMessage(), "table does not exist")
                        || Chars.contains(e.getFlyweightMessage(), "does not exist in table")
                        || e.isTableDropped()) {
                    return true;
                } else {
                    throw e;
                }
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }
}
