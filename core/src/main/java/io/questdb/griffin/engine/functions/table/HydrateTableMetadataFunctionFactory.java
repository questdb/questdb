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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * Force re-hydrates the MetadataCache cache.
 * Either give:
 * A wildcard on its own: hydrate_table_metadata('*')
 * or  A set of table names:  hydrate_table_metadata('foo', 'bah')
 * This should only be run when there are no other metadata changes in play, as it will force push re-read metadata.
 */
public class HydrateTableMetadataFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(HydrateTableMetadataFunctionFactory.class);
    private static final String SIGNATURE = "hydrate_table_metadata(V)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // check if there are no args
        if (args == null || args.size() == 0) {
            throw SqlException.$(position, "no arguments provided");
        }

        final CairoEngine engine = sqlExecutionContext.getCairoEngine();
        ObjList<TableToken> tableTokens = new ObjList<>();

        // check for hydrate_table_metadata('*') case
        if (args.size() == 1) {
            final CharSequence tableName = args.getQuick(0).getStrA(null);
            if (tableName.length() == 1 && tableName.charAt(0) == '*') {
                ObjHashSet<TableToken> tableHashSet = new ObjHashSet<>();
                engine.getTableTokens(tableHashSet, false);
                tableTokens = tableHashSet.getList();
                LOG.info().$("rehydrating all tables").$();
            }
        }

        // if we didn't hit the above case, then hydrate based on the other table names
        if (tableTokens.size() == 0) {
            for (int i = 0, n = args.size(); i < n; i++) {
                final CharSequence tableName = args.getQuick(i).getStrA(null);

                if (tableName.length() == 1 && tableName.charAt(0) == '*' && args.size() > 1) {
                    throw SqlException.$(position, "cannot use wildcard alongside other table names");
                }

                final TableToken tableToken = engine.getTableTokenIfExists(tableName);
                if (tableToken == null) {
                    LOG.error().$("table does not exist [table=").$safe(tableName).I$();
                } else {
                    tableTokens.add(tableToken);
                }
            }
        }

        if (tableTokens.size() > 0) {
            return new HydrateTableMetadataFunction(tableTokens, engine, position);
        } else {
            throw SqlException.$(position, "no valid table names provided");
        }
    }

    private static class HydrateTableMetadataFunction extends BooleanFunction {
        private final int functionPosition;
        private final ObjList<TableToken> tableTokens;
        private CairoEngine engine;

        public HydrateTableMetadataFunction(@NotNull ObjList<TableToken> tableTokens, @NotNull CairoEngine engine, int functionPosition) {
            this.tableTokens = tableTokens;
            this.engine = engine;
            this.functionPosition = functionPosition;
        }

        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            executionContext.getSecurityContext().authorizeSystemAdmin();
            engine = executionContext.getCairoEngine();
            super.init(symbolTableSource, executionContext);
            for (int i = 0, n = tableTokens.size(); i < n; i++) {
                final TableToken tableToken = tableTokens.getQuick(i);
                if (!tableToken.isSystem()) {
                    try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                        metadataRW.hydrateTable(tableTokens.getQuick(i));
                    } catch (Throwable e) {
                        if (e instanceof CairoException) {
                            ((CairoException) e).position(functionPosition);
                        }
                        throw e;
                    }
                }
            }
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }
}
