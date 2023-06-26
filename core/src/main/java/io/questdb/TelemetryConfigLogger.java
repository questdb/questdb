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

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.NanosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClock;

import java.io.Closeable;

public class TelemetryConfigLogger implements Closeable {
    public static final String OS_NAME = "os.name";
    public static final CharSequence TELEMETRY_CONFIG_TABLE_NAME = "telemetry_config";
    static final String QDB_PACKAGE = "QDB_PACKAGE";
    private static final Log LOG = LogFactory.getLog(TelemetryConfigLogger.class);
    private final CharSequence questDBVersion;
    private final TelemetryConfiguration telemetryConfiguration;
    private final SCSequence tempSequence = new SCSequence();
    private TableWriter configWriter;

    public TelemetryConfigLogger(CairoEngine engine) {
        questDBVersion = engine.getConfiguration().getBuildInformation().getSwVersion();
        telemetryConfiguration = engine.getConfiguration().getTelemetryConfiguration();
    }

    @Override
    public void close() {
        configWriter = Misc.free(configWriter);
    }

    private void appendConfigRow(SqlCompiler compiler, TableWriter configWriter, Long256 id, boolean enabled) {
        final TableWriter.Row row = configWriter.newRow();
        if (id == null) {
            final MicrosecondClock clock = compiler.getEngine().getConfiguration().getMicrosecondClock();
            final NanosecondClock nanosecondClock = compiler.getEngine().getConfiguration().getNanosecondClock();
            final long a = nanosecondClock.getTicks();
            final long b = clock.getTicks();
            row.putLong256(0, a, b, 0, 0);
            LOG.info()
                    .$("new instance [id=").$256(a, b, 0, 0)
                    .$(", enabled=").$(enabled)
                    .$(']').$();
        } else {
            row.putLong256(0, id);
        }
        row.putBool(1, enabled);
        row.putSym(2, questDBVersion);
        row.putSym(3, System.getProperty(OS_NAME));
        final String packageStr = System.getenv().get(QDB_PACKAGE);
        if (packageStr != null) {
            row.putSym(4, packageStr);
        }
        row.append();
        configWriter.commit();
    }

    private void tryAddColumn(SqlCompiler compiler, SqlExecutionContext executionContext, CharSequence columnDetails) {
        try {
            CompiledQuery cc = compiler.query().$("ALTER TABLE ").$(TELEMETRY_CONFIG_TABLE_NAME).$(" ADD COLUMN ").$(columnDetails).compile(executionContext);
            try (OperationFuture fut = cc.execute(tempSequence)) {
                fut.await();
            }
        } catch (SqlException ex) {
            LOG.info().$("Failed to alter telemetry table [table=").$(TELEMETRY_CONFIG_TABLE_NAME).$(", error=").$(ex.getFlyweightMessage()).I$();
        }
    }

    private TableWriter updateTelemetryConfig(
            SqlCompiler compiler,
            SqlExecutionContextImpl sqlExecutionContext,
            TableToken tableToken
    ) throws SqlException {
        final TableWriter configWriter = compiler.getEngine().getWriter(
                tableToken,
                "telemetryConfig"
        );
        final CompiledQuery cc = compiler.query().$(TELEMETRY_CONFIG_TABLE_NAME).$(" LIMIT -1").compile(sqlExecutionContext);
        try (
                final RecordCursorFactory factory = cc.getRecordCursorFactory();
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final boolean enabled = telemetryConfiguration.getEnabled();
            if (cursor.hasNext()) {
                final Record record = cursor.getRecord();
                final boolean _enabled = record.getBool(1);
                final Long256 l256 = record.getLong256A(0);
                final CharSequence _questDBVersion = record.getSym(2);

                // if the configuration changed to enable or disable telemetry
                // we need to update the table to reflect that
                if (enabled != _enabled || !questDBVersion.equals(_questDBVersion)) {
                    appendConfigRow(compiler, configWriter, l256, enabled);
                    LOG.advisory()
                            .$("instance config changes [id=").$256(l256.getLong0(), l256.getLong1(), 0, 0)
                            .$(", enabled=").$(enabled)
                            .$(']').$();
                } else {
                    LOG.advisory()
                            .$("instance [id=").$256(l256.getLong0(), l256.getLong1(), 0, 0)
                            .$(", enabled=").$(enabled)
                            .$(']').$();
                }
            } else {
                // if there are no record for telemetry id we need to create one using clocks
                appendConfigRow(compiler, configWriter, null, enabled);
            }
        }
        return configWriter;
    }

    void init(SqlCompiler compiler, SqlExecutionContextImpl sqlExecutionContext) throws SqlException {
        final TableToken configTableToken = compiler.query()
                .$("CREATE TABLE IF NOT EXISTS ")
                .$(TELEMETRY_CONFIG_TABLE_NAME)
                .$(" (id long256, enabled boolean, version symbol, os symbol, package symbol)")
                .compile(sqlExecutionContext)
                .getTableToken();

        tryAddColumn(compiler, sqlExecutionContext, "version symbol");
        tryAddColumn(compiler, sqlExecutionContext, "os symbol");
        tryAddColumn(compiler, sqlExecutionContext, "package symbol");

        // TODO: close configWriter, we currently keep it open to prevent users from modifying the table.
        // Once we have a permission system, we can use that instead.
        try {
            configWriter = updateTelemetryConfig(compiler, sqlExecutionContext, configTableToken);
        } catch (CairoException ex) {
            LOG.error()
                    .$("could not open [table=`").utf8(TELEMETRY_CONFIG_TABLE_NAME)
                    .$("`, ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .$(']').$();
        }
    }
}
