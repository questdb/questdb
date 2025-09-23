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

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SCSequence;
import io.questdb.preferences.PreferencesMap;
import io.questdb.preferences.PreferencesUpdateListener;
import io.questdb.std.Chars;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.datetime.Clock;

import java.io.Closeable;

public class TelemetryConfigLogger implements PreferencesUpdateListener, Closeable {
    public static final String OS_NAME = "os.name";
    public static final String TELEMETRY_CONFIG_TABLE_NAME = "telemetry_config";
    private static final String INSTANCE_DESC = "instance_description";
    private static final String INSTANCE_NAME = "instance_name";
    private static final String INSTANCE_TYPE = "instance_type";
    private static final Log LOG = LogFactory.getLog(TelemetryConfigLogger.class);
    private static final String QDB_PACKAGE = "QDB_PACKAGE";
    private final CairoEngine engine;
    private final CharSequence questDBVersion;
    private final TelemetryConfiguration telemetryConfiguration;
    private final SCSequence tempSequence = new SCSequence();
    private TableToken configTableToken;
    private TableWriter configWriter;
    private CharSequence instanceDesc;
    private CharSequence instanceName;
    private CharSequence instanceType;


    public TelemetryConfigLogger(CairoEngine engine) {
        this.engine = engine;

        questDBVersion = engine.getConfiguration().getBuildInformation().getSwVersion();
        telemetryConfiguration = engine.getConfiguration().getTelemetryConfiguration();
    }

    @Override
    public void close() {
        configWriter = Misc.free(configWriter);
    }

    @Override
    public void update(PreferencesMap preferencesMap) {
        instanceName = preferencesMap.get(INSTANCE_NAME);
        instanceName = instanceName == null ? "" : instanceName;
        instanceType = preferencesMap.get(INSTANCE_TYPE);
        instanceType = instanceType == null ? "" : instanceType;
        instanceDesc = preferencesMap.get(INSTANCE_DESC);
        instanceDesc = instanceDesc == null ? "" : instanceDesc;

        try (final SqlCompiler compiler = engine.getSqlCompiler()) {
            final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            sqlExecutionContext.with(
                    engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            updateTelemetryConfig(engine, compiler, sqlExecutionContext, configTableToken);
        } catch (Throwable th) {
            LOG.error().$("could not update config telemetry [table=").$(configTableToken).$("]").$(th).$();
        }
    }

    private void appendConfigRow(CairoEngine engine, TableWriter configWriter, Long256 id, boolean enabled) {
        final TableWriter.Row row = configWriter.newRow();
        if (id == null) {
            final io.questdb.std.datetime.Clock clock = engine.getConfiguration().getMicrosecondClock();
            final Clock nanosecondClock = engine.getConfiguration().getNanosecondClock();
            final long a = nanosecondClock.getTicks();
            final long b = clock.getTicks();
            row.putLong256(0, a, b, 0, 0);
            LOG.info()
                    .$("new instance [id=").$256(a, b, 0, 0)
                    .$(", enabled=").$(enabled)
                    .I$();
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
        row.putSym(5, instanceName);
        row.putSym(6, instanceType);
        row.putSym(7, instanceDesc);

        row.append();
        configWriter.commit();
    }

    private void tryAddColumn(SqlCompiler compiler, SqlExecutionContext executionContext, CharSequence columnDetails) {
        try {
            CompiledQuery cc = compiler.query().$("ALTER TABLE ").$(TELEMETRY_CONFIG_TABLE_NAME).$(" ADD COLUMN IF NOT EXISTS ").$(columnDetails).compile(executionContext);
            try (OperationFuture fut = cc.execute(tempSequence)) {
                fut.await();
            }
        } catch (SqlException ignore) {
        }
    }

    private void updateTelemetryConfig(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContextImpl sqlExecutionContext,
            TableToken tableToken
    ) throws SqlException {
        try (TableWriter configWriter = engine.getWriter(tableToken, "telemetryConfig")) {
            final CompiledQuery cc = compiler.query().$(TELEMETRY_CONFIG_TABLE_NAME).$(" LIMIT -1").compile(sqlExecutionContext);
            try (
                    final RecordCursorFactory factory = cc.getRecordCursorFactory();
                    final RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final boolean enabled = telemetryConfiguration.getEnabled();
                if (cursor.hasNext()) {
                    final Record record = cursor.getRecord();
                    final Long256 l256 = record.getLong256A(0);
                    final boolean _enabled = record.getBool(1);
                    final CharSequence _questDBVersion = record.getSymA(2);
                    final CharSequence _instanceName = record.getSymA(5);
                    final CharSequence _instanceType = record.getSymA(6);
                    final CharSequence _instanceDesc = record.getSymA(7);

                    // if the configuration or instance information changed (enable or disable telemetry, for example),
                    // we need to update the table to reflect that
                    if (enabled != _enabled
                            || !Chars.equalsNc(questDBVersion, _questDBVersion)
                            || !Chars.equalsNc(instanceName, _instanceName)
                            || !Chars.equalsNc(instanceType, _instanceType)
                            || !Chars.equalsNc(instanceDesc, _instanceDesc)
                    ) {
                        appendConfigRow(engine, configWriter, l256, enabled);
                        LOG.advisory()
                                .$("instance config changes [id=").$256(l256.getLong0(), l256.getLong1(), 0, 0)
                                .$(", enabled=").$(enabled)
                                .I$();
                    } else {
                        LOG.advisory()
                                .$("instance [id=").$256(l256.getLong0(), l256.getLong1(), 0, 0)
                                .$(", enabled=").$(enabled)
                                .I$();
                    }
                } else {
                    // if there are no record for telemetry id, we need to create one using clocks
                    appendConfigRow(engine, configWriter, null, enabled);
                }
            }
        } catch (CairoException ex) {
            LOG.error()
                    .$("could not update config telemetry [table=").$(tableToken)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
        }
    }

    void init(CairoEngine engine, SqlCompiler compiler, SqlExecutionContextImpl sqlExecutionContext) throws SqlException {
        configTableToken = compiler.query()
                .$("CREATE TABLE IF NOT EXISTS ")
                .$(TELEMETRY_CONFIG_TABLE_NAME)
                .$(" (id long256, enabled boolean, version symbol, os symbol, package symbol, instance_name symbol, instance_type symbol, instance_desc symbol)")
                .createTable(sqlExecutionContext);

        tryAddColumn(compiler, sqlExecutionContext, "version symbol");
        tryAddColumn(compiler, sqlExecutionContext, "os symbol");
        tryAddColumn(compiler, sqlExecutionContext, "package symbol");
        tryAddColumn(compiler, sqlExecutionContext, "instance_name symbol");
        tryAddColumn(compiler, sqlExecutionContext, "instance_type symbol");
        tryAddColumn(compiler, sqlExecutionContext, "instance_desc symbol");

        engine.getSettingsStore().registerListener(this);
    }
}
