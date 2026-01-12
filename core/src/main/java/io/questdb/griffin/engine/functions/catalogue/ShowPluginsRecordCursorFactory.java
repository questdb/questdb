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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PluginManager;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;

/**
 * Implements SHOW PLUGINS command.
 * Returns a list of all available plugins along with their loaded status.
 */
public class ShowPluginsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private static final int COLUMN_NAME = 0;
    private static final int COLUMN_LOADED = 1;

    private final ShowPluginsCursor cursor = new ShowPluginsCursor();

    public ShowPluginsRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final CairoEngine engine = executionContext.getCairoEngine();
        final PluginManager pluginManager = engine.getPluginManager();
        cursor.of(pluginManager.getAvailablePlugins(), pluginManager.getLoadedPlugins());
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_plugins");
    }

    static {
        METADATA.add(new TableColumnMetadata("name", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("loaded", ColumnType.BOOLEAN));
    }

    private static class ShowPluginsCursor implements NoRandomAccessRecordCursor {
        private final ShowPluginsRecord record = new ShowPluginsRecord();
        private ObjList<CharSequence> availablePlugins;
        private ObjList<CharSequence> loadedPlugins;
        private int index = -1;

        @Override
        public void close() {
            // nothing to close
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (index < availablePlugins.size() - 1) {
                index++;
                final CharSequence pluginName = availablePlugins.getQuick(index);
                record.of(pluginName, isLoaded(pluginName));
                return true;
            }
            return false;
        }

        @Override
        public long size() {
            return availablePlugins.size();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void toTop() {
            index = -1;
        }

        public void of(ObjList<CharSequence> availablePlugins, ObjList<CharSequence> loadedPlugins) {
            this.availablePlugins = availablePlugins;
            this.loadedPlugins = loadedPlugins;
            this.index = -1;
        }

        private boolean isLoaded(CharSequence pluginName) {
            for (int i = 0, n = loadedPlugins.size(); i < n; i++) {
                if (loadedPlugins.getQuick(i).toString().equalsIgnoreCase(pluginName.toString())) {
                    return true;
                }
            }
            return false;
        }

        private static class ShowPluginsRecord implements Record {
            private CharSequence name;
            private boolean loaded;

            @Override
            public boolean getBool(int col) {
                return col == COLUMN_LOADED && loaded;
            }

            @Override
            public CharSequence getStrA(int col) {
                if (col == COLUMN_NAME) {
                    return name;
                }
                return null;
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                final CharSequence str = getStrA(col);
                return str != null ? str.length() : -1;
            }

            public void of(CharSequence name, boolean loaded) {
                this.name = name;
                this.loaded = loaded;
            }
        }
    }
}
