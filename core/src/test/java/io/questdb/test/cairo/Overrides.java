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

package io.questdb.test.cairo;

import io.questdb.BuildInformationHolder;
import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.FreeOnExit;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfigurationException;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cutlass.json.JsonException;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.AbstractCairoTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.questdb.cairo.DebugUtils.LOG;

public class Overrides {
    private static final BuildInformationHolder buildInformationHolder = new BuildInformationHolder();
    private final Properties defaultProperties = new Properties();
    private final Properties properties = new Properties();
    private boolean changed = true;
    private long currentMicros = -1;
    private final MicrosecondClock defaultMicrosecondClock = () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    private MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    private CairoConfiguration defaultConfiguration;
    private Map<String, String> env = null;
    private FactoryProvider factoryProvider = null;
    private FilesFacade ff;
    private boolean freeLeakedReaders = false;
    private boolean isHiddenTelemetryTable = false;
    private boolean mangleTableDirNames = true;
    private CairoConfiguration propsConfig;
    private RostiAllocFacade rostiAllocFacade = null;
    private long spinLockTimeout = AbstractCairoTest.DEFAULT_SPIN_LOCK_TIMEOUT;

    public Overrides() {
        resetToDefaultTestProperties(defaultProperties);
    }

    public void freeLeakedReaders(boolean freeLeakedReaders) {
        this.freeLeakedReaders = freeLeakedReaders;
    }

    public boolean freeLeakedReaders() {
        return freeLeakedReaders;
    }

    public CairoConfiguration getConfiguration(String root) {
        if (!properties.isEmpty()) {
            if (changed) {
                this.propsConfig = getTestConfiguration(root, defaultProperties, properties);
                changed = false;
            }
            return this.propsConfig;
        } else {
            return getDefaultConfiguration(root);
        }
    }

    public Map<String, String> getEnv() {
        return env != null ? env : System.getenv();
    }

    public FactoryProvider getFactoryProvider() {
        return factoryProvider;
    }

    public FilesFacade getFilesFacade() {
        return ff;
    }

    public String getInputRoot() {
        return null;
    }

    public String getInputWorkRoot() {
        return null;
    }

    public RostiAllocFacade getRostiAllocFacade() {
        return rostiAllocFacade;
    }

    public long getSpinLockTimeout() {
        return spinLockTimeout;
    }

    public MicrosecondClock getTestMicrosClock() {
        return testMicrosClock;
    }

    public boolean isHidingTelemetryTable() {
        return isHiddenTelemetryTable;
    }

    public boolean mangleTableDirNames() {
        return mangleTableDirNames;
    }

    public void reset() {
        currentMicros = -1;
        testMicrosClock = defaultMicrosecondClock;
        rostiAllocFacade = null;
        ff = null;
        mangleTableDirNames = true;
        factoryProvider = null;
        env = null;
        isHiddenTelemetryTable = false;
        properties.clear();
        changed = true;
        spinLockTimeout = AbstractCairoTest.DEFAULT_SPIN_LOCK_TIMEOUT;
        freeLeakedReaders = false;
    }

    public void setCurrentMicros(long currentMicros) {
        this.currentMicros = currentMicros;
    }

    public void setIsHidingTelemetryTable(boolean val) {
        this.isHiddenTelemetryTable = val;
    }

    public void setMangleTableDirNames(boolean mangle) {
        this.mangleTableDirNames = mangle;
    }

    public void setProperty(PropertyKey propertyKey, long value) {
        setProperty(propertyKey, String.valueOf(value));
    }

    public void setProperty(PropertyKey propertyKey, String value) {
        String propertyPath = propertyKey.getPropertyPath();
        if (value != null) {
            String existing = properties.getProperty(propertyPath);
            if (existing == null) {
                if (value.equals(defaultProperties.get(propertyPath))) {
                    return;
                }
            }
            properties.setProperty(propertyPath, value);
            changed = !Chars.equalsNc(value, existing);
        } else {
            changed = properties.remove(propertyPath) != null;
        }
    }

    public void setProperty(PropertyKey propertyKey, boolean value) {
        setProperty(propertyKey, value ? "true" : "false");
    }

    public void setRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        this.rostiAllocFacade = rostiAllocFacade;
    }

    private static CairoConfiguration getTestConfiguration(String installRoot, Properties defaultProperties, Properties properties) {
        Properties props = new Properties();
        props.putAll(defaultProperties);
        if (properties != null) {
            props.putAll(properties);
        }

        PropServerConfiguration propCairoConfiguration;
        try {
            propCairoConfiguration = new PropServerConfiguration(
                    installRoot,
                    props,
                    null,
                    new HashMap<>(),
                    LOG,
                    buildInformationHolder,
                    FilesFacadeImpl.INSTANCE,
                    MicrosecondClockImpl.INSTANCE,
                    (configuration, engine, freeOnExitList) -> DefaultFactoryProvider.INSTANCE,
                    false
            );
        } catch (ServerConfigurationException | JsonException e) {
            throw new RuntimeException(e);
        }
        propCairoConfiguration.init(null, new FreeOnExit());
        return propCairoConfiguration.getCairoConfiguration();
    }

    private static void resetToDefaultTestProperties(Properties properties) {
        properties.clear();
        properties.setProperty(PropertyKey.DEBUG_ALLOW_TABLE_REGISTRY_SHARED_WRITE.getPropertyPath(), "true");
        properties.setProperty(PropertyKey.CIRCUIT_BREAKER_THROTTLE.getPropertyPath(), "5");
        properties.setProperty(PropertyKey.QUERY_TIMEOUT.getPropertyPath(), "0");
        properties.setProperty(PropertyKey.CAIRO_SQL_CREATE_TABLE_COLUMN_MODEL_POOL_CAPACITY.getPropertyPath(), "32");
        properties.setProperty(PropertyKey.CAIRO_COLUMN_INDEXER_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER.getPropertyPath(), "2");
        properties.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_TASK_POOL_CAPACITY.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.CAIRO_SQL_COPY_MODEL_POOL_CAPACITY.getPropertyPath(), "16");
        properties.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE.getPropertyPath(), "2097152");
        properties.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE.getPropertyPath(), "16384");
        properties.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE.getPropertyPath(), "1048576");
        properties.setProperty(PropertyKey.CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE.getPropertyPath(), "1048576");
        properties.setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_CAPACITY.getPropertyPath(), "128");
        properties.setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE.getPropertyPath(), "16384");
        properties.setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_MAX_CHUNK_SIZE.getPropertyPath(), "1073741824");
        properties.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_MERGE_QUEUE_CAPACITY.getPropertyPath(), "32");
        properties.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD.getPropertyPath(), "1000");
        properties.setProperty(PropertyKey.CAIRO_ID_GENERATE_STEP.getPropertyPath(), "512");
        properties.setProperty(PropertyKey.CAIRO_IDLE_CHECK_INTERVAL.getPropertyPath(), "100");
        properties.setProperty(PropertyKey.CAIRO_INACTIVE_READER_TTL.getPropertyPath(), "-10000");
        properties.setProperty(PropertyKey.CAIRO_INACTIVE_WRITER_TTL.getPropertyPath(), "-10000");
        properties.setProperty(PropertyKey.CAIRO_WAL_INACTIVE_WRITER_TTL.getPropertyPath(), "-10000");
        properties.setProperty(PropertyKey.CAIRO_SQL_INSERT_MODEL_POOL_CAPACITY.getPropertyPath(), "8");
        properties.setProperty(PropertyKey.CAIRO_MAX_CRASH_FILES.getPropertyPath(), "1");
        properties.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS.getPropertyPath(), "1000");
        properties.setProperty(PropertyKey.CAIRO_O3_CALLBACK_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE.getPropertyPath(), "1048576");
        properties.setProperty(PropertyKey.CAIRO_O3_COPY_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS.getPropertyPath(), "15");
        properties.setProperty(PropertyKey.CAIRO_COMMIT_LAG.getPropertyPath(), "300000000");
        properties.setProperty(PropertyKey.CAIRO_O3_OPEN_COLUMN_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_O3_PARTITION_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_O3_PURGE_DISCOVERY_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY.getPropertyPath(), "32");
        properties.setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY.getPropertyPath(), "32");
        properties.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE.getPropertyPath(), "524288");
        properties.setProperty(PropertyKey.CAIRO_O3_PARTITION_PURGE_LIST_INITIAL_CAPACITY.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.CAIRO_SQL_QUERY_REGISTRY_POOL_SIZE.getPropertyPath(), "8");
        properties.setProperty(PropertyKey.CAIRO_READER_POOL_MAX_SEGMENTS.getPropertyPath(), "5");
        properties.setProperty(PropertyKey.CAIRO_SQL_RENAME_TABLE_MODEL_POOL_CAPACITY.getPropertyPath(), "8");
        properties.setProperty(PropertyKey.CAIRO_REPEAT_MIGRATION_FROM_VERSION.getPropertyPath(), "-1");
        properties.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT.getPropertyPath(), "5000");
        properties.setProperty(PropertyKey.CAIRO_CHARACTER_STORE_CAPACITY.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.CAIRO_SQL_COPY_BUFFER_SIZE.getPropertyPath(), "1048576");
        properties.setProperty(PropertyKey.CAIRO_SQL_COPY_MAX_INDEX_CHUNK_SIZE.getPropertyPath(), "1048576");
        properties.setProperty(PropertyKey.CAIRO_SQL_DISTINCT_TIMESTAMP_KEY_CAPACITY.getPropertyPath(), "256");
        properties.setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_MAX_PAGES.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_JOIN_METADATA_MAX_RESIZES.getPropertyPath(), "10");
        properties.setProperty(PropertyKey.CAIRO_SQL_MAP_MAX_PAGES.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.CAIRO_WRITER_FO_OPTS.getPropertyPath(), "o_async");
        properties.setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_SLOT_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY.getPropertyPath(), "4");
        properties.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT.getPropertyPath(), "1000");
        properties.setProperty(PropertyKey.CAIRO_WAL_WRITER_POOL_MAX_SEGMENTS.getPropertyPath(), "5");
        properties.setProperty(PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE.getPropertyPath(), "1000");
        properties.setProperty(PropertyKey.CAIRO_WAL_MAX_LAG_TXN_COUNT.getPropertyPath(), "20");
        properties.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA.getPropertyPath(), "1000");
        properties.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA.getPropertyPath(), "-1");
        properties.setProperty(PropertyKey.CAIRO_VECTOR_AGGREGATE_QUEUE_CAPACITY.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT.getPropertyPath(), "8192");
        properties.setProperty(PropertyKey.HTTP_TEXT_TIMESTAMP_ADAPTER_POOL_CAPACITY.getPropertyPath(), "16");
        properties.setProperty(PropertyKey.HTTP_TEXT_LEXER_STRING_POOL_CAPACITY.getPropertyPath(), "32");
        properties.setProperty(PropertyKey.HTTP_TEXT_ROLL_BUFFER_SIZE.getPropertyPath(), "4096");
        properties.setProperty(PropertyKey.HTTP_TEXT_ROLL_BUFFER_LIMIT.getPropertyPath(), "16384");
        properties.setProperty(PropertyKey.HTTP_TEXT_MAX_REQUIRED_DELIMITER_STDDEV.getPropertyPath(), "0.35");
        properties.setProperty(PropertyKey.TELEMETRY_QUEUE_CAPACITY.getPropertyPath(), "16");
        properties.setProperty(PropertyKey.CAIRO_TABLE_REGISTRY_COMPACTION_THRESHOLD.getPropertyPath(), "0");
        properties.setProperty(PropertyKey.CAIRO_SQL_WINDOW_TREE_PAGE_SIZE.getPropertyPath(), "4096");
        properties.setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_MAX_PAGES.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_PAGES.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_PAGES.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_PAGES.getPropertyPath(), "128");
        properties.setProperty(PropertyKey.CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE.getPropertyPath(), "1024");
        properties.setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE.getPropertyPath(), "32");
        properties.setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getPropertyPath(), "1000");
        properties.setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT.getPropertyPath(), "4");
        properties.setProperty(PropertyKey.DEBUG_ENABLE_TEST_FACTORIES.getPropertyPath(), "true");
        properties.setProperty(PropertyKey.CAIRO_O3_MAX_LAG.getPropertyPath(), "300000");
        properties.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED.getPropertyPath(), "true");
        properties.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED.getPropertyPath(), "true");
        properties.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getPropertyPath(), "false");
        properties.setProperty(PropertyKey.CAIRO_LEGACY_STRING_COLUMN_TYPE_DEFAULT.getPropertyPath(), "false");
        properties.setProperty(PropertyKey.CAIRO_O3_PARTITION_OVERWRITE_CONTROL_ENABLED.getPropertyPath(), "true");
        properties.setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getPropertyPath(), "false");
    }

    private CairoConfiguration getDefaultConfiguration(String root) {
        if (defaultConfiguration != null) {
            return defaultConfiguration;
        }
        defaultConfiguration = getTestConfiguration(root, defaultProperties, null);
        return defaultConfiguration;
    }
}
