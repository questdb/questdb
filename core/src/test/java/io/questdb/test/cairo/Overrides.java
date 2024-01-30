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

package io.questdb.test.cairo;

import io.questdb.FactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.RostiAllocFacade;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.test.tools.TestUtils;

import java.util.Map;
import java.util.Properties;

public class Overrides {
    private final Properties properties = new Properties();
    private boolean changed = true;
    private SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration;
    private long currentMicros = -1;
    private final MicrosecondClock defaultMicrosecondClock = () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();
    private boolean isHiddenTelemetryTable = false;
    private MicrosecondClock testMicrosClock = defaultMicrosecondClock;
    private Map<String, String> env = null;
    private FactoryProvider factoryProvider = null;
    private FilesFacade ff;
    private final String inputRoot = null;
    private final String inputWorkRoot = null;
    private boolean mangleTableDirNames = true;
    private CairoConfiguration propsConfig;
    private String root;
    private RostiAllocFacade rostiAllocFacade = null;

    public Overrides() {
        TestUtils.resetToDefaultTestProperties(properties);
    }

    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return circuitBreakerConfiguration;
    }

    public CairoConfiguration getConfiguration(String root) {
        if (changed || this.root != root) {
            this.propsConfig = io.questdb.test.tools.TestUtils.getTestConfiguration(root, properties);
            changed = false;
            this.root = root;
        }
        return this.propsConfig;
    }

    public long getCurrentMicros() {
        return currentMicros;
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
        return inputRoot;
    }

    public String getInputWorkRoot() {
        return inputWorkRoot;
    }

    public RostiAllocFacade getRostiAllocFacade() {
        return rostiAllocFacade;
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

        TestUtils.resetToDefaultTestProperties(properties);
        changed = true;
    }

    public void setCurrentMicros(long currentMicros) {
        this.currentMicros = currentMicros;
    }

    public void setEnv(Map<String, String> env) {
        this.env = env;
    }

    public void setIsHidingTelemetryTable(boolean val) {
        this.isHiddenTelemetryTable = val;
    }

    public void setMangleTableDirNames(boolean mangle) {
        this.mangleTableDirNames = mangle;
    }

    public void setProperty(PropertyKey propertyKey, long value) {
        properties.setProperty(propertyKey.getPropertyPath(), String.valueOf(value));
        changed = true;
    }

    public void setProperty(PropertyKey propertyKey, String value) {
        if (value != null) {
            properties.setProperty(propertyKey.getPropertyPath(), value);
        } else {
            properties.remove(propertyKey.getPropertyPath());
        }
        changed = true;
    }

    public void setProperty(PropertyKey propertyKey, boolean value) {
        properties.setProperty(propertyKey.getPropertyPath(), String.valueOf(value));
        changed = true;
    }

    public void setRostiAllocFacade(RostiAllocFacade rostiAllocFacade) {
        this.rostiAllocFacade = rostiAllocFacade;
    }
}
