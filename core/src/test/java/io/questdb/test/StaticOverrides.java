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

package io.questdb.test;

import io.questdb.FactoryProvider;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.test.cairo.Overrides;

public class StaticOverrides extends Overrides {

    public SqlExecutionCircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return AbstractCairoTest.circuitBreakerConfiguration;
    }

    public long getCurrentMicros() {
        return AbstractCairoTest.currentMicros;
    }

    public FactoryProvider getFactoryProvider() {
        return AbstractCairoTest.factoryProvider;
    }

    public FilesFacade getFilesFacade() {
        return AbstractCairoTest.ff;
    }

    public String getInputRoot() {
        return AbstractCairoTest.inputRoot;
    }

    public String getInputWorkRoot() {
        return AbstractCairoTest.inputWorkRoot;
    }

    public MicrosecondClock getTestMicrosClock() {
        return AbstractCairoTest.testMicrosClock;
    }

    @Override
    public void reset() {
        super.reset();

        AbstractCairoTest.currentMicros = -1;
        AbstractCairoTest.testMicrosClock = AbstractCairoTest.defaultMicrosecondClock;
        AbstractCairoTest.ff = null;
        AbstractCairoTest.factoryProvider = null;
    }

    public void setCircuitBreakerConfiguration(SqlExecutionCircuitBreakerConfiguration circuitBreakerConfiguration) {
        AbstractCairoTest.circuitBreakerConfiguration = circuitBreakerConfiguration;
    }

    public void setFactoryProvider(FactoryProvider factoryProvider) {
        AbstractCairoTest.factoryProvider = factoryProvider;
    }

    public void setFilesFacade(FilesFacade ff) {
        AbstractCairoTest.ff = ff;
    }

    public void setInputRoot(String inputRoot) {
        AbstractCairoTest.inputRoot = inputRoot;
    }

    public void setInputWorkRoot(String inputWorkRoot) {
        AbstractCairoTest.inputWorkRoot = inputWorkRoot;
    }

    public void setTestMicrosClock(MicrosecondClock testMicrosClock) {
        AbstractCairoTest.testMicrosClock = testMicrosClock;
    }
}
