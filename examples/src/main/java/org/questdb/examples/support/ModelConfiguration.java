/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2018 Appsicle
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

package org.questdb.examples.support;

import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;

import java.util.concurrent.TimeUnit;

public class ModelConfiguration {

    public static final JournalConfigurationBuilder CONFIG = new JournalConfigurationBuilder() {{
        $(Quote.class)
                .partitionBy(PartitionBy.MONTH)
                .lag(24, TimeUnit.HOURS)
                .keyColumn("sym")
                .$sym("sym").index().size(4).valueCountHint(15)
                .$sym("ex").size(2).valueCountHint(1)
                .$ts()
        ;

        $(Price.class)
                .keyColumn("sym")
                .partitionBy(PartitionBy.MONTH)
                .$sym("sym")//.index().size(4).valueCountHint(15)
                .$ts()
                .txCountHint(1000000)
        ;
    }};
}
