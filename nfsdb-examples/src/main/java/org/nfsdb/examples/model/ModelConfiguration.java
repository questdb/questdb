/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package org.nfsdb.examples.model;

import com.nfsdb.PartitionType;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;

import java.util.concurrent.TimeUnit;

public class ModelConfiguration {

    public static final JournalConfigurationBuilder CONFIG = new JournalConfigurationBuilder() {{
        $(Quote.class)
                .partitionBy(PartitionType.MONTH)
                .lag(24, TimeUnit.HOURS)
                .keyColumn("sym")
                .$sym("sym").index().size(4).valueCountHint(15)
                .$sym("ex").size(2).valueCountHint(1)
                .$ts()
        ;

        $(Price.class)
                .keyColumn("sym")
                .partitionBy(PartitionType.MONTH)
                .$sym("sym").index().size(4).valueCountHint(15)
                .$ts()
        ;
    }};
}
