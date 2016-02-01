/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.model.configuration;

import com.nfsdb.PartitionType;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.model.*;

import java.util.concurrent.TimeUnit;

public class ModelConfiguration {

    public static final JournalConfigurationBuilder MAIN = new JournalConfigurationBuilder() {{
        $(Quote.class).recordCountHint(10000)
                .partitionBy(PartitionType.MONTH)
                .lag(12, TimeUnit.HOURS)
                .location("quote")
                .keyColumn("sym")
                .$sym("sym").index().valueCountHint(15)
                .$sym("ex").index().valueCountHint(5)
                .$sym("mode")
                .$ts()
        ;

        $(Trade.class).recordCountHint(10000)
                .partitionBy(PartitionType.MONTH)
                .$sym("sym").valueCountHint(14)
                .$sym("ex").valueCountHint(5)
                .$sym("cond").valueCountHint(30)
                .$ts()
        ;

        $(RDFNode.class).recordCountHint(10000)
                .$sym("subj").index().valueCountHint(12000)
                .$sym("subjType").index().valueCountHint(5)
                .$sym("predicate").index().valueCountHint(5)
                .$sym("obj").sameAs("subj").index().valueCountHint(5)
                .$sym("objType").sameAs("subjType").index().valueCountHint(5)
                .$ts()
        ;

        $(TestEntity.class).recordCountHint(10000)
//                .partitionBy(PartitionType.MONTH)
                .keyColumn("sym")
                .$sym("sym").index().valueCountHint(15)
                .$ts()
        ;

        $(Band.class).recordCountHint(10000)
                .$sym("name").index().valueCountHint(1200)
                .$sym("type").valueCountHint(10)
                .$bin("image").size(10000)
                .$ts()
        ;

        $(Album.class)
                .$sym("band").index()
                .$sym("name").index().valueCountHint(1000000)
                .$ts("releaseDate");
    }};
}
