/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.model.configuration;

import com.questdb.model.*;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;

import java.util.concurrent.TimeUnit;

public class ModelConfiguration {

    public static final JournalConfigurationBuilder MAIN = new JournalConfigurationBuilder() {{
        $(Quote.class, "quote").recordCountHint(10000)
                .partitionBy(PartitionBy.MONTH)
                .openFileTTL(5000L, TimeUnit.MILLISECONDS)
                .lag(12, TimeUnit.HOURS)
                .keyColumn("sym")
                .$sym("sym").index().valueCountHint(15)
                .$sym("ex").index().valueCountHint(5)
                .$sym("mode")
                .$ts()
        ;

        $(Trade.class).recordCountHint(10000)
                .partitionBy(PartitionBy.MONTH)
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
//                .partitionBy(PartitionBy.MONTH)
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
