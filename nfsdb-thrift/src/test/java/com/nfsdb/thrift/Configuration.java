/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.thrift;

import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.thrift.model.Quote;
import com.nfsdb.thrift.model.Trade2;

import java.util.concurrent.TimeUnit;

public class Configuration {
    @SuppressWarnings("unchecked")
    public static final JournalConfigurationBuilder MAIN = new JournalConfigurationBuilder() {{
        $(Quote.class)
                .partitionBy(PartitionType.MONTH)
                .lag(12, TimeUnit.HOURS)
                .location("quote")
                .key("sym")
                .$sym("sym").index().valueCountHint(15)
                .$sym("ex").index().valueCountHint(5)
                .$ts()
                .nullsFactory(new ThriftNullsAdaptorFactory())
        ;

        $(Trade2.class)
                .$sym("sym").valueCountHint(14)
                .$sym("ex").valueCountHint(5)
                .$sym("cond").valueCountHint(30)
                .$ts()
                .nullsFactory(new ThriftNullsAdaptorFactory())
        ;
    }};

}
