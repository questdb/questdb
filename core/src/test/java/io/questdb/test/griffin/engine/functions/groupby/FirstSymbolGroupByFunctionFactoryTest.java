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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class FirstSymbolGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testKeyed() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "a\tsym\n" +
                        "-1\tbb\n" +
                        "0\taa\n" +
                        "1\tcc\n",
                "select a, first(sym) sym from tab order by a",
                "create table tab as (select rnd_int() % 2 a, rnd_symbol('aa', 'bb', 'cc') sym from long_sequence(10))",
                null,
                true,
                true
        ));
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "sym\n" +
                        "aa\n",
                "select first(sym) sym from tab",
                "create table tab as (select rnd_int() % 2 a, rnd_symbol('aa', 'bb', 'cc') sym from long_sequence(10))",
                null,
                false,
                true
        ));
    }

    @Test
    public void testSampleFill() throws Exception {
        assertQuery("b\ta\tk\n" +
                        "\tkl2\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\tl1\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tkl2\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\tkl2\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tss4\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "\tl1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\tl1\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\tkl2\t1970-01-03T06:00:00.000000Z\n" +
                        "\tkl2\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tss4\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tl1\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tkl2\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tss4\t1970-01-03T09:00:00.000000Z\n" +
                        "\tl1\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\tss4\t1970-01-03T09:00:00.000000Z\n",
                "select b, first(a) a, k from x sample by 3h align to first observation",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('l1', 'kl2', 'ss4') a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_symbol('zz8', 'zz9') a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\ta\tk\n" +
                        "\tkl2\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\tl1\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tkl2\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\tkl2\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tss4\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "\tl1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\tl1\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tss4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\tkl2\t1970-01-03T06:00:00.000000Z\n" +
                        "\tkl2\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tss4\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tl1\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tkl2\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tss4\t1970-01-03T09:00:00.000000Z\n" +
                        "\tl1\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tss4\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\tss4\t1970-01-03T09:00:00.000000Z\n" +
                        "\tzz9\t1970-01-04T03:00:00.000000Z\n" +
                        "ZMZV\tzz8\t1970-01-04T03:00:00.000000Z\n" +
                        "QLDG\tzz9\t1970-01-04T06:00:00.000000Z\n" +
                        "LOGI\tzz9\t1970-01-04T06:00:00.000000Z\n" +
                        "QEBN\tzz8\t1970-01-04T06:00:00.000000Z\n" +
                        "\tzz8\t1970-01-04T06:00:00.000000Z\n" +
                        "FOUS\tzz9\t1970-01-04T06:00:00.000000Z\n",
                false
        );
    }
}