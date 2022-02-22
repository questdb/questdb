/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class RndDateCCCFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAllNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "\n",
                1);
    }

    @Test
    public void testNoNans() throws Exception {
        testNaNRate("testCol\n" +
                        "2018-05-08T06:04:41.473Z\n" +
                        "2014-09-06T16:53:49.876Z\n" +
                        "2013-02-15T08:15:22.949Z\n" +
                        "2012-08-29T17:20:09.561Z\n" +
                        "2010-06-11T11:03:44.564Z\n" +
                        "2012-03-18T18:32:33.273Z\n" +
                        "2013-11-16T01:40:32.601Z\n" +
                        "2020-03-10T09:16:43.203Z\n" +
                        "2021-10-13T17:41:47.224Z\n" +
                        "2013-06-01T20:27:07.720Z\n" +
                        "2011-03-05T17:36:53.097Z\n" +
                        "2012-01-03T07:46:56.930Z\n" +
                        "2019-10-25T01:26:26.095Z\n" +
                        "2013-04-29T01:30:33.617Z\n" +
                        "2012-07-04T19:02:17.538Z\n" +
                        "2018-09-16T05:32:16.132Z\n" +
                        "2018-10-08T11:38:33.420Z\n" +
                        "2019-04-28T10:38:04.293Z\n" +
                        "2020-04-22T10:49:25.119Z\n" +
                        "2019-08-05T16:53:39.244Z\n",
                0);
    }

    @Test
    public void testWithNaNs() throws Exception {
        testNaNRate("testCol\n" +
                        "\n" +
                        "2013-02-15T08:15:22.949Z\n" +
                        "2010-06-11T11:03:44.564Z\n" +
                        "2013-11-16T01:40:32.601Z\n" +
                        "2021-10-13T17:41:47.224Z\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "2013-04-29T01:30:33.617Z\n" +
                        "2018-09-16T05:32:16.132Z\n" +
                        "\n" +
                        "2020-04-22T10:49:25.119Z\n" +
                        "2017-07-20T17:46:08.172Z\n" +
                        "2021-06-18T12:26:01.702Z\n" +
                        "2015-03-03T23:19:58.294Z\n" +
                        "\n" +
                        "2018-02-19T19:00:08.115Z\n" +
                        "2017-08-15T06:53:27.594Z\n" +
                        "2013-12-07T10:27:20.038Z\n" +
                        "\n",
                4);
    }

    @Test
    public void testNegativeNaNRate() {
        assertFailure("[44] invalid NaN rate", "select rnd_date(1266851450000,1645542650000,-1) as testCol from long_sequence(20)");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndDateCCCFunctionFactory();
    }

    private void testNaNRate(CharSequence expectedData, int nanRate) throws Exception {
        assertQuery(expectedData, "select rnd_date(1266851450000,1645542650000," + nanRate + ") as testCol from long_sequence(20)");
    }
}
