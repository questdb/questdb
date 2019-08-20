/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.CairoEngine;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.DefaultCairoConfiguration;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.AbstractGriffinTest;
import com.questdb.griffin.SqlCompiler;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Chars;
import com.questdb.std.FilesFacade;
import com.questdb.std.FilesFacadeImpl;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SampleByTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadFunction() throws Exception {
        assertFailure("select b, sum(a), sum(c), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                22,
                "Invalid column: c");
    }

    @Test
    public void testGroupByAllTypes() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\n" +
                        "HYRX\t108.4198\t129.399112218477\t2127224767\t95\t-8329\t1696566079386694074\n" +
                        "\t680.7651\t771.092262202840\t2135522192\t77\t815\t-5259855777509188759\n" +
                        "CPSW\t101.2276\t111.113584037391\t-1727443926\t33\t-22282\t7594916031131877487\n" +
                        "PEHN\t104.2904\t100.877261378303\t-940643167\t18\t17565\t-4882690809235649274\n" +
                        "RXGZ\t96.4029\t42.020442539326\t712702244\t46\t22661\t2762535352290012031\n",
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\n" +
                        "HYRX\t108.4198\t129.399112218477\t2127224767\t95\t-8329\t1696566079386694074\n" +
                        "\t779.3558\t869.932373151714\t-247384018\t107\t18639\t3597805051091659961\n" +
                        "CPSW\t101.2276\t111.113584037391\t-1727443926\t33\t-22282\t7594916031131877487\n" +
                        "PEHN\t104.2904\t100.877261378303\t-940643167\t18\t17565\t-4882690809235649274\n" +
                        "RXGZ\t96.4029\t42.020442539326\t712702244\t46\t22661\t2762535352290012031\n" +
                        "ZGHW\t50.2589\t38.422543844715\t597366062\t21\t23702\t7037372650941669660\n" +
                        "LOPJ\t76.6815\t5.158459929274\t1920398380\t38\t16628\t3527911398466283309\n" +
                        "VDKF\t4.3606\t35.681110212277\t503883303\t38\t10895\t7202923278768687325\n" +
                        "OXPK\t45.9207\t76.062526341246\t2043541236\t21\t19278\t1832315370633201942\n",
                true);
    }

    @Test
    public void testGroupByCount() throws Exception {
        assertQuery("c\tcount\n" +
                        "XY\t6\n" +
                        "\t5\n" +
                        "ZP\t5\n" +
                        "UU\t4\n",
                "select c, count() from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tcount\n" +
                        "XY\t6\n" +
                        "\t5\n" +
                        "ZP\t5\n" +
                        "UU\t4\n" +
                        "PL\t4\n" +
                        "KK\t1\n",
                true);
    }

    @Test
    public void testGroupByCountFromSubQuery() throws Exception {
        assertQuery("c\tcount\n" +
                        "UU\t1\n" +
                        "XY\t1\n" +
                        "ZP\t1\n" +
                        "\t1\n",
                "select c, count() from (x latest by c)",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tcount\n" +
                        "UU\t1\n" +
                        "XY\t1\n" +
                        "ZP\t1\n" +
                        "\t1\n" +
                        "KK\t1\n" +
                        "PL\t1\n",
                true);
    }

    @Test
    public void testGroupByEmpty() throws Exception {
        assertQuery("c\tsum_t\n",
                "select c, sum_t(d) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(0)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tsum_t\n" +
                        "PL\t1.088880189118\n" +
                        "KK\t2.614956708936\n",
                true);
    }

    @Test
    public void testGroupByFail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            compiler.compile(("create table x as " +
                    "(" +
                    "select" +
                    " x," +
                    " rnd_double(0) d," +
                    " rnd_symbol('XY','ZP', null, 'UU') c" +
                    " from" +
                    " long_sequence(1000000)" +
                    ")")
            );

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            final FilesFacade ff = new FilesFacadeImpl() {
                int count = 5;

                @Override
                public long mmap(long fd, long len, long offset, int mode) {
                    if (count-- > 0) {
                        return super.mmap(fd, len, offset, mode);
                    }
                    return -1;
                }
            };

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (SqlCompiler compiler = new SqlCompiler(engine)) {
                    try {
                        try (RecordCursorFactory factory = compiler.compile("select c, sum_t(d) from x")) {
                            factory.getCursor(sqlExecutionContext);
                        }
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), "Cannot mmap");
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }

    @Test
    public void testGroupByFreesFunctions() throws Exception {
        assertQuery("c\tsum_t\n" +
                        "UU\t4.192763851972\n" +
                        "XY\t5.326379743132\n" +
                        "\t1.858671018923\n" +
                        "ZP\t0.783663562521\n",
                "select c, sum_t(d) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tsum_t\n" +
                        "UU\t4.192763851972\n" +
                        "XY\t5.326379743132\n" +
                        "\t1.858671018923\n" +
                        "ZP\t0.783663562521\n" +
                        "KK\t1.643569909151\n" +
                        "PL\t1.162716966946\n",
                true);
    }

    @Test
    public void testSampleBadFunction() throws Exception {
        assertFailure(
                "select b, sumx(a, 'ab') k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "exception in function factory"
        );
    }

    @Test
    public void testSampleBadFunctionInterpolated() throws Exception {
        assertFailure(
                "select b, sumx(a, 'ac') k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "exception in function factory"
        );
    }

    @Test
    public void testSampleCountFillLinear() throws Exception {
        assertQuery("b\tcount\tk\n" +
                        "\t15\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t5\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "\t14\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t5\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "\t17\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t2\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8\t1970-01-03T06:00:00.000000Z\n" +
                        "\t4\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t11\t1970-01-03T09:00:00.000000Z\n",

                "select b, count(), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tcount\tk\n" +
                        "\t15\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t5\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t-8\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-8\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t14\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t5\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t-7\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-7\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t17\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t2\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t-6\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-6\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t4\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t11\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t-5\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-5\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t4\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t2\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t4\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t1\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t14\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t-4\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-4\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t5\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t1\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t1\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t5\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t17\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t-3\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-3\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t5\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-1\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t-2\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-2\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t6\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t7\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-2\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t23\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t6\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-2\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-2\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t8\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-3\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t26\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t7\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t1\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t1\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t-3\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-3\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t9\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-4\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t29\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t3\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t2\t1970-01-04T06:00:00.000000Z\n" +
                        "\t14\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t2\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t3\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-4\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-4\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t10\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-5\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t32\t1970-01-04T06:00:00.000000Z\n",
                true);
    }

    @Test
    public void testSampleCountFillLinearFromSubQuery() throws Exception {
        assertQuery("b\tcount\tk\n" +
                        "CPSW\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T09:00:00.000000Z\n",

                "select b, count(), k from (x latest by b) sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tcount\tk\n" +
                        "CPSW\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n",
                true);
    }

    @Test
    public void testSampleFillAllTypesLinear() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "HYRX\t11.4280\t42.177688419694\t426455968\t42\t4924\t4086802474270249591\t1970-01-03T00:00:00.000000Z\n" +
                        "\t42.2436\t70.943604871712\t1631244228\t50\t10900\t8349358446893356086\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t33.6083\t76.756730707961\t422941535\t27\t32312\t4442449726822927731\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t140.1138\t-63.368134807422\t2147483647\t9\t16851\t9223372036854775807\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t81.4681\t12.503042190293\t2085282008\t9\t11472\t8955092533521658248\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t67.6193\t34.356853329430\t2144581835\t6\t10942\t3152466304308949756\t1970-01-03T03:00:00.000000Z\n" +
                        "\t41.3816\t55.224941705116\t667031149\t38\t22298\t5536695302686527374\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t23.0646\t50.777860678019\t435411399\t41\t9083\t5351051939379353600\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t101.6448\t92.160793080664\t-1479788204\t80\t-26526\t-7038722756553554443\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t22.8223\t88.374219188009\t1269042121\t9\t6093\t4608960730952244094\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t34.7012\t59.378032936345\t444366830\t41\t13242\t6615301404488457216\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t101.6304\t-8.043024049102\t2147483647\t-15\t-10428\t1862482881794971392\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t153.9420\t103.119806202559\t-2112878268\t31\t-26762\t-4203926486423760584\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t96.4029\t42.020442539326\t712702244\t46\t22661\t2762535352290012031\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t46.3378\t67.978205194670\t453322261\t40\t17401\t7879550869597561856\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t135.6415\t-50.442901427633\t2147483647\t-36\t-31798\t572499459280992448\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-35.8234\t164.245396185725\t452802234\t9\t714\t262828928382831328\t1970-01-03T09:00:00.000000Z\n" +
                        "\t82.3556\t189.817280645823\t-1385332048\t54\t2779\t-238979168606022602\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t57.9745\t76.578377452995\t462277692\t40\t21561\t9143800334706665900\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t169.6526\t-92.842778806165\t2147483647\t-57\t12368\t-717483963232985728\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-94.4692\t240.116573183440\t-363437653\t9\t-4665\t-4083302874186582528\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t39.0173\t10.643046345788\t1238491107\t13\t30722\t6912707344119330199\t1970-01-03T15:00:00.000000Z\n" +
                        "\t107.8614\t139.306941555642\t2116801049\t40\t-19858\t-3504226003016057166\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t203.6637\t-135.242656184696\t2147483647\t-78\t-9002\t-2007467385746963968\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-153.1149\t315.987750181156\t-1179677539\t9\t-10044\t-8429434676755996672\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t151.3361\t120.518894141322\t-1596523010\t40\t-27552\t-4160055112489677424\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.0601\t-55.292284761419\t2014704521\t-14\t-25653\t4681614353531994112\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t237.6748\t-177.642533563228\t2147483647\t-99\t-30372\t-3297450808260941824\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-211.7607\t391.858927178872\t-1995917427\t9\t-15423\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n",

                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "HYRX\t11.4280\t42.177688419694\t426455968\t42\t4924\t4086802474270249591\t1970-01-03T00:00:00.000000Z\n" +
                        "\t42.2436\t70.943604871712\t1631244228\t50\t10900\t8349358446893356086\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t33.6083\t76.756730707961\t422941535\t27\t32312\t4442449726822927731\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t140.1138\t-63.368134807422\t2147483647\t9\t16851\t9223372036854775807\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t81.4681\t12.503042190293\t2085282008\t9\t11472\t8955092533521658248\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t67.6193\t34.356853329430\t2144581835\t6\t10942\t3152466304308949756\t1970-01-03T03:00:00.000000Z\n" +
                        "\t41.3816\t55.224941705116\t667031149\t38\t22298\t5536695302686527374\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t23.0646\t50.777860678019\t435411399\t41\t9083\t5351051939379353600\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t101.6448\t92.160793080664\t-1479788204\t80\t-26526\t-7038722756553554443\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t22.8223\t88.374219188009\t1269042121\t9\t6093\t4608960730952244094\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t34.7012\t59.378032936345\t444366830\t41\t13242\t6615301404488457216\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t101.6304\t-8.043024049102\t2147483647\t-15\t-10428\t1862482881794971392\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t153.9420\t103.119806202559\t-2112878268\t31\t-26762\t-4203926486423760584\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t96.4029\t42.020442539326\t712702244\t46\t22661\t2762535352290012031\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t46.3378\t67.978205194670\t453322261\t40\t17401\t7879550869597561856\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t135.6415\t-50.442901427633\t2147483647\t-36\t-31798\t572499459280992448\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-35.8234\t164.245396185725\t452802234\t9\t714\t262828928382831328\t1970-01-03T09:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t82.3556\t189.817280645823\t-1385332048\t54\t2779\t-238979168606022602\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t57.9745\t76.578377452995\t462277692\t40\t21561\t9143800334706665900\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t169.6526\t-92.842778806165\t2147483647\t-57\t12368\t-717483963232985728\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-94.4692\t240.116573183440\t-363437653\t9\t-4665\t-4083302874186582528\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t39.0173\t10.643046345788\t1238491107\t13\t30722\t6912707344119330199\t1970-01-03T15:00:00.000000Z\n" +
                        "\t107.8614\t139.306941555642\t2116801049\t40\t-19858\t-3504226003016057166\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t203.6637\t-135.242656184696\t2147483647\t-78\t-9002\t-2007467385746963968\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-153.1149\t315.987750181156\t-1179677539\t9\t-10044\t-8429434676755996672\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t151.3361\t120.518894141322\t-1596523010\t40\t-27552\t-4160055112489677424\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.0601\t-55.292284761419\t2014704521\t-14\t-25653\t4681614353531994112\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t237.6748\t-177.642533563228\t2147483647\t-99\t-30372\t-3297450808260941824\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-211.7607\t391.858927178872\t-1995917427\t9\t-15423\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t1.1030\t-121.227615868626\t2147483647\t-41\t-16492\t2450521362944657408\t1970-01-03T21:00:00.000000Z\n" +
                        "\t133.7543\t113.292633077173\t-426994978\t36\t-12426\t179183534540497952\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t271.6859\t-220.042410941759\t2147483647\t-120\t13794\t-4587434230774920192\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-270.4064\t467.730104176587\tNaN\t9\t-20802\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-17.8542\t-187.162946975833\t2147483647\t-68\t-7331\t219428372357321856\t1970-01-04T00:00:00.000000Z\n" +
                        "\t116.1725\t106.066372013024\t742533053\t33\t2698\t4518422181570673664\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t305.6970\t-262.442288320291\t2147483647\t115\t-7576\t-5877417653288898560\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-329.0522\t543.601281174303\tNaN\t9\t-26181\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t98.5907\t98.840110948875\t1912061086\t30\t17824\t8857660828600848720\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-36.8113\t-253.098278083040\t2147483647\t-95\t1830\t-2011664618230010368\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t339.7081\t-304.842165698822\t2147483647\t94\t-28946\t-7167401075802876928\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-387.6979\t619.472458172019\tNaN\t9\t-31560\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZGHW\t50.2589\t38.422543844715\t597366062\t21\t23702\t7037372650941669660\t1970-01-04T06:00:00.000000Z\n" +
                        "LOPJ\t76.6815\t5.158459929274\t1920398380\t38\t16628\t3527911398466283309\t1970-01-04T06:00:00.000000Z\n" +
                        "VDKF\t4.3606\t35.681110212277\t503883303\t38\t10895\t7202923278768687325\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-55.7685\t-319.033609190247\t2147483647\t-122\t10991\t-4242757608817349120\t1970-01-04T06:00:00.000000Z\n" +
                        "\t81.0089\t91.613849884725\t2147483647\t27\t-32586\t9223372036854775807\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t373.7192\t-347.242043077354\t2147483647\t73\t15220\t-8457384498316854272\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-446.3437\t695.343635169734\tNaN\t9\t28597\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "OXPK\t45.9207\t76.062526341246\t2043541236\t21\t19278\t1832315370633201942\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t-74.7257\t-384.968940297454\t2147483647\t107\t20152\t-6473850599404687360\t1970-01-04T09:00:00.000000Z\n" +
                        "\t63.4271\t84.387588820576\t2147483647\t24\t-17460\t9223372036854775807\t1970-01-04T09:00:00.000000Z\n" +
                        "CPSW\t407.7303\t-389.641920455885\t2147483647\t52\t-6150\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t-504.9894\t771.214812167450\tNaN\t9\t23218\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testSampleFillAllTypesLinearNoData() throws Exception {
        // sum_t tests memory leak
        assertQuery("b\tsum_t\tsum\tsum1\tsum2\tsum3\tsum4\tk\n",
                "select b, sum_t(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum_t\tsum\tsum1\tsum2\tsum3\tsum4\tk\n" +
                        "\t0.359833240509\t32.881769076795\t1253890363\t49\t27809\t7199909180655756830\t1970-01-04T03:00:00.000000Z\n" +
                        "DEYY\t164.434740066528\t117.535158666089\t22049944\t64\t3136\t4552387273114894848\t1970-01-04T03:00:00.000000Z\n" +
                        "SXUX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t24.808811187744\t63.816075311785\t461611463\t48\t514\t7709707078566863064\t1970-01-04T06:00:00.000000Z\n" +
                        "DEYY\t96.874229431152\t67.004763918011\t44173540\t34\t3282\t6794405451419334859\t1970-01-04T06:00:00.000000Z\n" +
                        "SXUX\t26.922100067139\t52.984059417621\t936627841\t16\t5741\t7153335833712179123\t1970-01-04T06:00:00.000000Z\n" +
                        "DEYY\t29.313718795776\t16.474369169932\t66297136\t4\t3428\t9036423629723776443\t1970-01-04T09:00:00.000000Z\n" +
                        "\t49.257789134979\t94.750381546775\t-330667436\t47\t-26781\t8219504976477969408\t1970-01-04T09:00:00.000000Z\n" +
                        "SXUX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t60.419130298418\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t269.080849555870\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t44.391962619325\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t183.395940508191\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t46.606236818956\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t82.960330608558\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t51.034785218218\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-73.658786634846\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t53.249059417849\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-159.343695682525\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t55.463333617480\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t13.557627225594\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-245.028604730204\t1970-01-03T18:00:00.000000Z\n",

                "select b, sum(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t60.419130298418\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t269.080849555870\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t44.391962619325\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t183.395940508191\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t46.606236818956\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t82.960330608558\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t51.034785218218\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-73.658786634846\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t53.249059417849\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-159.343695682525\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t55.463333617480\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t13.557627225594\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-245.028604730204\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t75.557134544295\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t57.677607817111\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-21.889850047664\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-330.713513777883\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t65.024342379742\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t59.891882016742\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-57.337327320922\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-416.398422825561\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t62.106156216373\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-92.784804594181\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-502.083331873240\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t64.320430416004\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-128.232281867439\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-587.768240920919\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n" +
                        "\t217.180417349163\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t66.534704615635\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t-163.679759140697\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t-673.453149968598\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testSampleFillLinearBadType() throws Exception {
        assertFailure(
                "select b, sum_t(b), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(1,1,2) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "Unsupported type"
        );
    }

    @Test
    public void testSampleFillLinearByMonth() throws Exception {
        assertQuery("b\tsum_t\tk\n" +
                        "\t54112.404059386576\t1970-01-01T00:00:00.000000Z\n" +
                        "VTJW\t11209.880434660998\t1970-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t9939.438287132382\t1970-01-01T00:00:00.000000Z\n" +
                        "PEHN\t11042.882403279874\t1970-01-01T00:00:00.000000Z\n" +
                        "HYRX\t11080.174817969956\t1970-01-01T00:00:00.000000Z\n" +
                        "CPSW\t9310.397369439000\t1970-01-01T00:00:00.000000Z\n" +
                        "\t53936.039113863768\t1970-04-01T00:00:00.000000Z\n" +
                        "HYRX\t10382.092656987054\t1970-04-01T00:00:00.000000Z\n" +
                        "CPSW\t11677.451781387846\t1970-04-01T00:00:00.000000Z\n" +
                        "RXGZ\t12082.973980924520\t1970-04-01T00:00:00.000000Z\n" +
                        "VTJW\t11574.354700279142\t1970-04-01T00:00:00.000000Z\n" +
                        "PEHN\t11225.427167029598\t1970-04-01T00:00:00.000000Z\n" +
                        "\t53719.385598369832\t1970-07-01T00:00:00.000000Z\n" +
                        "VTJW\t10645.216313875992\t1970-07-01T00:00:00.000000Z\n" +
                        "RXGZ\t12441.881371617534\t1970-07-01T00:00:00.000000Z\n" +
                        "HYRX\t10478.918039106036\t1970-07-01T00:00:00.000000Z\n" +
                        "CPSW\t11215.534064219256\t1970-07-01T00:00:00.000000Z\n" +
                        "PEHN\t12053.625707887684\t1970-07-01T00:00:00.000000Z\n" +
                        "\t54106.362147164440\t1970-10-01T00:00:00.000000Z\n" +
                        "HYRX\t11883.354138407446\t1970-10-01T00:00:00.000000Z\n" +
                        "RXGZ\t11608.715762809448\t1970-10-01T00:00:00.000000Z\n" +
                        "CPSW\t11623.362686708584\t1970-10-01T00:00:00.000000Z\n" +
                        "PEHN\t11258.550294609914\t1970-10-01T00:00:00.000000Z\n" +
                        "VTJW\t10865.136275604094\t1970-10-01T00:00:00.000000Z\n" +
                        "\t33152.562899296544\t1971-01-01T00:00:00.000000Z\n" +
                        "PEHN\t7219.259660624380\t1971-01-01T00:00:00.000000Z\n" +
                        "CPSW\t6038.834871820060\t1971-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t5862.505042201944\t1971-01-01T00:00:00.000000Z\n" +
                        "VTJW\t6677.581919995402\t1971-01-01T00:00:00.000000Z\n" +
                        "HYRX\t5998.730211949622\t1971-01-01T00:00:00.000000Z\n",
                "select b, sum_t(a), k from x sample by 3M fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(10000)" +
                        ") timestamp(k) partition by NONE",
                "k",
                true);
    }

    @Test
    public void testSampleFillLinearConstructorFail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {

                compiler.compile(("create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20000000)" +
                        ") timestamp(k) partition by NONE"));

                FilesFacade ff = new FilesFacadeImpl() {
                    int count = 2;

                    @Override
                    public long mmap(long fd, long len, long offset, int mode) {
                        if (count-- > 0) {
                            return super.mmap(fd, len, offset, mode);
                        }
                        return -1;
                    }
                };

                CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                };

                try (CairoEngine engine = new CairoEngine(configuration)) {
                    try (SqlCompiler compiler = new SqlCompiler(engine)) {
                        try {
                            compiler.compile("select b, sum(a), k from x sample by 3h fill(linear)");
                            Assert.fail();
                        } catch (SqlException e) {
                            Assert.assertTrue(Chars.contains(e.getMessage(), "Cannot mmap"));
                        }
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                        Assert.assertEquals(0, engine.getBusyWriterCount());
                    }
                }

            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testSampleFillLinearFail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {

                compiler.compile(("create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20000000)" +
                        ") timestamp(k) partition by NONE")
                );

                FilesFacade ff = new FilesFacadeImpl() {
                    int count = 5;

                    @Override
                    public long mmap(long fd, long len, long offset, int mode) {
                        if (count-- > 0) {
                            return super.mmap(fd, len, offset, mode);
                        }
                        return -1;
                    }
                };

                CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                };

                try (CairoEngine engine = new CairoEngine(configuration)) {
                    try (SqlCompiler compiler = new SqlCompiler(engine)) {
                        try {
                            try (RecordCursorFactory factory = compiler.compile("select b, sum(a), k from x sample by 3h fill(linear)")) {
                                // with mmap count = 5 we should get failure in cursor
                                factory.getCursor(sqlExecutionContext);
                            }
                            Assert.fail();
                        } catch (CairoException e) {
                            Assert.assertTrue(Chars.contains(e.getMessage(), "Cannot mmap"));
                        }
                        Assert.assertEquals(0, engine.getBusyReaderCount());
                        Assert.assertEquals(0, engine.getBusyWriterCount());
                    }
                }

            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneAllTypes() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.197525059489\t113.1213\t-1737520119\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.359836721543\t76.7567\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.642567535961\t55.2249\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900\t34.3569\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599555\t12.5030\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.059401417446\t92.1608\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.641589147185\t88.3742\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.781182496875\t103.1198\t-1265361864\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863681\t42.0204\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.609378432567\t189.8173\t-577162926\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107\t76.5784\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621127\t139.3070\t-1706978251\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.683686301370\t10.6430\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.171605517508\t120.5189\t-1932725894\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.197525059489\t113.1213\t-1737520119\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.359836721543\t76.7567\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.642567535961\t55.2249\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900\t34.3569\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599555\t12.5030\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.059401417446\t92.1608\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.641589147185\t88.3742\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.781182496875\t103.1198\t-1265361864\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863681\t42.0204\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.609378432567\t189.8173\t-577162926\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107\t76.5784\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621127\t139.3070\t-1706978251\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.683686301370\t10.6430\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.171605517508\t120.5189\t-1932725894\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t20.585069039325\t98.8401\t1278547815\t17250\t3\t-6703401424236463520\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "EZGH\t5.024615679069\t38.4225\t370796356\t5422\t3\t4959459375462458218\t1970-01-04T06:00:00.000000Z\n" +
                        "FLOP\t17.180291960857\t5.1585\t532016913\t-3028\t7\t2282781332678491916\t1970-01-04T06:00:00.000000Z\n" +
                        "WVDK\t54.669009214053\t35.6811\t874367915\t-23001\t10\t9089874911309539983\t1970-01-04T06:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\t0\t0\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "JOXP\t67.294055907736\t76.0625\t1165635863\t2316\t9\t-4547802916868961458\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneDataGaps() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T01:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T02:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "\t87.996347253916\t1970-01-03T04:00:00.000000Z\n" +
                        "\t32.881769076795\t1970-01-03T05:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T07:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T08:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T10:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T11:00:00.000000Z\n" +
                        "\t52.984059417621\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T13:00:00.000000Z\n" +
                        "\t97.501988537251\t1970-01-03T14:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t80.011211397392\t1970-01-03T16:00:00.000000Z\n" +
                        "\t92.050039469858\t1970-01-03T17:00:00.000000Z\n" +
                        "\t45.634456960908\t1970-01-03T18:00:00.000000Z\n" +
                        "\t40.455469747939\t1970-01-03T19:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 30m fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T01:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T02:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "\t87.996347253916\t1970-01-03T04:00:00.000000Z\n" +
                        "\t32.881769076795\t1970-01-03T05:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T07:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T08:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T10:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T11:00:00.000000Z\n" +
                        "\t52.984059417621\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T13:00:00.000000Z\n" +
                        "\t97.501988537251\t1970-01-03T14:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t80.011211397392\t1970-01-03T16:00:00.000000Z\n" +
                        "\t92.050039469858\t1970-01-03T17:00:00.000000Z\n" +
                        "\t45.634456960908\t1970-01-03T18:00:00.000000Z\n" +
                        "\t40.455469747939\t1970-01-03T19:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T05:00:00.000000Z\n" +
                        "\t76.923818943378\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T07:00:00.000000Z\n" +
                        "\t58.912164838798\t1970-01-04T08:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneEmpty() throws Exception {
        assertQuery("b\tsum_t\tk\n",
                "select b, sum_t(a), k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum_t\tk\n" +
                        "IBBT\t0.359836721543\t1970-01-04T03:00:00.000000Z\n" +
                        "\t202.746073098277\t1970-01-04T06:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNull() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNullBadType() throws Exception {
        assertFailure(
                "select b, sum_t(b), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(1,1,2) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "Unsupported type"
        );
    }

    @Test
    public void testSampleFillNullDay() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t5777.418391000034\t1970-01-01T00:00:00.000000Z\n" +
                        "VTJW\t1625.610380713817\t1970-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t1412.038590639350\t1970-01-01T00:00:00.000000Z\n" +
                        "PEHN\t1103.691956777804\t1970-01-01T00:00:00.000000Z\n" +
                        "HYRX\t1244.932150897452\t1970-01-01T00:00:00.000000Z\n" +
                        "CPSW\t767.342385768801\t1970-01-01T00:00:00.000000Z\n" +
                        "\t4695.687057334280\t1970-01-13T00:00:00.000000Z\n" +
                        "VTJW\t878.513464919190\t1970-01-13T00:00:00.000000Z\n" +
                        "RXGZ\t719.035202186062\t1970-01-13T00:00:00.000000Z\n" +
                        "PEHN\t751.454927852314\t1970-01-13T00:00:00.000000Z\n" +
                        "HYRX\t732.944056645521\t1970-01-13T00:00:00.000000Z\n" +
                        "CPSW\t1012.865585349937\t1970-01-13T00:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 12d fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(400)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillNullMonth() throws Exception {
        assertQuery(
                "b\tsum\tk\n" +
                        "\t54112.404059386576\t1970-01-01T00:00:00.000000Z\n" +
                        "VTJW\t11209.880434660998\t1970-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t9939.438287132382\t1970-01-01T00:00:00.000000Z\n" +
                        "PEHN\t11042.882403279874\t1970-01-01T00:00:00.000000Z\n" +
                        "HYRX\t11080.174817969956\t1970-01-01T00:00:00.000000Z\n" +
                        "CPSW\t9310.397369439000\t1970-01-01T00:00:00.000000Z\n" +
                        "\t2030.155438151525\t1970-04-01T00:00:00.000000Z\n" +
                        "VTJW\t680.416344135958\t1970-04-01T00:00:00.000000Z\n" +
                        "RXGZ\t534.733385281313\t1970-04-01T00:00:00.000000Z\n" +
                        "PEHN\t359.891979199214\t1970-04-01T00:00:00.000000Z\n" +
                        "HYRX\t487.361172719468\t1970-04-01T00:00:00.000000Z\n" +
                        "CPSW\t444.970484588731\t1970-04-01T00:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3M fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(2200)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillNullYear() throws Exception {
        assertQuery(
                "b\tsum\tk\n" +
                        "\t433413.612901176960\t1970-01-01T00:00:00.000000Z\n" +
                        "VTJW\t87351.541927459600\t1970-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t88650.231612464544\t1970-01-01T00:00:00.000000Z\n" +
                        "PEHN\t91229.031954294304\t1970-01-01T00:00:00.000000Z\n" +
                        "HYRX\t87026.718735230112\t1970-01-01T00:00:00.000000Z\n" +
                        "CPSW\t87762.478425828496\t1970-01-01T00:00:00.000000Z\n" +
                        "\t315056.784555843712\t1972-01-01T00:00:00.000000Z\n" +
                        "VTJW\t61084.814285205680\t1972-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t63886.423482740584\t1972-01-01T00:00:00.000000Z\n" +
                        "PEHN\t64376.579921839208\t1972-01-01T00:00:00.000000Z\n" +
                        "HYRX\t64680.372828808416\t1972-01-01T00:00:00.000000Z\n" +
                        "CPSW\t60814.765754379168\t1972-01-01T00:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 2y fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(30000)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrev() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevAllTypes() throws Exception {
        assertQuery("a\tb\tc\td\te\tf\tg\ti\tj\tl\tm\tp\tsum\tk\n" +
                        "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\t1970-01-01T00:00:00.000000Z\t0.157866355996\t1970-01-03T00:00:00.000000Z\n" +
                        "-2132716300\ttrue\tU\t0.381797580478\tNaN\t813\t2015-07-01T22:08:50.655Z\tHYRX\t-6186964045554120476\t34\t00000000 07 42 fc 31 79 5f 8b 81 2b 93\t1970-01-01T01:00:00.000000Z\t0.041428124702\t1970-01-03T00:00:00.000000Z\n" +
                        "-360860352\ttrue\tM\t0.456344569609\tNaN\t1013\t2015-01-15T20:11:07.487Z\tHYRX\t5271904137583983788\t30\t00000000 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64\n" +
                        "00000010 0e 2c\t1970-01-01T02:00:00.000000Z\t0.675250954711\t1970-01-03T00:00:00.000000Z\n" +
                        "2060263242\tfalse\tL\tNaN\t0.3495\t869\t2015-05-15T18:43:06.827Z\tCPSW\t-5439556746612026472\t11\t\t1970-01-01T03:00:00.000000Z\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "502711083\tfalse\tH\t0.017185009856\t0.0977\t605\t2015-07-12T07:33:54.007Z\tVTJW\t-6187389706549636253\t32\t00000000 29 8e 29 5e 69 c6 eb ea c3 c9 73\t1970-01-01T04:00:00.000000Z\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\t1970-01-01T00:00:00.000000Z\t0.157866355996\t1970-01-03T03:00:00.000000Z\n" +
                        "-2132716300\ttrue\tU\t0.381797580478\tNaN\t813\t2015-07-01T22:08:50.655Z\tHYRX\t-6186964045554120476\t34\t00000000 07 42 fc 31 79 5f 8b 81 2b 93\t1970-01-01T01:00:00.000000Z\t0.041428124702\t1970-01-03T03:00:00.000000Z\n" +
                        "-360860352\ttrue\tM\t0.456344569609\tNaN\t1013\t2015-01-15T20:11:07.487Z\tHYRX\t5271904137583983788\t30\t00000000 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64\n" +
                        "00000010 0e 2c\t1970-01-01T02:00:00.000000Z\t0.675250954711\t1970-01-03T03:00:00.000000Z\n" +
                        "2060263242\tfalse\tL\tNaN\t0.3495\t869\t2015-05-15T18:43:06.827Z\tCPSW\t-5439556746612026472\t11\t\t1970-01-01T03:00:00.000000Z\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "502711083\tfalse\tH\t0.017185009856\t0.0977\t605\t2015-07-12T07:33:54.007Z\tVTJW\t-6187389706549636253\t32\t00000000 29 8e 29 5e 69 c6 eb ea c3 c9 73\t1970-01-01T04:00:00.000000Z\t0.226315234342\t1970-01-03T03:00:00.000000Z\n",
                "select a,b,c,d,e,f,g,i,j,l,m,p,sum(o), k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str(1,1,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long() j," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(to_timestamp(0), 3600000000) p," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrevDuplicateKey() throws Exception {
        assertQuery("b\tb1\tb2\tsum\tk\n" +
                        "\t\t\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t\t\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "\t\t\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "\t\t\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\n" +
                        "\t\t\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\n" +
                        "\t\t\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\n",
                "select b, b, b, sum(a), k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tb1\tb2\tsum\tk\n" +
                        "\t\t\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t\t\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t\t\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t\t\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t\t\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t\t\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t\t\t86.089926708847\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t\t\t86.089926708847\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t\t\t54.491550215189\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t\t\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\t\t\t135.835983782176\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018587\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.905290108465\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.005104498852\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\t49.428905119585\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevDuplicateTimestamp1() throws Exception {
        assertQuery("b\tsum\tk\tk1\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k, k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\tk1\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevDuplicateTimestamp2() throws Exception {
        assertQuery("b\tsum\tk1\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k k1, k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k1",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk1\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevEmptyBase() throws Exception {
        assertQuery(null,
                "select a,b,c,d,e,f,g,i,j,l,m,p,sum(o), k from x where 0!=0 sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str(1,1,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long() j," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(to_timestamp(0), 3600000000) p," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrevNoTimestamp() throws Exception {
        assertQuery("b\tsum\n" +
                        "\t11.427984775756\n" +
                        "VTJW\t42.177688419694\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\tNaN\n" +
                        "HYRX\tNaN\n" +
                        "\t120.878116330711\n" +
                        "VTJW\t42.177688419694\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t70.943604871712\n" +
                        "HYRX\tNaN\n" +
                        "\t57.934663268622\n" +
                        "VTJW\t42.177688419694\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t81.468079445006\n" +
                        "HYRX\t97.711031460512\n" +
                        "\t26.922103479745\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t81.468079445006\n" +
                        "HYRX\t12.026122412833\n" +
                        "\t150.486047954871\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t84.452581772111\n" +
                        "HYRX\t12.026122412833\n" +
                        "\t172.061250867250\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "\t86.089926708847\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n",
                "select b, sum(a) from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\n" +
                        "\t11.427984775756\n" +
                        "VTJW\t42.177688419694\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\tNaN\n" +
                        "HYRX\tNaN\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t120.878116330711\n" +
                        "VTJW\t42.177688419694\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t70.943604871712\n" +
                        "HYRX\tNaN\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t57.934663268622\n" +
                        "VTJW\t42.177688419694\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t81.468079445006\n" +
                        "HYRX\t97.711031460512\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t26.922103479745\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t81.468079445006\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t150.486047954871\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t84.452581772111\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t172.061250867250\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t86.089926708847\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t86.089926708847\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t86.089926708847\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t54.491550215189\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t135.835983782176\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\t49.428905119585\n" +
                        "KGHV\tNaN\n" +
                        "\t135.835983782176\n" +
                        "VTJW\t48.820511018587\n" +
                        "RXGZ\t23.905290108465\n" +
                        "PEHN\t49.005104498852\n" +
                        "HYRX\t12.026122412833\n" +
                        "UVSD\t49.428905119585\n" +
                        "KGHV\t67.525095471124\n",
                false);
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.177688419694\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.878116330711\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.943604871712\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.934663268622\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.468079445006\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.711031460512\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479745\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.486047954871\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.452581772111\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.061250867250\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.089926708847\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.491550215189\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillValueAllKeyTypes() throws Exception {
        assertQuery("b\th\ti\tj\tl\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t11.427984775756\t42.1777\t1432278050\t13216\t4\t5539350449504785212\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t12.026122412833\t48.8205\t458818940\t3282\t8\t-6253307669002054137\t1970-01-03T00:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t5.048190020054\t0.1108\t66297136\t-5637\t7\t9036423629723776443\t1970-01-03T00:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t11.585982949541\t81.6418\t998315423\t-5585\t7\t8587391969565958670\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t19.751370382305\t68.0687\t544695670\t-1464\t6\t-5024542231726589509\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t84.384595639148\t48.9274\t1100812407\t-32358\t10\t5398991075259361292\t1970-01-03T03:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t85.931314807243\t10.5273\t2105201404\t5667\t8\t-8994301462266164776\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t63.412928948436\t5.0246\t1377625589\t-25710\t3\t2151565237758036093\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t85.843084380450\t54.6690\t903066492\t-2990\t4\t-1134031357796740497\t1970-01-03T06:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t21.485589614091\t6.2027\t358259591\t-29980\t8\t-8841102831894340636\t1970-01-03T09:00:00.000000Z\n",
                "select b, h, i, j, l, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " rnd_str(5,8,2) h," +
                        " rnd_bin(10, 20, 2) i," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) j," +
                        " rnd_boolean() l," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillValueAllTypes() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.197525059489\t113.1213\t-1737520119\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.359836721543\t76.7567\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.642567535961\t55.2249\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900\t34.3569\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599555\t12.5030\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.059401417446\t92.1608\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.641589147185\t88.3742\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.781182496875\t103.1198\t-1265361864\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863681\t42.0204\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.609378432567\t189.8173\t-577162926\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107\t76.5784\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621127\t139.3070\t-1706978251\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.683686301370\t10.6430\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.171605517508\t120.5189\t-1932725894\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.197525059489\t113.1213\t-1737520119\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.359836721543\t76.7567\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.642567535961\t55.2249\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900\t34.3569\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599555\t12.5030\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.059401417446\t92.1608\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.641589147185\t88.3742\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.781182496875\t103.1198\t-1265361864\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863681\t42.0204\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.609378432567\t189.8173\t-577162926\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107\t76.5784\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621127\t139.3070\t-1706978251\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.683686301370\t10.6430\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.171605517508\t120.5189\t-1932725894\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t20.585069039325\t98.8401\t1278547815\t17250\t3\t-6703401424236463520\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "EZGH\t5.024615679069\t38.4225\t370796356\t5422\t3\t4959459375462458218\t1970-01-04T06:00:00.000000Z\n" +
                        "FLOP\t17.180291960857\t5.1585\t532016913\t-3028\t7\t2282781332678491916\t1970-01-04T06:00:00.000000Z\n" +
                        "WVDK\t54.669009214053\t35.6811\t874367915\t-23001\t10\t9089874911309539983\t1970-01-04T06:00:00.000000Z\n" +
                        "JOXP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "EZGH\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "FLOP\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "WVDK\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "JOXP\t67.294055907736\t76.0625\t1165635863\t2316\t9\t-4547802916868961458\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillValueAllTypesAndTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile((
                        "create table x as " +
                                "(" +
                                "select" +
                                " rnd_double(0)*100 a," +
                                " rnd_symbol(5,4,4,1) b," +
                                " rnd_float(0)*100 c," +
                                " abs(rnd_int()) d," +
                                " rnd_short() e," +
                                " rnd_byte(3,10) f," +
                                " rnd_long() g," +
                                " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                                " from" +
                                " long_sequence(20)" +
                                ") timestamp(k) partition by NONE"));

                try (final RecordCursorFactory factory = compiler.compile("select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0, 0)")) {
                    assertTimestamp("k", factory);
                    String expected = "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                            "\t74.197525059489\t113.1213\t-1737520119\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                            "CPSW\t0.359836721543\t76.7567\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                            "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                            "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                            "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                            "\t76.642567535961\t55.2249\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                            "CPSW\t13.450170570900\t34.3569\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                            "PEHN\t15.786635599555\t12.5030\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                            "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                            "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                            "\t85.059401417446\t92.1608\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                            "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                            "PEHN\t86.641589147185\t88.3742\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                            "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                            "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                            "\t106.781182496875\t103.1198\t-1265361864\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                            "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                            "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                            "RXGZ\t3.831785863681\t42.0204\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                            "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                            "\t117.609378432567\t189.8173\t-577162926\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                            "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                            "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                            "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                            "HYRX\t24.008362859107\t76.5784\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                            "\t28.087836621127\t139.3070\t-1706978251\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                            "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                            "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                            "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                            "HYRX\t2.683686301370\t10.6430\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                            "\t75.171605517508\t120.5189\t-1932725894\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                            "CPSW\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                            "PEHN\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                            "RXGZ\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                            "HYRX\t20.560000000000\t0.0000\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n";
                    assertCursor(expected, factory, false);
                    // make sure we get the same outcome when we get factory to create new cursor
                    assertCursor(expected, factory, false);
                    // make sure strings, binary fields and symbols are compliant with expected record behaviour
                    assertVariableColumns(factory);

                    compiler.compile("truncate table x");
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        sink.clear();
                        printer.print(cursor, factory.getMetadata(), true);
                        TestUtils.assertEquals("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n", sink);
                    }
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testSampleFillValueBadType() throws Exception {
        assertFailure(
                "select b, sum_t(b), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(1,1,2) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "Unsupported type"
        );
    }

    @Test
    public void testSampleFillValueEmpty() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillValueFromSubQuery() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "\t40.455469747939\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from (x latest by b) sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(to_timestamp(277200000000), 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "RXGZ\t23.905290108465\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018587\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.005104498852\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.428905119585\t1970-01-04T06:00:00.000000Z\n" +
                        "\t58.912164838798\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t20.560000000000\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "\t20.560000000000\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.525095471124\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillValueInvalid() throws Exception {
        assertFailure(
                "select b, sum_t(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, none, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                94,
                "invalid number"
        );
    }

    @Test
    public void testSampleFillValueNotEnough() throws Exception {
        assertFailure(
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(to_timestamp(172800000000), 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                0,
                "not enough values"
        );
    }
}
