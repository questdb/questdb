package io.questdb.griffin;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.OutOfOrderUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler.RecordToRowCopier;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class CommitHysteresisTest {
    private final static Log LOG = LogFactory.getLog(CommitHysteresisTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private CairoConfiguration configuration;
    private CairoEngine engine;
    private SqlCompiler compiler;
    private SqlExecutionContext sqlExecutionContext;
    private static final StringSink sink = new StringSink();
    private static final StringSink sink2 = new StringSink();
    private long minTimestamp;
    private long maxTimestamp;
    private RecordToRowCopier copier;

    @Test
    public void testNoHysteresis() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<=495 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=495", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>495 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testNoHysteresisWithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=250) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>250 and i<=375 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>375 order by f";
                insertUncommitted(sql, writer);
                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n375\n");

                sql = "select * from x where i>380 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n495\n");
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375 or i>380", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testNoHysteresisEndingAtPartitionBoundary() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<=184 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=184", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>184 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisWithinPartition() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=490) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=490", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>490 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testHysteresisWithinPartitionWithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=250) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>250 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n375\n");

                sql = "select * from x where i>380 order by f";
                insertUncommitted(sql, writer);
                writer.commit();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n495\n");
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=375 or i>380", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisStaggeringPartitions() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<200 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>=200 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testHysteresisStaggeringPartitionsWithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<200 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n175\n");

                sql = "select * from x where i>=200 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp) and (i<=175 or i>=200)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=175 or i>=200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundary() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<200 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-06T23:59:59.000Z");
                minTimestamp = maxTimestamp - lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>=200 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testHysteresisEndingAtPartitionBoundaryWithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<200 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-06T23:59:59.000Z");
                minTimestamp = maxTimestamp - lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select i, ts from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n184\n");

                sql = "select * from x where i>=200 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp) and (i<=184 or i>=200)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=184 or i>=200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisEndingAtPartitionBoundaryPlus1() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<200 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-07T00:00:00.000Z");
                minTimestamp = maxTimestamp - lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>=200 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testHysteresisEndingAtPartitionBoundaryPlus1WithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            // i=184 is the last entry in date 1970-01-06
            sql = "create table y as (select * from x where i<=150) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=150", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>150 and i<200 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = maxTimestamp - TimestampFormatUtils.parseTimestamp("1970-01-07T00:00:00.000Z");
                minTimestamp = maxTimestamp - lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n185\n");

                sql = "select * from x where i>=200 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp) and (i<=185 or i>=200)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=185 or i>=200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisStaggeringMultiplePartitions() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(8000)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=1200) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=1200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>1200 and i<6800 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>=6800 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testHysteresisStaggeringMultiplePartitionsWithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(8000)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=1200) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=1200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>1200 and i<6800 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n4000\n");

                sql = "select * from x where i>=6800 order by f";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp) and (i<=4000 or i>=6800)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=4000 or i>=6800", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisOnInOrderCommit() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(8000)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=1200) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=1200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>1200 and i<6800";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                sql = "select * from x where i>=6800";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Ignore
    @Test
    public void testHysteresisOnInOrderCommitWithRollback() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(8000)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where i<=1200) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=1200", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>1200 and i<6800";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.rollback();
                TestUtils.printSql(compiler, sqlExecutionContext, "select count(*) from y", sink);
                TestUtils.assertEquals(sink, "count\n4000\n");

                sql = "select * from x where i>=6800";
                insertUncommitted(sql, writer);
                lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp) and (i<=4000 or i>=6800)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=4000 or i>=6800", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisWithLargeOutOfOrder() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where (i<=50 or i>=100) and i<=250) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where (i<=50 or i>=100) and i<=250", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>250 or (i>50 and i<100) order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testHysteresisWithInOrderBatchFollowedByOutOfOrderBatch() throws Exception {
        executeVanilla(() -> {
            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(500000000000L,100000000L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(500)" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where  i<=250) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=250", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                sql = "select * from x where i>250 and i<300";
                insertUncommitted(sql, writer);
                sql = "select * from x where i>=300 order by f";
                insertUncommitted(sql, writer);
                long lastTimestampHysteresisInMicros = (maxTimestamp - minTimestamp) / 2;
                minTimestamp += lastTimestampHysteresisInMicros;
                writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where ts<=cast(" + minTimestamp + " as timestamp)", sink);
                TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
                TestUtils.assertEquals(sink, sink2);

                writer.commit();
            }

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    @Test
    public void testContinousBatchedCommit() throws Exception {
        executeVanilla(() -> {
            int nTotalRows = 20000;
            int nInitialStateRows = 150;
            long microsBetweenRows = 100000000;
            int maxBatchedRows = 10;
            int maxConcurrentBatches = 4;
            int nRowsPerCommit = 100;
            long lastTimestampHysteresisInMicros = microsBetweenRows * (nRowsPerCommit / 2);

            String sql = "create table x as (" +
                    "select" +
                    " cast(x as int) i," +
                    " rnd_symbol('msft','ibm', 'googl') sym," +
                    " round(rnd_double(0)*100, 3) amt," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                    " rnd_boolean() b," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) ik," +
                    " rnd_long() j," +
                    " timestamp_sequence(0L," + microsBetweenRows + "L) ts," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() t" +
                    " from long_sequence(" + nTotalRows + ")" +
                    "), index(sym) timestamp (ts) partition by DAY";
            compiler.compile(sql, sqlExecutionContext);

            sql = "create table y as (select * from x where  i<=" + nInitialStateRows + ") partition by DAY";
            compiler.compile(sql, sqlExecutionContext);
            LOG.info().$("committed initial state").$();

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x where i<=" + nInitialStateRows, sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);

            Random rand = new Random(0);
            IntList batchRowEnd = new IntList((int) ((nTotalRows - nInitialStateRows) * 0.6 * maxBatchedRows));
            int atRow = nInitialStateRows;
            batchRowEnd.add(-atRow); // negative row means this has been commited
            while (atRow < nTotalRows) {
                int nRows = rand.nextInt(maxBatchedRows) + 1;
                atRow += nRows;
                batchRowEnd.add(atRow);
            }

            int nCommitsWithHysteresis = 0;
            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "y")) {
                int nHeadBatch = 1;
                int nRowsAppended = 0;
                while (nHeadBatch < batchRowEnd.size()) {
                    int nBatch = nHeadBatch + rand.nextInt(maxConcurrentBatches);
                    while (nBatch >= batchRowEnd.size() || batchRowEnd.get(nBatch) < 0) {
                        nBatch--;
                    }
                    assert nBatch >= nHeadBatch;
                    if (nBatch == nHeadBatch) {
                        do {
                            nHeadBatch++;
                        } while (nHeadBatch < batchRowEnd.size() && batchRowEnd.get(nHeadBatch) < 0);
                    }
                    int fromRow = Math.abs(batchRowEnd.get(nBatch - 1));
                    int toRow = batchRowEnd.get(nBatch);
                    batchRowEnd.set(nBatch, -toRow);

                    LOG.info().$("inserting rows from ").$(fromRow).$(" to ").$(toRow).$();
                    sql = "select * from x where ts>=cast(" + fromRow * microsBetweenRows + " as timestamp) and ts<cast(" + toRow * microsBetweenRows + " as timestamp)";
                    insertUncommitted(sql, writer);

                    nRowsAppended += toRow - fromRow;
                    if (nRowsAppended >= nRowsPerCommit) {
                        LOG.info().$("committing with hysteresis").$();
                        nRowsAppended = 0;
                        writer.commitWithHysteresis(lastTimestampHysteresisInMicros);
                        nCommitsWithHysteresis++;
                    }
                }
                writer.commit();
            }
            LOG.info().$("committed final state with ").$(nCommitsWithHysteresis).$(" commits with hysteresis").$();

            TestUtils.printSql(compiler, sqlExecutionContext, "select * from x", sink);
            TestUtils.printSql(compiler, sqlExecutionContext, "select * from y", sink2);
            TestUtils.assertEquals(sink, sink2);
        });
    }

    protected void insertUncommitted(String sql, TableWriter writer) throws SqlException {
        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();) {
            RecordMetadata metadata = factory.getMetadata();
            int timestampIndex = writer.getMetadata().getTimestampIndex();
            EntityColumnFilter toColumnFilter = new EntityColumnFilter();
            toColumnFilter.of(metadata.getColumnCount());
            if (null == copier) {
                copier = SqlCompiler.assembleRecordToRowCopier(new BytecodeAssembler(), metadata, writer.getMetadata(), toColumnFilter);
            }
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    long timestamp = record.getTimestamp(timestampIndex);
                    if (timestamp > maxTimestamp) {
                        maxTimestamp = timestamp;
                    }
                    if (timestamp < minTimestamp) {
                        minTimestamp = timestamp;
                    }
                    Row row = writer.newRow(timestamp);
                    copier.copy(record, row);
                    row.append();
                }
            }
        }
    }

    @Before
    public void clearRecordToRowCopier() {
        copier = null;
    }

    @Before
    public void before() throws IOException {
        LOG.info().$("begin").$();
        // instantiate these paths so that they are not included in memory leak test
        Path.PATH.get();
        Path.PATH2.get();

        CharSequence root = temp.newFolder().getAbsolutePath();
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public boolean isOutOfOrderEnabled() {
                return true;
            }
        };

        engine = new CairoEngine(configuration);
        BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
        compiler = new SqlCompiler(engine);
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                        .with(
                                AllowAllCairoSecurityContext.INSTANCE,
                                bindVariableService,
                                null,
                                -1,
                                null);
        bindVariableService.clear();

        SharedRandom.RANDOM.set(new Rnd());
    }

    @After
    public void after() {
        LOG.info().$("finished").$();
        Misc.free(compiler);
        Misc.free(engine);
    }

    private void executeVanilla(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                OutOfOrderUtils.initBuf();
                code.run();
                engine.releaseInactive();
                Assert.assertEquals(0, engine.getBusyWriterCount());
                Assert.assertEquals(0, engine.getBusyReaderCount());
            } finally {
                engine.clear();
                OutOfOrderUtils.freeBuf();
            }
        });
    }
}
