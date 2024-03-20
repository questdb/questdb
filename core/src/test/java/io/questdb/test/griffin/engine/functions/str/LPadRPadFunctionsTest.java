package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.SqlException;
import org.junit.Test;
import io.questdb.test.AbstractCairoTest;

public class LPadRPadFunctionsTest extends AbstractCairoTest {
    @Test
    public void testAscii() throws SqlException {
        ddl("create table x as (select rnd_varchar('abc','def','ghi') vc from long_sequence(5))");
        assertSql(
                "lpad\trpad\n" +
                "  abc\tabc  \n" +
                "  abc\tabc  \n" +
                "  def\tdef  \n" +
                "  ghi\tghi  \n" +
                "  ghi\tghi  \n",
                "select lpad(vc, 5), rpad(vc, 5) from x order by vc"
        );
    }
    @Test
    public void testAsciiFill() throws SqlException {
        ddl("create table x as (select rnd_varchar('abc','def','ghi') vc from long_sequence(5))");
        assertSql(
                "lpad\trpad\n" +
                "..abc\tabc..\n" +
                "..abc\tabc..\n" +
                "..def\tdef..\n" +
                "..ghi\tghi..\n" +
                "..ghi\tghi..\n",
                "select lpad(vc, 5, '.'), rpad(vc, 5, '.') from x order by vc"
        );
    }

    @Test
    public void testUtf8() throws SqlException {
        ddl("create table x as (select rnd_varchar('ганьба','слава','добрий','вечір') vc from long_sequence(6))");
        assertSql(
                "lpad\trpad\n" +
                "     вечір\tвечір     \n" +
                "     вечір\tвечір     \n" +
                "    ганьба\tганьба    \n" +
                "    добрий\tдобрий    \n" +
                "     слава\tслава     \n" +
                "     слава\tслава     \n",
                "select lpad(vc, 10), rpad(vc, 10) from x order by vc"
        );
    }

    @Test
    public void testUtf8Fill() throws SqlException {
        ddl("create table x as (select rnd_varchar('ганьба','слава','добрий','вечір') vc from long_sequence(6))");
        assertSql(
                "lpad\trpad\n" +
                ".....вечір\tвечір.....\n" +
                ".....вечір\tвечір.....\n" +
                "....ганьба\tганьба....\n" +
                "....добрий\tдобрий....\n" +
                ".....слава\tслава.....\n" +
                ".....слава\tслава.....\n",
                "select lpad(vc, 10, '.'), rpad(vc, 10, '.') from x order by vc"
        );
    }

    @Test
    public void testUtf8Random() throws SqlException {
        ddl("create table x as (select rnd_varchar(1, 40, 0) vc1, rnd_varchar(1, 4, 0) vc2 from long_sequence(100))");
        assertSql(
                "count\n" +
                "100\n",
                "select count (*) from (select length(lpad(vc1, 20)) ll, length(rpad(vc2, 20)) lr from x order by vc1, vc2) where ll = 20 and lr = 20"
        );
    }

    @Test
    public void testUtf8RandomFill() throws SqlException {
        ddl("create table x as (select rnd_varchar(1, 40, 0) vc1, rnd_varchar(1, 4, 0) vc2 from long_sequence(100))");
        assertSql(
                "count\n" +
                "100\n",
                "select count (*) from (select length(lpad(vc1, 20, '.')) ll, length(rpad(vc2, 20, '.')) lr from x order by vc1, vc2) where ll = 20 and lr = 20"
        );
    }

    @Test
    public void testPadNulls() throws SqlException {
        ddl("create table x as (select null::varchar as vc from long_sequence(10))");
        assertSql(
                "count\n10\n",
                "select count (*) from (select lpad(vc, 20, '.'), rpad(vc, 20, '.') from x order by vc) where lpad = null and rpad = null"
        );
    }
}
