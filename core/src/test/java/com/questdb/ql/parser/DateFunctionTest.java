package com.questdb.ql.parser;

import com.questdb.ex.ParserException;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DateFunctionTest extends AbstractOptimiserTest {

    @Test
    public void testCannotParse() throws Exception {
        assertThat("\n", "select toDate('2017', 'MM/y') from dual");
        assertThat("\n", "select TO_DATE('2017', 'MM/y') from dual");
    }

    @Test
    public void testDefaultLocale() throws Exception {
        assertThat("2017-03-01T00:00:00.000Z\n", "select toDate('03/2017', 'MM/y') from dual");
        assertThat("2017-03-01T00:00:00.000Z\n", "select TO_DATE('03/2017', 'MM/y') from dual");
    }

    @Test
    public void testParseWithLocale() throws Exception {
        assertThat("2017-04-01T00:00:00.000Z\n", "select toDate('Abril 2017', 'MMM y', 'es') from dual");
        assertThat("2017-04-01T00:00:00.000Z\n", "select TO_DATE('Abril 2017', 'MMM y', 'es') from dual");
    }

    @Test
    public void testWrongLocale() throws Exception {
        try {
            expectFailure("select toDate('Abril 2017', 'MMM y', 'wrong') from dual");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(37, QueryError.getPosition());
            TestUtils.assertEquals("Invalid locale", QueryError.getMessage());
        }
    }
}
