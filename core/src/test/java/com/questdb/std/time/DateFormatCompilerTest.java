package com.questdb.std.time;

import com.questdb.ex.NumericException;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntHashSet;
import com.questdb.std.time.*;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateFormatCompilerTest {

    private static final DateFormatCompiler compiler = new DateFormatCompiler();

    @BeforeClass
    public static void setUp() throws Exception {
        DateFormatImpl.updateReferenceYear(Dates.toMillis(1997, 1, 1, 0, 0));
    }

    @Test(expected = NumericException.class)
    public void testBadAmPm() throws Exception {
        assertThat("KaMMy", "", "11 0910 am");
    }

    @Test(expected = NumericException.class)
    public void testBadAmPm2() throws Exception {
        assertThat("KaMMy", "", "11az0910");
    }

    @Test(expected = NumericException.class)
    public void testBadDelimiter() throws Exception {
        assertThat("y.MM", "0001-03-01T00:00:00.000Z", "1-03");
    }

    @Test(expected = NumericException.class)
    public void testBadMonth() throws Exception {
        assertThat("yMM", "0001-03-01T00:00:00.000Z", "133");
    }

    @Test
    public void testDayGreedy() throws Exception {
        assertThat("d, MM-yy", "2011-10-03T00:00:00.000Z", "3, 10-11");
        assertThat("d, MM-yy", "2011-10-03T00:00:00.000Z", "03, 10-11");
        assertThat("d, MM-yy", "2011-10-25T00:00:00.000Z", "25, 10-11");
    }

    @Test
    public void testDayMonthYear() throws Exception {
        assertThat("dd-MM-yyyy", "2010-03-10T00:00:00.000Z", "10-03-2010");
    }

    @Test
    public void testDayMonthYearNoDelim() throws Exception {
        assertThat("yyyyddMM", "2010-03-10T00:00:00.000Z", "20101003");
    }

    @Test
    public void testDayOneDigit() throws Exception {
        assertThat("dyyyy", "2014-01-03T00:00:00.000Z", "32014");
    }

    @Test
    public void testEra() throws Exception {
        assertThat("E, dd-MM-yyyy G", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 AD");
        assertThat("E, dd-MM-yyyy G", "-2013-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 BC");
    }

    @Test
    public void testGreedyYear() throws Exception {
        assertThat("y-MM", "1564-03-01T00:00:00.000Z", "1564-03");
        assertThat("y-MM", "1936-03-01T00:00:00.000Z", "36-03");
        assertThat("y-MM", "2015-03-01T00:00:00.000Z", "15-03");
        assertThat("y-MM", "0137-03-01T00:00:00.000Z", "137-03");
    }

    @Test
    public void testGreedyYear2() throws Exception {
        long referenceYear = DateFormatImpl.getReferenceYear();
        try {
            DateFormatImpl.updateReferenceYear(Dates.toMillis(2015, 1, 20, 0, 0));
            assertThat("y-MM", "1564-03-01T00:00:00.000Z", "1564-03");
            assertThat("y-MM", "2006-03-01T00:00:00.000Z", "06-03");
            assertThat("y-MM", "1955-03-01T00:00:00.000Z", "55-03");
            assertThat("y-MM", "0137-03-01T00:00:00.000Z", "137-03");
        } finally {
            DateFormatImpl.updateReferenceYear(referenceYear);
        }
    }

    @Test(expected = NumericException.class)
    public void testHour12BadAM() throws Exception {
        assertThat("K MMy a", "2010-09-01T04:00:00.000Z", "13 0910 am");
    }

    @Test(expected = NumericException.class)
    public void testHour12BadPM() throws Exception {
        assertThat("K MMy a", "2010-09-01T04:00:00.000Z", "13 0910 pm");
    }

    @Test
    public void testHour12Greedy() throws Exception {
        assertThat("K MMy a", "2010-09-01T23:00:00.000Z", "11 0910 pm");
        assertThat("KaMMy", "2010-09-01T23:00:00.000Z", "11pm0910");
    }

    @Test
    public void testHour12GreedyOneBased() throws Exception {
        assertThat("h MMy a", "2010-09-01T22:00:00.000Z", "11 0910 pm");
        assertThat("haMMy", "2010-09-01T22:00:00.000Z", "11pm0910");
    }

    @Test
    public void testHour12OneDigit() throws Exception {
        assertThat("KMMy a", "2010-09-01T04:00:00.000Z", "40910 am");
        assertThat("KMMy a", "2010-09-01T16:00:00.000Z", "40910 pm");
    }

    @Test
    public void testHour12OneDigitDefaultAM() throws Exception {
        assertThat("KMMy", "2010-09-01T04:00:00.000Z", "40910");
    }

    @Test
    public void testHour12OneDigitOneBased() throws Exception {
        assertThat("hMMy a", "2010-09-01T03:00:00.000Z", "40910 am");
        assertThat("hMMy a", "2010-09-01T15:00:00.000Z", "40910 pm");
    }

    @Test
    public void testHour12TwoDigits() throws Exception {
        assertThat("KKMMy a", "2010-09-01T04:00:00.000Z", "040910 am");
        assertThat("KKMMy a", "2010-09-01T23:00:00.000Z", "110910 pm");
    }

    @Test
    public void testHour12TwoDigitsOneBased() throws Exception {
        assertThat("hhMMy a", "2010-09-01T03:00:00.000Z", "040910 am");
        assertThat("hhMMy a", "2010-09-01T22:00:00.000Z", "110910 pm");
    }

    @Test
    public void testHour24Greedy() throws Exception {
        assertThat("H, dd-MM", "1970-11-04T03:00:00.000Z", "3, 04-11");
        assertThat("H, dd-MM", "1970-11-04T19:00:00.000Z", "19, 04-11");

        assertThat("k, dd-MM", "1970-11-04T02:00:00.000Z", "3, 04-11");
        assertThat("k, dd-MM", "1970-11-04T18:00:00.000Z", "19, 04-11");
    }

    @Test
    public void testHour24OneDigit() throws Exception {
        assertThat("HMMy", "2010-09-01T04:00:00.000Z", "40910");
        assertThat("kMMy", "2010-09-01T03:00:00.000Z", "40910");
    }

    @Test
    public void testHour24TwoDigits() throws Exception {
        assertThat("HHMMy", "2010-09-01T04:00:00.000Z", "040910");
        assertThat("HHMMy", "2010-09-01T23:00:00.000Z", "230910");

        assertThat("kkMMy", "2010-09-01T03:00:00.000Z", "040910");
        assertThat("kkMMy", "2010-09-01T22:00:00.000Z", "230910");
    }

    @Test
    public void testLeapYear() throws Exception {
        assertThat("dd-MM-yyyy", "2016-02-29T00:00:00.000Z", "29-02-2016");
    }

    @Test(expected = NumericException.class)
    public void testLeapYearFailure() throws Exception {
        assertThat("dd-MM-yyyy", "", "29-02-2015");
    }

    @Test
    public void testMillisGreedy() throws Exception {
        assertThat("ddMMy HH:mm:ss.S", "1978-03-19T21:20:45.678Z", "190378 21:20:45.678");
    }

    @Test(expected = NumericException.class)
    public void testMillisGreedyShort() throws Exception {
        assertThat("ddMMy HH:mm:ss.SSS", "1978-03-19T21:20:45.678Z", "190378 21:20:45.");
    }

    @Test
    public void testMillisOneDigit() throws Exception {
        assertThat("mmsSHH MMy", "2010-09-01T13:55:03.002Z", "553213 0910");
    }

    @Test
    public void testMillisThreeDigits() throws Exception {
        assertThat("ddMMy HH:mm:ss.SSS", "1978-03-19T21:20:45.678Z", "190378 21:20:45.678");
    }

    @Test
    public void testMinuteGreedy() throws Exception {
        assertThat("dd-MM-yy HH:m", "2010-09-03T14:54:00.000Z", "03-09-10 14:54");
    }

    @Test
    public void testMinuteOneDigit() throws Exception {
        assertThat("mHH MMy", "2010-09-01T13:05:00.000Z", "513 0910");
    }

    @Test
    public void testMinuteTwoDigits() throws Exception {
        assertThat("mm:HH MMy", "2010-09-01T13:45:00.000Z", "45:13 0910");
    }

    @Test
    public void testMonthGreedy() throws Exception {
        assertThat("M-y", "2012-11-01T00:00:00.000Z", "11-12");
        assertThat("M-y", "2012-02-01T00:00:00.000Z", "2-12");
    }

    @Test
    public void testMonthName() throws Exception {
        assertThat("dd-MMM-y", "2012-11-15T00:00:00.000Z", "15-NOV-12");
        assertThat("dd MMMM yyy", "2013-09-18T00:00:00.000Z", "18 September 2013");
    }

    @Test
    public void testMonthOneDigit() throws Exception {
        assertThat("My", "2010-04-01T00:00:00.000Z", "410");
    }

    @Test
    public void testOperationUniqueness() throws Exception {

        Assert.assertTrue(DateFormatCompiler.opList.size() > 0);

        IntHashSet codeSet = new IntHashSet();
        CharSequenceHashSet nameSet = new CharSequenceHashSet();
        for (int i = 0, n = DateFormatCompiler.opList.size(); i < n; i++) {
            String name = DateFormatCompiler.opList.getQuick(i);
            int code = DateFormatCompiler.opMap.get(name);
            Assert.assertTrue(codeSet.add(code));
            Assert.assertTrue(nameSet.add(name));
        }
    }

    @Test
    public void testSecondGreedy() throws Exception {
        assertThat("ddMMy HH:mm:s", "1978-03-19T21:20:45.000Z", "190378 21:20:45");
    }

    @Test
    public void testSecondOneDigit() throws Exception {
        assertThat("mmsHH MMy", "2010-09-01T13:55:03.000Z", "55313 0910");
    }

    @Test
    public void testSecondTwoDigits() throws Exception {
        assertThat("ddMMy HH:mm:ss", "1978-03-19T21:20:45.000Z", "190378 21:20:45");
    }

    @Test
    public void testSingleDigitYear() throws Exception {
        assertThat("yMM", "0001-03-01T00:00:00.000Z", "103");
    }

    @Test
    public void testTimeZone1() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T18:54:00.000Z", "03-09-10 14:54 EST");
    }

    @Test
    public void testTimeZone2() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T18:50:00.000Z", "03-09-10 21:50 MSK");
    }

    @Test
    public void testTimeZone3() throws Exception {
        assertThat("dd-MM-yy HH:m z", "2010-09-03T20:50:00.000Z", "03-09-10 21:50 BST");
    }

    @Test
    public void testTimeZone4() throws Exception {
        DateFormat format = compiler.create("dd-MM-yy HH:m z");
        TestUtils.assertEquals("2010-09-03T21:01:00.000Z", Dates.toString(format.parse("03-09-10 23:01 Hora de verano de Sudáfrica", DateLocale.LOCALES.get("es-PA"))));
    }

    @Test
    public void testTimeZone5() throws Exception {
        DateFormat format = compiler.create("dd-MM-yy HH:m [z]");
        TestUtils.assertEquals("2010-09-03T21:01:00.000Z", Dates.toString(format.parse("03-09-10 23:01 [Hora de verano de Sudáfrica]", DateLocale.LOCALES.get("es-PA"))));
    }

    @Test(expected = NumericException.class)
    public void testTooLongInput() throws Exception {
        assertThat("E, dd-MM-yyyy G", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014 ADD");
    }

    @Test
    public void testTwoDigitYear() throws Exception {
        assertThat("MMyy", "2010-11-01T00:00:00.000Z", "1110");
        assertThat("MM, yy", "2010-11-01T00:00:00.000Z", "11, 10");
    }

    @Test
    public void testWeekdayDigit() throws Exception {
        assertThat("u, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "5, 03-04-2014");
    }

    @Test(expected = NumericException.class)
    public void testWeekdayIncomplete() throws Exception {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Tu, 03-04-2014");
    }

    @Test(expected = NumericException.class)
    public void testWeekdayIncomplete2() throws Exception {
        assertThat("dd-MM-yyyy, E", "2014-04-03T00:00:00.000Z", "03-04-2014, Fr");
    }

    @Test
    public void testWeekdayLong() throws Exception {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014");
        assertThat("EE, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Tuesday, 03-04-2014");
    }

    @Test
    public void testWeekdayShort() throws Exception {
        assertThat("E, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Fri, 03-04-2014");
        assertThat("EE, dd-MM-yyyy", "2014-04-03T00:00:00.000Z", "Fri, 03-04-2014");
    }

    private void assertThat(String pattern, String expected, String input) throws NumericException {
        DateFormat format = compiler.create(pattern);
        TestUtils.assertEquals(expected, Dates.toString(format.parse(input, DateLocale.LOCALES.get("en-GB"))));
    }
}