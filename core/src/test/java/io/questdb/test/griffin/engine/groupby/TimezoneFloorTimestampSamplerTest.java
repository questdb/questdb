/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.TimezoneFloorTimestampSampler;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Direct unit tests for {@link TimezoneFloorTimestampSampler}. The wrap is
 * exercised end-to-end through SAMPLE BY FILL queries in
 * {@link SampleByFillTest}, but several entry points on the
 * {@link TimestampSampler} interface are not reachable from the SQL fast
 * path:
 * <ul>
 *     <li>{@link TimestampSampler#previousTimestamp(long)} - never invoked
 *     by the fill cursor.</li>
 *     <li>{@link TimestampSampler#nextTimestamp(long, long)} - the fill
 *     cursor only walks one bucket at a time.</li>
 *     <li>{@link TimestampSampler#toSink} - the factory's {@code toPlan}
 *     prints the {@code stride} attribute from the parsed interval+unit
 *     directly, never delegating to the sampler.</li>
 *     <li>{@link TimestampSampler#setLocalAnchor(long)} vs
 *     {@link TimestampSampler#setStart(long)} - both anchor the grid in the
 *     SQL fast path, but the asymmetric translation
 *     ({@code setStart} converts UTC -> local, {@code setLocalAnchor}
 *     forwards as-is) is invariant of the offset value, so the SQL tests
 *     cannot distinguish them.</li>
 * </ul>
 * The SQL tests {@code testFillNullDstBerlin*} cover {@code nextTimestamp}
 * and {@code round} via 1d/1w strides; this suite extends coverage to 1M
 * and 1y strides plus the four entry points listed above.
 */
@RunWith(Parameterized.class)
public class TimezoneFloorTimestampSamplerTest {

    private final TestTimestampType timestampType;

    public TimezoneFloorTimestampSamplerTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testGettersDelegate() throws Exception {
        // Size getters and getTimestampType pass through to the wrapped
        // sampler unchanged. SimpleTimestampSampler is the only wrapped
        // sampler that implements getBucketSize, so use the day stride.
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(1, 'd', 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, "Europe/Berlin", 'd');
        Assert.assertEquals(local.getApproxBucketSize(), wrap.getApproxBucketSize());
        Assert.assertEquals(local.getBucketSize(), wrap.getBucketSize());
        Assert.assertEquals(local.getTimestampType(), wrap.getTimestampType());
    }

    @Test
    public void testNextTimestampDayBerlinFallBack() throws Exception {
        // CEST -> CET on 2024-10-27 03:00 local (01:00 UTC) stretches the
        // Oct 27 local-day bucket to 25 h in UTC. The wrap must walk the
        // local-calendar grid so consecutive buckets land at the right UTC
        // labels; a uniform-UTC 24 h sampler would drift one hour starting
        // at the Oct 28 boundary.
        assertWalk('d', 1, "Europe/Berlin",
                "2024-10-25T22:00:00.000000Z",
                new String[]{
                        "2024-10-26T22:00:00.000000Z",
                        "2024-10-27T23:00:00.000000Z",
                        "2024-10-28T23:00:00.000000Z",
                        "2024-10-29T23:00:00.000000Z",
                });
    }

    @Test
    public void testNextTimestampDayBerlinSpringForward() throws Exception {
        // CET -> CEST on 2024-03-31 02:00 local (01:00 UTC) shrinks the
        // Mar 31 local-day bucket to 23 h in UTC.
        assertWalk('d', 1, "Europe/Berlin",
                "2024-03-29T23:00:00.000000Z",
                new String[]{
                        "2024-03-30T23:00:00.000000Z",
                        "2024-03-31T22:00:00.000000Z",
                        "2024-04-01T22:00:00.000000Z",
                });
    }

    @Test
    public void testNextTimestampMonthBerlinAcrossDst() throws Exception {
        // Local-month grid: Mar starts in CET (+1h), Apr starts in CEST
        // (+2h) after spring-forward, Oct starts in CEST, Nov starts in
        // CET after fall-back. The wrap forwards each utcTimestamp + tzOff
        // to MonthTimestampMicrosSampler, which advances by stepMonths in
        // local space, then offsetFlooredUtcResult re-derives the UTC
        // label using the offset at the post-floor local instant.
        assertWalk('M', 1, "Europe/Berlin",
                "2023-12-31T23:00:00.000000Z",
                new String[]{
                        "2024-01-31T23:00:00.000000Z", // Feb 1 00:00 CET
                        "2024-02-29T23:00:00.000000Z", // Mar 1 00:00 CET (leap)
                        "2024-03-31T22:00:00.000000Z", // Apr 1 00:00 CEST
                        "2024-04-30T22:00:00.000000Z", // May 1 00:00 CEST
                        "2024-05-31T22:00:00.000000Z", // Jun 1 00:00 CEST
                        "2024-06-30T22:00:00.000000Z", // Jul 1 00:00 CEST
                        "2024-07-31T22:00:00.000000Z", // Aug 1 00:00 CEST
                        "2024-08-31T22:00:00.000000Z", // Sep 1 00:00 CEST
                        "2024-09-30T22:00:00.000000Z", // Oct 1 00:00 CEST
                        "2024-10-31T23:00:00.000000Z", // Nov 1 00:00 CET
                        "2024-11-30T23:00:00.000000Z", // Dec 1 00:00 CET
                        "2024-12-31T23:00:00.000000Z", // Jan 1 2025 00:00 CET
                });
    }

    @Test
    public void testNextTimestampMultiStepDayBerlin() throws Exception {
        // The fill cursor calls only the single-step nextTimestamp; the
        // (long, long numSteps) overload exists for parity with the
        // TimestampSampler interface and is never invoked through the
        // wrap by production code. Lock its semantics here against
        // accidental deletion or signature drift: an N-step jump must
        // equal N iterated single steps even when the path crosses DST.
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(1, 'd', 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, "Europe/Berlin", 'd');
        final long start = driver.parseFloorLiteral("2024-10-25T22:00:00.000000Z");

        long iterated = start;
        for (int i = 0; i < 4; i++) {
            iterated = wrap.nextTimestamp(iterated);
        }
        // Four single steps from Oct 25 22:00Z must hit Oct 29 23:00Z.
        Assert.assertEquals(driver.parseFloorLiteral("2024-10-29T23:00:00.000000Z"), iterated);

        final long multiStep = wrap.nextTimestamp(start, 4);
        Assert.assertEquals(iterated, multiStep);
    }

    @Test
    public void testNextTimestampYearBerlin() throws Exception {
        // Year stride: every Jan 1 00:00 local in Berlin is in CET, so
        // every label is one hour before midnight UTC. Confirms the
        // YearTimestamp*Sampler delegation walks the local-year grid.
        assertWalk('y', 1, "Europe/Berlin",
                "2023-12-31T23:00:00.000000Z",
                new String[]{
                        "2024-12-31T23:00:00.000000Z",
                        "2025-12-31T23:00:00.000000Z",
                        "2026-12-31T23:00:00.000000Z",
                });
    }

    @Test
    public void testPreviousTimestampDayBerlin() throws Exception {
        // Production never invokes previousTimestamp on the wrap (the
        // fill cursor only walks forward; SampleByInterpolateRecordCursorFactory
        // uses previousTimestamp but does not enter the fill fast path).
        // Lock the symmetry: previousTimestamp(nextTimestamp(t)) == t for
        // any t aligned to the local-day grid, including across both DST
        // transitions of 2024.
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(1, 'd', 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, "Europe/Berlin", 'd');

        final String[] anchors = {
                "2024-03-29T23:00:00.000000Z", // before spring-forward
                "2024-03-30T23:00:00.000000Z", // 23 h bucket follows
                "2024-03-31T22:00:00.000000Z", // first CEST midnight
                "2024-10-25T22:00:00.000000Z", // before fall-back
                "2024-10-26T22:00:00.000000Z", // 25 h bucket follows
                "2024-10-27T23:00:00.000000Z", // first CET midnight
        };
        for (String anchor : anchors) {
            final long t = driver.parseFloorLiteral(anchor);
            final long next = wrap.nextTimestamp(t);
            final long prev = wrap.previousTimestamp(next);
            Assert.assertEquals("previousTimestamp(nextTimestamp(" + anchor + "))", t, prev);
        }
    }

    @Test
    public void testRoundDayBerlin() throws Exception {
        // round() floors a UTC instant onto the local-day grid. Anchor the
        // wrapped sampler at local-grid 0 (= midnight local raw) via
        // setLocalAnchor; setStart(0) would translate UTC 0 -> local +1h
        // (CET in January) and skew the grid lines off midnight.
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(1, 'd', 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, "Europe/Berlin", 'd');
        wrap.setLocalAnchor(0);

        // CEST: Oct 26 12:00Z is local Oct 26 14:00; the local-day bucket
        // is [Oct 26 00:00 CEST, Oct 27 00:00 CEST) = [Oct 25 22:00Z,
        // Oct 26 22:00Z). round() returns the lower bound.
        Assert.assertEquals(
                driver.parseFloorLiteral("2024-10-25T22:00:00.000000Z"),
                wrap.round(driver.parseFloorLiteral("2024-10-26T12:00:00.000000Z"))
        );
        // CET (post fall-back): Oct 28 12:00Z is local Oct 28 13:00;
        // bucket [Oct 28 00:00 CET, Oct 29 00:00 CET) = [Oct 27 23:00Z,
        // Oct 28 23:00Z). round() returns Oct 27 23:00Z.
        Assert.assertEquals(
                driver.parseFloorLiteral("2024-10-27T23:00:00.000000Z"),
                wrap.round(driver.parseFloorLiteral("2024-10-28T12:00:00.000000Z"))
        );
        // Spring-forward: Mar 31 12:00Z is local Mar 31 14:00 CEST.
        // The bucket containing it is [Mar 31 00:00 local, Apr 1 00:00
        // local) = [Mar 30 23:00Z, Mar 31 22:00Z) -- 23 h wide because
        // the CET -> CEST jump at 02:00 CET removes one local hour. Mar
        // 31 00:00 local is in CET (the jump fires at 02:00, after 00:00),
        // so the bucket starts at Mar 30 23:00Z.
        Assert.assertEquals(
                driver.parseFloorLiteral("2024-03-30T23:00:00.000000Z"),
                wrap.round(driver.parseFloorLiteral("2024-03-31T12:00:00.000000Z"))
        );
    }

    @Test
    public void testSetLocalAnchorBypassesUtcConversion() throws Exception {
        // setLocalAnchor must NOT translate the input from UTC to local;
        // it is the entry point for callers that already hold a value in
        // the wrapped sampler's grid space (e.g. {@code from + offset} as
        // {@code timestamp_floor_utc} treats it).
        // <p>
        // Compare two SimpleTimestampSampler-backed wraps anchored with
        // the SAME numeric input, one via setStart (UTC) and one via
        // setLocalAnchor (raw local). When tzOff is non-zero, their
        // grids must diverge: setStart shifts the anchor up by tzOff,
        // setLocalAnchor leaves it as-is. round() of the same UTC input
        // observes the divergence because SimpleTimestampSampler.round
        // depends on start.
        final TimestampDriver driver = timestampType.getDriver();
        final long anchor = driver.parseFloorLiteral("2024-10-26T12:00:00.000000Z");

        // With setStart, the wrap puts localSampler.start at anchor + 2h
        // (Berlin CEST). round(anchor) round-trips to anchor.
        final TimezoneFloorTimestampSampler wA = wrap(driver, driver.getTimestampSampler(1, 'd', 0), "Europe/Berlin", 'd');
        wA.setStart(anchor);
        Assert.assertEquals(anchor, wA.round(anchor));

        // With setLocalAnchor, localSampler.start sits AT anchor (no
        // tzOff applied). round(anchor) computes:
        //   localFloored = (anchor + 2h - anchor) / 1d * 1d + anchor
        //   = 0 + anchor = anchor (in local-grid space)
        //   resultTzOff = getOffset(anchor - 2h) = +2h still (CEST)
        //   return anchor - 2h
        // So round(anchor) returns anchor - 2h, observably below anchor.
        final TimezoneFloorTimestampSampler wL = wrap(driver, driver.getTimestampSampler(1, 'd', 0), "Europe/Berlin", 'd');
        wL.setLocalAnchor(anchor);
        Assert.assertEquals(anchor - driver.fromHours(2), wL.round(anchor));
    }

    @Test
    public void testSetOffsetForwarded() throws Exception {
        // setOffset must reach the wrapped sampler untranslated. Use
        // SimpleTimestampSampler-backed day stride: its setOffset binds
        // start = offset, and round(value) computes (value - start) / bucket.
        // Setting offset then calling round verifies the offset arrived
        // without UTC->local conversion (the sampler operates entirely in
        // local-time space because all values handed to it have already
        // had tzOff applied).
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(1, 'd', 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, "Europe/Berlin", 'd');
        // Anchor at 03:00 local-grid (an offset that does NOT match a UTC
        // boundary). The wrap forwards 03:00 to the local sampler as-is.
        final long offset = driver.fromHours(3);
        wrap.setOffset(offset);

        // round of UTC Oct 26 03:00Z (local 05:00 CEST):
        //   localFloored = round_local(Oct 26 03:00Z + 2h = Oct 26 05:00 local)
        //     SimpleTimestampSampler.round with start=03:00:
        //     q = (05:00 - 03:00) / 1d = 0; return 03:00 + 0 = 03:00
        //   resultTzOff = getOffset(03:00 - 2h = Oct 26 01:00 UTC) = +2h
        //   return 03:00 - 2h = Oct 26 01:00 UTC
        Assert.assertEquals(
                driver.parseFloorLiteral("2024-10-26T01:00:00.000000Z"),
                wrap.round(driver.parseFloorLiteral("2024-10-26T03:00:00.000000Z"))
        );
    }

    @Test
    public void testToSink() throws Exception {
        // toSink delegates the wrapped sampler's signature wrapped in
        // TzAware(...). The factory's toPlan never invokes this method
        // (it builds the stride attribute from the parsed interval+unit
        // directly), but the Sinkable interface contract still applies.
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(1, 'd', 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, "Europe/Berlin", 'd');
        final StringSink sink = new StringSink();
        wrap.toSink(sink);
        TestUtils.assertEquals("TzAware(BaseTsSampler)", sink);
    }

    private static TimezoneFloorTimestampSampler wrap(
            TimestampDriver driver,
            TimestampSampler local,
            String tz,
            char unit
    ) throws NumericException {
        final TimeZoneRules rules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, tz.length())),
                driver.getTZRuleResolution()
        );
        return new TimezoneFloorTimestampSampler(local, rules, unit);
    }

    private void assertWalk(char unit, long stride, String tz, String startUtc, String[] expectedNextUtc)
            throws SqlException, NumericException {
        final TimestampDriver driver = timestampType.getDriver();
        final TimestampSampler local = driver.getTimestampSampler(stride, unit, 0);
        final TimezoneFloorTimestampSampler wrap = wrap(driver, local, tz, unit);

        long ts = driver.parseFloorLiteral(startUtc);
        for (String next : expectedNextUtc) {
            final long actual = wrap.nextTimestamp(ts);
            Assert.assertEquals("nextTimestamp expected " + next + ", from raw=" + ts, driver.parseFloorLiteral(next), actual);
            ts = actual;
        }
    }
}
