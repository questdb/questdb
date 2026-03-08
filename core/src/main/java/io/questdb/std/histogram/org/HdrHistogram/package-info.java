/* ******************************************************************************
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

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

// Version HdrHistogram-2.1.12, 2b35e19cb387625f2cbf420c9f12829b44e4fe94
// We have a few QuestDB-specific modifications, e.g.:
// * Removed all thread-safe histogram and removed atomic operations from AbstractHistogram
// * Replaced ArrayIndexOutOfBoundsException allocations with reusable CairoException where possible
// * Ported the tests to JUnit 4

/**
 * <h2>A High Dynamic Range (HDR) Histogram Package</h2>
 * <p>
 * An HdrHistogram histogram supports the recording and analyzing sampled data value counts across a configurable
 * integer value range with configurable value precision within the range. Value precision is expressed as the number
 * of significant digits in the value recording, and provides control over value quantization behavior across the
 * value range and the subsequent value resolution at any given level.
 * </p>
 * <p>
 * In contrast to traditional histograms that use linear, logarithmic, or arbitrary sized bins or buckets,
 * HdrHistograms use a fixed storage internal data representation that simultaneously supports an arbitrarily high
 * dynamic range and arbitrary precision throughout that dynamic range. This capability makes HdrHistograms extremely
 * useful for tracking and reporting on the distribution of percentile values with high resolution and across a wide
 * dynamic range -- a common need in latency behavior characterization.
 * </p>
 * <p>
 * The HdrHistogram package was specifically designed with latency and performance sensitive applications in mind.
 * Experimental u-benchmark measurements show value recording times as low as 3-6 nanoseconds on modern
 * (circa 2012) Intel CPUs. All Histogram variants can maintain a fixed cost in both space and time. When not
 * configured to auto-resize, a Histogram's memory footprint is constant, with no allocation operations involved in
 * recording data values or in iterating through them. The memory footprint is fixed regardless of the number of data
 * value samples recorded, and depends solely on the dynamic range and precision chosen. The amount of work involved in
 * recording a sample is constant, and directly computes storage index locations such that no iteration or searching
 * is ever involved in recording data values.
 * <p>
 * NOTE: Histograms can optionally be configured to auto-resize their dynamic range as a convenience feature.
 * When configured to auto-resize, recording operations that need to expand a histogram will auto-resize its
 * dynamic range to include recorded values as they are encountered. Note that recording calls that cause
 * auto-resizing may take longer to execute, and that resizing incurs allocation and copying of internal data
 * structures.
 * </p>
 * <p>
 * The combination of high dynamic range and precision is useful for collection and accurate post-recording
 * analysis of sampled value data distribution in various forms. Whether it's calculating or
 * plotting arbitrary percentiles, iterating through and summarizing values in various ways, or deriving mean and
 * standard deviation values, the fact that the recorded value count information is kept in high
 * resolution allows for accurate post-recording analysis with low [and ultimately configurable] loss in
 * accuracy when compared to performing the same analysis directly on the potentially infinite series of sourced
 * data values samples.
 * </p>
 * <p>
 * An HdrHistogram histogram is usually configured to maintain value count data with a resolution good enough
 * to support a desired precision in post-recording analysis and reporting on the collected data. Analysis can include
 * the computation and reporting of distribution by percentiles, linear or logarithmic arbitrary value buckets, mean
 * and standard deviation, as well as any other computations that can supported using the various iteration techniques
 * available on the collected value count data. In practice, a precision levels of 2 or 3 decimal points are most
 * commonly used, as they maintain a value accuracy of +/- ~1% or +/- ~0.1% respectively for derived distribution
 * statistics.
 * </p>
 * <p>
 * A good example of HdrHistogram use would be tracking of latencies across a wide dynamic range. E.g. from a
 * microsecond to an hour. A Histogram can be configured to track and later report on the counts of observed integer
 * usec-unit  latency values between 0 and 3,600,000,000 while maintaining a value precision of 3 significant digits
 * across that range. Such an example Histogram would simply be created with a
 * <b><code>highestTrackableValue</code></b> of 3,600,000,000, and a
 * <b><code>numberOfSignificantValueDigits</code></b> of 3, and would occupy a fixed, unchanging memory footprint
 * of around 185KB (see "Footprint estimation" below).
 * <br>
 * Code for this use example would include these basic elements:
 * <br>
 * <pre>
 * <code>
 * {@link io.questdb.std.histogram.org.HdrHistogram.Histogram} histogram = new {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}(3600000000L, 3);
 * .
 * .
 * .
 * // Repeatedly record measured latencies:
 * histogram.{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValue(long) recordValue}(latency);
 * .
 * .
 * .
 * // Report histogram percentiles, expressed in msec units:
 * histogram.{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#outputPercentileDistribution(java.io.PrintStream, Double) outputPercentileDistribution}(histogramLog, 1000.0)};
 * </code>
 * </pre>
 * Specifying 3 decimal points of precision in this example guarantees that value quantization within the value range
 * will be no larger than 1/1,000th (or 0.1%) of any recorded value. This example Histogram can be therefor used to
 * track, analyze and report the counts of observed latencies ranging between 1 microsecond and 1 hour in magnitude,
 * while maintaining a value resolution 1 microsecond (or better) up to 1 millisecond, a resolution of 1 millisecond
 * (or better) up to one second, and a resolution of 1 second (or better) up to 1,000 seconds. At it's maximum tracked
 * value (1 hour), it would still maintain a resolution of 3.6 seconds (or better).
 * <h2>Histogram variants and internal representation</h2>
 * The HdrHistogram package includes multiple implementations of the {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram} class:
 * <ul>
 *  <li> {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}, which is the commonly used Histogram form and tracks value counts
 * in <b><code>long</code></b> fields. </li>
 *  <li>{@link io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram} and {@link io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram}, which track value counts
 * in <b><code>int</code></b> and
 * <b><code>short</code></b> fields respectively, are provided for use cases where smaller count ranges are practical
 * and smaller overall storage is beneficial (e.g. systems where tens of thousands of in-memory histogram are
 * being tracked).</li>
 * </ul>
 * <p>
 * Internally, data in HdrHistogram variants is maintained using a concept somewhat similar to that of floating
 * point number representation: Using a an exponent a (non-normalized) mantissa to
 * support a wide dynamic range at a high but varying (by exponent value) resolution.
 * AbstractHistogram uses exponentially increasing bucket value ranges (the parallel of
 * the exponent portion of a floating point number) with each bucket containing
 * a fixed number (per bucket) set of linear sub-buckets (the parallel of a non-normalized mantissa portion
 * of a floating point number).
 * Both dynamic range and resolution are configurable, with <b><code>highestTrackableValue</code></b>
 * controlling dynamic range, and <b><code>numberOfSignificantValueDigits</code></b> controlling
 * resolution.
 * </p>
 * <h2>Iteration</h2>
 * Histograms supports multiple convenient forms of iterating through the histogram data set, including linear,
 * logarithmic, and percentile iteration mechanisms, as well as means for iterating through each recorded value or
 * each possible value level. The iteration mechanisms all provide {@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue}
 * data points along the histogram's iterated data set, and are available via the following methods:
 * <ul>
 *     <li>{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#percentiles percentiles} :
 *     An {@link java.lang.Iterable}{@literal <}{@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue}{@literal >} through the
 *     histogram using a {@link io.questdb.std.histogram.org.HdrHistogram.PercentileIterator} </li>
 *     <li>{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#linearBucketValues linearBucketValues} :
 *     An {@link java.lang.Iterable}{@literal <}{@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue}{@literal >} through
 *     the histogram using a {@link io.questdb.std.histogram.org.HdrHistogram.LinearIterator} </li>
 *     <li>{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#logarithmicBucketValues logarithmicBucketValues} :
 *     An {@link java.lang.Iterable}{@literal <}{@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue}{@literal >}
 *     through the histogram using a {@link io.questdb.std.histogram.org.HdrHistogram.LogarithmicIterator} </li>
 *     <li>{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordedValues recordedValues} :
 *     An {@link java.lang.Iterable}{@literal <}{@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue}{@literal >} through
 *     the histogram using a {@link io.questdb.std.histogram.org.HdrHistogram.RecordedValuesIterator} </li>
 *     <li>{@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#allValues allValues} :
 *     An {@link java.lang.Iterable}{@literal <}{@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue}{@literal >} through
 *     the histogram using a {@link io.questdb.std.histogram.org.HdrHistogram.AllValuesIterator} </li>
 * </ul>
 * <p>
 * Iteration is typically done with a for-each loop statement. E.g.:
 * <br><pre><code>
 * for (HistogramIterationValue v : histogram.percentiles(<i>percentileTicksPerHalfDistance</i>)) {
 *     ...
 * }
 * </code></pre>
 * or
 * <br><pre><code>
 * for (HistogramIterationValue v : histogram.linearBucketValues(<i>valueUnitsPerBucket</i>)) {
 *     ...
 * }
 * </code>
 * </pre>
 * The iterators associated with each iteration method are resettable, such that a caller that would like to avoid
 * allocating a new iterator object for each iteration loop can re-use an iterator to repeatedly iterate through the
 * histogram. This iterator re-use usually takes the form of a traditional for loop using the Iterator's
 * <b><code>hasNext()</code></b> and <b><code>next()</code></b> methods:
 * <p>
 * to avoid allocating a new iterator object for each iteration loop:
 * <br>
 * <pre>
 * <code>
 * PercentileIterator iter = histogram.percentiles().iterator(<i>percentileTicksPerHalfDistance</i>);
 * ...
 * iter.reset(<i>percentileTicksPerHalfDistance</i>);
 * for (iter.hasNext() {
 *     HistogramIterationValue v = iter.next();
 *     ...
 * }
 * </code>
 * </pre>
 * <h3>Equivalent Values and value ranges</h3>
 * <p>
 * Due to the finite (and configurable) resolution of the histogram, multiple adjacent integer data values can
 * be "equivalent". Two values are considered "equivalent" if samples recorded for both are always counted in a
 * common total count due to the histogram's resolution level. Histogram provides methods for determining the
 * lowest and highest equivalent values for any given value, as we as determining whether two values are equivalent,
 * and for finding the next non-equivalent value for a given value (useful when looping through values, in order
 * to avoid double-counting count).
 * </p>
 * <h3>Raw vs. corrected recording</h3>
 * <p>
 * Regular, raw value data recording into an HdrHistogram is achieved with the
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValue(long) recordValue()} method.
 * <p>
 * Histogram variants also provide an auto-correcting
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval()}
 * form in support of a common use case found when histogram values are used to track response time
 * distribution in the presence of Coordinated Omission - an extremely common phenomenon found in latency recording
 * systems.
 * This correcting form is useful in [e.g. load generator] scenarios where measured response times may exceed the
 * expected interval between issuing requests, leading to the "omission" of response time measurements that would
 * typically correlate with "bad" results. This coordinated (non random) omission of source data, if left uncorrected,
 * will then dramatically skew any overall latency stats computed on the recorded information, as the recorded data set
 * itself will be significantly skewed towards good results.
 * </p>
 * <p>
 * When a value recorded in the histogram exceeds the
 * <b><code>expectedIntervalBetweenValueSamples</code></b> parameter, recorded histogram data will
 * reflect an appropriate number of additional values, linearly decreasing in steps of
 * <b><code>expectedIntervalBetweenValueSamples</code></b>, down to the last value
 * that would still be higher than <b><code>expectedIntervalBetweenValueSamples</code></b>).
 * </p>
 * <p>
 * To illustrate why this corrective behavior is critically needed in order to accurately represent value
 * distribution when large value measurements may lead to missed samples, imagine a system for which response
 * times samples are taken once every 10 msec to characterize response time distribution.
 * The hypothetical system behaves "perfectly" for 100 seconds (10,000 recorded samples), with each sample
 * showing a 1msec response time value. At each sample for 100 seconds (10,000 logged samples
 * at 1msec each). The hypothetical system then encounters a 100 sec pause during which only a single sample is
 * recorded (with a 100 second value).
 * An normally recorded (uncorrected) data histogram collected for such a hypothetical system (over the 200 second
 * scenario above) would show ~99.99% of results at 1msec or below, which is obviously "not right". In contrast, a
 * histogram that records the same data using the auto-correcting
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval()}
 * method with the knowledge of an expectedIntervalBetweenValueSamples of 10msec will correctly represent the
 * real world response time distribution of this hypothetical system. Only ~50% of results will be at 1msec or below,
 * with the remaining 50% coming from the auto-generated value records covering the missing increments spread between
 * 10msec and 100 sec.
 * </p>
 * <p>
 * Data sets recorded with and with
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValue(long) recordValue()}
 * and with
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval()}
 * will differ only if at least one value recorded was greater than it's
 * associated <b><code>expectedIntervalBetweenValueSamples</code></b> parameter.
 * Data sets recorded with
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval()}
 * parameter will be identical to ones recorded with
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#recordValue(long) recordValue()}
 * it if all values recorded via the <b><code>recordValue</code></b> calls were smaller
 * than their associated <b><code>expectedIntervalBetweenValueSamples</code></b> parameters.
 * </p>
 * <p>
 * In addition to at-recording-time correction option, Histrogram variants also provide the post-recording correction
 * methods
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#copyCorrectedForCoordinatedOmission(long) copyCorrectedForCoordinatedOmission()}
 * and
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#addWhileCorrectingForCoordinatedOmission(AbstractHistogram, long) addWhileCorrectingForCoordinatedOmission()}.
 * These methods can be used for post-recording correction, and are useful when the
 * <b><code>expectedIntervalBetweenValueSamples</code></b> parameter is estimated to be the same for all recorded
 * values. However, for obvious reasons, it is important to note that only one correction method (during or post
 * recording) should be be used on a given histogram data set.
 * </p>
 * <p>
 * When used for response time characterization, the recording with the optional
 * <code><b>expectedIntervalBetweenValueSamples</b></code> parameter will tend to produce data sets that would
 * much more accurately reflect the response time distribution that a random, uncoordinated request would have
 * experienced.
 * </p>
 * <h3>Floating point values and DoubleHistogram variants</h3>
 * The above discussion relates to integer value histograms (the various subclasses of
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram} and their related supporting classes). HdrHistogram supports floating
 * point value recording and reporting with a similar set of classes, including the
 * {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram} histogram classes. Support for floating point value
 * iteration is provided with {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogramIterationValue} and related iterator classes (
 * {@link io.questdb.std.histogram.org.HdrHistogram.DoubleLinearIterator}, {@link io.questdb.std.histogram.org.HdrHistogram.DoubleLogarithmicIterator},
 * {@link io.questdb.std.histogram.org.HdrHistogram.DoublePercentileIterator}, {@link io.questdb.std.histogram.org.HdrHistogram.DoubleRecordedValuesIterator},
 * {@link io.questdb.std.histogram.org.HdrHistogram.DoubleAllValuesIterator}).
 * <h4>Auto-ranging in floating point histograms</h4>
 * Unlike integer value based histograms, the specific value range tracked by a {@link
 * io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram} (and variants) is not specified upfront. Only the dynamic range of values
 * that the histogram can cover is (optionally) specified. E.g. When a {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram}
 * is created to track a dynamic range of 3600000000000 (enough to track values from a nanosecond to an hour),
 * values could be recorded into into it in any consistent unit of time as long as the ratio between the highest
 * and lowest non-zero values stays within the specified dynamic range, so recording in units of nanoseconds
 * (1.0 thru 3600000000000.0), milliseconds (0.000001 thru 3600000.0) seconds (0.000000001 thru 3600.0), hours
 * (1/3.6E12 thru 1.0) will all work just as well.
 * <h3>Footprint estimation</h3>
 * Due to it's dynamic range representation, Histogram is relatively efficient in memory space requirements given
 * the accuracy and dynamic range it covers. Still, it is useful to be able to estimate the memory footprint involved
 * for a given <b><code>highestTrackableValue</code></b> and <b><code>numberOfSignificantValueDigits</code></b>
 * combination. Beyond a relatively small fixed-size footprint used for internal fields and stats (which can be
 * estimated as "fixed at well less than 1KB"), the bulk of a Histogram's storage is taken up by it's data value
 * recording counts array. The total footprint can be conservatively estimated by:
 * <pre><code>
 *     largestValueWithSingleUnitResolution = 2 * (10 ^ numberOfSignificantValueDigits);
 *     subBucketSize = roundedUpToNearestPowerOf2(largestValueWithSingleUnitResolution);
 *
 *     expectedHistogramFootprintInBytes = 512 +
 *          ({primitive type size} / 2) *
 *          (log2RoundedUp((highestTrackableValue) / subBucketSize) + 2) *
 *          subBucketSize
 *
 * </code></pre>
 * A conservative (high) estimate of a Histogram's footprint in bytes is available via the
 * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#getEstimatedFootprintInBytes() getEstimatedFootprintInBytes()} method.
 */

package io.questdb.std.histogram.org.HdrHistogram;
