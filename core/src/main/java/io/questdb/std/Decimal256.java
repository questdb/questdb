package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Decimal256 - A mutable decimal number implementation.
 * <p>
 * This class represents decimal numbers with a fixed scale (number of decimal places)
 * using 256-bit integer arithmetic for precise calculations. All operations are
 * performed in-place to eliminate object allocation and improve performance.
 * </p>
 * <p>
 * This type tries to but doesn't follow IEEE 754; one of the main goals is using 256 bits to store
 * the sign and trailing significant field (T).
 * Using 1 bit for the sign, we have 255 bits for T, which gives us 77 digits of precision.
 * </p>
 */
public class Decimal256 implements Sinkable {
    /**
     * Maximum allowed scale (number of decimal places)
     */
    public static final int MAX_SCALE = 76;
    public static final Decimal256 MAX_VALUE = new Decimal256(Long.MAX_VALUE, -1, -1, -1, 0);
    public static final Decimal256 MIN_VALUE = new Decimal256(Long.MIN_VALUE, -1, -1, -1, 0);
    private static final long[] POWERS_TEN_TABLE_HH = new long[]{ // from 10⁵⁸ to 10⁷⁶
            1L, 15L,
            159L, 1593L, 15930L, 159309L, 1593091L,
            15930919L, 159309191L, 1593091911L, 15930919111L, 159309191113L,
            1593091911132L, 15930919111324L, 159309191113245L, 1593091911132452L, 15930919111324522L,
            159309191113245227L, 1593091911132452277L,
    };
    private static final long[] POWERS_TEN_TABLE_HL = new long[]{ // from 10³⁹ to 10⁷⁶
            2L,
            29L, 293L, 2938L, 29387L, 293873L,
            2938735L, 29387358L, 293873587L, 2938735877L, 29387358770L,
            293873587705L, 2938735877055L, 29387358770557L, 293873587705571L, 2938735877055718L,
            29387358770557187L, 293873587705571876L, 2938735877055718769L, -7506129376861915533L, -1274317473780948864L,
            5703569335900062977L, 1695461137871974930L, -1492132694989802312L, 3525417123811528497L, -1639316909303818259L,
            2053574980671369030L, 2089005733004138687L, 2443313256331835254L, 5986388489608800929L, 4523652674959354447L,
            8343038602174441244L, -8803334346803345639L, 4200376900514301694L, 5110280857723913709L, -4237423643889517749L,
            -5480748291476074254L, 532749306367912313L,
    };
    private static final long[] POWERS_TEN_TABLE_LH = new long[]{ // from 10²⁰ to 10⁷⁶
            5L, 54L, 542L, 5421L, 54210L,
            542101L, 5421010L, 54210108L, 542101086L, 5421010862L,
            54210108624L, 542101086242L, 5421010862427L, 54210108624275L, 542101086242752L,
            5421010862427522L, 54210108624275221L, 542101086242752217L, 5421010862427522170L, -1130123596853433148L,
            7145508105175220139L, -2331895243086005067L, -4872208357150499052L, 6618148649623664334L, -7605489798601563120L,
            -2267921691177424736L, -4232472838064695744L, -5431240233227854204L, 1027829888850112811L, -8168445185208423502L,
            -7897475557246028547L, -5187779277622078999L, 3462439444907864858L, -2269093698340454644L, -4244192909694994819L,
            -5548440949530844953L, -144177274179794675L, -1441772741797946749L, 4029016655730084128L, 3396678409881738056L,
            -2926704048601722663L, 7626447661401876602L, 2477500319180559562L, 6328259118096044006L, 7942358959831785217L,
            5636613303479645706L, 1025900813667802212L, -8187735937031529496L, -8090383075477088496L, -7116854459932678496L,
            2618431695511421504L, 7737572881404663424L, 3588752519208427776L, -1005962955334825472L, 8387114520361296896L,
            -8362575164934789120L, 8607968719199866880L,
    };
    private static final long[] POWERS_TEN_TABLE_LL = new long[]{ // from 10⁰ to 10⁷⁶
            1L, 10L, 100L, 1000L, 10000L,
            100000L, 1000000L, 10000000L, 100000000L, 1000000000L,
            10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L,
            1000000000000000L, 10000000000000000L, 100000000000000000L, 1000000000000000000L, -8446744073709551616L,
            7766279631452241920L, 3875820019684212736L, 1864712049423024128L, 200376420520689664L, 2003764205206896640L,
            1590897978359414784L, -2537764290115403776L, -6930898827444486144L, 4477988020393345024L, 7886392056514347008L,
            5076944270305263616L, -4570789518076018688L, -8814407033341083648L, 4089650035136921600L, 4003012203950112768L,
            3136633892082024448L, -5527149226598858752L, 68739955140067328L, 687399551400673280L, 6873995514006732800L,
            -5047021154770878464L, 4870020673419870208L, -6640025486929952768L, 7386721425538678784L, 80237960548581376L,
            802379605485813760L, 8023796054858137600L, 6450984253743169536L, 9169610316303040512L, -537617205517352960L,
            -5376172055173529600L, 1578511669393358848L, -2661627379775963136L, -8169529724050079744L, -7908320945662590976L,
            -5296233161787703296L, 2377900603251621888L, 5332261958806667264L, -2017612633061982208L, -1729382256910270464L,
            1152921504606846976L, -6917529027641081856L, 4611686018427387904L, -9223372036854775808L, 0L,
            0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
    };
    private static final long[] POWERS_TEN_TABLE_THRESHOLD_HH = new long[]{
            9223372036854775807L, 922337203685477580L, 92233720368547758L, 9223372036854775L, 922337203685477L,
            92233720368547L, 9223372036854L, 922337203685L, 92233720368L, 9223372036L,
            922337203L, 92233720L, 9223372L, 922337L, 92233L,
            9223L, 922L, 92L, 9L,
    };
    private static final long[] POWERS_TEN_TABLE_THRESHOLD_HL = new long[]{
            -9223372036854775808L, -4611686018427387904L, 1383505805528216371L, -3550998234189088687L, -7733797452902729516L,
            -4462728560032183275L, -4135621670745128651L, 8809809869780262942L, -8342391049876749514L, -2678913512358630113L,
            -5801914573348728497L, 6798506172148947796L, 679850617214894779L, 3757333876463399801L, -5158289834466525505L,
            6862868646037168095L, 6220310086716582294L, 4311379823413568552L, 4120486797083267178L, -1432625727662628444L,
            1701411834604692317L, 170141183460469231L, 17014118346046923L, 1701411834604692L, 170141183460469L,
            17014118346046L, 1701411834604L, 170141183460L, 17014118346L, 1701411834L,
            170141183L, 17014118L, 1701411L, 170141L, 17014L,
            1701L, 170L, 17L, 1L,
    };
    private static final long[] POWERS_TEN_TABLE_THRESHOLD_LH = new long[]{
            -9223372036854775808L, 922337203685477580L, 3781582535110458081L, -1466516153859909354L, -146651615385990936L,
            5519358060574266391L, 6085959028170292123L, -1236078504553925950L, 5410415371657472889L, 541041537165747288L,
            -1790570253654380433L, -3868405840107348367L, -5920863806123600322L, 3097262434129550291L, 5843749465525820513L,
            -1260299460818373111L, 7252667683401983335L, -6653430861143622313L, 8558028950740413576L, 4545151709815951680L,
            4143863985723505491L, -5119636823540514936L, 3177385132387858829L, 5851761735351651367L, 4274524988277075459L,
            -1417221908543247616L, -5675745412967190247L, 8655797495558056783L, 865579749555805678L, -7292139654528240079L,
            8494158071401951800L, 6383439029253060664L, -3051004911816604257L, 3384248323560249897L, 2183099239726980151L,
            7597007553456518661L, 2604375162716607027L, 260437516271660702L, -5507979470485699415L, 3138550867693340381L,
            313855086769334038L, 31385508676933403L, 3138550867693340L, 313855086769334L, 31385508676933L,
            3138550867693L, 313855086769L, 31385508676L, 3138550867L, 313855086L,
            31385508L, 3138550L, 313855L, 31385L, 3138L,
            313L, 31L, 3L,
    };
    private static final long[] POWERS_TEN_TABLE_THRESHOLD_LL = new long[]{
            -9223372036854775808L, -2767011611056432743L, 5257322061007222210L, -8697639830754053587L, -6403787205188270844L,
            4893644501594038400L, -1355309957211551322L, -3824879810463065456L, -2227162388417261708L, -222716238841726171L,
            5511751598228692867L, -8672196877031906522L, -8245917317187011299L, 1020082675652254031L, -1742666139805729759L,
            -3863615428722483300L, 6992336086611572316L, 2543908016032112393L, -5279632420509654246L, -2372637649421920587L,
            5296759457170673426L, 6063699167829932827L, -6772327712700827364L, -4366581586011993060L, -436658158601199306L,
            7335031813623700715L, -6645194448121450575L, 6714178184671675588L, 6205441040580033043L, 7999241733541823950L,
            2644598580725137556L, -3424888956669396568L, 5191534326445925828L, -8704218604210183226L, 6508275769062802323L,
            -8572544459948495576L, -4546603260736759881L, -4144009140815586312L, 8808971122773217176L, -4653126109835543768L,
            3224036203758355946L, -3366945194366074729L, 7042003110047213173L, 704200311004721317L, 7449117660584292778L,
            6278934988171294762L, 6161916720929994961L, -1228482735277955666L, -5656871495640661052L, -4255035964305976429L,
            -5959526818543463128L, -2440627089225301475L, 1600611698448425014L, -9063310867009933307L, -8285028716184813978L,
            -2673177278989436560L, 7111379901584876990L, 2555812397529442860L, 5789604461865809770L, 578960446186580977L,
            57896044618658097L, 5789604461865809L, 578960446186580L, 57896044618658L, 5789604461865L,
            578960446186L, 57896044618L, 5789604461L, 578960446L, 57896044L,
            5789604L, 578960L, 57896L, 5789L, 578L,
            57L, 5L,
    };
    private static final BigDecimal[] ZERO_SCALED = new BigDecimal[32];
    // holders for in-place mutations that doesn't have a mutable structure available
    private static final ThreadLocal<Decimal256> tl = new ThreadLocal<>(Decimal256::new);
    private final DecimalKnuthDivider divider = new DecimalKnuthDivider();
    private long hh; // Highest 64 bits (bits 192-255)
    private long hl;    // High 64 bits (bits 128-191)
    private long lh;     // Mid 64 bits (bits 64-127)
    private long ll;     // Low 64 bits (bits 0-63)
    private int scale;    // Number of decimal places

    /**
     * Default constructor - creates zero with scale 0
     */
    public Decimal256() {
        this.hh = 0;
        this.hl = 0;
        this.lh = 0;
        this.ll = 0;
        this.scale = 0;
    }

    /**
     * Constructor with initial values.
     *
     * @param hh    the highest 64 bits of the decimal value (bits 192-255)
     * @param hl    the high 64 bits of the decimal value (bits 128-191)
     * @param lh    the mid 64 bits of the decimal value (bits 64-127)
     * @param ll    the low 64 bits of the decimal value (bits 0-63)
     * @param scale the number of decimal places
     * @throws IllegalArgumentException if scale is invalid
     */
    public Decimal256(long hh, long hl, long lh, long ll, int scale) {
        validateScale(scale);
        this.hh = hh;
        this.hl = hl;
        this.lh = lh;
        this.ll = ll;
        this.scale = scale;
    }

    /**
     * Static addition method.
     *
     * @param a      the first operand
     * @param b      the second operand
     * @param result the result (can be the same as a or b for in-place operation)
     * @throws NumericException if overflow occurs
     */
    public static void add(Decimal256 a, Decimal256 b, Decimal256 result) {
        result.copyFrom(a);
        result.add(b);
    }

    /**
     * Static division method.
     *
     * @param dividend     the dividend
     * @param divisor      the divisor
     * @param result       the result
     * @param scale        the desired scale of the result
     * @param roundingMode the rounding mode
     * @throws NumericException if division by zero or overflow occurs
     */
    public static void divide(Decimal256 dividend, Decimal256 divisor, Decimal256 result, int scale, RoundingMode roundingMode) {
        divide(
                result.divider,
                dividend.hh, dividend.hl, dividend.lh, dividend.ll, dividend.scale,
                divisor.hh, divisor.hl, divisor.lh, divisor.ll, divisor.scale,
                result, scale, roundingMode
        );
    }

    /**
     * Static division method using raw 256-bit values.
     *
     * @param dividendHH    the dividend's highest 64 bits
     * @param dividendHL    the dividend's high 64 bits
     * @param dividendLH    the dividend's mid 64 bits
     * @param dividendLL    the dividend's low 64 bits
     * @param dividendScale the dividend's scale
     * @param divisorHH     the divisor's highest 64 bits
     * @param divisorHL     the divisor's high 64 bits
     * @param divisorLH     the divisor's mid 64 bits
     * @param divisorLL     the divisor's low 64 bits
     * @param divisorScale  the divisor's scale
     * @param result        the result Decimal256 to store the quotient
     * @param scale         the desired scale of the result
     * @param roundingMode  the rounding mode
     * @throws NumericException if division by zero or overflow occurs
     */
    public static void divide(
            DecimalKnuthDivider divider,
            long dividendHH, long dividendHL, long dividendLH, long dividendLL, int dividendScale,
            long divisorHH, long divisorHL, long divisorLH, long divisorLL, int divisorScale,
            Decimal256 result, int scale, RoundingMode roundingMode) {
        validateScale(scale);

        // Compute the delta: how much power of 10 we should raise either the dividend or divisor.
        int delta = scale + (divisorScale - dividendScale);

        // Fail early if we're sure to overflow.
        if (delta > 0 && (scale + delta) > MAX_SCALE) {
            throw NumericException.instance().put("Overflow");
        }

        final boolean isNegative = (dividendHH < 0) ^ (divisorHH < 0);

        // We need to have both dividend and divisor positive for scaling and division.
        if (dividendHH < 0) {
            dividendLL = ~dividendLL + 1;
            long c = dividendLL == 0L ? 1L : 0L;
            dividendLH = ~dividendLH + c;
            c = (c == 1L && dividendLH == 0L) ? 1L : 0L;
            dividendHL = ~dividendHL + c;
            c = (c == 1L && dividendHL == 0L) ? 1L : 0L;
            dividendHH = ~dividendHH + c;
        }

        if (divisorHH < 0) {
            divisorLL = ~divisorLL + 1;
            long c = divisorLL == 0L ? 1L : 0L;
            divisorLH = ~divisorLH + c;
            c = (c == 1L && divisorLH == 0L) ? 1L : 0L;
            divisorHL = ~divisorHL + c;
            c = (c == 1L && divisorHL == 0L) ? 1L : 0L;
            divisorHH = ~divisorHH + c;
        }

        if (delta > 0) {
            // We need to raise the dividend to 10^delta
            result.of(dividendHH, dividendHL, dividendLH, dividendLL, scale);
            result.multiplyByPowerOf10InPlace(delta);
            dividendHH = result.hh;
            dividendHL = result.hl;
            dividendLH = result.lh;
            dividendLL = result.ll;
        } else if (delta < 0) {
            result.of(divisorHH, divisorHL, divisorLH, divisorLL, scale);
            result.multiplyByPowerOf10InPlace(-delta);
            divisorHH = result.hh;
            divisorHL = result.hl;
            divisorLH = result.lh;
            divisorLL = result.ll;
        }

        divider.clear();
        divider.ofDividend(dividendHH, dividendHL, dividendLH, dividendLL);
        divider.ofDivisor(divisorHH, divisorHL, divisorLH, divisorLL);
        divider.divide(isNegative, roundingMode);
        divider.sink(result, scale);

        if (isNegative) {
            result.negate();
        }
    }

    /**
     * Create Decimal256 from a BigDecimal.
     *
     * @param bd the BigDecimal value
     * @return new Decimal256 instance
     * @throws NumericException if the value cannot be represented in 256 bits
     */
    public static Decimal256 fromBigDecimal(BigDecimal bd) {
        if (bd == null) {
            throw new IllegalArgumentException("BigDecimal cannot be null");
        }

        BigInteger unscaledValue = bd.unscaledValue();
        int scale = bd.scale();

        if (scale < 0) {
            // We don't support negative scale, we must transform the value to match
            // our format.
            unscaledValue = unscaledValue.multiply(new BigInteger("10").pow(-scale));
            scale = 0;
        }

        // Check if the value fits in 256 bits
        if (unscaledValue.bitLength() > 255) {
            throw NumericException.instance().put("BigDecimal value too large for Decimal256");
        }

        boolean negative = unscaledValue.signum() == -1;
        if (negative) {
            unscaledValue = unscaledValue.negate();
        }

        // Convert to 256-bit representation
        byte[] bytes = unscaledValue.toByteArray();
        Decimal256 result = new Decimal256();
        result.scale = scale;

        // Fill the 256-bit value from the byte array
        result.setFromByteArray(bytes);
        validateScale(scale);

        if (negative) {
            result.negate();
        }

        return result;
    }

    /**
     * Create Decimal256 from a double value with specified scale.
     *
     * @param value the double value
     * @param scale the desired scale
     * @return new Decimal256 instance
     * @throws IllegalArgumentException if scale is invalid
     */
    public static Decimal256 fromDouble(double value, int scale) {
        validateScale(scale);
        BigDecimal bd = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        return fromBigDecimal(bd);
    }

    /**
     * Create Decimal256 from a long value with specified scale.
     *
     * @param value the long value
     * @param scale the desired scale
     * @return new Decimal256 instance
     * @throws IllegalArgumentException if scale is invalid
     */
    public static Decimal256 fromLong(long value, int scale) {
        validateScale(scale);

        Decimal256 result = new Decimal256();
        result.setFromLong(value, scale);
        return result;
    }

    /**
     * Static modulo method.
     *
     * @param dividend the dividend
     * @param divisor  the divisor
     * @param result   the result
     * @throws NumericException if division by zero occurs
     */
    public static void modulo(Decimal256 dividend, Decimal256 divisor, Decimal256 result) {
        result.copyFrom(dividend);
        result.modulo(divisor);
    }

    /**
     * Static multiplication method.
     *
     * @param a      the first operand
     * @param b      the second operand
     * @param result the result (can be the same as a or b for in-place operation)
     * @throws NumericException if overflow occurs
     */
    public static void multiply(Decimal256 a, Decimal256 b, Decimal256 result) {
        result.copyFrom(a);
        result.multiply(b);
    }

    /**
     * Static subtraction method.
     *
     * @param a      the first operand
     * @param b      the second operand
     * @param result the result (can be the same as a or b for in-place operation)
     * @throws NumericException if overflow occurs
     */
    public static void subtract(Decimal256 a, Decimal256 b, Decimal256 result) {
        result.copyFrom(a);
        result.subtract(b);
    }

    public static @NotNull BigDecimal toBigDecimal(long hh, long hl, long lh, long ll, int scale) {
        // Convert 256-bit value to BigInteger
        byte[] bytes = new byte[32]; // 256 bits = 32 bytes
        // Fill bytes in big-endian order
        putLongIntoBytes(bytes, 0, hh);
        putLongIntoBytes(bytes, 8, hl);
        putLongIntoBytes(bytes, 16, lh);
        putLongIntoBytes(bytes, 24, ll);

        BigInteger unscaledValue = new BigInteger(bytes);
        return new BigDecimal(unscaledValue, scale);
    }

    /**
     * In-place addition.
     *
     * @param other the Decimal256 to add
     * @throws NumericException if overflow occurs
     */
    public void add(Decimal256 other) {
        add(this, hh, hl, lh, ll, scale, other.hh, other.hl, other.lh, other.ll, other.scale);
    }

    /**
     * Compare this Decimal256 with another.
     *
     * @param other the Decimal256 to compare with
     * @return -1, 0, or 1 as this is less than, equal to, or greater than other
     */
    public int compareTo(Decimal256 other) {
        boolean aNeg = isNegative();
        boolean bNeg = other.isNegative();
        if (aNeg != bNeg) {
            return aNeg ? -1 : 1;
        }

        int diffQ = aNeg ? -1 : 1;

        if (this.scale == other.scale) {
            // Same scale - direct comparison
            if (this.hh != other.hh) {
                return Long.compare(this.hh, other.hh);
            }
            if (this.hl != other.hl) {
                return Long.compareUnsigned(this.hl, other.hl) * diffQ;
            }
            if (this.lh != other.lh) {
                return Long.compareUnsigned(this.lh, other.lh) * diffQ;
            }
            return Long.compareUnsigned(this.ll, other.ll) * diffQ;
        }

        // Stores the coefficient to apply to the response, if both numbers are negative, then
        // we have to reverse the result
        long aHH = this.hh;
        long aHL = this.hl;
        long aLH = this.lh;
        long aLL = this.ll;
        int aScale = this.scale;
        long bHH = other.hh;
        long bHL = other.hl;
        long bLH = other.lh;
        long bLL = other.ll;
        int bScale = other.scale;

        if (aNeg) {
            aLL = ~aLL + 1;
            long c = aLL == 0L ? 1L : 0L;
            aLH = ~aLH + c;
            c = (c == 1L && aLH == 0L) ? 1L : 0L;
            aHL = ~aHL + c;
            c = (c == 1L && aHL == 0L) ? 1L : 0L;
            aHH = ~aHH + c;

            // Negate b
            bLL = ~bLL + 1;
            c = bLL == 0L ? 1L : 0L;
            bLH = ~bLH + c;
            c = (c == 1L && bLH == 0L) ? 1L : 0L;
            bHL = ~bHL + c;
            c = (c == 1L && bHL == 0L) ? 1L : 0L;
            bHH = ~bHH + c;
        }

        Decimal256 holder = tl.get();
        if (aScale < bScale) {
            holder.of(aHH, aHL, aLH, aLL, aScale);
            holder.multiplyByPowerOf10InPlace(bScale - aScale);
            aHH = holder.hh;
            aHL = holder.hl;
            aLH = holder.lh;
            aLL = holder.ll;
        } else {
            holder.of(bHH, bHL, bLH, bLL, bScale);
            holder.multiplyByPowerOf10InPlace(aScale - bScale);
            bHH = holder.hh;
            bHL = holder.hl;
            bLH = holder.lh;
            bLL = holder.ll;
        }

        if (aHH != bHH) {
            return Long.compare(aHH, bHH) * diffQ;
        }
        if (aHL != bHL) {
            return Long.compareUnsigned(aHL, bHL) * diffQ;
        }
        if (aLH != bLH) {
            return Long.compareUnsigned(aLH, bLH) * diffQ;
        }
        return Long.compareUnsigned(aLL, bLL) * diffQ;
    }

    /**
     * Copy values from another Decimal256 instance.
     *
     * @param other the Decimal256 instance to copy from
     */
    public void copyFrom(Decimal256 other) {
        this.hh = other.hh;
        this.hl = other.hl;
        this.lh = other.lh;
        this.ll = other.ll;
        this.scale = other.scale;
    }

    /**
     * In-place division.
     *
     * @param divisor      the Decimal256 to divide by
     * @param targetScale  the desired scale of the result
     * @param roundingMode the rounding mode
     * @throws NumericException if division by zero or overflow occurs
     */
    public void divide(Decimal256 divisor, int targetScale, RoundingMode roundingMode) {
        divide(this, divisor, this, targetScale, roundingMode);
    }

    /**
     * Checks if this Decimal256 is equal to another object.
     * Two Decimal256 instances are equal if they have the same 256-bit value and scale.
     *
     * @param obj the object to compare with
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        Decimal256 other = (Decimal256) obj;
        return hh == other.hh &&
                hl == other.hl &&
                lh == other.lh &&
                ll == other.ll &&
                scale == other.scale;
    }

    /**
     * Gets the highest 64 bits of the 256-bit decimal value (bits 192-255).
     *
     * @return the highest 64 bits
     */
    public long getHh() {
        return hh;
    }

    /**
     * Gets the high 64 bits of the 256-bit decimal value (bits 128-191).
     *
     * @return the high 64 bits
     */
    public long getHl() {
        return hl;
    }

    /**
     * Gets the mid 64 bits of the 256-bit decimal value (bits 64-127).
     *
     * @return the mid 64 bits
     */
    public long getLh() {
        return lh;
    }

    /**
     * Gets the low 64 bits of the 256-bit decimal value (bits 0-63).
     *
     * @return the low 64 bits
     */
    public long getLl() {
        return ll;
    }

    /**
     * Gets the scale (number of decimal places) of this decimal number.
     *
     * @return the scale
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns the hash code for this Decimal256.
     * The hash code is computed based on all 256 bits of the value and the scale.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        int result = Long.hashCode(hh);
        result = 31 * result + Long.hashCode(hl);
        result = 31 * result + Long.hashCode(lh);
        result = 31 * result + Long.hashCode(ll);
        result = 31 * result + scale;
        return result;
    }

    /**
     * Check if this Decimal256 is negative.
     *
     * @return true if negative, false otherwise
     */
    public boolean isNegative() {
        return hh < 0;
    }

    /**
     * Check if this Decimal256 represents zero.
     *
     * @return true if zero, false otherwise
     */
    public boolean isZero() {
        return hh == 0 && hl == 0 && lh == 0 && ll == 0;
    }

    /**
     * In-place modulo operation.
     *
     * @param divisor the Decimal256 to take modulo by
     * @throws NumericException if division by zero occurs
     */
    public void modulo(Decimal256 divisor) {
        if (divisor.isZero()) {
            throw NumericException.instance().put("Division by zero");
        }

        // Result scale should be the larger of the two scales
        int resultScale = Math.max(this.scale, divisor.scale);

        // Use simple repeated subtraction for modulo: a % b = a - (a / b) * b
        // First compute integer division (a / b)
        // We store this for later usage
        long thisHH = this.hh;
        long thisHL = this.hl;
        long thisLH = this.lh;
        long thisLL = this.ll;
        int thisScale = this.scale;

        this.divide(divisor, 0, RoundingMode.DOWN);

        // Now compute this * divisor
        this.multiply(divisor);

        long qHH = this.hh;
        long qHL = this.hl;
        long qLH = this.lh;
        long qLL = this.ll;
        int qScale = this.scale;
        // restore this as a
        this.of(thisHH, thisHL, thisLH, thisLL, thisScale);
        // Finally compute remainder: a - (a / b) * b
        this.subtract(qHH, qHL, qLH, qLL, qScale);

        // Handle scale adjustment
        if (this.scale != resultScale) {
            int scaleUp = resultScale - this.scale;
            boolean isNegative = isNegative();
            if (isNegative) {
                negate();
            }
            multiplyByPowerOf10InPlace(scaleUp);
            if (isNegative) {
                negate();
            }
            this.scale = resultScale;
        }
    }

    /**
     * Multiplies this Decimal256 by another Decimal256 in-place.
     * The result scale is the sum of both operands' scales.
     *
     * @param other the Decimal256 to multiply by
     * @throws NumericException if overflow occurs or resulting scale exceeds MAX_SCALE
     */
    public void multiply(Decimal256 other) {
        int finalScale = scale + other.scale;
        validateScale(finalScale);

        boolean isNegative = isNegative() ^ other.isNegative();

        long otherHH = other.hh;
        long otherHL = other.hl;
        long otherLH = other.lh;
        long otherLL = other.ll;
        if (other.isNegative()) {
            otherLL = ~otherLL + 1;
            long c = otherLL == 0L ? 1L : 0L;
            otherLH = ~otherLH + c;
            c = (c == 1L && otherLH == 0L) ? 1L : 0L;
            otherHL = ~otherHL + c;
            c = (c == 1L && otherHL == 0L) ? 1L : 0L;
            otherHH = ~otherHH + c;
        }

        if (isNegative()) {
            negate();
        }

        if (otherHH != 0 || otherHL < 0) {
            multiply256(otherHH, otherHL, otherLH, otherLL);
        } else if (otherHL != 0 || otherLH < 0) {
            multiply192(otherHL, otherLH, otherLL);
        } else if (otherLH != 0 || otherLL < 0) {
            multiply128(otherLH, otherLL);
        } else {
            multiply64(otherLL);
        }

        if (isNegative) {
            negate();
        }

        scale = finalScale;
    }

    /**
     * Negates this Decimal256 in-place using two's complement arithmetic.
     * Changes the sign of the decimal number (positive becomes negative and vice versa).
     */
    public void negate() {
        ll = ~ll + 1;
        long c = ll == 0L ? 1L : 0L;
        lh = ~lh + c;
        c = (c == 1L && lh == 0L) ? 1L : 0L;
        hl = ~hl + c;
        c = (c == 1L && hl == 0L) ? 1L : 0L;
        hh = ~hh + c;
    }

    /**
     * Sets this Decimal256 to the specified 256-bit value and scale.
     *
     * @param hh    the highest 64 bits (bits 192-255)
     * @param hl    the high 64 bits (bits 128-191)
     * @param lh    the mid 64 bits (bits 64-127)
     * @param ll    the low 64 bits (bits 0-63)
     * @param scale the number of decimal places
     */
    public void of(long hh, long hl, long lh, long ll, int scale) {
        this.hh = hh;
        this.hl = hl;
        this.lh = lh;
        this.ll = ll;
        this.scale = scale;
    }

    /**
     * Rescale this Decimal256 in place
     *
     * @param newScale The new scale (must be >= current scale)
     */
    public void rescale(int newScale) {
        if (newScale < scale) {
            throw new IllegalArgumentException("New scale must be >= current scale");
        }

        int scaleDiff = newScale - this.scale;

        boolean isNegative = isNegative();
        if (isNegative) {
            negate();
        }

        // Multiply by 10^scaleDiff
        multiplyByPowerOf10InPlace(scaleDiff);

        if (isNegative) {
            negate();
        }

        this.scale = newScale;
    }

    /**
     * Round to specified scale.
     *
     * @param targetScale  the target scale
     * @param roundingMode the rounding mode
     * @throws IllegalArgumentException if target scale is invalid
     */
    public void round(int targetScale, RoundingMode roundingMode) {
        if (targetScale == this.scale) {
            // No rounding needed
            return;
        }

        validateScale(targetScale);

        if (roundingMode == RoundingMode.UNNECESSARY) {
            // UNNECESSARY mode is a no-op in this implementation
            return;
        }

        if (isZero()) {
            this.scale = targetScale;
            return;
        }

        if (this.scale < targetScale) {
            boolean isNegative = isNegative();
            if (isNegative) {
                negate();
            }

            // Need to increase scale (add trailing zeros)
            int scaleIncrease = targetScale - this.scale;
            multiplyByPowerOf10InPlace(scaleIncrease);
            this.scale = targetScale;

            if (isNegative) {
                negate();
            }
            return;
        }

        divide(divider, hh, hl, lh, ll, scale, 0L, 0L, 0L, 1L, 0, this, targetScale, roundingMode);
    }

    /**
     * Set this Decimal256 from a long value with specified scale.
     *
     * @param value the long value
     * @param scale the desired scale
     */
    public void setFromLong(long value, int scale) {
        validateScale(scale);

        this.scale = scale;
        this.ll = value;
        this.lh = value < 0 ? -1L : 0L;  // Sign extension
        this.hl = value < 0 ? -1L : 0L;
        this.hh = value < 0 ? -1L : 0L;
    }

    /**
     * In-place subtraction.
     *
     * @param other the Decimal256 to subtract
     * @throws NumericException if overflow occurs
     */
    public void subtract(Decimal256 other) {
        subtract(other.hh, other.hl, other.lh, other.ll, other.scale);
    }

    /**
     * In-place subtraction using raw 256-bit values.
     *
     * @param bHH    the subtrahend's highest 64 bits
     * @param bHL    the subtrahend's high 64 bits
     * @param bLH    the subtrahend's mid 64 bits
     * @param bLL    the subtrahend's low 64 bits
     * @param bScale the subtrahend's scale
     * @throws NumericException if overflow occurs
     */
    public void subtract(long bHH, long bHL, long bLH, long bLL, int bScale) {
        // Negate other and perform addition
        if (bHH != 0 || bHL != 0 || bLH != 0 || bLL != 0) {
            bLL = ~bLL + 1;
            long c = bLL == 0L ? 1L : 0L;
            bLH = ~bLH + c;
            c = (c == 1L && bLH == 0L) ? 1L : 0L;
            bHL = ~bHL + c;
            c = (c == 1L && bHL == 0L) ? 1L : 0L;
            bHH = ~bHH + c;

            add(this, hh, hl, lh, ll, scale, bHH, bHL, bLH, bLL, bScale);
        }
    }

    /**
     * Convert to BigDecimal.
     *
     * @return BigDecimal representation
     */
    public BigDecimal toBigDecimal() {
        if (isZero() && scale < ZERO_SCALED.length) {
            return ZERO_SCALED[scale];
        }
        return toBigDecimal(hh, hl, lh, ll, scale);
    }

    /**
     * Convert to double (may lose precision).
     *
     * @return double representation
     */
    public double toDouble() {
        return toBigDecimal().doubleValue();
    }

    /**
     * Writes the string representation of this Decimal256 to the specified CharSink.
     * The output format is a plain decimal string without scientific notation.
     *
     * @param sink the CharSink to write to
     */
    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        BigDecimal bd = toBigDecimal();
        sink.put(bd.toPlainString());
    }

    /**
     * Returns the string representation of this Decimal256.
     * The output format is a plain decimal string without scientific notation.
     *
     * @return string representation of this decimal number
     */
    @Override
    public String toString() {
        return toBigDecimal().toPlainString();
    }

    private static void add(Decimal256 result,
                            long aHH, long aHL, long aLH, long aLL, int aScale,
                            long bHH, long bHL, long bLH, long bLL, int bScale) {
        result.scale = aScale;
        if (aScale < bScale) {
            // We need to rescale a to the same scale as b
            result.of(aHH, aHL, aLH, aLL, aScale);
            result.rescale(bScale);
            aHH = result.hh;
            aHL = result.hl;
            aLH = result.lh;
            aLL = result.ll;
        } else if (aScale > bScale) {
            // We need to rescale b to the same scale as a
            result.of(bHH, bHL, bLH, bLL, bScale);
            result.rescale(aScale);
            bHH = result.hh;
            bHL = result.hl;
            bLH = result.lh;
            bLL = result.ll;
        }

        // Perform 256-bit addition
        long r = aLL + bLL;
        long carry = hasCarry(aLL, r) ? 1L : 0L;
        result.ll = r;

        long t = aLH + carry;
        carry = hasCarry(aLH, t) ? 1L : 0L;
        r = t + bLH;
        carry |= hasCarry(t, r) ? 1L : 0L;
        result.lh = r;

        t = aHL + carry;
        carry = hasCarry(aHL, t) ? 1L : 0L;
        r = t + bHL;
        carry |= hasCarry(t, r) ? 1L : 0L;
        result.hl = r;

        t = aHH + carry;
        if (((aHH ^ t) & (carry ^ t)) < 0L) {
            throw NumericException.instance().put("Overflow");
        }
        r = t + bHH;
        if (((bHH ^ r) & (t ^ r)) < 0L) {
            throw NumericException.instance().put("Overflow");
        }
        result.hh = r;
    }

    private static void putLongIntoBytes(byte[] bytes, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            bytes[offset + i] = (byte) (value >>> ((7 - i) * 8));
        }
    }

    /**
     * Validates that the scale is within the allowed range.
     *
     * @param scale the scale to validate
     * @throws NumericException if scale is invalid
     */
    private static void validateScale(int scale) {
        if (scale < 0 || scale > MAX_SCALE) {
            throw NumericException.instance().put("Invalid scale: " + scale);
        }
    }

    private int compareTo(long bHH, long bHL, long bLH, long bLL) {
        if (hh != bHH) {
            return Long.compare(hh, bHH);
        }
        if (hl != bHL) {
            return Long.compareUnsigned(hl, bHL);
        }
        if (lh != bLH) {
            return Long.compareUnsigned(lh, bLH);
        }
        return Long.compareUnsigned(ll, bLL);
    }

    private void multiply128(long h, long l) {
        // Perform 256-bit × 128-bit multiplication
        // Result is at most 384 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = hh >>> 32;
        long a6 = hh & 0xFFFFFFFFL;
        long a5 = hl >>> 32;
        long a4 = hl & 0xFFFFFFFFL;
        long a3 = lh >>> 32;
        long a2 = lh & 0xFFFFFFFFL;
        long a1 = ll >>> 32;
        long a0 = ll & 0xFFFFFFFFL;

        long b3 = h >>> 32;
        long b2 = h & 0xFFFFFFFFL;
        long b1 = l >>> 32;
        long b0 = l & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p42 = a4 * b2;
        long p43 = a4 * b3;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p52 = a5 * b2;
        long p53 = a5 * b3;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p62 = a6 * b2;
        long p63 = a6 * b3;
        long p70 = a7 * b0;
        long p71 = a7 * b1;
        long p72 = a7 * b2;
        long p73 = a7 * b3;

        long overflow = p53 | p62 | p63 | p71 | p72 | p73;
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p13 & 0xFFFFFFFFL) + (p22 & 0xFFFFFFFFL) + (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p23 & 0xFFFFFFFFL) + (p32 & 0xFFFFFFFFL) + (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p23 >>> 32) + (p32 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p33 & 0xFFFFFFFFL) + (p42 & 0xFFFFFFFFL) + (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p33 >>> 32) + (p42 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p43 & 0xFFFFFFFFL) + (p52 & 0xFFFFFFFFL) + (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);
        overflow = (r7 >>> 31) | (p43 >>> 32) | (p52 >>> 32) | (p61 >>> 32) | (p70 >>> 32);
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply128Unchecked(long h, long l) {
        // Perform 256-bit × 128-bit multiplication
        // Result is at most 384 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = hh >>> 32;
        long a6 = hh & 0xFFFFFFFFL;
        long a5 = hl >>> 32;
        long a4 = hl & 0xFFFFFFFFL;
        long a3 = lh >>> 32;
        long a2 = lh & 0xFFFFFFFFL;
        long a1 = ll >>> 32;
        long a0 = ll & 0xFFFFFFFFL;

        long b3 = h >>> 32;
        long b2 = h & 0xFFFFFFFFL;
        long b1 = l >>> 32;
        long b0 = l & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p42 = a4 * b2;
        long p43 = a4 * b3;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p52 = a5 * b2;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p70 = a7 * b0;

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p13 & 0xFFFFFFFFL) + (p22 & 0xFFFFFFFFL) + (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p23 & 0xFFFFFFFFL) + (p32 & 0xFFFFFFFFL) + (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p23 >>> 32) + (p32 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p33 & 0xFFFFFFFFL) + (p42 & 0xFFFFFFFFL) + (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p33 >>> 32) + (p42 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p43 & 0xFFFFFFFFL) + (p52 & 0xFFFFFFFFL) + (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply192(long h, long m, long l) {
        // Perform 256-bit × 192-bit multiplication
        // Result is at most 448 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = hh >>> 32;
        long a6 = hh & 0xFFFFFFFFL;
        long a5 = hl >>> 32;
        long a4 = hl & 0xFFFFFFFFL;
        long a3 = lh >>> 32;
        long a2 = lh & 0xFFFFFFFFL;
        long a1 = ll >>> 32;
        long a0 = ll & 0xFFFFFFFFL;

        long b5 = h >>> 32;
        long b4 = h & 0xFFFFFFFFL;
        long b3 = m >>> 32;
        long b2 = m & 0xFFFFFFFFL;
        long b1 = l >>> 32;
        long b0 = l & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p04 = a0 * b4;
        long p05 = a0 * b5;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p14 = a1 * b4;
        long p15 = a1 * b5;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p24 = a2 * b4;
        long p25 = a2 * b5;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;
        long p34 = a3 * b4;
        long p35 = a3 * b5;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p42 = a4 * b2;
        long p43 = a4 * b3;
        long p44 = a4 * b4;
        long p45 = a4 * b5;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p52 = a5 * b2;
        long p53 = a5 * b3;
        long p54 = a5 * b4;
        long p55 = a5 * b5;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p62 = a6 * b2;
        long p63 = a6 * b3;
        long p64 = a6 * b4;
        long p65 = a6 * b5;
        long p70 = a7 * b0;
        long p71 = a7 * b1;
        long p72 = a7 * b2;
        long p73 = a7 * b3;
        long p74 = a7 * b4;
        long p75 = a7 * b5;

        long overflow = p35 | p44 | p45 | p53 | p54 | p55 | p62 | p63 | p64 | p65 | p71 | p72 | p73 | p74 | p75;
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p04 & 0xFFFFFFFFL) + (p13 & 0xFFFFFFFFL) + (p22 & 0xFFFFFFFFL) + (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p04 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p05 & 0xFFFFFFFFL) + (p14 & 0xFFFFFFFFL) + (p23 & 0xFFFFFFFFL) + (p32 & 0xFFFFFFFFL) + (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p05 >>> 32) + (p14 >>> 32) + (p23 >>> 32) + (p32 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p15 & 0xFFFFFFFFL) + (p24 & 0xFFFFFFFFL) + (p33 & 0xFFFFFFFFL) + (p42 & 0xFFFFFFFFL) + (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p15 >>> 32) + (p24 >>> 32) + (p33 >>> 32) + (p42 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p25 & 0xFFFFFFFFL) + (p34 & 0xFFFFFFFFL) + (p43 & 0xFFFFFFFFL) + (p52 & 0xFFFFFFFFL) + (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);
        overflow = (r7 >>> 31) | (p25 >>> 32) | (p34 >>> 32) | (p43 >>> 32) | (p52 >>> 32) | (p61 >>> 32) | (p70 >>> 32);
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply192Unchecked(long h, long m, long l) {
        // Perform 256-bit × 192-bit multiplication
        // Result is at most 448 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = hh >>> 32;
        long a6 = hh & 0xFFFFFFFFL;
        long a5 = hl >>> 32;
        long a4 = hl & 0xFFFFFFFFL;
        long a3 = lh >>> 32;
        long a2 = lh & 0xFFFFFFFFL;
        long a1 = ll >>> 32;
        long a0 = ll & 0xFFFFFFFFL;

        long b5 = h >>> 32;
        long b4 = h & 0xFFFFFFFFL;
        long b3 = m >>> 32;
        long b2 = m & 0xFFFFFFFFL;
        long b1 = l >>> 32;
        long b0 = l & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p04 = a0 * b4;
        long p05 = a0 * b5;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p14 = a1 * b4;
        long p15 = a1 * b5;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p24 = a2 * b4;
        long p25 = a2 * b5;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;
        long p34 = a3 * b4;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p42 = a4 * b2;
        long p43 = a4 * b3;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p52 = a5 * b2;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p70 = a7 * b0;

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p04 & 0xFFFFFFFFL) + (p13 & 0xFFFFFFFFL) + (p22 & 0xFFFFFFFFL) + (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p04 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p05 & 0xFFFFFFFFL) + (p14 & 0xFFFFFFFFL) + (p23 & 0xFFFFFFFFL) + (p32 & 0xFFFFFFFFL) + (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p05 >>> 32) + (p14 >>> 32) + (p23 >>> 32) + (p32 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p15 & 0xFFFFFFFFL) + (p24 & 0xFFFFFFFFL) + (p33 & 0xFFFFFFFFL) + (p42 & 0xFFFFFFFFL) + (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p15 >>> 32) + (p24 >>> 32) + (p33 >>> 32) + (p42 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p25 & 0xFFFFFFFFL) + (p34 & 0xFFFFFFFFL) + (p43 & 0xFFFFFFFFL) + (p52 & 0xFFFFFFFFL) + (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply256(long hh, long hl, long lh, long ll) {
        // Perform 256-bit × 256-bit multiplication
        // Result is at most 512 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = this.hh >>> 32;
        long a6 = this.hh & 0xFFFFFFFFL;
        long a5 = this.hl >>> 32;
        long a4 = this.hl & 0xFFFFFFFFL;
        long a3 = this.lh >>> 32;
        long a2 = this.lh & 0xFFFFFFFFL;
        long a1 = this.ll >>> 32;
        long a0 = this.ll & 0xFFFFFFFFL;

        long b7 = hh >>> 32;
        long b6 = hh & 0xFFFFFFFFL;
        long b5 = hl >>> 32;
        long b4 = hl & 0xFFFFFFFFL;
        long b3 = lh >>> 32;
        long b2 = lh & 0xFFFFFFFFL;
        long b1 = ll >>> 32;
        long b0 = ll & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p04 = a0 * b4;
        long p05 = a0 * b5;
        long p06 = a0 * b6;
        long p07 = a0 * b7;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p14 = a1 * b4;
        long p15 = a1 * b5;
        long p16 = a1 * b6;
        long p17 = a1 * b7;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p24 = a2 * b4;
        long p25 = a2 * b5;
        long p26 = a2 * b6;
        long p27 = a2 * b7;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;
        long p34 = a3 * b4;
        long p35 = a3 * b5;
        long p36 = a3 * b6;
        long p37 = a3 * b7;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p42 = a4 * b2;
        long p43 = a4 * b3;
        long p44 = a4 * b4;
        long p45 = a4 * b5;
        long p46 = a4 * b6;
        long p47 = a4 * b7;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p52 = a5 * b2;
        long p53 = a5 * b3;
        long p54 = a5 * b4;
        long p55 = a5 * b5;
        long p56 = a5 * b6;
        long p57 = a5 * b7;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p62 = a6 * b2;
        long p63 = a6 * b3;
        long p64 = a6 * b4;
        long p65 = a6 * b5;
        long p66 = a6 * b6;
        long p67 = a6 * b7;
        long p70 = a7 * b0;
        long p71 = a7 * b1;
        long p72 = a7 * b2;
        long p73 = a7 * b3;
        long p74 = a7 * b4;
        long p75 = a7 * b5;
        long p76 = a7 * b6;
        long p77 = a7 * b7;

        long overflow = p17 | p26 | p27 | p35 | p36 | p37 | p44 | p45 | p46 | p47 | p53 | p54 | p55 | p56 | p57 |
                p62 | p63 | p64 | p65 | p66 | p67 | p71 | p72 | p73 | p74 | p75 | p76 | p77;
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p04 & 0xFFFFFFFFL) + (p13 & 0xFFFFFFFFL) + (p22 & 0xFFFFFFFFL) + (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p04 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p05 & 0xFFFFFFFFL) + (p14 & 0xFFFFFFFFL) + (p23 & 0xFFFFFFFFL) + (p32 & 0xFFFFFFFFL) + (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p05 >>> 32) + (p14 >>> 32) + (p23 >>> 32) + (p32 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p06 & 0xFFFFFFFFL) + (p15 & 0xFFFFFFFFL) + (p24 & 0xFFFFFFFFL) + (p33 & 0xFFFFFFFFL) + (p42 & 0xFFFFFFFFL) + (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p06 >>> 32) + (p15 >>> 32) + (p24 >>> 32) + (p33 >>> 32) + (p42 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p07 & 0xFFFFFFFFL) + (p16 & 0xFFFFFFFFL) + (p25 & 0xFFFFFFFFL) + (p34 & 0xFFFFFFFFL) + (p43 & 0xFFFFFFFFL) + (p52 & 0xFFFFFFFFL) + (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);
        overflow = (r7 >>> 31) | (p07 >>> 32) | (p16 >>> 32) | (p25 >>> 32) | (p34 >>> 32) | (p43 >>> 32) | (p52 >>> 32) | (p61 >>> 32) | (p70 >>> 32);
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply256Unchecked(long hh, long hl, long lh, long ll) {
        // Perform 256-bit × 256-bit multiplication
        // Result is at most 512 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = this.hh >>> 32;
        long a6 = this.hh & 0xFFFFFFFFL;
        long a5 = this.hl >>> 32;
        long a4 = this.hl & 0xFFFFFFFFL;
        long a3 = this.lh >>> 32;
        long a2 = this.lh & 0xFFFFFFFFL;
        long a1 = this.ll >>> 32;
        long a0 = this.ll & 0xFFFFFFFFL;

        long b7 = hh >>> 32;
        long b6 = hh & 0xFFFFFFFFL;
        long b5 = hl >>> 32;
        long b4 = hl & 0xFFFFFFFFL;
        long b3 = lh >>> 32;
        long b2 = lh & 0xFFFFFFFFL;
        long b1 = ll >>> 32;
        long b0 = ll & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p02 = a0 * b2;
        long p03 = a0 * b3;
        long p04 = a0 * b4;
        long p05 = a0 * b5;
        long p06 = a0 * b6;
        long p07 = a0 * b7;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p12 = a1 * b2;
        long p13 = a1 * b3;
        long p14 = a1 * b4;
        long p15 = a1 * b5;
        long p16 = a1 * b6;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p22 = a2 * b2;
        long p23 = a2 * b3;
        long p24 = a2 * b4;
        long p25 = a2 * b5;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p32 = a3 * b2;
        long p33 = a3 * b3;
        long p34 = a3 * b4;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p42 = a4 * b2;
        long p43 = a4 * b3;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p52 = a5 * b2;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p70 = a7 * b0;

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p02 & 0xFFFFFFFFL) + (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p02 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p03 & 0xFFFFFFFFL) + (p12 & 0xFFFFFFFFL) + (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p03 >>> 32) + (p12 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p04 & 0xFFFFFFFFL) + (p13 & 0xFFFFFFFFL) + (p22 & 0xFFFFFFFFL) + (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p04 >>> 32) + (p13 >>> 32) + (p22 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p05 & 0xFFFFFFFFL) + (p14 & 0xFFFFFFFFL) + (p23 & 0xFFFFFFFFL) + (p32 & 0xFFFFFFFFL) + (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p05 >>> 32) + (p14 >>> 32) + (p23 >>> 32) + (p32 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p06 & 0xFFFFFFFFL) + (p15 & 0xFFFFFFFFL) + (p24 & 0xFFFFFFFFL) + (p33 & 0xFFFFFFFFL) + (p42 & 0xFFFFFFFFL) + (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p06 >>> 32) + (p15 >>> 32) + (p24 >>> 32) + (p33 >>> 32) + (p42 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p07 & 0xFFFFFFFFL) + (p16 & 0xFFFFFFFFL) + (p25 & 0xFFFFFFFFL) + (p34 & 0xFFFFFFFFL) + (p43 & 0xFFFFFFFFL) + (p52 & 0xFFFFFFFFL) + (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply64(long multiplier) {
        // Perform 256-bit × 64-bit multiplication
        // Result is at most 320 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = hh >>> 32;
        long a6 = hh & 0xFFFFFFFFL;
        long a5 = hl >>> 32;
        long a4 = hl & 0xFFFFFFFFL;
        long a3 = lh >>> 32;
        long a2 = lh & 0xFFFFFFFFL;
        long a1 = ll >>> 32;
        long a0 = ll & 0xFFFFFFFFL;

        long b1 = multiplier >>> 32;
        long b0 = multiplier & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p70 = a7 * b0;
        long p71 = a7 * b1;
        if (p71 != 0) {
            throw NumericException.instance().put("Overflow");
        }

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);
        long overflow = (r7 >>> 31) | (p61 >>> 32) | (p70 >>> 32);
        if (overflow != 0) {
            throw NumericException.instance().put("Overflow");
        }

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    private void multiply64Unchecked(long multiplier) {
        // Perform 256-bit × 64-bit multiplication
        // Result is at most 320 bits, but we keep only the lower 256 bits

        // Split this into eight 32-bit parts
        long a7 = hh >>> 32;
        long a6 = hh & 0xFFFFFFFFL;
        long a5 = hl >>> 32;
        long a4 = hl & 0xFFFFFFFFL;
        long a3 = lh >>> 32;
        long a2 = lh & 0xFFFFFFFFL;
        long a1 = ll >>> 32;
        long a0 = ll & 0xFFFFFFFFL;

        long b1 = multiplier >>> 32;
        long b0 = multiplier & 0xFFFFFFFFL;

        // Compute all partial products
        long p00 = a0 * b0;
        long p01 = a0 * b1;
        long p10 = a1 * b0;
        long p11 = a1 * b1;
        long p20 = a2 * b0;
        long p21 = a2 * b1;
        long p30 = a3 * b0;
        long p31 = a3 * b1;
        long p40 = a4 * b0;
        long p41 = a4 * b1;
        long p50 = a5 * b0;
        long p51 = a5 * b1;
        long p60 = a6 * b0;
        long p61 = a6 * b1;
        long p70 = a7 * b0;

        // Gather results into 256-bit result
        long r0 = (p00 & 0xFFFFFFFFL);
        long r1 = (p00 >>> 32) + (p01 & 0xFFFFFFFFL) + (p10 & 0xFFFFFFFFL);
        long r2 = (r1 >>> 32) + (p01 >>> 32) + (p10 >>> 32) +
                (p11 & 0xFFFFFFFFL) + (p20 & 0xFFFFFFFFL);
        long r3 = (r2 >>> 32) + (p11 >>> 32) + (p20 >>> 32) +
                (p21 & 0xFFFFFFFFL) + (p30 & 0xFFFFFFFFL);
        long r4 = (r3 >>> 32) + (p21 >>> 32) + (p30 >>> 32) +
                (p31 & 0xFFFFFFFFL) + (p40 & 0xFFFFFFFFL);
        long r5 = (r4 >>> 32) + (p31 >>> 32) + (p40 >>> 32) +
                (p41 & 0xFFFFFFFFL) + (p50 & 0xFFFFFFFFL);
        long r6 = (r5 >>> 32) + (p41 >>> 32) + (p50 >>> 32) +
                (p51 & 0xFFFFFFFFL) + (p60 & 0xFFFFFFFFL);
        long r7 = (r6 >>> 32) + (p51 >>> 32) + (p60 >>> 32) +
                (p61 & 0xFFFFFFFFL) + (p70 & 0xFFFFFFFFL);

        this.ll = (r0 & 0xFFFFFFFFL) | ((r1 & 0xFFFFFFFFL) << 32);
        this.lh = (r2 & 0xFFFFFFFFL) | ((r3 & 0xFFFFFFFFL) << 32);
        this.hl = (r4 & 0xFFFFFFFFL) | ((r5 & 0xFFFFFFFFL) << 32);
        this.hh = (r6 & 0xFFFFFFFFL) | ((r7 & 0xFFFFFFFFL) << 32);
    }

    /**
     * Multiply this (unsigned) by 10^n in place
     */
    private void multiplyByPowerOf10InPlace(int n) {
        if (n <= 0 || isZero()) {
            return;
        }

        // For small powers, use lookup table
        if (n < 18) {
            long multiplier = POWERS_TEN_TABLE_LL[n];
            // Special case: if high is 0, use simple 64-bit multiplication
            if (hh == 0 && hl == 0 && lh == 0 && ll > 0) {
                // Check if result will overflow 64 bits
                if (ll <= Long.MAX_VALUE / multiplier) {
                    ll *= multiplier;
                    return;
                }
            }
        }

        // For larger powers, break down into smaller chunks, first check the threshold to ensure that we won't overflow
        // and then apply either a fast multiplyBy64 or multiplyBy128 depending on high/low.
        // The bound checks for these tables already happens at the beginning of the method.
        final long thresholdHH = n >= POWERS_TEN_TABLE_THRESHOLD_HH.length ? 0 : POWERS_TEN_TABLE_THRESHOLD_HH[n];
        final long thresholdHL = n >= POWERS_TEN_TABLE_THRESHOLD_HL.length ? 0 : POWERS_TEN_TABLE_THRESHOLD_HL[n];
        final long thresholdLH = n >= POWERS_TEN_TABLE_THRESHOLD_LH.length ? 0 : POWERS_TEN_TABLE_THRESHOLD_LH[n];
        final long thresholdLL = POWERS_TEN_TABLE_THRESHOLD_LL[n];

        if (compareTo(thresholdHH, thresholdHL, thresholdLH, thresholdLL) > 0) {
            throw NumericException.instance().put("Overflow");
        }

        final long multiplierHH = n >= 58 ? POWERS_TEN_TABLE_HH[n - 58] : 0L;
        final long multiplierHL = n >= 39 ? POWERS_TEN_TABLE_HL[n - 39] : 0L;
        final long multiplierLH = n >= 20 ? POWERS_TEN_TABLE_LH[n - 20] : 0L;
        final long multiplierLL = POWERS_TEN_TABLE_LL[n];

        if (multiplierHH != 0L /* || multiplierHL < 0L */) { // multiplierHL always false, keep comment to ack
            multiply256Unchecked(multiplierHH, multiplierHL, multiplierLH, multiplierLL);
        } else if (multiplierHL != 0L /* || multiplierLH < 0L */) { // multiplierLH always false, keep comment to ack
            multiply192Unchecked(multiplierHL, multiplierLH, multiplierLL);
        } else if (multiplierLH != 0L || multiplierLL < 0L) {
            multiply128Unchecked(multiplierLH, multiplierLL);
        } else {
            multiply64Unchecked(multiplierLL);
        }
    }

    /**
     * Set this Decimal256 from a byte array representation.
     *
     * @param bytes the byte array
     */
    private void setFromByteArray(byte[] bytes) {
        // Clear all fields first
        this.hh = 0;
        this.hl = 0;
        this.lh = 0;
        this.ll = 0;

        // Fill from the least significant bytes
        int byteIndex = bytes.length - 1;

        // Fill low 64 bits
        for (int i = 0; i < 8 && byteIndex >= 0; i++, byteIndex--) {
            this.ll |= ((long) (bytes[byteIndex] & 0xFF)) << (i * 8);
        }

        // Fill mid 64 bits
        for (int i = 0; i < 8 && byteIndex >= 0; i++, byteIndex--) {
            this.lh |= ((long) (bytes[byteIndex] & 0xFF)) << (i * 8);
        }

        // Fill high 64 bits
        for (int i = 0; i < 8 && byteIndex >= 0; i++, byteIndex--) {
            this.hl |= ((long) (bytes[byteIndex] & 0xFF)) << (i * 8);
        }

        // Fill highest 64 bits
        for (int i = 0; i < 8 && byteIndex >= 0; i++, byteIndex--) {
            this.hh |= ((long) (bytes[byteIndex] & 0xFF)) << (i * 8);
        }
    }

    /**
     * Check if addition resulted in a carry
     * When adding two unsigned numbers a + b = sum, carry occurs iff sum < a (or sum < b)
     * This works because:
     * - No carry: sum = a + b, so sum >= a and sum >= b
     * - Carry: sum = a + b - 2^64, so sum < a and sum < b
     */
    static boolean hasCarry(long a, long sum) {
        // We can check against either a or b - both work
        // Using a for consistency, b parameter kept for clarity
        return Long.compareUnsigned(sum, a) < 0;
    }

    static {
        for (int i = 0; i < ZERO_SCALED.length; i++) {
            ZERO_SCALED[i] = toBigDecimal(0, 0, 0, 0, i);
        }
    }
}