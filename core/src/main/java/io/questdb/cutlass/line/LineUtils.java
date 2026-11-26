package io.questdb.cutlass.line;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PhysicalDescriptor;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TypeDriver;
import io.questdb.cutlass.line.tcp.LineProtocolException;

public final class LineUtils {
    // columnKind() by the low byte of the column type, filled at init from columnKind(short)
    private static final int[] COLUMN_KIND_BY_CODE = new int[256];

    private LineUtils() {
    }

    /**
     * The tag the ILP appenders switch on for a column: the tag its accessor family is named after
     * (the column's own tag for every existing type), {@link ColumnType#GEOHASH} for every geohash
     * width, {@link ColumnType#DECIMAL} for every stored decimal width, {@link ColumnType#NULL} for
     * NULL, and {@link ColumnType#UNDEFINED} for the other pseudo tags, for VARCHAR_SLICE and for a
     * type unlike its family's namesake ({@link PhysicalDescriptor#isLikeFamilyNamesake}), none of
     * which an ILP entity can be cast to. The appenders' inner switches (one per entity type) label
     * their arms with these values and keep a throwing default for the pairs ILP does not convert.
     * One array read per call; the mapping is computed once, at class init.
     */
    public static int columnKind(int columnType) {
        return COLUMN_KIND_BY_CODE[columnType & 0xFF];
    }

    /**
     * Converts a line protocol timestamp to the precision of the target timestamp column. Every ILP
     * timestamp conversion must funnel through this method: {@link TimestampDriver#from(long, byte)}
     * signals a value the column cannot hold with an {@link ArithmeticException} and a unit it does
     * not support with an {@link UnsupportedOperationException}, and neither classifies as a
     * per-message rejection. The ILP callers cannot tell either apart from an infrastructure
     * failure, so they escalate to closing the table writer, wedging every other producer writing
     * to the same table. A {@link LineProtocolException} instead rejects just the offending message
     * and leaves the writer alone.
     *
     * @param driver timestamp driver of the target timestamp column
     * @param ts     timestamp, in the units the producer sent
     * @param unit   units of {@code ts}
     * @return the timestamp converted to the column's precision
     */
    public static long from(TimestampDriver driver, long ts, byte unit) {
        try {
            return driver.from(ts, unit);
        } catch (ArithmeticException e) {
            throw LineProtocolException.timestampValueOverflow(ts);
        } catch (UnsupportedOperationException e) {
            // TIMESTAMP_UNIT_UNSET lands here: a producer that sends the value as a plain integer
            // field leaves the unit unset, and so does a binary entity carrying an unknown unit byte
            throw LineProtocolException.unsupportedTimestampUnit(unit);
        }
    }

    /**
     * Converts a line protocol designated timestamp to the precision of the designated timestamp
     * column and checks it against the bounds the storage layer enforces. Every ILP entry point
     * must funnel the designated timestamp through this method: {@code TableWriter}/{@code WalWriter}
     * reject an out-of-range value with a plain {@link CairoException}, which the ILP callers
     * cannot tell apart from an infrastructure failure and therefore escalate to closing the table
     * writer. A {@link LineProtocolException} instead classifies as a per-message rejection and
     * leaves the writer alone.
     * <p>
     * The ceiling is the column's own: {@link TimestampDriver#validateBounds(long)} caps a
     * micros designated timestamp at 9999-12-31 and a nanos one at 2261-12-31, and the writer
     * applies the same check, so nothing this method accepts is refused downstream.
     *
     * @param driver         timestamp driver of the designated timestamp column
     * @param ts             designated timestamp, in the units the producer sent
     * @param unit           units of {@code ts}
     * @param tableNameUtf16 table name, for the error message
     * @return the timestamp converted to the column's precision
     */
    public static long fromDesignatedTimestamp(TimestampDriver driver, long ts, byte unit, String tableNameUtf16) {
        if (ts < 0) {
            // Numbers.LONG_NULL is negative, so this rejects a NULL designated timestamp too
            throw LineProtocolException.designatedTimestampMustBePositive(tableNameUtf16, ts);
        }
        final long timestamp = from(driver, ts, unit);
        try {
            driver.validateBounds(timestamp);
        } catch (CairoException e) {
            throw LineProtocolException.designatedTimestampOutOfBounds(tableNameUtf16, timestamp, e.getFlyweightMessage());
        }
        return timestamp;
    }

    /**
     * Maps a tag to its ILP kind by its accessor family: ILP parses and writes a value with its
     * family's parser and putter, and a field the line leaves out is stored as the column's NULL,
     * so a new type that reads through an existing family takes that family's arms. A tag without a
     * family (a pseudo tag, VARCHAR_SLICE) maps to UNDEFINED, except NULL, which maps to itself. A
     * type unlike its family's namesake maps to UNDEFINED as well, since the family's parser would
     * read its values and NULL as the namesake's; ILP then refuses the column's values.
     */
    private static int columnKind(short code) {
        final TypeDriver driver = PhysicalDescriptor.storedTypeDriverOf(code);
        if (driver == null) {
            return code == ColumnType.NULL ? ColumnType.NULL : ColumnType.UNDEFINED;
        }
        if (!PhysicalDescriptor.isLikeFamilyNamesake(driver)) {
            return ColumnType.UNDEFINED;
        }
        final PhysicalDescriptor.Accessor accessor = driver.getAccessor();
        return switch (accessor) {
            case BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, STRING, SYMBOL, LONG256,
                 BINARY, UUID, LONG128, IPv4, VARCHAR, ARRAY, INTERVAL -> accessor.opcode();
            case GEOBYTE, GEOSHORT, GEOINT, GEOLONG -> ColumnType.GEOHASH;
            case DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 -> ColumnType.DECIMAL;
        };
    }

    static {
        for (short code = 0; code < COLUMN_KIND_BY_CODE.length; code++) {
            COLUMN_KIND_BY_CODE[code] = columnKind(code);
        }
    }
}
