package io.questdb.cutlass.line;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class CairoLineProtoParserSupport {
    private final static Log LOG = LogFactory.getLog(CairoLineProtoParserSupport.class);
    public static final ObjList<ColumnWriter> writers = new ObjList<>();

    static {
        writers.extendAndSet(ColumnType.LONG, CairoLineProtoParserSupport::putLong);
        writers.extendAndSet(ColumnType.BOOLEAN, CairoLineProtoParserSupport::putBoolean);
        writers.extendAndSet(ColumnType.STRING, CairoLineProtoParserSupport::putStr);
        writers.extendAndSet(ColumnType.SYMBOL, CairoLineProtoParserSupport::putSymbol);
        writers.extendAndSet(ColumnType.DOUBLE, CairoLineProtoParserSupport::putDouble);
    }

    public interface ColumnWriter {
        void write(TableWriter.Row row, int columnIndex, CharSequence value) throws BadCastException;
    }

    public static class BadCastException extends Exception {
        private static final BadCastException INSTANCE = new BadCastException();
    }

    public static int getValueType(CharSequence token) {
        int len = token.length();
        switch (token.charAt(len - 1)) {
            case 'i':
                return ColumnType.LONG;
            case 'e':
                // tru(e)
                // fals(e)
            case 't':
            case 'T':
                // t
                // T
            case 'f':
            case 'F':
                // f
                // F
                return ColumnType.BOOLEAN;
            case '"':
                if (len < 2 || token.charAt(0) != '\"') {
                    LOG.error().$("incorrectly quoted string: ").$(token).$();
                    return -1;
                }
                return ColumnType.STRING;
            default:
                return ColumnType.DOUBLE;
        }
    }

    private static boolean isTrue(CharSequence value) {
        return (value.charAt(0) | 32) == 't';
    }

    public static void putSymbol(TableWriter.Row row, int index, CharSequence value) {
        row.putSym(index, value);
    }

    public static void putStr(TableWriter.Row row, int index, CharSequence value) {
        row.putStr(index, value, 1, value.length() - 2);
    }

    public static void putBoolean(TableWriter.Row row, int index, CharSequence value) {
        row.putBool(index, isTrue(value));
    }

    public static void putDouble(TableWriter.Row row, int index, CharSequence value) throws BadCastException {
        try {
            row.putDouble(index, Numbers.parseDouble(value));
        } catch (NumericException e) {
            LOG.error().$("not a DOUBLE: ").$(value).$();
            throw BadCastException.INSTANCE;
        }
    }

    public static void putLong(TableWriter.Row row, int index, CharSequence value) throws BadCastException {
        try {
            row.putLong(index, Numbers.parseLong(value, 0, value.length() - 1));
        } catch (NumericException e) {
            LOG.error().$("not an INT: ").$(value).$();
            throw BadCastException.INSTANCE;
        }
    }
}
