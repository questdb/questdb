/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.common.ColumnType;
import com.questdb.common.Record;
import com.questdb.common.RecordColumnMetadata;
import com.questdb.common.SymbolTable;
import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.engine.functions.*;
import com.questdb.griffin.lexer.ExprAstBuilder;
import com.questdb.griffin.lexer.ExprParser;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Lexer2;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectPool;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class FunctionParserTest extends AbstractCairoTest {
    private static final ObjectPool<ExprNode> exprNodeObjectPool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private static final Lexer2 lexer = new Lexer2();
    private static final ExprParser parser = new ExprParser(exprNodeObjectPool);
    private static final ExprAstBuilder astBuilder = new ExprAstBuilder();
    private static final CharSequenceObjHashMap<Parameter> params = new CharSequenceObjHashMap<>();
    private static final ArrayList<FunctionFactory> iterable = new ArrayList<>();

    @Before
    public void setUp2() {
        params.clear();
        exprNodeObjectPool.clear();
        iterable.clear();
        ExprParser.configureLexer(lexer);
    }

    @Test
    public void testByteAndLongToFloatCast() throws ParserException {
        assertCastToFloat(363, ColumnType.BYTE, ColumnType.LONG, new Record() {

            @Override
            public byte getByte(int col) {
                return 18;
            }

            @Override
            public long getLong(int col) {
                return 345;
            }
        });
    }

    @Test
    public void testByteAndShortToIntCast() throws ParserException {
        final FunctionFactory ff = new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(+I+I)";
            }

            @Override
            public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration1) {
                return new IntFunction() {
                    final Function left = args.getQuick(0);
                    final Function right = args.getQuick(1);

                    @Override
                    public int getInt(Record rec) {
                        return left.getInt(rec) + right.getInt(rec);
                    }
                };
            }
        };

        iterable.add(ff);

        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new TestColumnMetadata("a", ColumnType.BYTE));
        metadata.add(new TestColumnMetadata("b", ColumnType.SHORT));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        Assert.assertEquals(ColumnType.INT, function.getType());
        Assert.assertEquals(33, function.getInt(new Record() {
            @Override
            public byte getByte(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        }));
    }

    @Test
    public void testByteToDoubleCast() throws ParserException {
        assertCastToDouble(131, ColumnType.BYTE, ColumnType.BYTE, new Record() {
            @Override
            public byte getByte(int col) {
                if (col == 0) {
                    return 41;
                }
                return 90;
            }
        });
    }

    @Test
    public void testByteToLongCast() throws ParserException {
        assertCastToLong(131, ColumnType.BYTE, ColumnType.BYTE, new Record() {
            @Override
            public byte getByte(int col) {
                if (col == 0) {
                    return 41;
                }
                return 90;
            }
        });
    }

    @Test
    public void testByteToShortCast() throws ParserException {
        final FunctionFactory ff = new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(+E+E)";
            }

            @Override
            public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration1) {
                return new ShortFunction() {
                    final Function left = args.getQuick(0);
                    final Function right = args.getQuick(1);

                    @Override
                    public short getShort(Record rec) {
                        return (short) (left.getShort(rec) + right.getShort(rec));
                    }
                };
            }
        };

        iterable.add(ff);

        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new TestColumnMetadata("a", ColumnType.BYTE));
        metadata.add(new TestColumnMetadata("b", ColumnType.BYTE));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        Assert.assertEquals(ColumnType.SHORT, function.getType());
        Assert.assertEquals(131, function.getShort(new Record() {
            @Override
            public byte getByte(int col) {
                if (col == 0) {
                    return 41;
                }
                return 90;
            }
        }));
    }

    @Test
    public void testFloatAndLongToDoubleCast() throws ParserException {
        assertCastToDouble(468.3, ColumnType.FLOAT, ColumnType.LONG, new Record() {
            @Override
            public float getFloat(int col) {
                return 123.3f;
            }

            @Override
            public long getLong(int col) {
                return 345;
            }
        });
    }

    @Test
    public void testIntAndShortToDoubleCast() throws ParserException {
        assertCastToDouble(33, ColumnType.INT, ColumnType.SHORT, new Record() {
            @Override
            public int getInt(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        });
    }

    @Test
    public void testIntAndShortToFloatCast() throws ParserException {
        assertCastToFloat(33, ColumnType.INT, ColumnType.SHORT, new Record() {
            @Override
            public int getInt(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        });
    }

    @Test
    public void testIntAndShortToLongCast() throws ParserException {
        assertCastToLong(33, ColumnType.INT, ColumnType.SHORT, new Record() {
            @Override
            public int getInt(int col) {
                return 12;
            }

            @Override
            public short getShort(int col) {
                return 21;
            }
        });
    }

    @Test
    public void testSimpleFunction() throws ParserException {
        assertCastToDouble(13.1, ColumnType.DOUBLE, ColumnType.DOUBLE, new Record() {
            @Override
            public double getDouble(int col) {
                if (col == 0) {
                    return 5.3;
                }
                return 7.8;
            }
        });
    }

    private void assertCastToDouble(double expected, int type1, int type2, Record record) throws ParserException {
        final FunctionFactory ff = new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(+D+D)";
            }

            @Override
            public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
                return new DoubleFunction() {
                    final Function left = args.getQuick(0);
                    final Function right = args.getQuick(1);

                    @Override
                    public double getDouble(Record rec) {
                        return left.getDouble(rec) + right.getDouble(rec);
                    }
                };
            }
        };

        iterable.add(ff);

        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new TestColumnMetadata("a", type1));
        metadata.add(new TestColumnMetadata("b", type2));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        Assert.assertEquals(ColumnType.DOUBLE, function.getType());
        Assert.assertEquals(expected, function.getDouble(record), 0.00001);
    }

    private void assertCastToFloat(float expected, int type1, int type2, Record record) throws ParserException {
        final FunctionFactory ff = new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(+F+F)";
            }

            @Override
            public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
                return new FloatFunction() {
                    final Function left = args.getQuick(0);
                    final Function right = args.getQuick(1);

                    @Override
                    public float getFloat(Record rec) {
                        return left.getFloat(rec) + right.getFloat(rec);
                    }
                };
            }
        };

        iterable.add(ff);

        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new TestColumnMetadata("a", type1));
        metadata.add(new TestColumnMetadata("b", type2));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        Assert.assertEquals(ColumnType.FLOAT, function.getType());
        Assert.assertEquals(expected, function.getFloat(record), 0.00001);
    }

    private void assertCastToLong(long expected, int type1, int type2, Record record) throws ParserException {
        final FunctionFactory ff = new FunctionFactory() {
            @Override
            public String getSignature() {
                return "+(+L+L)";
            }

            @Override
            public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
                return new LongFunction() {
                    final Function left = args.getQuick(0);
                    final Function right = args.getQuick(1);

                    @Override
                    public long getLong(Record rec) {
                        return left.getLong(rec) + right.getLong(rec);
                    }
                };
            }
        };

        iterable.add(ff);

        final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        metadata.add(new TestColumnMetadata("a", type1));
        metadata.add(new TestColumnMetadata("b", type2));
        FunctionParser functionParser = createFunctionParser();
        Function function = parseFunction("a+b", metadata, functionParser);
        Assert.assertEquals(ColumnType.LONG, function.getType());
        Assert.assertEquals(expected, function.getLong(record));
    }

    @NotNull
    private FunctionParser createFunctionParser() {
        return new FunctionParser(configuration, iterable);
    }

    private ExprNode expr(CharSequence expression) throws ParserException {
        lexer.setContent(expression);
        astBuilder.reset();
        parser.parseExpr(lexer, astBuilder);
        return astBuilder.poll();
    }

    private Function parseFunction(CharSequence expression, CollectionRecordMetadata metadata, FunctionParser functionParser) throws ParserException {
        return functionParser.parseFunction(expr(expression), metadata, params);
    }

    private class TestColumnMetadata implements RecordColumnMetadata {
        private final String name;
        private final int type;

        public TestColumnMetadata(String name, int type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public int getBucketCount() {
            return 0;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public SymbolTable getSymbolTable() {
            return null;
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public boolean isIndexed() {
            return false;
        }
    }
}