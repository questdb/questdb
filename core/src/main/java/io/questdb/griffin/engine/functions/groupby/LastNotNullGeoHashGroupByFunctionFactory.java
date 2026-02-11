/*******************************************************************************
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class LastNotNullGeoHashGroupByFunctionFactory implements FunctionFactory {

    public static final String NAME = "last_not_null";

    @Override
    public String getSignature() {
        return "last_not_null(G)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function function = args.getQuick(0);
        int type = function.getType();

        return switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE -> new LastNotNullGeoHashGroupByFunctionByte(type, function);
            case ColumnType.GEOSHORT -> new LastNotNullGeoHashGroupByFunctionShort(type, function);
            case ColumnType.GEOINT -> new LastNotNullGeoHashGroupByFunctionInt(type, function);
            default -> new LastNotNullGeoHashGroupByFunctionLong(type, function);
        };
    }

    private static class LastNotNullGeoHashGroupByFunctionByte extends FirstGeoHashGroupByFunctionByte {
        public LastNotNullGeoHashGroupByFunctionByte(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                long hi = ptr + count - 1;
                for (; hi >= ptr; hi--) {
                    byte value = Unsafe.getUnsafe().getByte(hi);
                    if (value != GeoHashes.BYTE_NULL) {
                        mapValue.putByte(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (arg.getGeoByte(record) != GeoHashes.BYTE_NULL) {
                computeFirst(mapValue, record, rowId);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            byte srcVal = srcValue.getGeoByte(valueIndex + 1);
            if (srcVal == GeoHashes.BYTE_NULL) {
                return;
            }
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putByte(valueIndex + 1, srcVal);
            }
        }
    }

    private static class LastNotNullGeoHashGroupByFunctionInt extends FirstGeoHashGroupByFunctionInt {
        public LastNotNullGeoHashGroupByFunctionInt(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                long hi = ptr + (count - 1) * 4L;
                for (; hi >= ptr; hi -= 4L) {
                    int value = Unsafe.getUnsafe().getInt(hi);
                    if (value != GeoHashes.INT_NULL) {
                        mapValue.putInt(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (arg.getGeoInt(record) != GeoHashes.INT_NULL) {
                computeFirst(mapValue, record, rowId);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            int srcVal = srcValue.getGeoInt(valueIndex + 1);
            if (srcVal == GeoHashes.INT_NULL) {
                return;
            }
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putInt(valueIndex + 1, srcVal);
            }
        }
    }

    private static class LastNotNullGeoHashGroupByFunctionLong extends FirstGeoHashGroupByFunctionLong {
        public LastNotNullGeoHashGroupByFunctionLong(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                long hi = ptr + (count - 1) * 8L;
                for (; hi >= ptr; hi -= 8L) {
                    long value = Unsafe.getUnsafe().getLong(hi);
                    if (value != GeoHashes.NULL) {
                        mapValue.putLong(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (arg.getGeoLong(record) != GeoHashes.NULL) {
                computeFirst(mapValue, record, rowId);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long srcVal = srcValue.getGeoLong(valueIndex + 1);
            if (srcVal == GeoHashes.NULL) {
                return;
            }
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putLong(valueIndex + 1, srcVal);
            }
        }
    }

    private static class LastNotNullGeoHashGroupByFunctionShort extends FirstGeoHashGroupByFunctionShort {
        public LastNotNullGeoHashGroupByFunctionShort(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                long hi = ptr + (count - 1) * 2L;
                for (; hi >= ptr; hi -= 2L) {
                    short value = Unsafe.getUnsafe().getShort(hi);
                    if (value != GeoHashes.SHORT_NULL) {
                        mapValue.putShort(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (arg.getGeoShort(record) != GeoHashes.SHORT_NULL) {
                computeFirst(mapValue, record, rowId);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            short srcVal = srcValue.getGeoShort(valueIndex + 1);
            if (srcVal == GeoHashes.SHORT_NULL) {
                return;
            }
            long srcRowId = srcValue.getLong(valueIndex);
            long destRowId = destValue.getLong(valueIndex);
            if (srcRowId > destRowId) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putShort(valueIndex + 1, srcVal);
            }
        }
    }
}
