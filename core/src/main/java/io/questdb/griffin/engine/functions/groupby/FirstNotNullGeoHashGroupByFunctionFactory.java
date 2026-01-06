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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class FirstNotNullGeoHashGroupByFunctionFactory implements FunctionFactory {
    public static final String NAME = "first_not_null";

    @Override
    public String getSignature() {
        return "first_not_null(G)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function function = args.getQuick(0);
        int type = function.getType();

        return switch (ColumnType.tagOf(type)) {
            case ColumnType.GEOBYTE -> new FirstNotNullGeoHashGroupByFunctionByte(type, function);
            case ColumnType.GEOSHORT -> new FirstNotNullGeoHashGroupByFunctionShort(type, function);
            case ColumnType.GEOINT -> new FirstNotNullGeoHashGroupByFunctionInt(type, function);
            default -> new FirstNotNullGeoHashGroupByFunctionLong(type, function);
        };
    }

    private static class FirstNotNullGeoHashGroupByFunctionByte extends FirstGeoHashGroupByFunctionByte {

        public FirstNotNullGeoHashGroupByFunctionByte(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                final long hi = ptr + count;
                for (; ptr < hi; ptr++) {
                    byte value = Unsafe.getUnsafe().getByte(ptr);
                    if (value != GeoHashes.BYTE_NULL) {
                        mapValue.putByte(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (mapValue.getGeoByte(valueIndex + 1) == GeoHashes.BYTE_NULL) {
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
            // srcRowId is non-null at this point since we know that the value is non-null
            if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putByte(valueIndex + 1, srcVal);
            }
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionInt extends FirstGeoHashGroupByFunctionInt {
        public FirstNotNullGeoHashGroupByFunctionInt(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                final long hi = ptr + count * 4L;
                for (; ptr < hi; ptr += 4L) {
                    int value = Unsafe.getUnsafe().getInt(ptr);
                    if (value != GeoHashes.INT_NULL) {
                        mapValue.putInt(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (mapValue.getGeoInt(valueIndex + 1) == GeoHashes.INT_NULL) {
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
            // srcRowId is non-null at this point since we know that the value is non-null
            if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putInt(valueIndex + 1, srcVal);
            }
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionLong extends FirstGeoHashGroupByFunctionLong {
        public FirstNotNullGeoHashGroupByFunctionLong(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                final long hi = ptr + count * 8L;
                for (; ptr < hi; ptr += 8L) {
                    long value = Unsafe.getUnsafe().getLong(ptr);
                    if (value != GeoHashes.NULL) {
                        mapValue.putLong(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (mapValue.getGeoLong(valueIndex + 1) == GeoHashes.NULL) {
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
            // srcRowId is non-null at this point since we know that the value is non-null
            if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putLong(valueIndex + 1, srcVal);
            }
        }
    }

    private static class FirstNotNullGeoHashGroupByFunctionShort extends FirstGeoHashGroupByFunctionShort {
        public FirstNotNullGeoHashGroupByFunctionShort(int type, Function function) {
            super(type, function);
        }

        @Override
        public void computeBatch(MapValue mapValue, long ptr, int count) {
            if (count > 0) {
                final long hi = ptr + count * 2L;
                for (; ptr < hi; ptr += 2L) {
                    short value = Unsafe.getUnsafe().getShort(ptr);
                    if (value != GeoHashes.SHORT_NULL) {
                        mapValue.putShort(valueIndex + 1, value);
                        break;
                    }
                }
            }
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            if (mapValue.getGeoShort(valueIndex + 1) == GeoHashes.SHORT_NULL) {
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
            // srcRowId is non-null at this point since we know that the value is non-null
            if (srcRowId < destRowId || destRowId == Numbers.LONG_NULL) {
                destValue.putLong(valueIndex, srcRowId);
                destValue.putShort(valueIndex + 1, srcVal);
            }
        }
    }
}
