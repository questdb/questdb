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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.bool.AndFunctionFactory;
import io.questdb.griffin.engine.functions.bool.BetweenTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InLongFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InStrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.NotFunctionFactory;
import io.questdb.griffin.engine.functions.bool.OrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastBooleanToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastByteToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastCharToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDateToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastDoubleToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastFloatToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastGeoHashToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIPv4ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastIntToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLong256ToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemAvgFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemMaxFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemMinFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayElemSumFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToUuidFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastUuidToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToGeoHashFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToLong256FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToUuidFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToLongFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayPositionFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayInsertionPointFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayInsertionPointAfterEqualFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayFlattenFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayTransposeFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayCumSumFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayShiftFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayShiftDefaultNaNFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayRoundFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqDoubleArrayFunctionFactory;
import io.questdb.griffin.engine.functions.array.ArrayCreateFunctionFactory;
import io.questdb.griffin.engine.functions.array.ArrayDimLengthFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayAvgFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayCountFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayMaxFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayMinFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayReverseFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArraySortDescFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArraySortFullFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArraySortFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayStdDevFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayStdDevPopFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayStdDevSampFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArraySumFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArrayAccessFunctionFactory;
import io.questdb.griffin.engine.functions.array.DoubleArraySliceFunctionFactory;
import io.questdb.griffin.engine.functions.array.IntIntervalFunctionFactory;
import io.questdb.griffin.engine.functions.array.IntIntervalRightOpenFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastNullTypeFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastShortToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastTimestampToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToBooleanFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToCharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.CaseFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.NullIfDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.NullIfIntFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.NullIfLongFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.NullIfStrFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.NullIfVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.date.AddLongToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.DateTruncFunctionFactory;
import io.questdb.griffin.engine.functions.date.DayOfMonthFunctionFactory;
import io.questdb.griffin.engine.functions.date.DayOfWeekFunctionFactory;
import io.questdb.griffin.engine.functions.date.DayOfWeekSundayFirstFunctionFactory;
import io.questdb.griffin.engine.functions.date.DaysPerMonthFunctionFactory;
import io.questdb.griffin.engine.functions.date.ExtractFromTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.HourOfDayFunctionFactory;
import io.questdb.griffin.engine.functions.date.IsEndOfMonthFunctionFactory;
import io.questdb.griffin.engine.functions.date.IsLeapYearFunctionFactory;
import io.questdb.griffin.engine.functions.date.MicrosOfMillsFunctionFactory;
import io.questdb.griffin.engine.functions.date.MillisOfSecondFunctionFactory;
import io.questdb.griffin.engine.functions.date.MinuteOfHourFunctionFactory;
import io.questdb.griffin.engine.functions.date.MonthOfYearFunctionFactory;
import io.questdb.griffin.engine.functions.date.NanosOfMicrosFunctionFactory;
import io.questdb.griffin.engine.functions.date.SecondOfMinuteFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampCeilFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampDiffFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFromOffsetUtcFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToNanoTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToNanoTimestampVCFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToStrDateFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToStrTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToTimestampVCFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToTimezoneTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.WeekOfYearFunctionFactory;
import io.questdb.griffin.engine.functions.date.YearFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqCharCharFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqDateFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIPv4FunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIPv4StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqUuidFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqUuidStrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqLong256FunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqLong256StrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntStrCFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqStrCharFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqStrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymCharFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymLongFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqVarcharStrFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtCharFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtDateFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtDoubleVVFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtIntFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtStrFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtStrVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtVarcharStrFunctionFactory;
import io.questdb.griffin.engine.functions.math.AbsDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.AbsIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.AbsLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.AbsShortFunctionFactory;
import io.questdb.griffin.engine.functions.math.AcosDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.AsinDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.Atan2DoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.AtanDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseAndIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseAndLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseNotIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseNotLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseOrIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseOrLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseXorIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.BitwiseXorLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.CeilDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.CeilFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.CeilingDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.CeilingFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.CosDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.CotDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.DegreesDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.DivDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.DivFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.DivIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.DivLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.ExpDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.FloorDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.FloorFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.LnDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.LogDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.MulDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.MulFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.MulIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.MulLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegByteFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.NegShortFunctionFactory;
import io.questdb.griffin.engine.functions.math.PIDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.PowDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RadiansDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RemDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RemFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.RemIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.RemLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundDoubleZeroScaleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundDownDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundHalfEvenDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.RoundUpDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.SignByteFunctionFactory;
import io.questdb.griffin.engine.functions.math.SignDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.SignFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.SignIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.SignLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.SignShortFunctionFactory;
import io.questdb.griffin.engine.functions.math.SinDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.SqrtDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubFloatFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.math.TanDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.regex.ILikeStrFunctionFactory;
import io.questdb.griffin.engine.functions.regex.ILikeSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.ILikeVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.regex.LikeStrFunctionFactory;
import io.questdb.griffin.engine.functions.regex.LikeSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.LikeVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.ConcatFunctionFactory;
import io.questdb.griffin.engine.functions.str.LTrimStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.LTrimVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.LeftStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.LeftVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthBytesVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.LowerFunctionFactory;
import io.questdb.griffin.engine.functions.str.LowerVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.PositionFunctionFactory;
import io.questdb.griffin.engine.functions.str.PositionVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.RTrimStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.RTrimVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.ReplaceStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.ReplaceVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.RightStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.RightVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StartsWithStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.StartsWithVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.SubStringFunctionFactory;
import io.questdb.griffin.engine.functions.str.SubStringVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToLowercaseFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToLowercaseVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToUppercaseFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToUppercaseVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.TrimStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.TrimVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.UpperFunctionFactory;
import io.questdb.griffin.engine.functions.str.UpperVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountGroupByFunctionFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;

public class FunctionFactoryDescriptor {
    private static final int ARRAY_MASK = 1 << 31;
    private static final int CONST_MASK = 1 << 30;
    private static final int TYPE_MASK = ~(ARRAY_MASK | CONST_MASK);
    private static final IntObjHashMap<String> typeNameMap = new IntObjHashMap<>();
    private final long[] argTypes;
    private FunctionFactoryDescriptor commutedEquality;
    private final FunctionFactory factory;
    private final boolean isAnd;
    private final boolean isArrayAccess;
    private final boolean isArrayColumnLayoutSensitive;
    private final boolean isArrayElementWiseScalar;
    private final boolean isCase;
    private final boolean isOr;
    private final boolean isOrderSensitiveAggregate;
    private final boolean isRelocatableScalar;
    private final boolean isRowCount;
    private final boolean isSwitch;
    private final String name;
    private final int openParenIndex;
    private final int sigArgCount;

    public FunctionFactoryDescriptor(FunctionFactory factory) throws SqlException {
        this(factory, factory.getClass() == AddDoubleFunctionFactory.class
                || factory.getClass() == AddFloatFunctionFactory.class
                || factory.getClass() == AddIntFunctionFactory.class
                || factory.getClass() == AddLongFunctionFactory.class
                || factory.getClass() == AbsShortFunctionFactory.class
                || factory.getClass() == AbsIntFunctionFactory.class
                || factory.getClass() == AbsLongFunctionFactory.class
                || factory.getClass() == AbsDoubleFunctionFactory.class
                || factory.getClass() == AcosDoubleFunctionFactory.class
                || factory.getClass() == AsinDoubleFunctionFactory.class
                || factory.getClass() == Atan2DoubleFunctionFactory.class
                || factory.getClass() == AtanDoubleFunctionFactory.class
                || factory.getClass() == CeilDoubleFunctionFactory.class
                || factory.getClass() == CeilFloatFunctionFactory.class
                || factory.getClass() == CeilingDoubleFunctionFactory.class
                || factory.getClass() == CeilingFloatFunctionFactory.class
                || factory.getClass() == CosDoubleFunctionFactory.class
                || factory.getClass() == CotDoubleFunctionFactory.class
                || factory.getClass() == DegreesDoubleFunctionFactory.class
                || factory.getClass() == ExpDoubleFunctionFactory.class
                || factory.getClass() == FloorDoubleFunctionFactory.class
                || factory.getClass() == FloorFloatFunctionFactory.class
                || factory.getClass() == LnDoubleFunctionFactory.class
                || factory.getClass() == LogDoubleFunctionFactory.class
                || factory.getClass() == PIDoubleFunctionFactory.class
                || factory.getClass() == PowDoubleFunctionFactory.class
                || factory.getClass() == RadiansDoubleFunctionFactory.class
                || factory.getClass() == RemDoubleFunctionFactory.class
                || factory.getClass() == RemFloatFunctionFactory.class
                || factory.getClass() == RemIntFunctionFactory.class
                || factory.getClass() == RemLongFunctionFactory.class
                || factory.getClass() == RoundDoubleZeroScaleFunctionFactory.class
                || factory.getClass() == RoundDoubleFunctionFactory.class
                || factory.getClass() == RoundDownDoubleFunctionFactory.class
                || factory.getClass() == RoundUpDoubleFunctionFactory.class
                || factory.getClass() == RoundHalfEvenDoubleFunctionFactory.class
                || factory.getClass() == SignByteFunctionFactory.class
                || factory.getClass() == SignDoubleFunctionFactory.class
                || factory.getClass() == SignFloatFunctionFactory.class
                || factory.getClass() == SignIntFunctionFactory.class
                || factory.getClass() == SignLongFunctionFactory.class
                || factory.getClass() == SignShortFunctionFactory.class
                || factory.getClass() == SinDoubleFunctionFactory.class
                || factory.getClass() == SqrtDoubleFunctionFactory.class
                || factory.getClass() == TanDoubleFunctionFactory.class
                || factory.getClass() == NegByteFunctionFactory.class
                || factory.getClass() == NegShortFunctionFactory.class
                || factory.getClass() == AddLongToTimestampFunctionFactory.class
                || factory.getClass() == DateTruncFunctionFactory.class
                || factory.getClass() == DayOfMonthFunctionFactory.class
                || factory.getClass() == DayOfWeekFunctionFactory.class
                || factory.getClass() == DayOfWeekSundayFirstFunctionFactory.class
                || factory.getClass() == DaysPerMonthFunctionFactory.class
                || factory.getClass() == ExtractFromTimestampFunctionFactory.class
                || factory.getClass() == HourOfDayFunctionFactory.class
                || factory.getClass() == IsEndOfMonthFunctionFactory.class
                || factory.getClass() == IsLeapYearFunctionFactory.class
                || factory.getClass() == MicrosOfMillsFunctionFactory.class
                || factory.getClass() == MillisOfSecondFunctionFactory.class
                || factory.getClass() == MinuteOfHourFunctionFactory.class
                || factory.getClass() == MonthOfYearFunctionFactory.class
                || factory.getClass() == NanosOfMicrosFunctionFactory.class
                || factory.getClass() == SecondOfMinuteFunctionFactory.class
                || factory.getClass() == TimestampAddFunctionFactory.class
                || factory.getClass() == TimestampCeilFunctionFactory.class
                || factory.getClass() == TimestampDiffFunctionFactory.class
                || factory.getClass() == TimestampFloorFunctionFactory.class
                || factory.getClass() == TimestampFloorFromFunctionFactory.class
                || factory.getClass() == TimestampFloorFromOffsetUtcFunctionFactory.class
                || factory.getClass() == ToDateFunctionFactory.class
                || factory.getClass() == ToNanoTimestampFunctionFactory.class
                || factory.getClass() == ToNanoTimestampVCFunctionFactory.class
                || factory.getClass() == ToStrDateFunctionFactory.class
                || factory.getClass() == ToStrTimestampFunctionFactory.class
                || factory.getClass() == ToTimestampFunctionFactory.class
                || factory.getClass() == ToTimestampVCFunctionFactory.class
                || factory.getClass() == ToTimezoneTimestampFunctionFactory.class
                || factory.getClass() == ToUTCTimestampFunctionFactory.class
                || factory.getClass() == WeekOfYearFunctionFactory.class
                || factory.getClass() == YearFunctionFactory.class
                || factory.getClass() == EqDateFunctionFactory.class
                || factory.getClass() == EqCharCharFunctionFactory.class
                || factory.getClass() == LtDateFunctionFactory.class
                || factory.getClass() == LtCharFunctionFactory.class
                || factory.getClass() == InLongFunctionFactory.class
                || factory.getClass() == InDoubleFunctionFactory.class
                || factory.getClass() == BetweenTimestampFunctionFactory.class
                || factory.getClass() == InTimestampTimestampFunctionFactory.class
                || factory.getClass() == CastIPv4ToStrFunctionFactory.class
                || factory.getClass() == LeftStrFunctionFactory.class
                || factory.getClass() == RightStrFunctionFactory.class
                || factory.getClass() == SubStringFunctionFactory.class
                || factory.getClass() == ReplaceStrFunctionFactory.class
                || factory.getClass() == StrPosFunctionFactory.class
                || factory.getClass() == PositionFunctionFactory.class
                || factory.getClass() == LeftVarcharFunctionFactory.class
                || factory.getClass() == RightVarcharFunctionFactory.class
                || factory.getClass() == SubStringVarcharFunctionFactory.class
                || factory.getClass() == ReplaceVarcharFunctionFactory.class
                || factory.getClass() == StrPosVarcharFunctionFactory.class
                || factory.getClass() == PositionVarcharFunctionFactory.class
                || factory.getClass() == ConcatFunctionFactory.class
                || factory.getClass() == LengthVarcharFunctionFactory.class
                || factory.getClass() == LengthBytesVarcharFunctionFactory.class
                || factory.getClass() == LowerVarcharFunctionFactory.class
                || factory.getClass() == UpperVarcharFunctionFactory.class
                || factory.getClass() == ToLowercaseVarcharFunctionFactory.class
                || factory.getClass() == ToUppercaseVarcharFunctionFactory.class
                || factory.getClass() == TrimVarcharFunctionFactory.class
                || factory.getClass() == LTrimVarcharFunctionFactory.class
                || factory.getClass() == RTrimVarcharFunctionFactory.class
                || factory.getClass() == CastStrToVarcharFunctionFactory.class
                || factory.getClass() == CastVarcharToStrFunctionFactory.class
                || factory.getClass() == EqSymStrFunctionFactory.class
                || factory.getClass() == EqSymCharFunctionFactory.class
                || factory.getClass() == EqSymFunctionFactory.class
                || factory.getClass() == InSymbolFunctionFactory.class
                || factory.getClass() == LikeSymbolFunctionFactory.class
                || factory.getClass() == ILikeSymbolFunctionFactory.class
                || factory.getClass() == LengthSymbolFunctionFactory.class
                || factory.getClass() == CastSymbolToStrFunctionFactory.class
                || factory.getClass() == CastSymbolToVarcharFunctionFactory.class
                || factory.getClass() == EqVarcharFunctionFactory.class
                || factory.getClass() == EqVarcharStrFunctionFactory.class
                || factory.getClass() == LtVarcharFunctionFactory.class
                || factory.getClass() == LtStrVarcharFunctionFactory.class
                || factory.getClass() == LtVarcharStrFunctionFactory.class
                || factory.getClass() == StartsWithStrFunctionFactory.class
                || factory.getClass() == StartsWithVarcharFunctionFactory.class
                || factory.getClass() == InStrFunctionFactory.class
                || factory.getClass() == InVarcharFunctionFactory.class
                || factory.getClass() == NullIfStrFunctionFactory.class
                || factory.getClass() == NullIfVarcharFunctionFactory.class
                || factory.getClass() == LikeStrFunctionFactory.class
                || factory.getClass() == ILikeStrFunctionFactory.class
                || factory.getClass() == LikeVarcharFunctionFactory.class
                || factory.getClass() == ILikeVarcharFunctionFactory.class
                || factory.getClass() == LengthStrFunctionFactory.class
                || factory.getClass() == LowerFunctionFactory.class
                || factory.getClass() == UpperFunctionFactory.class
                || factory.getClass() == ToLowercaseFunctionFactory.class
                || factory.getClass() == ToUppercaseFunctionFactory.class
                || factory.getClass() == TrimStrFunctionFactory.class
                || factory.getClass() == LTrimStrFunctionFactory.class
                || factory.getClass() == RTrimStrFunctionFactory.class
                || factory.getClass() == CaseFunctionFactory.class
                || factory.getClass() == SwitchFunctionFactory.class
                || factory.getClass() == AndFunctionFactory.class
                || factory.getClass() == BitwiseAndIntFunctionFactory.class
                || factory.getClass() == BitwiseAndLongFunctionFactory.class
                || factory.getClass() == BitwiseNotIntFunctionFactory.class
                || factory.getClass() == BitwiseNotLongFunctionFactory.class
                || factory.getClass() == BitwiseOrIntFunctionFactory.class
                || factory.getClass() == BitwiseOrLongFunctionFactory.class
                || factory.getClass() == BitwiseXorIntFunctionFactory.class
                || factory.getClass() == BitwiseXorLongFunctionFactory.class
                || factory.getClass() == CastBooleanToByteFunctionFactory.class
                || factory.getClass() == CastBooleanToCharFunctionFactory.class
                || factory.getClass() == CastBooleanToDoubleFunctionFactory.class
                || factory.getClass() == CastBooleanToFloatFunctionFactory.class
                || factory.getClass() == CastBooleanToIntFunctionFactory.class
                || factory.getClass() == CastBooleanToLongFunctionFactory.class
                || factory.getClass() == CastBooleanToShortFunctionFactory.class
                || factory.getClass() == CastBooleanToStrFunctionFactory.class
                || factory.getClass() == CastBooleanToVarcharFunctionFactory.class
                || factory.getClass() == CastByteToBooleanFunctionFactory.class
                || factory.getClass() == CastByteToCharFunctionFactory.class
                || factory.getClass() == CastByteToDoubleFunctionFactory.class
                || factory.getClass() == CastByteToFloatFunctionFactory.class
                || factory.getClass() == CastByteToIntFunctionFactory.class
                || factory.getClass() == CastByteToLongFunctionFactory.class
                || factory.getClass() == CastByteToShortFunctionFactory.class
                || factory.getClass() == CastByteToStrFunctionFactory.class
                || factory.getClass() == CastByteToVarcharFunctionFactory.class
                || factory.getClass() == CastCharToBooleanFunctionFactory.class
                || factory.getClass() == CastCharToByteFunctionFactory.class
                || factory.getClass() == CastCharToDoubleFunctionFactory.class
                || factory.getClass() == CastCharToFloatFunctionFactory.class
                || factory.getClass() == CastCharToIntFunctionFactory.class
                || factory.getClass() == CastCharToLongFunctionFactory.class
                || factory.getClass() == CastCharToShortFunctionFactory.class
                || factory.getClass() == CastCharToStrFunctionFactory.class
                || factory.getClass() == CastCharToVarcharFunctionFactory.class
                || factory.getClass() == CastDoubleToBooleanFunctionFactory.class
                || factory.getClass() == CastDoubleToByteFunctionFactory.class
                || factory.getClass() == CastDoubleToCharFunctionFactory.class
                || factory.getClass() == CastDoubleToShortFunctionFactory.class
                || factory.getClass() == CastDoubleToStrFunctionFactory.class
                || factory.getClass() == CastDoubleToVarcharFunctionFactory.class
                || factory.getClass() == CastFloatToBooleanFunctionFactory.class
                || factory.getClass() == CastFloatToByteFunctionFactory.class
                || factory.getClass() == CastFloatToCharFunctionFactory.class
                || factory.getClass() == CastFloatToShortFunctionFactory.class
                || factory.getClass() == CastFloatToStrFunctionFactory.class
                || factory.getClass() == CastFloatToVarcharFunctionFactory.class
                || factory.getClass() == CastIntToBooleanFunctionFactory.class
                || factory.getClass() == CastIntToByteFunctionFactory.class
                || factory.getClass() == CastIntToCharFunctionFactory.class
                || factory.getClass() == CastIntToShortFunctionFactory.class
                || factory.getClass() == CastIntToStrFunctionFactory.class
                || factory.getClass() == CastIntToVarcharFunctionFactory.class
                || factory.getClass() == CastLongToBooleanFunctionFactory.class
                || factory.getClass() == CastLongToByteFunctionFactory.class
                || factory.getClass() == CastLongToCharFunctionFactory.class
                || factory.getClass() == CastLongToShortFunctionFactory.class
                || factory.getClass() == CastLongToStrFunctionFactory.class
                || factory.getClass() == CastLongToVarcharFunctionFactory.class
                || factory.getClass() == CastShortToBooleanFunctionFactory.class
                || factory.getClass() == CastShortToByteFunctionFactory.class
                || factory.getClass() == CastShortToCharFunctionFactory.class
                || factory.getClass() == CastShortToDoubleFunctionFactory.class
                || factory.getClass() == CastShortToFloatFunctionFactory.class
                || factory.getClass() == CastShortToIntFunctionFactory.class
                || factory.getClass() == CastShortToLongFunctionFactory.class
                || factory.getClass() == CastShortToStrFunctionFactory.class
                || factory.getClass() == CastShortToVarcharFunctionFactory.class
                || factory.getClass() == CastStrToBooleanFunctionFactory.class
                || factory.getClass() == CastStrToByteFunctionFactory.class
                || factory.getClass() == CastStrToCharFunctionFactory.class
                || factory.getClass() == CastStrToDoubleFunctionFactory.class
                || factory.getClass() == CastStrToFloatFunctionFactory.class
                || factory.getClass() == CastStrToIntFunctionFactory.class
                || factory.getClass() == CastStrToLongFunctionFactory.class
                || factory.getClass() == CastStrToShortFunctionFactory.class
                || factory.getClass() == CastSymbolToByteFunctionFactory.class
                || factory.getClass() == CastSymbolToCharFunctionFactory.class
                || factory.getClass() == CastSymbolToDoubleFunctionFactory.class
                || factory.getClass() == CastSymbolToFloatFunctionFactory.class
                || factory.getClass() == CastSymbolToIntFunctionFactory.class
                || factory.getClass() == CastSymbolToLongFunctionFactory.class
                || factory.getClass() == CastSymbolToShortFunctionFactory.class
                || factory.getClass() == CastVarcharToBooleanFunctionFactory.class
                || factory.getClass() == CastVarcharToByteFunctionFactory.class
                || factory.getClass() == CastVarcharToCharFunctionFactory.class
                || factory.getClass() == CastVarcharToDoubleFunctionFactory.class
                || factory.getClass() == CastVarcharToFloatFunctionFactory.class
                || factory.getClass() == CastVarcharToIntFunctionFactory.class
                || factory.getClass() == CastVarcharToLongFunctionFactory.class
                || factory.getClass() == CastVarcharToShortFunctionFactory.class
                || factory.getClass() == CastDoubleToFloatFunctionFactory.class
                || factory.getClass() == CastDoubleToIntFunctionFactory.class
                || factory.getClass() == CastDoubleToLongFunctionFactory.class
                || factory.getClass() == CastFloatToDoubleFunctionFactory.class
                || factory.getClass() == CastFloatToIntFunctionFactory.class
                || factory.getClass() == CastFloatToLongFunctionFactory.class
                || factory.getClass() == CastIntToDoubleFunctionFactory.class
                || factory.getClass() == CastIntToFloatFunctionFactory.class
                || factory.getClass() == CastIntToLongFunctionFactory.class
                || factory.getClass() == CastLongToDoubleFunctionFactory.class
                || factory.getClass() == CastLongToFloatFunctionFactory.class
                || factory.getClass() == CastLongToIntFunctionFactory.class
                || factory.getClass() == CastBooleanToDateFunctionFactory.class
                || factory.getClass() == CastBooleanToTimestampFunctionFactory.class
                || factory.getClass() == CastByteToDateFunctionFactory.class
                || factory.getClass() == CastByteToTimestampFunctionFactory.class
                || factory.getClass() == CastCharToDateFunctionFactory.class
                || factory.getClass() == CastCharToTimestampFunctionFactory.class
                || factory.getClass() == CastDateToBooleanFunctionFactory.class
                || factory.getClass() == CastDateToByteFunctionFactory.class
                || factory.getClass() == CastDateToCharFunctionFactory.class
                || factory.getClass() == CastDateToDoubleFunctionFactory.class
                || factory.getClass() == CastDateToFloatFunctionFactory.class
                || factory.getClass() == CastDateToIntFunctionFactory.class
                || factory.getClass() == CastDateToLongFunctionFactory.class
                || factory.getClass() == CastDateToShortFunctionFactory.class
                || factory.getClass() == CastDateToStrFunctionFactory.class
                || factory.getClass() == CastDateToTimestampFunctionFactory.class
                || factory.getClass() == CastDateToVarcharFunctionFactory.class
                || factory.getClass() == CastDoubleToDateFunctionFactory.class
                || factory.getClass() == CastDoubleToTimestampFunctionFactory.class
                || factory.getClass() == CastFloatToDateFunctionFactory.class
                || factory.getClass() == CastFloatToTimestampFunctionFactory.class
                || factory.getClass() == CastIPv4ToIntFunctionFactory.class
                || factory.getClass() == CastIPv4ToVarcharFunctionFactory.class
                || factory.getClass() == CastIntToDateFunctionFactory.class
                || factory.getClass() == CastIntToIPv4FunctionFactory.class
                || factory.getClass() == CastIntToTimestampFunctionFactory.class
                || factory.getClass() == CastLongToDateFunctionFactory.class
                || factory.getClass() == CastLongToTimestampFunctionFactory.class
                || factory.getClass() == CastShortToDateFunctionFactory.class
                || factory.getClass() == CastShortToTimestampFunctionFactory.class
                || factory.getClass() == CastStrToDateFunctionFactory.class
                || factory.getClass() == CastStrToIPv4FunctionFactory.class
                || factory.getClass() == CastSymbolToDateFunctionFactory.class
                || factory.getClass() == CastSymbolToTimestampFunctionFactory.class
                || factory.getClass() == CastTimestampToBooleanFunctionFactory.class
                || factory.getClass() == CastTimestampToByteFunctionFactory.class
                || factory.getClass() == CastTimestampToCharFunctionFactory.class
                || factory.getClass() == CastTimestampToDateFunctionFactory.class
                || factory.getClass() == CastTimestampToDoubleFunctionFactory.class
                || factory.getClass() == CastTimestampToFloatFunctionFactory.class
                || factory.getClass() == CastTimestampToIntFunctionFactory.class
                || factory.getClass() == CastTimestampToLongFunctionFactory.class
                || factory.getClass() == CastTimestampToShortFunctionFactory.class
                || factory.getClass() == CastTimestampToStrFunctionFactory.class
                || factory.getClass() == CastTimestampToTimestampFunctionFactory.class
                || factory.getClass() == CastTimestampToVarcharFunctionFactory.class
                || factory.getClass() == CastVarcharToDateFunctionFactory.class
                || factory.getClass() == CastVarcharToIPv4FunctionFactory.class
                || factory.getClass() == CastVarcharToTimestampFunctionFactory.class
                || factory.getClass() == DoubleArrayPositionFunctionFactory.class
                || factory.getClass() == DoubleArrayInsertionPointFunctionFactory.class
                || factory.getClass() == DoubleArrayInsertionPointAfterEqualFunctionFactory.class
                || factory.getClass() == DoubleArrayElemAvgFunctionFactory.class
                || factory.getClass() == DoubleArrayElemMaxFunctionFactory.class
                || factory.getClass() == DoubleArrayElemMinFunctionFactory.class
                || factory.getClass() == DoubleArrayElemSumFunctionFactory.class
                || factory.getClass() == DoubleArrayFlattenFunctionFactory.class
                || factory.getClass() == DoubleArrayTransposeFunctionFactory.class
                || factory.getClass() == DoubleArrayCumSumFunctionFactory.class
                || factory.getClass() == DoubleArrayShiftFunctionFactory.class
                || factory.getClass() == DoubleArrayShiftDefaultNaNFunctionFactory.class
                || factory.getClass() == DoubleArrayRoundFunctionFactory.class
                || factory.getClass() == EqDoubleArrayFunctionFactory.class
                || factory.getClass() == ArrayCreateFunctionFactory.class
                || factory.getClass() == DoubleArrayAvgFunctionFactory.class
                || factory.getClass() == DoubleArrayCountFunctionFactory.class
                || factory.getClass() == DoubleArrayMaxFunctionFactory.class
                || factory.getClass() == DoubleArrayMinFunctionFactory.class
                || factory.getClass() == DoubleArrayReverseFunctionFactory.class
                || factory.getClass() == DoubleArraySortDescFunctionFactory.class
                || factory.getClass() == DoubleArraySortFullFunctionFactory.class
                || factory.getClass() == DoubleArraySortFunctionFactory.class
                || factory.getClass() == DoubleArrayStdDevFunctionFactory.class
                || factory.getClass() == DoubleArrayStdDevPopFunctionFactory.class
                || factory.getClass() == DoubleArrayStdDevSampFunctionFactory.class
                || factory.getClass() == DoubleArraySumFunctionFactory.class
                || factory.getClass() == IntIntervalFunctionFactory.class
                || factory.getClass() == IntIntervalRightOpenFunctionFactory.class
                || factory.getClass() == CastBooleanToLong256FunctionFactory.class
                || factory.getClass() == CastByteToLong256FunctionFactory.class
                || factory.getClass() == CastCharToLong256FunctionFactory.class
                || factory.getClass() == CastDateToLong256FunctionFactory.class
                || factory.getClass() == CastDoubleToLong256FunctionFactory.class
                || factory.getClass() == CastFloatToLong256FunctionFactory.class
                || factory.getClass() == CastGeoHashToGeoHashFunctionFactory.class
                || factory.getClass() == CastGeoHashToStrFunctionFactory.class
                || factory.getClass() == CastGeoHashToVarcharFunctionFactory.class
                || factory.getClass() == CastIntToLong256FunctionFactory.class
                || factory.getClass() == CastLong256ToBooleanFunctionFactory.class
                || factory.getClass() == CastLong256ToByteFunctionFactory.class
                || factory.getClass() == CastLong256ToCharFunctionFactory.class
                || factory.getClass() == CastLong256ToDateFunctionFactory.class
                || factory.getClass() == CastLong256ToDoubleFunctionFactory.class
                || factory.getClass() == CastLong256ToFloatFunctionFactory.class
                || factory.getClass() == CastLong256ToIntFunctionFactory.class
                || factory.getClass() == CastLong256ToLongFunctionFactory.class
                || factory.getClass() == CastLong256ToShortFunctionFactory.class
                || factory.getClass() == CastLong256ToStrFunctionFactory.class
                || factory.getClass() == CastLong256ToSymbolFunctionFactory.class
                || factory.getClass() == CastLong256ToTimestampFunctionFactory.class
                || factory.getClass() == CastLong256ToVarcharFunctionFactory.class
                || factory.getClass() == CastLongToGeoHashFunctionFactory.class
                || factory.getClass() == CastLongToLong256FunctionFactory.class
                || factory.getClass() == CastShortToLong256FunctionFactory.class
                || factory.getClass() == CastStrToGeoHashFunctionFactory.class
                || factory.getClass() == CastStrToLong256FunctionFactory.class
                || factory.getClass() == CastStrToUuidFunctionFactory.class
                || factory.getClass() == CastSymbolToLong256FunctionFactory.class
                || factory.getClass() == CastTimestampToLong256FunctionFactory.class
                || factory.getClass() == CastUuidToStrFunctionFactory.class
                || factory.getClass() == CastUuidToVarcharFunctionFactory.class
                || factory.getClass() == CastVarcharToGeoHashFunctionFactory.class
                || factory.getClass() == CastVarcharToLong256FunctionFactory.class
                || factory.getClass() == CastVarcharToUuidFunctionFactory.class
                || factory.getClass() == CastNullTypeFunctionFactory.class
                || factory.getClass() == CastStrToTimestampFunctionFactory.class
                || factory.getClass() == DivDoubleFunctionFactory.class
                || factory.getClass() == DivFloatFunctionFactory.class
                || factory.getClass() == DivIntFunctionFactory.class
                || factory.getClass() == DivLongFunctionFactory.class
                || factory.getClass() == EqDoubleFunctionFactory.class
                || factory.getClass() == EqIntFunctionFactory.class
                || factory.getClass() == EqIPv4FunctionFactory.class
                || factory.getClass() == EqIPv4StrFunctionFactory.class
                || factory.getClass() == EqUuidFunctionFactory.class
                || factory.getClass() == EqUuidStrFunctionFactory.class
                || factory.getClass() == EqLong256FunctionFactory.class
                || factory.getClass() == EqLong256StrFunctionFactory.class
                || factory.getClass() == EqIntStrCFunctionFactory.class
                || factory.getClass() == EqLongFunctionFactory.class
                || factory.getClass() == EqSymLongFunctionFactory.class
                || factory.getClass() == EqTimestampFunctionFactory.class
                || factory.getClass() == EqSymTimestampFunctionFactory.class
                || factory.getClass() == EqStrCharFunctionFactory.class
                || factory.getClass() == EqStrFunctionFactory.class
                || factory.getClass() == LtDoubleVVFunctionFactory.class
                || factory.getClass() == LtIntFunctionFactory.class
                || factory.getClass() == LtLongFunctionFactory.class
                || factory.getClass() == LtTimestampFunctionFactory.class
                || factory.getClass() == LtStrFunctionFactory.class
                || factory.getClass() == MulDoubleFunctionFactory.class
                || factory.getClass() == MulFloatFunctionFactory.class
                || factory.getClass() == MulIntFunctionFactory.class
                || factory.getClass() == MulLongFunctionFactory.class
                || factory.getClass() == NegDoubleFunctionFactory.class
                || factory.getClass() == NegFloatFunctionFactory.class
                || factory.getClass() == NegIntFunctionFactory.class
                || factory.getClass() == NegLongFunctionFactory.class
                || factory.getClass() == NotFunctionFactory.class
                || factory.getClass() == NullIfDoubleFunctionFactory.class
                || factory.getClass() == NullIfIntFunctionFactory.class
                || factory.getClass() == NullIfLongFunctionFactory.class
                || factory.getClass() == OrFunctionFactory.class
                || factory.getClass() == SubDoubleFunctionFactory.class
                || factory.getClass() == SubFloatFunctionFactory.class
                || factory.getClass() == SubIntFunctionFactory.class
                || factory.getClass() == SubLongFunctionFactory.class
                || factory.getClass() == SubTimestampFunctionFactory.class);
    }

    // Only the cache's audited argument-swapping/negating aliases inherit this contract.
    FunctionFactoryDescriptor(FunctionFactory factory, boolean isRelocatableScalar) throws SqlException {
        this.factory = factory;
        this.isAnd = factory.getClass() == AndFunctionFactory.class;
        this.isArrayAccess = factory.getClass() == DoubleArrayAccessFunctionFactory.class
                || factory.getClass() == DoubleArraySliceFunctionFactory.class;
        this.isArrayColumnLayoutSensitive = isArrayAccess || factory.getClass() == ArrayDimLengthFunctionFactory.class;
        this.isArrayElementWiseScalar = factory.getClass() == DoubleArrayElemAvgFunctionFactory.class
                || factory.getClass() == DoubleArrayElemMaxFunctionFactory.class
                || factory.getClass() == DoubleArrayElemMinFunctionFactory.class
                || factory.getClass() == DoubleArrayElemSumFunctionFactory.class;
        this.isCase = factory.getClass() == CaseFunctionFactory.class;
        this.isOr = factory.getClass() == OrFunctionFactory.class;
        this.isSwitch = factory.getClass() == SwitchFunctionFactory.class;
        this.isRelocatableScalar = isRelocatableScalar;
        this.isRowCount = factory.getClass() == CountGroupByFunctionFactory.class;

        final String sig = factory.getSignature();
        this.openParenIndex = validateSignatureAndGetNameSeparator(sig);
        this.name = sig.substring(0, openParenIndex);
        this.isOrderSensitiveAggregate = factory.isGroupBy() && (Chars.equalsIgnoreCase(name, "array_agg")
                || Chars.equalsIgnoreCase(name, "first") || Chars.equalsIgnoreCase(name, "first_not_null")
                || Chars.equalsIgnoreCase(name, "last") || Chars.equalsIgnoreCase(name, "last_not_null"));
        // validate data types
        int typeCount = 0;
        for (
                int i = openParenIndex + 1, n = sig.length() - 1;
                i < n; typeCount++
        ) {
            char cc = sig.charAt(i);
            if (FunctionFactoryDescriptor.getArgTypeTag(cc) == -1) {
                throw SqlException.position(0).put("illegal argument type: ").put('`').put(cc).put('`');
            }
            // check if this is an array
            i++;
            if (i < n && sig.charAt(i) == '[') {
                i++;
                if (i >= n || sig.charAt(i) != ']') {
                    throw SqlException.position(0).put("invalid array declaration: " + sig);
                }
                i++;
            }
        }

        // second loop, less paranoid
        long[] types = new long[typeCount % 2 == 0 ? typeCount / 2 : typeCount / 2 + 1];
        for (int i = openParenIndex + 1, n = sig.length() - 1, typeIndex = 0; i < n; ) {
            final char c = sig.charAt(i);
            int type = FunctionFactoryDescriptor.getArgTypeTag(c);
            final int arrayIndex = typeIndex / 2;
            final int arrayValueOffset = (typeIndex % 2) * 32;
            // check if this is an array
            i++;

            // array bit
            if (i < n && sig.charAt(i) == '[') {
                type |= ARRAY_MASK;
                i += 2;
            }
            // constant bit
            if ((c & 32) != 0) {
                type |= CONST_MASK;
            }
            types[arrayIndex] |= (toUnsignedLong(type) << (32 - arrayValueOffset));
            typeIndex++;
        }
        this.argTypes = types;
        this.sigArgCount = typeCount;
    }

    public static short getArgTypeTag(char c) {
        return switch (c | 32) {
            case 'a' -> ColumnType.CHAR;
            case 'b' -> ColumnType.BYTE;
            case 'c' -> ColumnType.CURSOR;
            case 'd' -> ColumnType.DOUBLE;
            case 'e' -> ColumnType.SHORT;
            case 'f' -> ColumnType.FLOAT;
            case 'g' -> ColumnType.GEOHASH;
            case 'h' -> ColumnType.LONG256;
            case 'i' -> ColumnType.INT;
            case 'j' -> ColumnType.LONG128;
            case 'k' -> ColumnType.SYMBOL;
            case 'l' -> ColumnType.LONG;
            case 'm' -> ColumnType.DATE;
            case 'n' -> ColumnType.TIMESTAMP;
            case 'o' -> ColumnType.NULL;
            case 'p' -> ColumnType.REGCLASS;
            case 'q' -> ColumnType.REGPROCEDURE;
            case 'r' -> ColumnType.RECORD;
            case 's' -> ColumnType.STRING;
            case 't' -> ColumnType.BOOLEAN;
            case 'u' -> ColumnType.BINARY;
            case 'v' -> ColumnType.VAR_ARG;
            case 'w' -> ColumnType.ARRAY_STRING;
            case 'x' -> ColumnType.IPv4;
            case 'z' -> ColumnType.UUID;
            case 'ø' -> ColumnType.VARCHAR;
            case 'δ' -> ColumnType.INTERVAL;
            case 'ξ' -> ColumnType.DECIMAL;
            default -> -1;
        };
    }

    public static boolean isArray(int mask) {
        return (mask & ARRAY_MASK) != 0;
    }

    public static boolean isConstant(int mask) {
        return (mask & CONST_MASK) != 0;
    }

    public static String replaceSignatureName(String name, String signature) throws SqlException {
        int openParenIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalSink();
        signatureBuilder.put(name);
        signatureBuilder.put(signature, openParenIndex, signature.length());
        return signatureBuilder.toString();
    }

    public static String replaceSignatureNameAndSwapArgs(String name, String signature) throws SqlException {
        int openParenIndex = validateSignatureAndGetNameSeparator(signature);
        StringSink signatureBuilder = Misc.getThreadLocalSink();
        signatureBuilder.put(name);
        signatureBuilder.put('(');
        boolean bracket = false;
        for (int i = signature.length() - 2; i > openParenIndex; i--) {
            char curr = signature.charAt(i);
            if (curr == '[') {
                bracket = true;
            } else if (curr != ']') {
                signatureBuilder.put(curr);
                if (bracket) {
                    signatureBuilder.put("[]");
                    bracket = false;
                }
            }
        }
        if (bracket) {
            signatureBuilder.put("[]");
        }
        signatureBuilder.put(')');
        return signatureBuilder.toString();
    }

    public static int toType(int typeWithFlags) {
        return typeWithFlags & TYPE_MASK;
    }

    public static short toTypeTag(int typeWithFlags) {
        return (short) (typeWithFlags & TYPE_MASK);
    }

    public static StringSink translateSignature(CharSequence funcName, String signature, StringSink sink) {
        int openParenIndex;
        try {
            openParenIndex = validateSignatureAndGetNameSeparator(signature);
        } catch (SqlException err) {
            throw new IllegalArgumentException("offending: '" + signature + "', reason: " + err.getMessage());
        }
        sink.put(funcName).put('(');
        for (int i = openParenIndex + 1, n = signature.length() - 1; i < n; i++) {
            char c = signature.charAt(i);
            String type = typeNameMap.get(c | 32);
            if (type == null) {
                throw new IllegalArgumentException("offending: '" + c + '\'');
            }
            if (c != '[') {
                if (Character.isLowerCase(c)) {
                    sink.put("const ");
                }
            } else {
                if (i < 3 || i + 2 > n || signature.charAt(i + 1) != ']') {
                    throw new IllegalArgumentException("offending array: '" + c + '\'');
                }
                sink.clear(sink.length() - 2); // remove the preceding comma
                i++; // skip closing bracket
            }
            sink.put(type);
            if (i + 1 < n) {
                sink.put(", ");
            }
        }
        sink.put(')');
        return sink;
    }

    public static int validateSignatureAndGetNameSeparator(String sig) throws SqlException {
        if (sig == null) {
            throw SqlException.$(0, "NULL signature");
        }

        int openParenIndex = sig.indexOf('(');
        if (openParenIndex == -1) {
            throw SqlException.$(0, "open brace expected");
        }

        if (openParenIndex == 0) {
            throw SqlException.$(0, "empty function name");
        }

        if (sig.charAt(sig.length() - 1) != ')') {
            throw SqlException.$(0, "close brace expected");
        }

        int c = sig.charAt(0);
        if (c >= '0' && c <= '9') {
            throw SqlException.$(0, "name must not start with digit");
        }

        for (int i = 0; i < openParenIndex; i++) {
            char cc = sig.charAt(i);
            if (FunctionFactoryCache.invalidFunctionNameChars.contains(cc)) {
                throw SqlException.position(0).put("invalid character: ").put(cc);
            }
        }

        if (FunctionFactoryCache.invalidFunctionNames.keyIndex(sig, 0, openParenIndex) < 0) {
            throw SqlException.position(0).put("invalid function name character: ").put(sig);
        }
        return openParenIndex;
    }

    void setCommutedEquality(FunctionFactoryDescriptor descriptor) {
        commutedEquality = descriptor;
    }

    public int getArgTypeWithFlags(int index) {
        int arrayIndex = index / 2;
        long mask = argTypes[arrayIndex];
        return (int) (mask >>> (32 - (index % 2) * 32));
    }

    public FunctionFactoryDescriptor getCommutedEquality() {
        return commutedEquality;
    }

    public FunctionFactory getFactory() {
        return factory;
    }

    public String getName() {
        return name;
    }

    public int getSigArgCount() {
        return sigArgCount;
    }

    public boolean isAnd() {
        return isAnd;
    }

    public boolean isArrayAccess() {
        return isArrayAccess;
    }

    public boolean isArrayColumnLayoutSensitive() {
        return isArrayColumnLayoutSensitive;
    }

    public boolean isArrayElementWiseScalar() {
        return isArrayElementWiseScalar;
    }

    public boolean isCase() {
        return isCase;
    }

    public boolean isOr() {
        return isOr;
    }

    /**
     * Audited scalar construction depends only on typed arguments: private leaves
     * may relocate before adoption, and selected calls may be constructed again.
     * FunctionBinder additionally enforces branch restrictions for NULL overloads.
     */
    public boolean isRelocatableScalar() {
        return isRelocatableScalar;
    }

    public boolean isOrderSensitiveAggregate() {
        return isOrderSensitiveAggregate;
    }

    public boolean isRowCount() {
        return isRowCount;
    }

    public boolean isSwitch() {
        return isSwitch;
    }

    private static long toUnsignedLong(int type) {
        return ((long) type) & 0xffffffffL;
    }

    static {
        typeNameMap.put('a', "char");
        typeNameMap.put('b', "byte");
        typeNameMap.put('c', "cursor");
        typeNameMap.put('d', "double");
        typeNameMap.put('e', "short");
        typeNameMap.put('f', "float");
        typeNameMap.put('g', "geohash");
        typeNameMap.put('h', "long256");
        typeNameMap.put('i', "int");
        typeNameMap.put('j', "long128");
        typeNameMap.put('k', "symbol");
        typeNameMap.put('l', "long");
        typeNameMap.put('m', "date");
        typeNameMap.put('n', "timestamp");
        typeNameMap.put('o', "null");
        typeNameMap.put('p', "reg_class");
        typeNameMap.put('q', "reg_procedure");
        typeNameMap.put('r', "record");
        typeNameMap.put('s', "string");
        typeNameMap.put('t', "boolean");
        typeNameMap.put('u', "binary");
        typeNameMap.put('v', "var_arg");
        typeNameMap.put('w', "array_string");
        typeNameMap.put('x', "ipv4");
        typeNameMap.put('z', "uuid");
        typeNameMap.put('ø', "varchar");
        typeNameMap.put('δ', "interval");
        typeNameMap.put('ξ', "decimal");
        typeNameMap.put('[' | 32, "[]");
    }
}
