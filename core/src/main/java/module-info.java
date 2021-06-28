/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.*;

open module io.questdb {
    requires transitive jdk.unsupported;
    requires static org.jetbrains.annotations;
    requires static java.sql;

    uses io.questdb.griffin.FunctionFactory;

    exports io.questdb;
    exports io.questdb.cairo;
    exports io.questdb.cairo.vm;
    exports io.questdb.cairo.map;
    exports io.questdb.cairo.sql;
    exports io.questdb.cairo.pool;
    exports io.questdb.cairo.pool.ex;
    exports io.questdb.cairo.security;

    exports io.questdb.cutlass.http;
    exports io.questdb.cutlass.http.processors;
    exports io.questdb.cutlass.http.ex;
    exports io.questdb.cutlass.json;
    exports io.questdb.cutlass.line;
    exports io.questdb.cutlass.line.udp;
    exports io.questdb.cutlass.line.tcp;
    exports io.questdb.cutlass.pgwire;
    exports io.questdb.cutlass.text;
    exports io.questdb.cutlass.text.types;

    exports io.questdb.griffin;
    exports io.questdb.griffin.engine;
    exports io.questdb.griffin.model;
    exports io.questdb.griffin.engine.functions;
    exports io.questdb.griffin.engine.functions.rnd;
    exports io.questdb.griffin.engine.functions.bind;
    exports io.questdb.griffin.engine.functions.bool;
    exports io.questdb.griffin.engine.functions.cast;
    exports io.questdb.griffin.engine.functions.catalogue;
    exports io.questdb.griffin.engine.functions.columns;
    exports io.questdb.griffin.engine.functions.conditional;
    exports io.questdb.griffin.engine.functions.constants;
    exports io.questdb.griffin.engine.functions.date;
    exports io.questdb.griffin.engine.functions.eq;
    exports io.questdb.griffin.engine.functions.groupby;
    exports io.questdb.griffin.engine.functions.lt;
    exports io.questdb.griffin.engine.functions.math;
    exports io.questdb.griffin.engine.functions.regex;
    exports io.questdb.griffin.engine.functions.str;
    exports io.questdb.griffin.engine.groupby;
    exports io.questdb.griffin.engine.groupby.vect;
    exports io.questdb.griffin.engine.analytic;

    exports io.questdb.std;
    exports io.questdb.std.datetime;
    exports io.questdb.std.datetime.microtime;
    exports io.questdb.std.datetime.millitime;
    exports io.questdb.std.str;
    exports io.questdb.std.ex;
    exports io.questdb.network;
    exports io.questdb.log;
    exports io.questdb.mp;
    exports io.questdb.tasks;
    exports io.questdb.metrics;

    provides FunctionFactory with
            // test functions
            io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory,
            io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory,
            io.questdb.griffin.engine.functions.test.TestSumXDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.test.TestNPEFactory,
            io.questdb.griffin.engine.functions.test.TestSumTDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.test.TestSumStringGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.bool.OrFunctionFactory,
            io.questdb.griffin.engine.functions.bool.AndFunctionFactory,
            io.questdb.griffin.engine.functions.bool.NotFunctionFactory,

            // [] operators
            io.questdb.griffin.engine.functions.array.StrArrayDereferenceFunctionFactory,
            // '=' operators
            io.questdb.griffin.engine.functions.eq.EqStrFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqLong256StrFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqLong256FunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqStrCharFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqSymCharFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqCharCharFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqIntStrCFunctionFactory,
            io.questdb.griffin.engine.functions.eq.EqTimestampFunctionFactory,

            //nullif
            io.questdb.griffin.engine.functions.eq.NullIfCharCharFunctionFactory,

//                   '<' operator
            io.questdb.griffin.engine.functions.lt.LtDoubleVVFunctionFactory,
            io.questdb.griffin.engine.functions.lt.LtTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.lt.LtIntFunctionFactory,

//                   '+' operator
            io.questdb.griffin.engine.functions.math.AddByteFunctionFactory,
            io.questdb.griffin.engine.functions.math.AddShortFunctionFactory,
            io.questdb.griffin.engine.functions.math.AddIntFunctionFactory,
            io.questdb.griffin.engine.functions.math.AddLongFunctionFactory,
            io.questdb.griffin.engine.functions.math.AddFloatFunctionFactory,
            io.questdb.griffin.engine.functions.math.AddDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.date.AddLongToTimestampFunctionFactory,
//                    # '-' operator,
            io.questdb.griffin.engine.functions.math.NegIntFunctionFactory,
            io.questdb.griffin.engine.functions.math.NegDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.NegFloatFunctionFactory,
            io.questdb.griffin.engine.functions.math.NegLongFunctionFactory,
            io.questdb.griffin.engine.functions.math.NegShortFunctionFactory,
            io.questdb.griffin.engine.functions.math.NegByteFunctionFactory,

            io.questdb.griffin.engine.functions.math.SubDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.SubIntFunctionFactory,
            io.questdb.griffin.engine.functions.math.SubLongFunctionFactory,
            io.questdb.griffin.engine.functions.math.SubTimestampFunctionFactory,
//                    # '/' operator,
            io.questdb.griffin.engine.functions.math.DivDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.DivLongFunctionFactory,
//                    # '%' operator,
            io.questdb.griffin.engine.functions.math.RemIntFunctionFactory,
            io.questdb.griffin.engine.functions.math.RemLongFunctionFactory,
            io.questdb.griffin.engine.functions.math.RemDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.RemFloatFunctionFactory,
//                    # '*' operator,
            io.questdb.griffin.engine.functions.math.MulFloatFunctionFactory,
            io.questdb.griffin.engine.functions.math.MulDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.MulLongFunctionFactory,
            io.questdb.griffin.engine.functions.math.MulIntFunctionFactory,
            io.questdb.griffin.engine.functions.math.AbsIntFunctionFactory,
            io.questdb.griffin.engine.functions.math.AbsShortFunctionFactory,
            io.questdb.griffin.engine.functions.math.AbsLongFunctionFactory,
            io.questdb.griffin.engine.functions.math.AbsDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.LogDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.SqrtDoubleFunctionFactory,
//                    # '~=',
            io.questdb.griffin.engine.functions.regex.MatchStrFunctionFactory,
            io.questdb.griffin.engine.functions.regex.MatchCharFunctionFactory,
//                    #like
            io.questdb.griffin.engine.functions.regex.LikeCharFunctionFactory,
            io.questdb.griffin.engine.functions.regex.LikeStrFunctionFactory,
            io.questdb.griffin.engine.functions.regex.ILikeStrFunctionFactory,
//                    # '!~',
            io.questdb.griffin.engine.functions.regex.NotMatchStrFunctionFactory,
            io.questdb.griffin.engine.functions.regex.NotMatchCharFunctionFactory,
//                    # 'to_char',
            io.questdb.griffin.engine.functions.date.ToStrDateFunctionFactory,
            io.questdb.griffin.engine.functions.date.ToStrTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.str.ToCharBinFunctionFactory,
//                    # 'length',
            io.questdb.griffin.engine.functions.str.LengthStrFunctionFactory,
            io.questdb.griffin.engine.functions.str.LengthSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.str.LengthBinFunctionFactory,
//                    # random generator functions,
            io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndBooleanFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndIntFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndIntCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndStrFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndStringRndListFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndDoubleCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndFloatCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndShortCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndShortFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndDateCCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndLongCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndLongFunctionFactory,
            io.questdb.griffin.engine.functions.date.TimestampSequenceFunctionFactory,
            io.questdb.griffin.engine.functions.date.TimestampShuffleFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndByteCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndBinCCCFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndSymbolListFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndStringListFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndCharFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndLong256FunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndLong256NFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndByteFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndFloatFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndBinFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.RndDateFunctionFactory,
            io.questdb.griffin.engine.functions.rnd.ListFunctionFactory,
//                  date conversion functions,
            io.questdb.griffin.engine.functions.date.SysdateFunctionFactory,
            io.questdb.griffin.engine.functions.date.ToTimestampVCFunctionFactory,
            io.questdb.griffin.engine.functions.date.ToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.date.SystimestampFunctionFactory,
            io.questdb.griffin.engine.functions.date.NowFunctionFactory,
            io.questdb.griffin.engine.functions.date.HourOfDayFunctionFactory,
            io.questdb.griffin.engine.functions.date.DayOfMonthFunctionFactory,
            io.questdb.griffin.engine.functions.date.DayOfWeekFunctionFactory,
            io.questdb.griffin.engine.functions.date.DayOfWeekSundayFirstFunctionFactory,
            io.questdb.griffin.engine.functions.date.MinuteOfHourFunctionFactory,
            io.questdb.griffin.engine.functions.date.SecondOfMinuteFunctionFactory,
            io.questdb.griffin.engine.functions.date.YearFunctionFactory,
            io.questdb.griffin.engine.functions.date.MonthOfYearFunctionFactory,
            io.questdb.griffin.engine.functions.date.DaysPerMonthFunctionFactory,
            io.questdb.griffin.engine.functions.date.MicrosOfSecondFunctionFactory,
            io.questdb.griffin.engine.functions.date.MillisOfSecondFunctionFactory,
            io.questdb.griffin.engine.functions.date.IsLeapYearFunctionFactory,
            io.questdb.griffin.engine.functions.date.TimestampDiffFunctionFactory,
            io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory,
            io.questdb.griffin.engine.functions.date.ToDateFunctionFactory,
            io.questdb.griffin.engine.functions.date.ToPgDateFunctionFactory,
//                  cast functions,
//                  cast double to ...,
            io.questdb.griffin.engine.functions.cast.CastDoubleToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDoubleToTimestampFunctionFactory,
//                  cast float to ...,
            io.questdb.griffin.engine.functions.cast.CastFloatToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastFloatToTimestampFunctionFactory,
//                  cast short to ...,
            io.questdb.griffin.engine.functions.cast.CastShortToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastShortToBooleanFunctionFactory,
//                  cast int to ...,
            io.questdb.griffin.engine.functions.cast.CastIntToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastIntToBooleanFunctionFactory,
//                  cast long to ...,
            io.questdb.griffin.engine.functions.cast.CastLongToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLongToBooleanFunctionFactory,
//                  cast long256 to ...,
            io.questdb.griffin.engine.functions.cast.CastLong256ToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastLong256ToBooleanFunctionFactory,
//                  cast date to ...,
            io.questdb.griffin.engine.functions.cast.CastDateToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastDateToBooleanFunctionFactory,
//                  cast timestamp to ...,
            io.questdb.griffin.engine.functions.cast.CastTimestampToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastTimestampToBooleanFunctionFactory,
//                  cast byte to ...,
            io.questdb.griffin.engine.functions.cast.CastByteToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastByteToBooleanFunctionFactory,
//                  cast boolean to ...,
            io.questdb.griffin.engine.functions.cast.CastBooleanToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastBooleanToBooleanFunctionFactory,
//                  cast char to ...,
            io.questdb.griffin.engine.functions.cast.CastCharToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToLong256FunctionFactory,
//                  cast str to ...,
            io.questdb.griffin.engine.functions.cast.CastStrToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastCharToBooleanFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastStrToBinaryFunctionFactory,
//                  cast symbol to ...
            io.questdb.griffin.engine.functions.cast.CastSymbolToIntFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToFloatFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToLong256FunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToLongFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToShortFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToByteFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToCharFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToDateFunctionFactory,
            io.questdb.griffin.engine.functions.cast.CastSymbolToTimestampFunctionFactory,
            // cast helpers
            io.questdb.griffin.engine.functions.cast.VarcharCastHelperFunctionFactory,
//                  'in'
            io.questdb.griffin.engine.functions.bool.InSymbolCursorFunctionFactory,
            io.questdb.griffin.engine.functions.bool.InStrFunctionFactory,
            io.questdb.griffin.engine.functions.bool.InCharFunctionFactory,
            io.questdb.griffin.engine.functions.bool.InSymbolFunctionFactory,
            io.questdb.griffin.engine.functions.bool.InTimestampStrFunctionFactory,
            io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.bool.BetweenTimestampFunctionFactory,
//                  'all'
            io.questdb.griffin.engine.functions.bool.AllNotEqStrFunctionFactory,
//                  'agg' group by function
            io.questdb.griffin.engine.functions.groupby.StringAggGroupByFunctionFactory,
//                  'sum' group by function
            io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.SumFloatGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.SumIntGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.KSumDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.NSumDoubleGroupByFunctionFactory,
//                  'last' group by function
            io.questdb.griffin.engine.functions.groupby.LastDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastFloatGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastIntGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastCharGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastShortGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastByteGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastSymbolGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastTimestampGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastDateGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.LastLongGroupByFunctionFactory,

//                  'first' group by function
            io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstFloatGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstIntGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstCharGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstShortGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstByteGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstTimestampGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstLongGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.FirstDateGroupByFunctionFactory,
//                  'max' group
            io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MaxIntGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MaxLongGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MaxTimestampGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MaxDateGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MaxFloatGroupByFunctionFactory,
//                  'min' group
            io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MinFloatGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MinLongGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MinIntGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MaxCharGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MinCharGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MinTimestampGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.MinDateGroupByFunctionFactory,
//                  'count' group by function
            io.questdb.griffin.engine.functions.groupby.CountGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.CountStringGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.CountSymbolGroupByFunctionFactory,
            io.questdb.griffin.engine.functions.groupby.CountLong256GroupByFunctionFactory,
            //      'haversine_dist_degree' group by function
            io.questdb.griffin.engine.functions.groupby.HaversineDistDegreeGroupByFunctionFactory,
//                  'isOrdered'
            io.questdb.griffin.engine.functions.groupby.IsLongOrderedGroupByFunctionFactory,
//                  round()
            io.questdb.griffin.engine.functions.math.RoundDoubleZeroScaleFunctionFactory,
            io.questdb.griffin.engine.functions.math.RoundDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.RoundDownDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.RoundUpDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.math.RoundHalfEvenDoubleFunctionFactory,
//                  case conditional statement
            io.questdb.griffin.engine.functions.conditional.CaseFunctionFactory,
            io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory,
            io.questdb.griffin.engine.functions.conditional.CoalesceFunctionFactory,
//                  PostgeSQL catalogue functions
            io.questdb.griffin.engine.functions.catalogue.AttrDefCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.AttributeCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.ClassCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedClassCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.IndexCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.InformationSchemaFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedTypeCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedDescriptionCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedNamespaceCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.NamespaceCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.IsTableVisibleCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.UserByIdCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.TypeCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.VersionFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.CurrentDatabaseFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.CurrentSchemaBooleanFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.CurrentSchemaFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedCurrentSchemasFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.CursorDereferenceFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.DescriptionCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.SessionUserFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.ClassResolveFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedPgGetPartKeyDefFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedPgGetSITExprFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedPgGetSIExprFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.NullIfIFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.FormatTypeFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.ProcCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.RangeCatalogueFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.PrefixedPgGetKeywordsFunctionFactory,
            io.questdb.griffin.engine.functions.catalogue.TableMetadataCursorFactory,
//                  concat()
            io.questdb.griffin.engine.functions.str.ConcatFunctionFactory,
            // replace()
            io.questdb.griffin.engine.functions.str.ReplaceStrFunctionFactory,
//                  avg()
            io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunctionFactory,
//                  ^
            io.questdb.griffin.engine.functions.math.PowDoubleFunctionFactory,
            io.questdb.griffin.engine.functions.table.AllTablesFunctionFactory,
            io.questdb.griffin.engine.functions.table.TableColumnsFunctionFactory,
            // first
            io.questdb.griffin.engine.functions.groupby.FirstSymbolGroupByFunctionFactory,
//                  Change string case
            io.questdb.griffin.engine.functions.str.ToUppercaseFunctionFactory,
            io.questdb.griffin.engine.functions.str.ToLowercaseFunctionFactory,

            // analytic functions
            io.questdb.griffin.engine.functions.analytic.RowNumberFunctionFactory,

            // metadata functions
            io.questdb.griffin.engine.functions.metadata.BuildFunctionFactory,

            // bit operations
            BitwiseAndLongFunctionFactory,
            BitwiseOrLongFunctionFactory,
            BitwiseNotLongFunctionFactory,
            BitwiseXorLongFunctionFactory,
            BitwiseAndIntFunctionFactory,
            BitwiseOrIntFunctionFactory,
            BitwiseNotIntFunctionFactory,
            BitwiseXorIntFunctionFactory,

            io.questdb.griffin.engine.functions.date.ToTimezoneTimestampFunctionFactory,
            io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory,

            io.questdb.griffin.engine.functions.catalogue.TypeOfFunctionFactory
            ;
}
