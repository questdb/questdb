//
// Created by Nick on 24/04/2024.
//

#ifndef CONVERTERS_H
#define CONVERTERS_H
#include <type_traits>

enum class ColumnType : int {
    UNDEFINED = 0,
    BOOLEAN = 1,
    BYTE = 2,
    SHORT = 3,
    CHAR = 4,
    INT = 5,
    LONG = 6,
    DATE = 7,
    TIMESTAMP = 8,
    FLOAT = 9,
    DOUBLE = 10,
    STRING = 11,
    SYMBOL = 12,
    LONG256 = 13,
    GEOBYTE = 14,
    GEOSHORT = 15,
    GEOINT = 16,
    GEOLONG = 17,
    BINARY = 18,
    UUID = 19,
    CURSOR = 20,
    VAR_ARG = 21,
    RECORD = 22,
    GEOHASH = 23,
    LONG128 = 24,
    IPV4 = 25,
    VARCHAR = 26,
    REGCLASS = 27,
    REGPROCEDURE = 28,
    ARRAY_STRING = 29,
    PARAMETER = 30,
    NULL_ = 31
};

constexpr size_t pack_column_types(ColumnType a, ColumnType b)
{
    return static_cast<size_t>(a) << 32 | static_cast<size_t>(b) & 0xffffffffL;
}

enum class ConversionError {
    NONE = 0,
    UNSUPPORTED_CAST = 1,
    TRUNCATION_DISALLOWED = 2,
};

template <typename T1, typename T2>
ConversionError convert_fixed_to_fixed(T1* srcMem, T2* dstMem, size_t rowCount, bool allowTruncation)
{
    if (!allowTruncation)
    {
        if (sizeof(T1) > sizeof(T2))
        {
            return ConversionError::TRUNCATION_DISALLOWED;
        }

        if constexpr (std::is_floating_point<T1>() && std::is_integral<T2>())
        {
            return ConversionError::TRUNCATION_DISALLOWED;
        }
    }

    for (int i = 0; i < rowCount; i++) {
        dstMem[i] = static_cast<T2>(srcMem[i]);
    }

    return ConversionError::NONE;
}

#endif //CONVERTERS_H


