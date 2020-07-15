#include "vec_ts_agg.h"
#include "vec_agg.h"

#define MAX_VECTOR_SIZE 512

#if INSTRSET >= 10

//#define HOUR_COUNT F_AVX512(keyedHourCount)

#elif INSTRSET >= 8

//#define HOUR_COUNT F_AVX2(keyedHourCount)

#elif INSTRSET >= 5

//#define HOUR_COUNT F_SSE41(keyedHourCount)

#elif INSTRSET >= 2

//#define HOUR_COUNT F_SSE2(keyedHourCount)

#else

#endif


#if INSTRSET < 5

// Dispatchers
//ROSTI_DISPATCHER(keyedHourCount)

#define HOUR_MICROS  3600000000L
#define DAY_HOURS  24

extern "C" {



}

#endif
