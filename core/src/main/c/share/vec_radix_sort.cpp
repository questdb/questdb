//
// Created by blues on 04/07/2020.
//
#include "jni.h"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <xmmintrin.h>
#include "vcl/vectorclass.h"

typedef struct {
    uint64_t c8[256];
    uint64_t c7[256];
    uint64_t c6[256];
    uint64_t c5[256];
    uint64_t c4[256];
    uint64_t c3[256];
    uint64_t c2[256];
    uint64_t c1[256];
} rscounts_t;

#define RADIX_SHUFFLE 0

#if RADIX_SHUFFLE == 0
template<uint16_t sh>
inline void radix_shuffle(uint64_t* counts, int64_t* src, int64_t* dest, uint64_t size) {
    _mm_prefetch(counts, _MM_HINT_NTA);
    for (uint64_t x = 0; x < size; x++) {
        _mm_prefetch(src + x + 64, _MM_HINT_T2);
        const uint16_t digit = (src[x] >> sh) & 0xff;
        dest[counts[digit]] = src[x];
        counts[digit]++;
    }
}
#elif RADIX_SHUFFLE == 1

template<uint16_t sh>
inline void radix_shuffle(uint64_t *counts, int64_t *src, int64_t *dest, uint64_t size) {
    _mm_prefetch(counts, _MM_HINT_NTA);
    Vec4q vec;
    Vec4q digitVec;
    int64_t values[4];
    int64_t digits[4];
    for (uint64_t x = 0; x < size; x += 4) {
        _mm_prefetch(src + x + 64, _MM_HINT_T0);
        vec.load(src + x);
        digitVec = (vec >> sh) & 0xff;

        vec.store(values);
        digitVec.store(digits);

        dest[counts[digits[0]]] = values[0];
        counts[digits[0]]++;

        dest[counts[digits[1]]] = values[1];
        counts[digits[1]]++;

        dest[counts[digits[2]]] = values[2];
        counts[digits[2]]++;

        dest[counts[digits[3]]] = values[3];
        counts[digits[3]]++;
    }
}

#elif RADIX_SHUFFLE == 2
template<uint16_t sh>
inline void radix_shuffle(uint64_t* counts, int64_t* src, int64_t* dest, uint64_t size) {
    _mm_prefetch(counts, _MM_HINT_NTA);
    Vec8q vec;
    Vec8q digitVec;
    int64_t values[8];
    int64_t digits[8];
    for (uint64_t x = 0; x < size; x += 8) {
        _mm_prefetch(src + x + 64, _MM_HINT_T0);
        vec.load(src + x);
        digitVec = (vec >> sh) & 0xff;

        vec.store(values);
        digitVec.store(digits);

        dest[counts[digits[0]]] = values[0];
        counts[digits[0]]++;

        dest[counts[digits[1]]] = values[1];
        counts[digits[1]]++;

        dest[counts[digits[2]]] = values[2];
        counts[digits[2]]++;

        dest[counts[digits[3]]] = values[3];
        counts[digits[3]]++;

        dest[counts[digits[4]]] = values[4];
        counts[digits[4]]++;

        dest[counts[digits[5]]] = values[5];
        counts[digits[5]]++;

        dest[counts[digits[6]]] = values[6];
        counts[digits[6]]++;

        dest[counts[digits[7]]] = values[7];
        counts[digits[7]]++;
    }
}
#endif

int64_t *radixSort(int64_t *array, uint64_t size) {
    rscounts_t counts;
    memset(&counts, 0, 256 * 8 * sizeof(uint64_t));
    auto *cpy = (int64_t *) malloc(size * sizeof(int64_t));
    int64_t o8 = 0, o7 = 0, o6 = 0, o5 = 0, o4 = 0, o3 = 0, o2 = 0, o1 = 0;
    int64_t t8, t7, t6, t5, t4, t3, t2, t1;
    int64_t x;

    // calculate counts
    _mm_prefetch(counts.c8, _MM_HINT_NTA);
    for (x = 0; x < size; x++) {
        _mm_prefetch(array + x + 64, _MM_HINT_T2);
        t8 = array[x] & 0xff;
        t7 = (array[x] >> 8u) & 0xff;
        t6 = (array[x] >> 16u) & 0xff;
        t5 = (array[x] >> 24u) & 0xff;
        counts.c8[t8]++;
        counts.c7[t7]++;
        counts.c6[t6]++;
        counts.c5[t5]++;
    }

    // calculate counts
    _mm_prefetch(counts.c4, _MM_HINT_NTA);
    for (x = 0; x < size; x++) {
        t4 = (array[x] >> 32) & 0xff;
        t3 = (array[x] >> 40) & 0xff;
        t2 = (array[x] >> 48) & 0xff;
        t1 = (array[x] >> 56) & 0xff;
        counts.c4[t4]++;
        counts.c3[t3]++;
        counts.c2[t2]++;
        counts.c1[t1]++;
    }

    // convert counts to offsets
    _mm_prefetch(&counts, _MM_HINT_NTA);
    for (x = 0; x < 256; x++) {
        t8 = o8 + counts.c8[x];
        t7 = o7 + counts.c7[x];
        t6 = o6 + counts.c6[x];
        t5 = o5 + counts.c5[x];
        counts.c8[x] = o8;
        counts.c7[x] = o7;
        counts.c6[x] = o6;
        counts.c5[x] = o5;
        o8 = t8;
        o7 = t7;
        o6 = t6;
        o5 = t5;
    }

    // convert counts to offsets
    _mm_prefetch(&counts, _MM_HINT_NTA);
    for (x = 0; x < 256; x++) {
        t4 = o4 + counts.c4[x];
        t3 = o3 + counts.c3[x];
        t2 = o2 + counts.c2[x];
        t1 = o1 + counts.c1[x];
        counts.c4[x] = o4;
        counts.c3[x] = o3;
        counts.c2[x] = o2;
        counts.c1[x] = o1;
        o4 = t4;
        o3 = t3;
        o2 = t2;
        o1 = t1;
    }

    // radix
    radix_shuffle<0> (counts.c8, array, cpy, size);
    radix_shuffle<8> (counts.c7, cpy, array, size);
    radix_shuffle<16>(counts.c6, array, cpy, size);
    radix_shuffle<24>(counts.c5, cpy, array, size);
    radix_shuffle<32>(counts.c4, array, cpy, size);
    radix_shuffle<40>(counts.c3, cpy, array, size);
    radix_shuffle<48>(counts.c2, array, cpy, size);
    radix_shuffle<56>(counts.c1, cpy, array, size);
    free(cpy);
    return array;
}

inline void kernel(int64_t *a, int64_t len, int p, int q) {
    const auto d = 1 << (p - q);
//    printf("in [len=%lldl, p=%d, q=%d]\n", len, p, q);

    for (uint64_t i = 0; i < len; i++) {
        bool up = ((i >> p) & 2) == 0;

        if ((i & d) == 0 && (a[i] > a[i | d]) == up) {
            int t = a[i];
            a[i] = a[i | d];
            a[i | d] = t;
        }
    }
}

inline void swap(int64_t * a, int64_t* b)
{
    const auto t = *a;
    *a = *b;
    *b = t;
}

/* This function takes last element as pivot, places
   the pivot element at its correct position in sorted
    array, and places all smaller (smaller than pivot)
   to left of pivot and all greater elements to right
   of pivot */
int64_t partition (int64_t *arr, int64_t low, int64_t high)
{
    const auto pivot = arr[high];    // pivot
    auto i = (low - 1);  // Index of smaller element

    for (int64_t j = low; j <= high- 1; j++)
    {
        // If current element is smaller than or
        // equal to pivot
        if (arr[j] <= pivot)
        {
            i++;    // increment index of smaller element
            swap(&arr[i], &arr[j]);
        }
    }
    swap(&arr[i + 1], &arr[high]);
    return (i + 1);
}

/* The main function that implements QuickSort
 arr[] --> Array to be sorted,
  low  --> Starting index,
  high  --> Ending index */
void quickSort(int64_t arr[], int64_t low, int64_t high)
{
    if (low < high)
    {
        /* pi is partitioning index, arr[p] is now
           at right place */
        int pi = partition(arr, low, high);

        // Separately sort elements before
        // partition and after partition
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

void insertionSort(int64_t arr[], int64_t n)
{
    _mm_prefetch(arr, _MM_HINT_T0);
    int64_t i, key, j;
    for (i = 1; i < n; i++)
    {
        key = arr[i];
        j = i - 1;

        /* Move elements of arr[0..i-1], that are
        greater than key, to one position ahead
        of their current position */
        while (j >= 0 && arr[j] > key)
        {
            arr[j + 1] = arr[j];
            j = j - 1;
        }
        arr[j + 1] = key;
    }
}

extern "C" {

JNIEXPORT void JNICALL Java_io_questdb_std_Vect_radixSort(JNIEnv *env, jclass cl, jlong pLong, jlong len) {
    radixSort(reinterpret_cast<int64_t *>(pLong), len);
//    quickSort(reinterpret_cast<int64_t *>(pLong), 0, len);
//    for (int i = 0; i < logn; i++) {
//        for (int j = 0; j <= i; j++) {
//            kernel(reinterpret_cast<int64_t *>(pLong), len, i, j);
//        }
//    }
}

}