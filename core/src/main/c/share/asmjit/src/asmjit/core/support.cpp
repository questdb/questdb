// AsmJit - Machine code generation for C++
//
//  * Official AsmJit Home Page: https://asmjit.com
//  * Official Github Repository: https://github.com/asmjit/asmjit
//
// Copyright (c) 2008-2020 The AsmJit Authors
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgment in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

#include "../core/api-build_p.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::Support - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
template<typename T>
static void testArrays(const T* a, const T* b, size_t size) noexcept {
  for (size_t i = 0; i < size; i++)
    EXPECT(a[i] == b[i], "Mismatch at %u", unsigned(i));
}

static void testAlignment() noexcept {
  INFO("Support::isAligned()");
  EXPECT(Support::isAligned<size_t>(0xFFFF,  4) == false);
  EXPECT(Support::isAligned<size_t>(0xFFF4,  4) == true);
  EXPECT(Support::isAligned<size_t>(0xFFF8,  8) == true);
  EXPECT(Support::isAligned<size_t>(0xFFF0, 16) == true);

  INFO("Support::alignUp()");
  EXPECT(Support::alignUp<size_t>(0xFFFF,  4) == 0x10000);
  EXPECT(Support::alignUp<size_t>(0xFFF4,  4) == 0x0FFF4);
  EXPECT(Support::alignUp<size_t>(0xFFF8,  8) == 0x0FFF8);
  EXPECT(Support::alignUp<size_t>(0xFFF0, 16) == 0x0FFF0);
  EXPECT(Support::alignUp<size_t>(0xFFF0, 32) == 0x10000);

  INFO("Support::alignUpDiff()");
  EXPECT(Support::alignUpDiff<size_t>(0xFFFF,  4) == 1);
  EXPECT(Support::alignUpDiff<size_t>(0xFFF4,  4) == 0);
  EXPECT(Support::alignUpDiff<size_t>(0xFFF8,  8) == 0);
  EXPECT(Support::alignUpDiff<size_t>(0xFFF0, 16) == 0);
  EXPECT(Support::alignUpDiff<size_t>(0xFFF0, 32) == 16);

  INFO("Support::alignUpPowerOf2()");
  EXPECT(Support::alignUpPowerOf2<size_t>(0x0000) == 0x00000);
  EXPECT(Support::alignUpPowerOf2<size_t>(0xFFFF) == 0x10000);
  EXPECT(Support::alignUpPowerOf2<size_t>(0xF123) == 0x10000);
  EXPECT(Support::alignUpPowerOf2<size_t>(0x0F00) == 0x01000);
  EXPECT(Support::alignUpPowerOf2<size_t>(0x0100) == 0x00100);
  EXPECT(Support::alignUpPowerOf2<size_t>(0x1001) == 0x02000);
}

static void testBitUtils() noexcept {
  uint32_t i;

  INFO("Support::shl() / shr()");
  EXPECT(Support::shl(int32_t(0x00001111), 16) == int32_t(0x11110000u));
  EXPECT(Support::shl(uint32_t(0x00001111), 16) == uint32_t(0x11110000u));
  EXPECT(Support::shr(int32_t(0x11110000u), 16) == int32_t(0x00001111u));
  EXPECT(Support::shr(uint32_t(0x11110000u), 16) == uint32_t(0x00001111u));
  EXPECT(Support::sar(int32_t(0xFFFF0000u), 16) == int32_t(0xFFFFFFFFu));
  EXPECT(Support::sar(uint32_t(0xFFFF0000u), 16) == uint32_t(0xFFFFFFFFu));

  INFO("Support::blsi()");
  for (i = 0; i < 32; i++) EXPECT(Support::blsi(uint32_t(1) << i) == uint32_t(1) << i);
  for (i = 0; i < 31; i++) EXPECT(Support::blsi(uint32_t(3) << i) == uint32_t(1) << i);
  for (i = 0; i < 64; i++) EXPECT(Support::blsi(uint64_t(1) << i) == uint64_t(1) << i);
  for (i = 0; i < 63; i++) EXPECT(Support::blsi(uint64_t(3) << i) == uint64_t(1) << i);

  INFO("Support::ctz()");
  for (i = 0; i < 32; i++) EXPECT(Support::ctz(uint32_t(1) << i) == i);
  for (i = 0; i < 64; i++) EXPECT(Support::ctz(uint64_t(1) << i) == i);
  for (i = 0; i < 32; i++) EXPECT(Support::constCtz(uint32_t(1) << i) == i);
  for (i = 0; i < 64; i++) EXPECT(Support::constCtz(uint64_t(1) << i) == i);

  INFO("Support::bitMask()");
  EXPECT(Support::bitMask(0, 1, 7) == 0x83u);
  for (i = 0; i < 32; i++)
    EXPECT(Support::bitMask(i) == (1u << i));

  INFO("Support::bitTest()");
  for (i = 0; i < 32; i++) {
    EXPECT(Support::bitTest((1 << i), i) == true, "Support::bitTest(%X, %u) should return true", (1 << i), i);
  }

  INFO("Support::lsbMask<uint32_t>()");
  for (i = 0; i < 32; i++) {
    uint32_t expectedBits = 0;
    for (uint32_t b = 0; b < i; b++)
      expectedBits |= uint32_t(1) << b;
    EXPECT(Support::lsbMask<uint32_t>(i) == expectedBits);
  }

  INFO("Support::lsbMask<uint64_t>()");
  for (i = 0; i < 64; i++) {
    uint64_t expectedBits = 0;
    for (uint32_t b = 0; b < i; b++)
      expectedBits |= uint64_t(1) << b;
    EXPECT(Support::lsbMask<uint64_t>(i) == expectedBits);
  }

  INFO("Support::popcnt()");
  for (i = 0; i < 32; i++) EXPECT(Support::popcnt((uint32_t(1) << i)) == 1);
  for (i = 0; i < 64; i++) EXPECT(Support::popcnt((uint64_t(1) << i)) == 1);
  EXPECT(Support::popcnt(0x000000F0) ==  4);
  EXPECT(Support::popcnt(0x10101010) ==  4);
  EXPECT(Support::popcnt(0xFF000000) ==  8);
  EXPECT(Support::popcnt(0xFFFFFFF7) == 31);
  EXPECT(Support::popcnt(0x7FFFFFFF) == 31);

  INFO("Support::isPowerOf2()");
  for (i = 0; i < 64; i++) {
    EXPECT(Support::isPowerOf2(uint64_t(1) << i) == true);
    EXPECT(Support::isPowerOf2((uint64_t(1) << i) ^ 0x001101) == false);
  }
}

static void testIntUtils() noexcept {
  INFO("Support::byteswap()");
  EXPECT(Support::byteswap32(int32_t(0x01020304)) == int32_t(0x04030201));
  EXPECT(Support::byteswap32(uint32_t(0x01020304)) == uint32_t(0x04030201));

  INFO("Support::bytepack()");
  union BytePackData {
    uint8_t bytes[4];
    uint32_t u32;
  } bpdata;

  bpdata.u32 = Support::bytepack32_4x8(0x00, 0x11, 0x22, 0x33);
  EXPECT(bpdata.bytes[0] == 0x00);
  EXPECT(bpdata.bytes[1] == 0x11);
  EXPECT(bpdata.bytes[2] == 0x22);
  EXPECT(bpdata.bytes[3] == 0x33);

  INFO("Support::isBetween()");
  EXPECT(Support::isBetween<int>(10 , 10, 20) == true);
  EXPECT(Support::isBetween<int>(11 , 10, 20) == true);
  EXPECT(Support::isBetween<int>(20 , 10, 20) == true);
  EXPECT(Support::isBetween<int>(9  , 10, 20) == false);
  EXPECT(Support::isBetween<int>(21 , 10, 20) == false);
  EXPECT(Support::isBetween<int>(101, 10, 20) == false);

  INFO("Support::isInt8()");
  EXPECT(Support::isInt8(-128) == true);
  EXPECT(Support::isInt8( 127) == true);
  EXPECT(Support::isInt8(-129) == false);
  EXPECT(Support::isInt8( 128) == false);

  INFO("Support::isInt16()");
  EXPECT(Support::isInt16(-32768) == true);
  EXPECT(Support::isInt16( 32767) == true);
  EXPECT(Support::isInt16(-32769) == false);
  EXPECT(Support::isInt16( 32768) == false);

  INFO("Support::isInt32()");
  EXPECT(Support::isInt32( 2147483647    ) == true);
  EXPECT(Support::isInt32(-2147483647 - 1) == true);
  EXPECT(Support::isInt32(uint64_t(2147483648u)) == false);
  EXPECT(Support::isInt32(uint64_t(0xFFFFFFFFu)) == false);
  EXPECT(Support::isInt32(uint64_t(0xFFFFFFFFu) + 1) == false);

  INFO("Support::isUInt8()");
  EXPECT(Support::isUInt8(0)   == true);
  EXPECT(Support::isUInt8(255) == true);
  EXPECT(Support::isUInt8(256) == false);
  EXPECT(Support::isUInt8(-1)  == false);

  INFO("Support::isUInt12()");
  EXPECT(Support::isUInt12(0)    == true);
  EXPECT(Support::isUInt12(4095) == true);
  EXPECT(Support::isUInt12(4096) == false);
  EXPECT(Support::isUInt12(-1)   == false);

  INFO("Support::isUInt16()");
  EXPECT(Support::isUInt16(0)     == true);
  EXPECT(Support::isUInt16(65535) == true);
  EXPECT(Support::isUInt16(65536) == false);
  EXPECT(Support::isUInt16(-1)    == false);

  INFO("Support::isUInt32()");
  EXPECT(Support::isUInt32(uint64_t(0xFFFFFFFF)) == true);
  EXPECT(Support::isUInt32(uint64_t(0xFFFFFFFF) + 1) == false);
  EXPECT(Support::isUInt32(-1) == false);
}

static void testReadWrite() noexcept {
  INFO("Support::readX() / writeX()");

  uint8_t arr[32] = { 0 };

  Support::writeU16uBE(arr + 1, 0x0102u);
  Support::writeU16uBE(arr + 3, 0x0304u);
  EXPECT(Support::readU32uBE(arr + 1) == 0x01020304u);
  EXPECT(Support::readU32uLE(arr + 1) == 0x04030201u);
  EXPECT(Support::readU32uBE(arr + 2) == 0x02030400u);
  EXPECT(Support::readU32uLE(arr + 2) == 0x00040302u);

  Support::writeU32uLE(arr + 5, 0x05060708u);
  EXPECT(Support::readU64uBE(arr + 1) == 0x0102030408070605u);
  EXPECT(Support::readU64uLE(arr + 1) == 0x0506070804030201u);

  Support::writeU64uLE(arr + 7, 0x1122334455667788u);
  EXPECT(Support::readU32uBE(arr + 8) == 0x77665544u);
}

static void testBitVector() noexcept {
  INFO("Support::bitVectorOp");
  {
    uint32_t vec[3] = { 0 };
    Support::bitVectorFill(vec, 1, 64);
    EXPECT(vec[0] == 0xFFFFFFFEu);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0x00000001u);

    Support::bitVectorClear(vec, 1, 1);
    EXPECT(vec[0] == 0xFFFFFFFCu);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0x00000001u);

    Support::bitVectorFill(vec, 0, 32);
    EXPECT(vec[0] == 0xFFFFFFFFu);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0x00000001u);

    Support::bitVectorClear(vec, 0, 32);
    EXPECT(vec[0] == 0x00000000u);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0x00000001u);

    Support::bitVectorFill(vec, 1, 30);
    EXPECT(vec[0] == 0x7FFFFFFEu);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0x00000001u);

    Support::bitVectorClear(vec, 1, 95);
    EXPECT(vec[0] == 0x00000000u);
    EXPECT(vec[1] == 0x00000000u);
    EXPECT(vec[2] == 0x00000000u);

    Support::bitVectorFill(vec, 32, 64);
    EXPECT(vec[0] == 0x00000000u);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0xFFFFFFFFu);

    Support::bitVectorSetBit(vec, 1, true);
    EXPECT(vec[0] == 0x00000002u);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0xFFFFFFFFu);

    Support::bitVectorSetBit(vec, 95, false);
    EXPECT(vec[0] == 0x00000002u);
    EXPECT(vec[1] == 0xFFFFFFFFu);
    EXPECT(vec[2] == 0x7FFFFFFFu);

    Support::bitVectorClear(vec, 33, 32);
    EXPECT(vec[0] == 0x00000002u);
    EXPECT(vec[1] == 0x00000001u);
    EXPECT(vec[2] == 0x7FFFFFFEu);
  }

  INFO("Support::bitVectorIndexOf");
  {
    uint32_t vec1[1] = { 0x80000000 };
    EXPECT(Support::bitVectorIndexOf(vec1, 0, true) == 31);
    EXPECT(Support::bitVectorIndexOf(vec1, 1, true) == 31);
    EXPECT(Support::bitVectorIndexOf(vec1, 31, true) == 31);

    uint32_t vec2[2] = { 0x00000000, 0x80000000 };
    EXPECT(Support::bitVectorIndexOf(vec2, 0, true) == 63);
    EXPECT(Support::bitVectorIndexOf(vec2, 1, true) == 63);
    EXPECT(Support::bitVectorIndexOf(vec2, 31, true) == 63);
    EXPECT(Support::bitVectorIndexOf(vec2, 32, true) == 63);
    EXPECT(Support::bitVectorIndexOf(vec2, 33, true) == 63);
    EXPECT(Support::bitVectorIndexOf(vec2, 63, true) == 63);

    uint32_t vec3[3] = { 0x00000001, 0x00000000, 0x80000000 };
    EXPECT(Support::bitVectorIndexOf(vec3, 0, true) == 0);
    EXPECT(Support::bitVectorIndexOf(vec3, 1, true) == 95);
    EXPECT(Support::bitVectorIndexOf(vec3, 2, true) == 95);
    EXPECT(Support::bitVectorIndexOf(vec3, 31, true) == 95);
    EXPECT(Support::bitVectorIndexOf(vec3, 32, true) == 95);
    EXPECT(Support::bitVectorIndexOf(vec3, 63, true) == 95);
    EXPECT(Support::bitVectorIndexOf(vec3, 64, true) == 95);
    EXPECT(Support::bitVectorIndexOf(vec3, 95, true) == 95);

    uint32_t vec4[3] = { ~vec3[0], ~vec3[1], ~vec3[2] };
    EXPECT(Support::bitVectorIndexOf(vec4, 0, false) == 0);
    EXPECT(Support::bitVectorIndexOf(vec4, 1, false) == 95);
    EXPECT(Support::bitVectorIndexOf(vec4, 2, false) == 95);
    EXPECT(Support::bitVectorIndexOf(vec4, 31, false) == 95);
    EXPECT(Support::bitVectorIndexOf(vec4, 32, false) == 95);
    EXPECT(Support::bitVectorIndexOf(vec4, 63, false) == 95);
    EXPECT(Support::bitVectorIndexOf(vec4, 64, false) == 95);
    EXPECT(Support::bitVectorIndexOf(vec4, 95, false) == 95);
  }

  INFO("Support::BitWordIterator<uint32_t>");
  {
    Support::BitWordIterator<uint32_t> it(0x80000F01u);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 0);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 8);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 9);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 10);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 11);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 31);
    EXPECT(!it.hasNext());

    // No bits set.
    it.init(0x00000000u);
    ASMJIT_ASSERT(!it.hasNext());

    // Only first bit set.
    it.init(0x00000001u);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 0);
    ASMJIT_ASSERT(!it.hasNext());

    // Only last bit set (special case).
    it.init(0x80000000u);
    ASMJIT_ASSERT(it.hasNext());
    ASMJIT_ASSERT(it.next() == 31);
    ASMJIT_ASSERT(!it.hasNext());
  }

  INFO("Support::BitWordIterator<uint64_t>");
  {
    Support::BitWordIterator<uint64_t> it(uint64_t(1) << 63);
    ASMJIT_ASSERT(it.hasNext());
    ASMJIT_ASSERT(it.next() == 63);
    ASMJIT_ASSERT(!it.hasNext());
  }

  INFO("Support::BitVectorIterator<uint32_t>");
  {
    // Border cases.
    static const uint32_t bitsNone[] = { 0xFFFFFFFFu };
    Support::BitVectorIterator<uint32_t> it(bitsNone, 0);

    EXPECT(!it.hasNext());
    it.init(bitsNone, 0, 1);
    EXPECT(!it.hasNext());
    it.init(bitsNone, 0, 128);
    EXPECT(!it.hasNext());

    static const uint32_t bits1[] = { 0x80000008u, 0x80000001u, 0x00000000u, 0x80000000u, 0x00000000u, 0x00000000u, 0x00003000u };
    it.init(bits1, ASMJIT_ARRAY_SIZE(bits1));

    EXPECT(it.hasNext());
    EXPECT(it.next() == 3);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 31);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 32);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 63);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 127);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 204);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 205);
    EXPECT(!it.hasNext());

    it.init(bits1, ASMJIT_ARRAY_SIZE(bits1), 4);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 31);

    it.init(bits1, ASMJIT_ARRAY_SIZE(bits1), 64);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 127);

    it.init(bits1, ASMJIT_ARRAY_SIZE(bits1), 127);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 127);

    static const uint32_t bits2[] = { 0x80000000u, 0x80000000u, 0x00000000u, 0x80000000u };
    it.init(bits2, ASMJIT_ARRAY_SIZE(bits2));

    EXPECT(it.hasNext());
    EXPECT(it.next() == 31);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 63);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 127);
    EXPECT(!it.hasNext());

    static const uint32_t bits3[] = { 0x00000000u, 0x00000000u, 0x00000000u, 0x00000000u };
    it.init(bits3, ASMJIT_ARRAY_SIZE(bits3));
    EXPECT(!it.hasNext());

    static const uint32_t bits4[] = { 0x00000000u, 0x00000000u, 0x00000000u, 0x80000000u };
    it.init(bits4, ASMJIT_ARRAY_SIZE(bits4));
    EXPECT(it.hasNext());
    EXPECT(it.next() == 127);
    EXPECT(!it.hasNext());
  }

  INFO("Support::BitVectorIterator<uint64_t>");
  {
    static const uint64_t bits1[] = { 0x80000000u, 0x80000000u, 0x00000000u, 0x80000000u };
    Support::BitVectorIterator<uint64_t> it(bits1, ASMJIT_ARRAY_SIZE(bits1));

    EXPECT(it.hasNext());
    EXPECT(it.next() == 31);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 95);
    EXPECT(it.hasNext());
    EXPECT(it.next() == 223);
    EXPECT(!it.hasNext());

    static const uint64_t bits2[] = { 0x8000000000000000u, 0, 0, 0 };
    it.init(bits2, ASMJIT_ARRAY_SIZE(bits2));

    EXPECT(it.hasNext());
    EXPECT(it.next() == 63);
    EXPECT(!it.hasNext());
  }
}

static void testSorting() noexcept {
  INFO("Support::qSort() - Testing qsort and isort of predefined arrays");
  {
    constexpr size_t kArraySize = 11;

    int ref_[kArraySize] = { -4, -2, -1, 0, 1, 9, 12, 13, 14, 19, 22 };
    int arr1[kArraySize] = { 0, 1, -1, 19, 22, 14, -4, 9, 12, 13, -2 };
    int arr2[kArraySize];

    memcpy(arr2, arr1, kArraySize * sizeof(int));

    Support::iSort(arr1, kArraySize);
    Support::qSort(arr2, kArraySize);
    testArrays(arr1, ref_, kArraySize);
    testArrays(arr2, ref_, kArraySize);
  }

  INFO("Support::qSort() - Testing qsort and isort of artificial arrays");
  {
    constexpr size_t kArraySize = 200;

    int arr1[kArraySize];
    int arr2[kArraySize];
    int ref_[kArraySize];

    for (size_t size = 2; size < kArraySize; size++) {
      for (size_t i = 0; i < size; i++) {
        arr1[i] = int(size - 1 - i);
        arr2[i] = int(size - 1 - i);
        ref_[i] = int(i);
      }

      Support::iSort(arr1, size);
      Support::qSort(arr2, size);
      testArrays(arr1, ref_, size);
      testArrays(arr2, ref_, size);
    }
  }

  INFO("Support::qSort() - Testing qsort and isort with an unstable compare function");
  {
    constexpr size_t kArraySize = 5;

    float arr1[kArraySize] = { 1.0f, 0.0f, 3.0f, -1.0f, std::numeric_limits<float>::quiet_NaN() };
    float arr2[kArraySize] = { };

    memcpy(arr2, arr1, kArraySize * sizeof(float));

    // We don't test as it's undefined where the NaN would be.
    Support::iSort(arr1, kArraySize);
    Support::qSort(arr2, kArraySize);
  }
}

UNIT(support) {
  testAlignment();
  testBitUtils();
  testIntUtils();
  testReadWrite();
  testBitVector();
  testSorting();
}
#endif

ASMJIT_END_NAMESPACE
