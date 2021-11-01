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
#include "../core/zone.h"
#include "../core/zonevector.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::ZoneVectorBase - Helpers]
// ============================================================================

Error ZoneVectorBase::_grow(ZoneAllocator* allocator, uint32_t sizeOfT, uint32_t n) noexcept {
  uint32_t threshold = Globals::kGrowThreshold / sizeOfT;
  uint32_t capacity = _capacity;
  uint32_t after = _size;

  if (ASMJIT_UNLIKELY(std::numeric_limits<uint32_t>::max() - n < after))
    return DebugUtils::errored(kErrorOutOfMemory);

  after += n;
  if (capacity >= after)
    return kErrorOk;

  // ZoneVector is used as an array to hold short-lived data structures used
  // during code generation. The growing strategy is simple - use small capacity
  // at the beginning (very good for ZoneAllocator) and then grow quicker to
  // prevent successive reallocations.
  if (capacity < 4)
    capacity = 4;
  else if (capacity < 8)
    capacity = 8;
  else if (capacity < 16)
    capacity = 16;
  else if (capacity < 64)
    capacity = 64;
  else if (capacity < 256)
    capacity = 256;

  while (capacity < after) {
    if (capacity < threshold)
      capacity *= 2;
    else
      capacity += threshold;
  }

  return _reserve(allocator, sizeOfT, capacity);
}

Error ZoneVectorBase::_reserve(ZoneAllocator* allocator, uint32_t sizeOfT, uint32_t n) noexcept {
  uint32_t oldCapacity = _capacity;
  if (oldCapacity >= n) return kErrorOk;

  uint32_t nBytes = n * sizeOfT;
  if (ASMJIT_UNLIKELY(nBytes < n))
    return DebugUtils::errored(kErrorOutOfMemory);

  size_t allocatedBytes;
  uint8_t* newData = static_cast<uint8_t*>(allocator->alloc(nBytes, allocatedBytes));

  if (ASMJIT_UNLIKELY(!newData))
    return DebugUtils::errored(kErrorOutOfMemory);

  void* oldData = _data;
  if (_size)
    memcpy(newData, oldData, size_t(_size) * sizeOfT);

  if (oldData)
    allocator->release(oldData, size_t(oldCapacity) * sizeOfT);

  _capacity = uint32_t(allocatedBytes / sizeOfT);
  ASMJIT_ASSERT(_capacity >= n);

  _data = newData;
  return kErrorOk;
}

Error ZoneVectorBase::_resize(ZoneAllocator* allocator, uint32_t sizeOfT, uint32_t n) noexcept {
  uint32_t size = _size;

  if (_capacity < n) {
    ASMJIT_PROPAGATE(_grow(allocator, sizeOfT, n - size));
    ASMJIT_ASSERT(_capacity >= n);
  }

  if (size < n)
    memset(static_cast<uint8_t*>(_data) + size_t(size) * sizeOfT, 0, size_t(n - size) * sizeOfT);

  _size = n;
  return kErrorOk;
}

// ============================================================================
// [asmjit::ZoneBitVector - Ops]
// ============================================================================

Error ZoneBitVector::copyFrom(ZoneAllocator* allocator, const ZoneBitVector& other) noexcept {
  BitWord* data = _data;
  uint32_t newSize = other.size();

  if (!newSize) {
    _size = 0;
    return kErrorOk;
  }

  if (newSize > _capacity) {
    // Realloc needed... Calculate the minimum capacity (in bytes) requied.
    uint32_t minimumCapacityInBits = Support::alignUp<uint32_t>(newSize, kBitWordSizeInBits);
    if (ASMJIT_UNLIKELY(minimumCapacityInBits < newSize))
      return DebugUtils::errored(kErrorOutOfMemory);

    // Normalize to bytes.
    uint32_t minimumCapacity = minimumCapacityInBits / 8;
    size_t allocatedCapacity;

    BitWord* newData = static_cast<BitWord*>(allocator->alloc(minimumCapacity, allocatedCapacity));
    if (ASMJIT_UNLIKELY(!newData))
      return DebugUtils::errored(kErrorOutOfMemory);

    // `allocatedCapacity` now contains number in bytes, we need bits.
    size_t allocatedCapacityInBits = allocatedCapacity * 8;

    // Arithmetic overflow should normally not happen. If it happens we just
    // change the `allocatedCapacityInBits` to the `minimumCapacityInBits` as
    // this value is still safe to be used to call `_allocator->release(...)`.
    if (ASMJIT_UNLIKELY(allocatedCapacityInBits < allocatedCapacity))
      allocatedCapacityInBits = minimumCapacityInBits;

    if (data)
      allocator->release(data, _capacity / 8);
    data = newData;

    _data = data;
    _capacity = uint32_t(allocatedCapacityInBits);
  }

  _size = newSize;
  _copyBits(data, other.data(), _wordsPerBits(newSize));

  return kErrorOk;
}

Error ZoneBitVector::_resize(ZoneAllocator* allocator, uint32_t newSize, uint32_t idealCapacity, bool newBitsValue) noexcept {
  ASMJIT_ASSERT(idealCapacity >= newSize);

  if (newSize <= _size) {
    // The size after the resize is lesser than or equal to the current size.
    uint32_t idx = newSize / kBitWordSizeInBits;
    uint32_t bit = newSize % kBitWordSizeInBits;

    // Just set all bits outside of the new size in the last word to zero.
    // There is a case that there are not bits to set if `bit` is zero. This
    // happens when `newSize` is a multiply of `kBitWordSizeInBits` like 64, 128,
    // and so on. In that case don't change anything as that would mean settings
    // bits outside of the `_size`.
    if (bit)
      _data[idx] &= (BitWord(1) << bit) - 1u;

    _size = newSize;
    return kErrorOk;
  }

  uint32_t oldSize = _size;
  BitWord* data = _data;

  if (newSize > _capacity) {
    // Realloc needed, calculate the minimum capacity (in bytes) requied.
    uint32_t minimumCapacityInBits = Support::alignUp<uint32_t>(idealCapacity, kBitWordSizeInBits);

    if (ASMJIT_UNLIKELY(minimumCapacityInBits < newSize))
      return DebugUtils::errored(kErrorOutOfMemory);

    // Normalize to bytes.
    uint32_t minimumCapacity = minimumCapacityInBits / 8;
    size_t allocatedCapacity;

    BitWord* newData = static_cast<BitWord*>(allocator->alloc(minimumCapacity, allocatedCapacity));
    if (ASMJIT_UNLIKELY(!newData))
      return DebugUtils::errored(kErrorOutOfMemory);

    // `allocatedCapacity` now contains number in bytes, we need bits.
    size_t allocatedCapacityInBits = allocatedCapacity * 8;

    // Arithmetic overflow should normally not happen. If it happens we just
    // change the `allocatedCapacityInBits` to the `minimumCapacityInBits` as
    // this value is still safe to be used to call `_allocator->release(...)`.
    if (ASMJIT_UNLIKELY(allocatedCapacityInBits < allocatedCapacity))
      allocatedCapacityInBits = minimumCapacityInBits;

    _copyBits(newData, data, _wordsPerBits(oldSize));

    if (data)
      allocator->release(data, _capacity / 8);
    data = newData;

    _data = data;
    _capacity = uint32_t(allocatedCapacityInBits);
  }

  // Start (of the old size) and end (of the new size) bits
  uint32_t idx = oldSize / kBitWordSizeInBits;
  uint32_t startBit = oldSize % kBitWordSizeInBits;
  uint32_t endBit = newSize % kBitWordSizeInBits;

  // Set new bits to either 0 or 1. The `pattern` is used to set multiple
  // bits per bit-word and contains either all zeros or all ones.
  BitWord pattern = Support::bitMaskFromBool<BitWord>(newBitsValue);

  // First initialize the last bit-word of the old size.
  if (startBit) {
    uint32_t nBits = 0;

    if (idx == (newSize / kBitWordSizeInBits)) {
      // The number of bit-words is the same after the resize. In that case
      // we need to set only bits necessary in the current last bit-word.
      ASMJIT_ASSERT(startBit < endBit);
      nBits = endBit - startBit;
    }
    else {
      // There is be more bit-words after the resize. In that case we don't
      // have to be extra careful about the last bit-word of the old size.
      nBits = kBitWordSizeInBits - startBit;
    }

    data[idx++] |= pattern << nBits;
  }

  // Initialize all bit-words after the last bit-word of the old size.
  uint32_t endIdx = _wordsPerBits(newSize);
  while (idx < endIdx) data[idx++] = pattern;

  // Clear unused bits of the last bit-word.
  if (endBit)
    data[endIdx - 1] = pattern & ((BitWord(1) << endBit) - 1);

  _size = newSize;
  return kErrorOk;
}

Error ZoneBitVector::_append(ZoneAllocator* allocator, bool value) noexcept {
  uint32_t kThreshold = Globals::kGrowThreshold * 8;
  uint32_t newSize = _size + 1;
  uint32_t idealCapacity = _capacity;

  if (idealCapacity < 128)
    idealCapacity = 128;
  else if (idealCapacity <= kThreshold)
    idealCapacity *= 2;
  else
    idealCapacity += kThreshold;

  if (ASMJIT_UNLIKELY(idealCapacity < _capacity)) {
    if (ASMJIT_UNLIKELY(_size == std::numeric_limits<uint32_t>::max()))
      return DebugUtils::errored(kErrorOutOfMemory);
    idealCapacity = newSize;
  }

  return _resize(allocator, newSize, idealCapacity, value);
}

// ============================================================================
// [asmjit::ZoneVector / ZoneBitVector - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
template<typename T>
static void test_zone_vector(ZoneAllocator* allocator, const char* typeName) {
  int i;
  int kMax = 100000;

  ZoneVector<T> vec;

  INFO("ZoneVector<%s> basic tests", typeName);
  EXPECT(vec.append(allocator, 0) == kErrorOk);
  EXPECT(vec.empty() == false);
  EXPECT(vec.size() == 1);
  EXPECT(vec.capacity() >= 1);
  EXPECT(vec.indexOf(0) == 0);
  EXPECT(vec.indexOf(-11) == Globals::kNotFound);

  vec.clear();
  EXPECT(vec.empty());
  EXPECT(vec.size() == 0);
  EXPECT(vec.indexOf(0) == Globals::kNotFound);

  for (i = 0; i < kMax; i++) {
    EXPECT(vec.append(allocator, T(i)) == kErrorOk);
  }
  EXPECT(vec.empty() == false);
  EXPECT(vec.size() == uint32_t(kMax));
  EXPECT(vec.indexOf(T(kMax - 1)) == uint32_t(kMax - 1));

  EXPECT(vec.rbegin()[0] == kMax - 1);

  vec.release(allocator);
}

static void test_zone_bitvector(ZoneAllocator* allocator) {
  Zone zone(8096 - Zone::kBlockOverhead);

  uint32_t i, count;
  uint32_t kMaxCount = 100;

  ZoneBitVector vec;
  EXPECT(vec.empty());
  EXPECT(vec.size() == 0);

  INFO("ZoneBitVector::resize()");
  for (count = 1; count < kMaxCount; count++) {
    vec.clear();
    EXPECT(vec.resize(allocator, count, false) == kErrorOk);
    EXPECT(vec.size() == count);

    for (i = 0; i < count; i++)
      EXPECT(vec.bitAt(i) == false);

    vec.clear();
    EXPECT(vec.resize(allocator, count, true) == kErrorOk);
    EXPECT(vec.size() == count);

    for (i = 0; i < count; i++)
      EXPECT(vec.bitAt(i) == true);
  }

  INFO("ZoneBitVector::fillBits() / clearBits()");
  for (count = 1; count < kMaxCount; count += 2) {
    vec.clear();
    EXPECT(vec.resize(allocator, count) == kErrorOk);
    EXPECT(vec.size() == count);

    for (i = 0; i < (count + 1) / 2; i++) {
      bool value = bool(i & 1);
      if (value)
        vec.fillBits(i, count - i * 2);
      else
        vec.clearBits(i, count - i * 2);
    }

    for (i = 0; i < count; i++) {
      EXPECT(vec.bitAt(i) == bool(i & 1));
    }
  }
}

UNIT(zone_vector) {
  Zone zone(8096 - Zone::kBlockOverhead);
  ZoneAllocator allocator(&zone);

  test_zone_vector<int>(&allocator, "int");
  test_zone_vector<int64_t>(&allocator, "int64_t");
  test_zone_bitvector(&allocator);
}
#endif

ASMJIT_END_NAMESPACE
