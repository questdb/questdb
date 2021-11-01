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
#include "../core/constpool.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::ConstPool - Construction / Destruction]
// ============================================================================

ConstPool::ConstPool(Zone* zone) noexcept { reset(zone); }
ConstPool::~ConstPool() noexcept {}

// ============================================================================
// [asmjit::ConstPool - Reset]
// ============================================================================

void ConstPool::reset(Zone* zone) noexcept {
  _zone = zone;

  size_t dataSize = 1;
  for (size_t i = 0; i < ASMJIT_ARRAY_SIZE(_tree); i++) {
    _tree[i].reset();
    _tree[i].setDataSize(dataSize);
    _gaps[i] = nullptr;
    dataSize <<= 1;
  }

  _gapPool = nullptr;
  _size = 0;
  _alignment = 0;
  _minItemSize = 0;
}

// ============================================================================
// [asmjit::ConstPool - Ops]
// ============================================================================

static ASMJIT_INLINE ConstPool::Gap* ConstPool_allocGap(ConstPool* self) noexcept {
  ConstPool::Gap* gap = self->_gapPool;
  if (!gap)
    return self->_zone->allocT<ConstPool::Gap>();

  self->_gapPool = gap->_next;
  return gap;
}

static ASMJIT_INLINE void ConstPool_freeGap(ConstPool* self, ConstPool::Gap* gap) noexcept {
  gap->_next = self->_gapPool;
  self->_gapPool = gap;
}

static void ConstPool_addGap(ConstPool* self, size_t offset, size_t size) noexcept {
  ASMJIT_ASSERT(size > 0);

  while (size > 0) {
    size_t gapIndex;
    size_t gapSize;

    if (size >= 16 && Support::isAligned<size_t>(offset, 16)) {
      gapIndex = ConstPool::kIndex16;
      gapSize = 16;
    }
    else if (size >= 8 && Support::isAligned<size_t>(offset, 8)) {
      gapIndex = ConstPool::kIndex8;
      gapSize = 8;
    }
    else if (size >= 4 && Support::isAligned<size_t>(offset, 4)) {
      gapIndex = ConstPool::kIndex4;
      gapSize = 4;
    }
    else if (size >= 2 && Support::isAligned<size_t>(offset, 2)) {
      gapIndex = ConstPool::kIndex2;
      gapSize = 2;
    }
    else {
      gapIndex = ConstPool::kIndex1;
      gapSize = 1;
    }

    // We don't have to check for errors here, if this failed nothing really
    // happened (just the gap won't be visible) and it will fail again at
    // place where the same check would generate `kErrorOutOfMemory` error.
    ConstPool::Gap* gap = ConstPool_allocGap(self);
    if (!gap)
      return;

    gap->_next = self->_gaps[gapIndex];
    self->_gaps[gapIndex] = gap;

    gap->_offset = offset;
    gap->_size = gapSize;

    offset += gapSize;
    size -= gapSize;
  }
}

Error ConstPool::add(const void* data, size_t size, size_t& dstOffset) noexcept {
  size_t treeIndex;

  if (size == 32)
    treeIndex = kIndex32;
  else if (size == 16)
    treeIndex = kIndex16;
  else if (size == 8)
    treeIndex = kIndex8;
  else if (size == 4)
    treeIndex = kIndex4;
  else if (size == 2)
    treeIndex = kIndex2;
  else if (size == 1)
    treeIndex = kIndex1;
  else
    return DebugUtils::errored(kErrorInvalidArgument);

  ConstPool::Node* node = _tree[treeIndex].get(data);
  if (node) {
    dstOffset = node->_offset;
    return kErrorOk;
  }

  // Before incrementing the current offset try if there is a gap that can
  // be used for the requested data.
  size_t offset = ~size_t(0);
  size_t gapIndex = treeIndex;

  while (gapIndex != kIndexCount - 1) {
    ConstPool::Gap* gap = _gaps[treeIndex];

    // Check if there is a gap.
    if (gap) {
      size_t gapOffset = gap->_offset;
      size_t gapSize = gap->_size;

      // Destroy the gap for now.
      _gaps[treeIndex] = gap->_next;
      ConstPool_freeGap(this, gap);

      offset = gapOffset;
      ASMJIT_ASSERT(Support::isAligned<size_t>(offset, size));

      gapSize -= size;
      if (gapSize > 0)
        ConstPool_addGap(this, gapOffset, gapSize);
    }

    gapIndex++;
  }

  if (offset == ~size_t(0)) {
    // Get how many bytes have to be skipped so the address is aligned accordingly
    // to the 'size'.
    size_t diff = Support::alignUpDiff<size_t>(_size, size);

    if (diff != 0) {
      ConstPool_addGap(this, _size, diff);
      _size += diff;
    }

    offset = _size;
    _size += size;
  }

  // Add the initial node to the right index.
  node = ConstPool::Tree::_newNode(_zone, data, size, offset, false);
  if (ASMJIT_UNLIKELY(!node))
    return DebugUtils::errored(kErrorOutOfMemory);

  _tree[treeIndex].insert(node);
  _alignment = Support::max<size_t>(_alignment, size);

  dstOffset = offset;

  // Now create a bunch of shared constants that are based on the data pattern.
  // We stop at size 4, it probably doesn't make sense to split constants down
  // to 1 byte.
  size_t pCount = 1;
  size_t smallerSize = size;

  while (smallerSize > 4) {
    pCount <<= 1;
    smallerSize >>= 1;

    ASMJIT_ASSERT(treeIndex != 0);
    treeIndex--;

    const uint8_t* pData = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < pCount; i++, pData += smallerSize) {
      node = _tree[treeIndex].get(pData);
      if (node) continue;

      node = ConstPool::Tree::_newNode(_zone, pData, smallerSize, offset + (i * smallerSize), true);
      _tree[treeIndex].insert(node);
    }
  }

  if (_minItemSize == 0)
    _minItemSize = size;
  else
    _minItemSize = Support::min(_minItemSize, size);

  return kErrorOk;
}

// ============================================================================
// [asmjit::ConstPool - Reset]
// ============================================================================

struct ConstPoolFill {
  inline ConstPoolFill(uint8_t* dst, size_t dataSize) noexcept :
    _dst(dst),
    _dataSize(dataSize) {}

  inline void operator()(const ConstPool::Node* node) noexcept {
    if (!node->_shared)
      memcpy(_dst + node->_offset, node->data(), _dataSize);
  }

  uint8_t* _dst;
  size_t _dataSize;
};

void ConstPool::fill(void* dst) const noexcept {
  // Clears possible gaps, asmjit should never emit garbage to the output.
  memset(dst, 0, _size);

  ConstPoolFill filler(static_cast<uint8_t*>(dst), 1);
  for (size_t i = 0; i < ASMJIT_ARRAY_SIZE(_tree); i++) {
    _tree[i].forEach(filler);
    filler._dataSize <<= 1;
  }
}

// ============================================================================
// [asmjit::ConstPool - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
UNIT(const_pool) {
  Zone zone(32384 - Zone::kBlockOverhead);
  ConstPool pool(&zone);

  uint32_t i;
  uint32_t kCount = BrokenAPI::hasArg("--quick") ? 1000 : 1000000;

  INFO("Adding %u constants to the pool", kCount);
  {
    size_t prevOffset;
    size_t curOffset;
    uint64_t c = 0x0101010101010101u;

    EXPECT(pool.add(&c, 8, prevOffset) == kErrorOk);
    EXPECT(prevOffset == 0);

    for (i = 1; i < kCount; i++) {
      c++;
      EXPECT(pool.add(&c, 8, curOffset) == kErrorOk);
      EXPECT(prevOffset + 8 == curOffset);
      EXPECT(pool.size() == (i + 1) * 8);
      prevOffset = curOffset;
    }

    EXPECT(pool.alignment() == 8);
  }

  INFO("Retrieving %u constants from the pool", kCount);
  {
    uint64_t c = 0x0101010101010101u;

    for (i = 0; i < kCount; i++) {
      size_t offset;
      EXPECT(pool.add(&c, 8, offset) == kErrorOk);
      EXPECT(offset == i * 8);
      c++;
    }
  }

  INFO("Checking if the constants were split into 4-byte patterns");
  {
    uint32_t c = 0x01010101;
    for (i = 0; i < kCount; i++) {
      size_t offset;
      EXPECT(pool.add(&c, 4, offset) == kErrorOk);
      EXPECT(offset == i * 8);
      c++;
    }
  }

  INFO("Adding 2 byte constant to misalign the current offset");
  {
    uint16_t c = 0xFFFF;
    size_t offset;

    EXPECT(pool.add(&c, 2, offset) == kErrorOk);
    EXPECT(offset == kCount * 8);
    EXPECT(pool.alignment() == 8);
  }

  INFO("Adding 8 byte constant to check if pool gets aligned again");
  {
    uint64_t c = 0xFFFFFFFFFFFFFFFFu;
    size_t offset;

    EXPECT(pool.add(&c, 8, offset) == kErrorOk);
    EXPECT(offset == kCount * 8 + 8);
  }

  INFO("Adding 2 byte constant to verify the gap is filled");
  {
    uint16_t c = 0xFFFE;
    size_t offset;

    EXPECT(pool.add(&c, 2, offset) == kErrorOk);
    EXPECT(offset == kCount * 8 + 2);
    EXPECT(pool.alignment() == 8);
  }

  INFO("Checking reset functionality");
  {
    pool.reset(&zone);
    zone.reset();

    EXPECT(pool.size() == 0);
    EXPECT(pool.alignment() == 0);
  }

  INFO("Checking pool alignment when combined constants are added");
  {
    uint8_t bytes[32] = { 0 };
    size_t offset;

    pool.add(bytes, 1, offset);
    EXPECT(pool.size() == 1);
    EXPECT(pool.alignment() == 1);
    EXPECT(offset == 0);

    pool.add(bytes, 2, offset);
    EXPECT(pool.size() == 4);
    EXPECT(pool.alignment() == 2);
    EXPECT(offset == 2);

    pool.add(bytes, 4, offset);
    EXPECT(pool.size() == 8);
    EXPECT(pool.alignment() == 4);
    EXPECT(offset == 4);

    pool.add(bytes, 4, offset);
    EXPECT(pool.size() == 8);
    EXPECT(pool.alignment() == 4);
    EXPECT(offset == 4);

    pool.add(bytes, 32, offset);
    EXPECT(pool.size() == 64);
    EXPECT(pool.alignment() == 32);
    EXPECT(offset == 32);
  }
}
#endif

ASMJIT_END_NAMESPACE
