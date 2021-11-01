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
#ifndef ASMJIT_NO_JIT

#include "../core/archtraits.h"
#include "../core/jitallocator.h"
#include "../core/osutils_p.h"
#include "../core/support.h"
#include "../core/virtmem.h"
#include "../core/zone.h"
#include "../core/zonelist.h"
#include "../core/zonetree.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::JitAllocator - Constants]
// ============================================================================

enum JitAllocatorConstants : uint32_t {
  //! Number of pools to use when `JitAllocator::kOptionUseMultiplePools` is set.
  //!
  //! Each pool increases granularity twice to make memory management more
  //! efficient. Ideal number of pools appears to be 3 to 4 as it distributes
  //! small and large functions properly.
  kJitAllocatorMultiPoolCount = 3,

  //! Minimum granularity (and the default granularity for pool #0).
  kJitAllocatorBaseGranularity = 64,

  //! Maximum block size (32MB).
  kJitAllocatorMaxBlockSize = 1024 * 1024 * 32
};

static inline uint32_t JitAllocator_defaultFillPattern() noexcept {
  // X86 and X86_64 - 4x 'int3' instruction.
  if (ASMJIT_ARCH_X86)
    return 0xCCCCCCCCu;

  // Unknown...
  return 0u;
}

// ============================================================================
// [asmjit::BitVectorRangeIterator]
// ============================================================================

template<typename T, uint32_t B>
class BitVectorRangeIterator {
public:
  const T* _ptr;
  size_t _idx;
  size_t _end;
  T _bitWord;

  enum : uint32_t { kBitWordSize = Support::bitSizeOf<T>() };
  enum : T { kXorMask = B == 0 ? Support::allOnes<T>() : T(0) };

  ASMJIT_INLINE BitVectorRangeIterator(const T* data, size_t numBitWords) noexcept {
    init(data, numBitWords);
  }

  ASMJIT_INLINE BitVectorRangeIterator(const T* data, size_t numBitWords, size_t start, size_t end) noexcept {
    init(data, numBitWords, start, end);
  }

  ASMJIT_INLINE void init(const T* data, size_t numBitWords) noexcept {
    init(data, numBitWords, 0, numBitWords * kBitWordSize);
  }

  ASMJIT_INLINE void init(const T* data, size_t numBitWords, size_t start, size_t end) noexcept {
    ASMJIT_ASSERT(numBitWords >= (end + kBitWordSize - 1) / kBitWordSize);
    DebugUtils::unused(numBitWords);

    size_t idx = Support::alignDown(start, kBitWordSize);
    const T* ptr = data + (idx / kBitWordSize);

    T bitWord = 0;
    if (idx < end)
      bitWord = (*ptr ^ kXorMask) & (Support::allOnes<T>() << (start % kBitWordSize));

    _ptr = ptr;
    _idx = idx;
    _end = end;
    _bitWord = bitWord;
  }

  ASMJIT_INLINE bool nextRange(size_t* rangeStart, size_t* rangeEnd, size_t rangeHint = std::numeric_limits<size_t>::max()) noexcept {
    // Skip all empty BitWords.
    while (_bitWord == 0) {
      _idx += kBitWordSize;
      if (_idx >= _end)
        return false;
      _bitWord = (*++_ptr) ^ kXorMask;
    }

    size_t i = Support::ctz(_bitWord);

    *rangeStart = _idx + i;
    _bitWord = ~(_bitWord ^ ~(Support::allOnes<T>() << i));

    if (_bitWord == 0) {
      *rangeEnd = Support::min(_idx + kBitWordSize, _end);
      while (*rangeEnd - *rangeStart < rangeHint) {
        _idx += kBitWordSize;
        if (_idx >= _end)
          break;

        _bitWord = (*++_ptr) ^ kXorMask;
        if (_bitWord != Support::allOnes<T>()) {
          size_t j = Support::ctz(~_bitWord);
          *rangeEnd = Support::min(_idx + j, _end);
          _bitWord = _bitWord ^ ~(Support::allOnes<T>() << j);
          break;
        }

        *rangeEnd = Support::min(_idx + kBitWordSize, _end);
        _bitWord = 0;
        continue;
      }

      return true;
    }
    else {
      size_t j = Support::ctz(_bitWord);
      *rangeEnd = Support::min(_idx + j, _end);

      _bitWord = ~(_bitWord ^ ~(Support::allOnes<T>() << j));
      return true;
    }
  }
};

// ============================================================================
// [asmjit::JitAllocator - Pool]
// ============================================================================

class JitAllocatorBlock;

class JitAllocatorPool {
public:
  ASMJIT_NONCOPYABLE(JitAllocatorPool)

  inline JitAllocatorPool(uint32_t granularity) noexcept
    : blocks(),
      cursor(nullptr),
      blockCount(0),
      granularity(uint16_t(granularity)),
      granularityLog2(uint8_t(Support::ctz(granularity))),
      emptyBlockCount(0),
      totalAreaSize(0),
      totalAreaUsed(0),
      totalOverheadBytes(0) {}

  inline void reset() noexcept {
    blocks.reset();
    cursor = nullptr;
    blockCount = 0;
    totalAreaSize = 0;
    totalAreaUsed = 0;
    totalOverheadBytes = 0;
  }

  inline size_t byteSizeFromAreaSize(uint32_t areaSize) const noexcept { return size_t(areaSize) * granularity; }
  inline uint32_t areaSizeFromByteSize(size_t size) const noexcept { return uint32_t((size + granularity - 1) >> granularityLog2); }

  inline size_t bitWordCountFromAreaSize(uint32_t areaSize) const noexcept {
    using namespace Support;
    return alignUp<size_t>(areaSize, kBitWordSizeInBits) / kBitWordSizeInBits;
  }

  //! Double linked list of blocks.
  ZoneList<JitAllocatorBlock> blocks;
  //! Where to start looking first.
  JitAllocatorBlock* cursor;

  //! Count of blocks.
  uint32_t blockCount;
  //! Allocation granularity.
  uint16_t granularity;
  //! Log2(granularity).
  uint8_t granularityLog2;
  //! Count of empty blocks (either 0 or 1 as we won't keep more blocks empty).
  uint8_t emptyBlockCount;

  //! Number of bits reserved across all blocks.
  size_t totalAreaSize;
  //! Number of bits used across all blocks.
  size_t totalAreaUsed;
  //! Overhead of all blocks (in bytes).
  size_t totalOverheadBytes;
};

// ============================================================================
// [asmjit::JitAllocator - Block]
// ============================================================================

class JitAllocatorBlock : public ZoneTreeNodeT<JitAllocatorBlock>,
                          public ZoneListNode<JitAllocatorBlock> {
public:
  ASMJIT_NONCOPYABLE(JitAllocatorBlock)

  enum Flags : uint32_t {
    //! Block is empty.
    kFlagEmpty = 0x00000001u,
    //! Block is dirty (largestUnusedArea, searchStart, searchEnd).
    kFlagDirty = 0x00000002u,
    //! Block is dual-mapped.
    kFlagDualMapped = 0x00000004u
  };

  //! Link to the pool that owns this block.
  JitAllocatorPool* _pool;
  //! Virtual memory mapping - either single mapping (both pointers equal) or
  //! dual mapping, where one pointer is Read+Execute and the second Read+Write.
  VirtMem::DualMapping _mapping;
  //! Virtual memory size (block size) [bytes].
  size_t _blockSize;

  //! Block flags.
  uint32_t _flags;
  //! Size of the whole block area (bit-vector size).
  uint32_t _areaSize;
  //! Used area (number of bits in bit-vector used).
  uint32_t _areaUsed;
  //! The largest unused continuous area in the bit-vector (or `areaSize` to initiate rescan).
  uint32_t _largestUnusedArea;
  //! Start of a search range (for unused bits).
  uint32_t _searchStart;
  //! End of a search range (for unused bits).
  uint32_t _searchEnd;

  //! Used bit-vector (0 = unused, 1 = used).
  Support::BitWord* _usedBitVector;
  //! Stop bit-vector (0 = don't care, 1 = stop).
  Support::BitWord* _stopBitVector;

  inline JitAllocatorBlock(
    JitAllocatorPool* pool,
    VirtMem::DualMapping mapping,
    size_t blockSize,
    uint32_t blockFlags,
    Support::BitWord* usedBitVector,
    Support::BitWord* stopBitVector,
    uint32_t areaSize) noexcept
    : ZoneTreeNodeT(),
      _pool(pool),
      _mapping(mapping),
      _blockSize(blockSize),
      _flags(blockFlags),
      _areaSize(areaSize),
      _areaUsed(0),
      _largestUnusedArea(areaSize),
      _searchStart(0),
      _searchEnd(areaSize),
      _usedBitVector(usedBitVector),
      _stopBitVector(stopBitVector) {}

  inline JitAllocatorPool* pool() const noexcept { return _pool; }

  inline uint8_t* roPtr() const noexcept { return static_cast<uint8_t*>(_mapping.ro); }
  inline uint8_t* rwPtr() const noexcept { return static_cast<uint8_t*>(_mapping.rw); }

  inline bool hasFlag(uint32_t f) const noexcept { return (_flags & f) != 0; }
  inline void addFlags(uint32_t f) noexcept { _flags |= f; }
  inline void clearFlags(uint32_t f) noexcept { _flags &= ~f; }

  inline bool isDirty() const noexcept { return hasFlag(kFlagDirty); }
  inline void makeDirty() noexcept { addFlags(kFlagDirty); }

  inline size_t blockSize() const noexcept { return _blockSize; }

  inline uint32_t areaSize() const noexcept { return _areaSize; }
  inline uint32_t areaUsed() const noexcept { return _areaUsed; }
  inline uint32_t areaAvailable() const noexcept { return _areaSize - _areaUsed; }
  inline uint32_t largestUnusedArea() const noexcept { return _largestUnusedArea; }

  inline void decreaseUsedArea(uint32_t value) noexcept {
    _areaUsed -= value;
    _pool->totalAreaUsed -= value;
  }

  inline void markAllocatedArea(uint32_t allocatedAreaStart, uint32_t allocatedAreaEnd) noexcept {
    uint32_t allocatedAreaSize = allocatedAreaEnd - allocatedAreaStart;

    // Mark the newly allocated space as occupied and also the sentinel.
    Support::bitVectorFill(_usedBitVector, allocatedAreaStart, allocatedAreaSize);
    Support::bitVectorSetBit(_stopBitVector, allocatedAreaEnd - 1, true);

    // Update search region and statistics.
    _pool->totalAreaUsed += allocatedAreaSize;
    _areaUsed += allocatedAreaSize;

    if (areaAvailable() == 0) {
      _searchStart = _areaSize;
      _searchEnd = 0;
      _largestUnusedArea = 0;
      clearFlags(kFlagDirty);
    }
    else {
      if (_searchStart == allocatedAreaStart)
        _searchStart = allocatedAreaEnd;
      if (_searchEnd == allocatedAreaEnd)
        _searchEnd = allocatedAreaStart;
      addFlags(kFlagDirty);
    }
  }

  inline void markReleasedArea(uint32_t releasedAreaStart, uint32_t releasedAreaEnd) noexcept {
    uint32_t releasedAreaSize = releasedAreaEnd - releasedAreaStart;

    // Update the search region and statistics.
    _pool->totalAreaUsed -= releasedAreaSize;
    _areaUsed -= releasedAreaSize;
    _searchStart = Support::min(_searchStart, releasedAreaStart);
    _searchEnd = Support::max(_searchEnd, releasedAreaEnd);

    // Unmark occupied bits and also the sentinel.
    Support::bitVectorClear(_usedBitVector, releasedAreaStart, releasedAreaSize);
    Support::bitVectorSetBit(_stopBitVector, releasedAreaEnd - 1, false);

    if (areaUsed() == 0) {
      _searchStart = 0;
      _searchEnd = _areaSize;
      _largestUnusedArea = _areaSize;
      addFlags(kFlagEmpty);
      clearFlags(kFlagDirty);
    }
    else {
      addFlags(kFlagDirty);
    }
  }

  inline void markShrunkArea(uint32_t shrunkAreaStart, uint32_t shrunkAreaEnd) noexcept {
    uint32_t shrunkAreaSize = shrunkAreaEnd - shrunkAreaStart;

    // Shrunk area cannot start at zero as it would mean that we have shrunk the first
    // block to zero bytes, which is not allowed as such block must be released instead.
    ASMJIT_ASSERT(shrunkAreaStart != 0);
    ASMJIT_ASSERT(shrunkAreaSize != 0);

    // Update the search region and statistics.
    _pool->totalAreaUsed -= shrunkAreaSize;
    _areaUsed -= shrunkAreaSize;
    _searchStart = Support::min(_searchStart, shrunkAreaStart);
    _searchEnd = Support::max(_searchEnd, shrunkAreaEnd);

    // Unmark the released space and move the sentinel.
    Support::bitVectorClear(_usedBitVector, shrunkAreaStart, shrunkAreaSize);
    Support::bitVectorSetBit(_stopBitVector, shrunkAreaEnd - 1, false);
    Support::bitVectorSetBit(_stopBitVector, shrunkAreaStart - 1, true);

    addFlags(kFlagDirty);
  }

  // RBTree default CMP uses '<' and '>' operators.
  inline bool operator<(const JitAllocatorBlock& other) const noexcept { return roPtr() < other.roPtr(); }
  inline bool operator>(const JitAllocatorBlock& other) const noexcept { return roPtr() > other.roPtr(); }

  // Special implementation for querying blocks by `key`, which must be in `[BlockPtr, BlockPtr + BlockSize)` range.
  inline bool operator<(const uint8_t* key) const noexcept { return roPtr() + _blockSize <= key; }
  inline bool operator>(const uint8_t* key) const noexcept { return roPtr() > key; }
};

// ============================================================================
// [asmjit::JitAllocator - PrivateImpl]
// ============================================================================

class JitAllocatorPrivateImpl : public JitAllocator::Impl {
public:
  inline JitAllocatorPrivateImpl(JitAllocatorPool* pools, size_t poolCount) noexcept
    : JitAllocator::Impl {},
      pools(pools),
      poolCount(poolCount) {}
  inline ~JitAllocatorPrivateImpl() noexcept {}

  //! Lock for thread safety.
  mutable Lock lock;
  //! System page size (also a minimum block size).
  uint32_t pageSize;

  //! Blocks from all pools in RBTree.
  ZoneTree<JitAllocatorBlock> tree;
  //! Allocator pools.
  JitAllocatorPool* pools;
  //! Number of allocator pools.
  size_t poolCount;
};

static const JitAllocator::Impl JitAllocatorImpl_none {};
static const JitAllocator::CreateParams JitAllocatorParams_none {};

// ============================================================================
// [asmjit::JitAllocator - Utilities]
// ============================================================================

static inline JitAllocatorPrivateImpl* JitAllocatorImpl_new(const JitAllocator::CreateParams* params) noexcept {
  VirtMem::Info vmInfo = VirtMem::info();

  if (!params)
    params = &JitAllocatorParams_none;

  uint32_t options = params->options;
  uint32_t blockSize = params->blockSize;
  uint32_t granularity = params->granularity;
  uint32_t fillPattern = params->fillPattern;

  // Setup pool count to [1..3].
  size_t poolCount = 1;
  if (options & JitAllocator::kOptionUseMultiplePools)
    poolCount = kJitAllocatorMultiPoolCount;;

  // Setup block size [64kB..256MB].
  if (blockSize < 64 * 1024 || blockSize > 256 * 1024 * 1024 || !Support::isPowerOf2(blockSize))
    blockSize = vmInfo.pageGranularity;

  // Setup granularity [64..256].
  if (granularity < 64 || granularity > 256 || !Support::isPowerOf2(granularity))
    granularity = kJitAllocatorBaseGranularity;

  // Setup fill-pattern.
  if (!(options & JitAllocator::kOptionCustomFillPattern))
    fillPattern = JitAllocator_defaultFillPattern();

  size_t size = sizeof(JitAllocatorPrivateImpl) + sizeof(JitAllocatorPool) * poolCount;
  void* p = ::malloc(size);
  if (ASMJIT_UNLIKELY(!p))
    return nullptr;

  JitAllocatorPool* pools = reinterpret_cast<JitAllocatorPool*>((uint8_t*)p + sizeof(JitAllocatorPrivateImpl));
  JitAllocatorPrivateImpl* impl = new(p) JitAllocatorPrivateImpl(pools, poolCount);

  impl->options = options;
  impl->blockSize = blockSize;
  impl->granularity = granularity;
  impl->fillPattern = fillPattern;
  impl->pageSize = vmInfo.pageSize;

  for (size_t poolId = 0; poolId < poolCount; poolId++)
    new(&pools[poolId]) JitAllocatorPool(granularity << poolId);

  return impl;
}

static inline void JitAllocatorImpl_destroy(JitAllocatorPrivateImpl* impl) noexcept {
  impl->~JitAllocatorPrivateImpl();
  ::free(impl);
}

static inline size_t JitAllocatorImpl_sizeToPoolId(const JitAllocatorPrivateImpl* impl, size_t size) noexcept {
  size_t poolId = impl->poolCount - 1;
  size_t granularity = size_t(impl->granularity) << poolId;

  while (poolId) {
    if (Support::alignUp(size, granularity) == size)
      break;
    poolId--;
    granularity >>= 1;
  }

  return poolId;
}

static inline size_t JitAllocatorImpl_bitVectorSizeToByteSize(uint32_t areaSize) noexcept {
  using Support::kBitWordSizeInBits;
  return ((areaSize + kBitWordSizeInBits - 1u) / kBitWordSizeInBits) * sizeof(Support::BitWord);
}

static inline size_t JitAllocatorImpl_calculateIdealBlockSize(JitAllocatorPrivateImpl* impl, JitAllocatorPool* pool, size_t allocationSize) noexcept {
  JitAllocatorBlock* last = pool->blocks.last();
  size_t blockSize = last ? last->blockSize() : size_t(impl->blockSize);

  if (blockSize < kJitAllocatorMaxBlockSize)
    blockSize *= 2u;

  if (allocationSize > blockSize) {
    blockSize = Support::alignUp(allocationSize, impl->blockSize);
    if (ASMJIT_UNLIKELY(blockSize < allocationSize))
      return 0; // Overflown.
  }

  return blockSize;
}

ASMJIT_FAVOR_SPEED static void JitAllocatorImpl_fillPattern(void* mem, uint32_t pattern, size_t sizeInBytes) noexcept {
  size_t n = sizeInBytes / 4u;
  uint32_t* p = static_cast<uint32_t*>(mem);

  for (size_t i = 0; i < n; i++)
    p[i] = pattern;
}

// Allocate a new `JitAllocatorBlock` for the given `blockSize`.
//
// NOTE: The block doesn't have `kFlagEmpty` flag set, because the new block
// is only allocated when it's actually needed, so it would be cleared anyway.
static JitAllocatorBlock* JitAllocatorImpl_newBlock(JitAllocatorPrivateImpl* impl, JitAllocatorPool* pool, size_t blockSize) noexcept {
  using Support::BitWord;
  using Support::kBitWordSizeInBits;

  uint32_t areaSize = uint32_t((blockSize + pool->granularity - 1) >> pool->granularityLog2);
  uint32_t numBitWords = (areaSize + kBitWordSizeInBits - 1u) / kBitWordSizeInBits;

  JitAllocatorBlock* block = static_cast<JitAllocatorBlock*>(::malloc(sizeof(JitAllocatorBlock)));
  BitWord* bitWords = nullptr;
  VirtMem::DualMapping virtMem {};
  Error err = kErrorOutOfMemory;

  if (block != nullptr)
    bitWords = static_cast<BitWord*>(::malloc(size_t(numBitWords) * 2 * sizeof(BitWord)));

  uint32_t blockFlags = 0;
  if (bitWords != nullptr) {
    if (impl->options & JitAllocator::kOptionUseDualMapping) {
      err = VirtMem::allocDualMapping(&virtMem, blockSize, VirtMem::kAccessRWX);
      blockFlags |= JitAllocatorBlock::kFlagDualMapped;
    }
    else {
      err = VirtMem::alloc(&virtMem.ro, blockSize, VirtMem::kAccessRWX);
      virtMem.rw = virtMem.ro;
    }
  }

  // Out of memory.
  if (ASMJIT_UNLIKELY(!block || !bitWords || err != kErrorOk)) {
    if (bitWords) ::free(bitWords);
    if (block) ::free(block);
    return nullptr;
  }

  // Fill the memory if the secure mode is enabled.
  if (impl->options & JitAllocator::kOptionFillUnusedMemory)
    JitAllocatorImpl_fillPattern(virtMem.rw, impl->fillPattern, blockSize);

  memset(bitWords, 0, size_t(numBitWords) * 2 * sizeof(BitWord));
  return new(block) JitAllocatorBlock(pool, virtMem, blockSize, blockFlags, bitWords, bitWords + numBitWords, areaSize);
}

static void JitAllocatorImpl_deleteBlock(JitAllocatorPrivateImpl* impl, JitAllocatorBlock* block) noexcept {
  DebugUtils::unused(impl);

  if (block->hasFlag(JitAllocatorBlock::kFlagDualMapped))
    VirtMem::releaseDualMapping(&block->_mapping, block->blockSize());
  else
    VirtMem::release(block->roPtr(), block->blockSize());

  ::free(block->_usedBitVector);
  ::free(block);
}

static void JitAllocatorImpl_insertBlock(JitAllocatorPrivateImpl* impl, JitAllocatorBlock* block) noexcept {
  JitAllocatorPool* pool = block->pool();

  if (!pool->cursor)
    pool->cursor = block;

  // Add to RBTree and List.
  impl->tree.insert(block);
  pool->blocks.append(block);

  // Update statistics.
  pool->blockCount++;
  pool->totalAreaSize += block->areaSize();
  pool->totalOverheadBytes += sizeof(JitAllocatorBlock) + JitAllocatorImpl_bitVectorSizeToByteSize(block->areaSize()) * 2u;
}

static void JitAllocatorImpl_removeBlock(JitAllocatorPrivateImpl* impl, JitAllocatorBlock* block) noexcept {
  JitAllocatorPool* pool = block->pool();

  // Remove from RBTree and List.
  if (pool->cursor == block)
    pool->cursor = block->hasPrev() ? block->prev() : block->next();

  impl->tree.remove(block);
  pool->blocks.unlink(block);

  // Update statistics.
  pool->blockCount--;
  pool->totalAreaSize -= block->areaSize();
  pool->totalOverheadBytes -= sizeof(JitAllocatorBlock) + JitAllocatorImpl_bitVectorSizeToByteSize(block->areaSize()) * 2u;
}

static void JitAllocatorImpl_wipeOutBlock(JitAllocatorPrivateImpl* impl, JitAllocatorBlock* block) noexcept {
  JitAllocatorPool* pool = block->pool();

  if (block->hasFlag(JitAllocatorBlock::kFlagEmpty))
    return;

  uint32_t areaSize = block->areaSize();
  uint32_t granularity = pool->granularity;
  size_t numBitWords = pool->bitWordCountFromAreaSize(areaSize);

  if (impl->options & JitAllocator::kOptionFillUnusedMemory) {
    uint8_t* rwPtr = block->rwPtr();
    for (size_t i = 0; i < numBitWords; i++) {
      Support::BitWordIterator<Support::BitWord> it(block->_usedBitVector[i]);
      while (it.hasNext()) {
        size_t index = it.next();
        JitAllocatorImpl_fillPattern(rwPtr + index * granularity , impl->fillPattern, granularity);
      }
      rwPtr += Support::bitSizeOf<Support::BitWord>() * granularity;
    }
  }

  memset(block->_usedBitVector, 0, size_t(numBitWords) * sizeof(Support::BitWord));
  memset(block->_stopBitVector, 0, size_t(numBitWords) * sizeof(Support::BitWord));

  block->_areaUsed = 0;
  block->_largestUnusedArea = areaSize;
  block->_searchStart = 0;
  block->_searchEnd = areaSize;
  block->addFlags(JitAllocatorBlock::kFlagEmpty);
  block->clearFlags(JitAllocatorBlock::kFlagDirty);
}

// ============================================================================
// [asmjit::JitAllocator - Construction / Destruction]
// ============================================================================

JitAllocator::JitAllocator(const CreateParams* params) noexcept {
  _impl = JitAllocatorImpl_new(params);
  if (ASMJIT_UNLIKELY(!_impl))
    _impl = const_cast<JitAllocator::Impl*>(&JitAllocatorImpl_none);
}

JitAllocator::~JitAllocator() noexcept {
  if (_impl == &JitAllocatorImpl_none)
    return;

  reset(Globals::kResetHard);
  JitAllocatorImpl_destroy(static_cast<JitAllocatorPrivateImpl*>(_impl));
}

// ============================================================================
// [asmjit::JitAllocator - Reset]
// ============================================================================

void JitAllocator::reset(uint32_t resetPolicy) noexcept {
  if (_impl == &JitAllocatorImpl_none)
    return;

  JitAllocatorPrivateImpl* impl = static_cast<JitAllocatorPrivateImpl*>(_impl);
  impl->tree.reset();
  size_t poolCount = impl->poolCount;

  for (size_t poolId = 0; poolId < poolCount; poolId++) {
    JitAllocatorPool& pool = impl->pools[poolId];
    JitAllocatorBlock* block = pool.blocks.first();

    JitAllocatorBlock* blockToKeep = nullptr;
    if (resetPolicy != Globals::kResetHard && !(impl->options & kOptionImmediateRelease)) {
      blockToKeep = block;
      block = block->next();
    }

    while (block) {
      JitAllocatorBlock* next = block->next();
      JitAllocatorImpl_deleteBlock(impl, block);
      block = next;
    }

    pool.reset();

    if (blockToKeep) {
      blockToKeep->_listNodes[0] = nullptr;
      blockToKeep->_listNodes[1] = nullptr;
      JitAllocatorImpl_wipeOutBlock(impl, blockToKeep);
      JitAllocatorImpl_insertBlock(impl, blockToKeep);
      pool.emptyBlockCount = 1;
    }
  }
}

// ============================================================================
// [asmjit::JitAllocator - Statistics]
// ============================================================================

JitAllocator::Statistics JitAllocator::statistics() const noexcept {
  Statistics statistics;
  statistics.reset();

  if (ASMJIT_LIKELY(_impl != &JitAllocatorImpl_none)) {
    JitAllocatorPrivateImpl* impl = static_cast<JitAllocatorPrivateImpl*>(_impl);
    LockGuard guard(impl->lock);

    size_t poolCount = impl->poolCount;
    for (size_t poolId = 0; poolId < poolCount; poolId++) {
      const JitAllocatorPool& pool = impl->pools[poolId];
      statistics._blockCount   += size_t(pool.blockCount);
      statistics._reservedSize += size_t(pool.totalAreaSize) * pool.granularity;
      statistics._usedSize     += size_t(pool.totalAreaUsed) * pool.granularity;
      statistics._overheadSize += size_t(pool.totalOverheadBytes);
    }
  }

  return statistics;
}

// ============================================================================
// [asmjit::JitAllocator - Alloc / Release]
// ============================================================================

Error JitAllocator::alloc(void** roPtrOut, void** rwPtrOut, size_t size) noexcept {
  if (ASMJIT_UNLIKELY(_impl == &JitAllocatorImpl_none))
    return DebugUtils::errored(kErrorNotInitialized);

  JitAllocatorPrivateImpl* impl = static_cast<JitAllocatorPrivateImpl*>(_impl);
  constexpr uint32_t kNoIndex = std::numeric_limits<uint32_t>::max();

  *roPtrOut = nullptr;
  *rwPtrOut = nullptr;

  // Align to the minimum granularity by default.
  size = Support::alignUp<size_t>(size, impl->granularity);
  if (ASMJIT_UNLIKELY(size == 0))
    return DebugUtils::errored(kErrorInvalidArgument);

  if (ASMJIT_UNLIKELY(size > std::numeric_limits<uint32_t>::max() / 2))
    return DebugUtils::errored(kErrorTooLarge);

  LockGuard guard(impl->lock);
  JitAllocatorPool* pool = &impl->pools[JitAllocatorImpl_sizeToPoolId(impl, size)];

  uint32_t areaIndex = kNoIndex;
  uint32_t areaSize = uint32_t(pool->areaSizeFromByteSize(size));

  // Try to find the requested memory area in existing blocks.
  JitAllocatorBlock* block = pool->blocks.first();
  if (block) {
    JitAllocatorBlock* initial = block;
    do {
      JitAllocatorBlock* next = block->hasNext() ? block->next() : pool->blocks.first();
      if (block->areaAvailable() >= areaSize) {
        if (block->isDirty() || block->largestUnusedArea() >= areaSize) {
          BitVectorRangeIterator<Support::BitWord, 0> it(block->_usedBitVector, pool->bitWordCountFromAreaSize(block->areaSize()), block->_searchStart, block->_searchEnd);

          size_t rangeStart = 0;
          size_t rangeEnd = block->areaSize();

          size_t searchStart = SIZE_MAX;
          size_t largestArea = 0;

          while (it.nextRange(&rangeStart, &rangeEnd, areaSize)) {
            size_t rangeSize = rangeEnd - rangeStart;
            if (rangeSize >= areaSize) {
              areaIndex = uint32_t(rangeStart);
              break;
            }

            searchStart = Support::min(searchStart, rangeStart);
            largestArea = Support::max(largestArea, rangeSize);
          }

          if (areaIndex != kNoIndex)
            break;

          if (searchStart != SIZE_MAX) {
            // Because we have iterated over the entire block, we can now mark the
            // largest unused area that can be used to cache the next traversal.
            size_t searchEnd = rangeEnd;

            block->_searchStart = uint32_t(searchStart);
            block->_searchEnd = uint32_t(searchEnd);
            block->_largestUnusedArea = uint32_t(largestArea);
            block->clearFlags(JitAllocatorBlock::kFlagDirty);
          }
        }
      }

      block = next;
    } while (block != initial);
  }

  // Allocate a new block if there is no region of a required width.
  if (areaIndex == kNoIndex) {
    size_t blockSize = JitAllocatorImpl_calculateIdealBlockSize(impl, pool, size);
    if (ASMJIT_UNLIKELY(!blockSize))
      return DebugUtils::errored(kErrorOutOfMemory);

    block = JitAllocatorImpl_newBlock(impl, pool, blockSize);
    areaIndex = 0;

    if (ASMJIT_UNLIKELY(!block))
      return DebugUtils::errored(kErrorOutOfMemory);

    JitAllocatorImpl_insertBlock(impl, block);
    block->_searchStart = areaSize;
    block->_largestUnusedArea = block->areaSize() - areaSize;
  }
  else if (block->hasFlag(JitAllocatorBlock::kFlagEmpty)) {
    pool->emptyBlockCount--;
    block->clearFlags(JitAllocatorBlock::kFlagEmpty);
  }

  // Update statistics.
  block->markAllocatedArea(areaIndex, areaIndex + areaSize);

  // Return a pointer to the allocated memory.
  size_t offset = pool->byteSizeFromAreaSize(areaIndex);
  ASMJIT_ASSERT(offset <= block->blockSize() - size);

  *roPtrOut = block->roPtr() + offset;
  *rwPtrOut = block->rwPtr() + offset;
  return kErrorOk;
}

Error JitAllocator::release(void* roPtr) noexcept {
  if (ASMJIT_UNLIKELY(_impl == &JitAllocatorImpl_none))
    return DebugUtils::errored(kErrorNotInitialized);

  if (ASMJIT_UNLIKELY(!roPtr))
    return DebugUtils::errored(kErrorInvalidArgument);

  JitAllocatorPrivateImpl* impl = static_cast<JitAllocatorPrivateImpl*>(_impl);
  LockGuard guard(impl->lock);

  JitAllocatorBlock* block = impl->tree.get(static_cast<uint8_t*>(roPtr));
  if (ASMJIT_UNLIKELY(!block))
    return DebugUtils::errored(kErrorInvalidState);

  // Offset relative to the start of the block.
  JitAllocatorPool* pool = block->pool();
  size_t offset = (size_t)((uint8_t*)roPtr - block->roPtr());

  // The first bit representing the allocated area and its size.
  uint32_t areaIndex = uint32_t(offset >> pool->granularityLog2);
  uint32_t areaEnd = uint32_t(Support::bitVectorIndexOf(block->_stopBitVector, areaIndex, true)) + 1;
  uint32_t areaSize = areaEnd - areaIndex;

  block->markReleasedArea(areaIndex, areaEnd);

  // Fill the released memory if the secure mode is enabled.
  if (impl->options & kOptionFillUnusedMemory)
    JitAllocatorImpl_fillPattern(block->rwPtr() + areaIndex * pool->granularity, impl->fillPattern, areaSize * pool->granularity);

  // Release the whole block if it became empty.
  if (block->areaUsed() == 0) {
    if (pool->emptyBlockCount || (impl->options & kOptionImmediateRelease)) {
      JitAllocatorImpl_removeBlock(impl, block);
      JitAllocatorImpl_deleteBlock(impl, block);
    }
    else {
      pool->emptyBlockCount++;
    }
  }

  return kErrorOk;
}

Error JitAllocator::shrink(void* roPtr, size_t newSize) noexcept {
  if (ASMJIT_UNLIKELY(_impl == &JitAllocatorImpl_none))
    return DebugUtils::errored(kErrorNotInitialized);

  if (ASMJIT_UNLIKELY(!roPtr))
    return DebugUtils::errored(kErrorInvalidArgument);

  if (ASMJIT_UNLIKELY(newSize == 0))
    return release(roPtr);

  JitAllocatorPrivateImpl* impl = static_cast<JitAllocatorPrivateImpl*>(_impl);
  LockGuard guard(impl->lock);
  JitAllocatorBlock* block = impl->tree.get(static_cast<uint8_t*>(roPtr));

  if (ASMJIT_UNLIKELY(!block))
    return DebugUtils::errored(kErrorInvalidArgument);

  // Offset relative to the start of the block.
  JitAllocatorPool* pool = block->pool();
  size_t offset = (size_t)((uint8_t*)roPtr - block->roPtr());

  // The first bit representing the allocated area and its size.
  uint32_t areaStart = uint32_t(offset >> pool->granularityLog2);
  uint32_t areaEnd = uint32_t(Support::bitVectorIndexOf(block->_stopBitVector, areaStart, true)) + 1;

  uint32_t areaPrevSize = areaEnd - areaStart;
  uint32_t areaShrunkSize = pool->areaSizeFromByteSize(newSize);

  if (ASMJIT_UNLIKELY(areaShrunkSize > areaPrevSize))
    return DebugUtils::errored(kErrorInvalidState);

  uint32_t areaDiff = areaPrevSize - areaShrunkSize;
  if (areaDiff) {
    block->markShrunkArea(areaStart + areaShrunkSize, areaEnd);

    // Fill released memory if the secure mode is enabled.
    if (impl->options & kOptionFillUnusedMemory)
      JitAllocatorImpl_fillPattern(block->rwPtr() + (areaStart + areaShrunkSize) * pool->granularity, fillPattern(), areaDiff * pool->granularity);
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::JitAllocator - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
// A pseudo random number generator based on a paper by Sebastiano Vigna:
//   http://vigna.di.unimi.it/ftp/papers/xorshiftplus.pdf
class Random {
public:
  // Constants suggested as `23/18/5`.
  enum Steps : uint32_t {
    kStep1_SHL = 23,
    kStep2_SHR = 18,
    kStep3_SHR = 5
  };

  inline explicit Random(uint64_t seed = 0) noexcept { reset(seed); }
  inline Random(const Random& other) noexcept = default;

  inline void reset(uint64_t seed = 0) noexcept {
    // The number is arbitrary, it means nothing.
    constexpr uint64_t kZeroSeed = 0x1F0A2BE71D163FA0u;

    // Generate the state data by using splitmix64.
    for (uint32_t i = 0; i < 2; i++) {
      seed += 0x9E3779B97F4A7C15u;
      uint64_t x = seed;
      x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9u;
      x = (x ^ (x >> 27)) * 0x94D049BB133111EBu;
      x = (x ^ (x >> 31));
      _state[i] = x != 0 ? x : kZeroSeed;
    }
  }

  inline uint32_t nextUInt32() noexcept {
    return uint32_t(nextUInt64() >> 32);
  }

  inline uint64_t nextUInt64() noexcept {
    uint64_t x = _state[0];
    uint64_t y = _state[1];

    x ^= x << kStep1_SHL;
    y ^= y >> kStep3_SHR;
    x ^= x >> kStep2_SHR;
    x ^= y;

    _state[0] = y;
    _state[1] = x;
    return x + y;
  }

  uint64_t _state[2];
};

// Helper class to verify that JitAllocator doesn't return addresses that overlap.
class JitAllocatorWrapper {
public:
  // Address to a memory region of a given size.
  class Range {
  public:
    inline Range(uint8_t* addr, size_t size) noexcept
      : addr(addr),
        size(size) {}
    uint8_t* addr;
    size_t size;
  };

  // Based on JitAllocator::Block, serves our purpose well...
  class Record : public ZoneTreeNodeT<Record>,
                 public Range {
  public:
    inline Record(uint8_t* addr, size_t size)
      : ZoneTreeNodeT<Record>(),
        Range(addr, size) {}

    inline bool operator<(const Record& other) const noexcept { return addr < other.addr; }
    inline bool operator>(const Record& other) const noexcept { return addr > other.addr; }

    inline bool operator<(const uint8_t* key) const noexcept { return addr + size <= key; }
    inline bool operator>(const uint8_t* key) const noexcept { return addr > key; }
  };

  Zone _zone;
  ZoneAllocator _heap;
  ZoneTree<Record> _records;
  JitAllocator _allocator;

  explicit JitAllocatorWrapper(const JitAllocator::CreateParams* params) noexcept
    : _zone(1024 * 1024),
      _heap(&_zone),
      _allocator(params) {}

  void _insert(void* p_, size_t size) noexcept {
    uint8_t* p = static_cast<uint8_t*>(p_);
    uint8_t* pEnd = p + size - 1;

    Record* record;

    record = _records.get(p);
    if (record)
      EXPECT(record == nullptr, "Address [%p:%p] collides with a newly allocated [%p:%p]\n", record->addr, record->addr + record->size, p, p + size);

    record = _records.get(pEnd);
    if (record)
      EXPECT(record == nullptr, "Address [%p:%p] collides with a newly allocated [%p:%p]\n", record->addr, record->addr + record->size, p, p + size);

    record = _heap.newT<Record>(p, size);
    EXPECT(record != nullptr, "Out of memory, cannot allocate 'Record'");

    _records.insert(record);
  }

  void _remove(void* p) noexcept {
    Record* record = _records.get(static_cast<uint8_t*>(p));
    EXPECT(record != nullptr, "Address [%p] doesn't exist\n", p);

    _records.remove(record);
    _heap.release(record, sizeof(Record));
  }

  void* alloc(size_t size) noexcept {
    void* roPtr;
    void* rwPtr;

    Error err = _allocator.alloc(&roPtr, &rwPtr, size);
    EXPECT(err == kErrorOk, "JitAllocator failed to allocate %zu bytes\n", size);

    _insert(roPtr, size);
    return roPtr;
  }

  void release(void* p) noexcept {
    _remove(p);
    EXPECT(_allocator.release(p) == kErrorOk, "JitAllocator failed to release '%p'\n", p);
  }

  void shrink(void* p, size_t newSize) noexcept {
    Record* record = _records.get(static_cast<uint8_t*>(p));
    EXPECT(record != nullptr, "Address [%p] doesn't exist\n", p);

    if (!newSize)
      return release(p);

    Error err = _allocator.shrink(p, newSize);
    EXPECT(err == kErrorOk, "JitAllocator failed to shrink %p to %zu bytes\n", p, newSize);

    record->size = newSize;
  }
};

static void JitAllocatorTest_shuffle(void** ptrArray, size_t count, Random& prng) noexcept {
  for (size_t i = 0; i < count; ++i)
    std::swap(ptrArray[i], ptrArray[size_t(prng.nextUInt32() % count)]);
}

static void JitAllocatorTest_usage(JitAllocator& allocator) noexcept {
  JitAllocator::Statistics stats = allocator.statistics();
  INFO("    Block Count       : %9llu [Blocks]"        , (unsigned long long)(stats.blockCount()));
  INFO("    Reserved (VirtMem): %9llu [Bytes]"         , (unsigned long long)(stats.reservedSize()));
  INFO("    Used     (VirtMem): %9llu [Bytes] (%.1f%%)", (unsigned long long)(stats.usedSize()), stats.usedSizeAsPercent());
  INFO("    Overhead (HeapMem): %9llu [Bytes] (%.1f%%)", (unsigned long long)(stats.overheadSize()), stats.overheadSizeAsPercent());
}

template<typename T, size_t kPatternSize, bool Bit>
static void BitVectorRangeIterator_testRandom(Random& rnd, size_t count) noexcept {
  for (size_t i = 0; i < count; i++) {
    T in[kPatternSize];
    T out[kPatternSize];

    for (size_t j = 0; j < kPatternSize; j++) {
      in[j] = T(uint64_t(rnd.nextUInt32() & 0xFFu) * 0x0101010101010101);
      out[j] = Bit == 0 ? Support::allOnes<T>() : T(0);
    }

    {
      BitVectorRangeIterator<T, Bit> it(in, kPatternSize);
      size_t rangeStart, rangeEnd;
      while (it.nextRange(&rangeStart, &rangeEnd)) {
        if (Bit)
          Support::bitVectorFill(out, rangeStart, rangeEnd - rangeStart);
        else
          Support::bitVectorClear(out, rangeStart, rangeEnd - rangeStart);
      }
    }

    for (size_t j = 0; j < kPatternSize; j++) {
      EXPECT(in[j] == out[j], "Invalid pattern detected at [%zu] (%llX != %llX", j, (unsigned long long)in[j], (unsigned long long)out[j]);
    }
  }
}

UNIT(jit_allocator) {
  size_t kCount = BrokenAPI::hasArg("--quick") ? 1000 : 100000;

  struct TestParams {
    const char* name;
    uint32_t options;
    uint32_t blockSize;
    uint32_t granularity;
  };

  static TestParams testParams[] = {
    { "Default", 0, 0, 0 },
    { "16MB blocks", 0, 16 * 1024 * 1024, 0 },
    { "256B granularity", 0, 0, 256 },
    { "kOptionUseDualMapping", JitAllocator::kOptionUseDualMapping, 0, 0 },
    { "kOptionUseMultiplePools", JitAllocator::kOptionUseMultiplePools, 0, 0 },
    { "kOptionFillUnusedMemory", JitAllocator::kOptionFillUnusedMemory, 0, 0 },
    { "kOptionImmediateRelease", JitAllocator::kOptionImmediateRelease, 0, 0 },
    { "kOptionUseDualMapping | kOptionFillUnusedMemory", JitAllocator::kOptionUseDualMapping | JitAllocator::kOptionFillUnusedMemory, 0, 0 }
  };

  INFO("BitVectorRangeIterator<uint32_t>");
  {
    Random rnd;
    BitVectorRangeIterator_testRandom<uint32_t, 64, 0>(rnd, kCount);
  }

  INFO("BitVectorRangeIterator<uint64_t>");
  {
    Random rnd;
    BitVectorRangeIterator_testRandom<uint64_t, 64, 0>(rnd, kCount);
  }

  for (uint32_t testId = 0; testId < ASMJIT_ARRAY_SIZE(testParams); testId++) {
    INFO("JitAllocator(%s)", testParams[testId].name);

    JitAllocator::CreateParams params {};
    params.options = testParams[testId].options;
    params.blockSize = testParams[testId].blockSize;
    params.granularity = testParams[testId].granularity;

    size_t fixedBlockSize = 256;

    JitAllocatorWrapper wrapper(&params);
    Random prng(100);

    size_t i;

    INFO("  Memory alloc/release test - %d allocations", kCount);

    void** ptrArray = (void**)::malloc(sizeof(void*) * size_t(kCount));
    EXPECT(ptrArray != nullptr,
          "Couldn't allocate '%u' bytes for pointer-array", unsigned(sizeof(void*) * size_t(kCount)));

    // Random blocks tests...
    INFO("  Allocating random blocks...");
    for (i = 0; i < kCount; i++)
      ptrArray[i] = wrapper.alloc((prng.nextUInt32() % 1024) + 8);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Releasing all allocated blocks from the beginning...");
    for (i = 0; i < kCount; i++)
      wrapper.release(ptrArray[i]);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Allocating random blocks again...", kCount);
    for (i = 0; i < kCount; i++)
      ptrArray[i] = wrapper.alloc((prng.nextUInt32() % 1024) + 8);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Shuffling allocated blocks...");
    JitAllocatorTest_shuffle(ptrArray, unsigned(kCount), prng);

    INFO("  Releasing 50%% of allocated blocks...");
    for (i = 0; i < kCount / 2; i++)
      wrapper.release(ptrArray[i]);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Allocating 50%% more blocks again...");
    for (i = 0; i < kCount / 2; i++)
      ptrArray[i] = wrapper.alloc((prng.nextUInt32() % 1024) + 8);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Releasing all allocated blocks from the end...");
    for (i = 0; i < kCount; i++)
      wrapper.release(ptrArray[kCount - i - 1]);
    JitAllocatorTest_usage(wrapper._allocator);

    // Fixed blocks tests...
    INFO("  Allocating %zuB blocks...", fixedBlockSize);
    for (i = 0; i < kCount / 2; i++)
      ptrArray[i] = wrapper.alloc(fixedBlockSize);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Shrinking each %zuB block to 1 byte", fixedBlockSize);
    for (i = 0; i < kCount / 2; i++)
      wrapper.shrink(ptrArray[i], 1);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Allocating more 64B blocks...", 64);
    for (i = kCount / 2; i < kCount; i++)
      ptrArray[i] = wrapper.alloc(64);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Releasing all blocks from the beginning...");
    for (i = 0; i < kCount; i++)
      wrapper.release(ptrArray[i]);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Allocating %zuB blocks...", fixedBlockSize);
    for (i = 0; i < kCount; i++)
      ptrArray[i] = wrapper.alloc(fixedBlockSize);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Shuffling allocated blocks...");
    JitAllocatorTest_shuffle(ptrArray, unsigned(kCount), prng);

    INFO("  Releasing 50%% of allocated blocks...");
    for (i = 0; i < kCount / 2; i++)
      wrapper.release(ptrArray[i]);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Allocating 50%% more %zuB blocks again...", fixedBlockSize);
    for (i = 0; i < kCount / 2; i++)
      ptrArray[i] = wrapper.alloc(fixedBlockSize);
    JitAllocatorTest_usage(wrapper._allocator);

    INFO("  Releasing all allocated blocks from the end...");
    for (i = 0; i < kCount; i++)
      wrapper.release(ptrArray[kCount - i - 1]);
    JitAllocatorTest_usage(wrapper._allocator);

    ::free(ptrArray);
  }
}
#endif

ASMJIT_END_NAMESPACE

#endif
