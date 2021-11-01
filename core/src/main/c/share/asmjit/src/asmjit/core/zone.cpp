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

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::Zone - Statics]
// ============================================================================

// Zero size block used by `Zone` that doesn't have any memory allocated.
// Should be allocated in read-only memory and should never be modified.
const Zone::Block Zone::_zeroBlock = { nullptr, nullptr, 0 };

// ============================================================================
// [asmjit::Zone - Init / Reset]
// ============================================================================

void Zone::_init(size_t blockSize, size_t blockAlignment, const Support::Temporary* temporary) noexcept {
  ASMJIT_ASSERT(blockSize >= kMinBlockSize);
  ASMJIT_ASSERT(blockSize <= kMaxBlockSize);
  ASMJIT_ASSERT(blockAlignment <= 64);

  // Just to make the compiler happy...
  constexpr size_t kBlockSizeMask = (Support::allOnes<size_t>() >> 4);
  constexpr size_t kBlockAlignmentShiftMask = 0x7u;

  _assignZeroBlock();
  _blockSize = blockSize & kBlockSizeMask;
  _isTemporary = temporary != nullptr;
  _blockAlignmentShift = Support::ctz(blockAlignment) & kBlockAlignmentShiftMask;

  // Setup the first [temporary] block, if necessary.
  if (temporary) {
    Block* block = temporary->data<Block>();
    block->prev = nullptr;
    block->next = nullptr;

    ASMJIT_ASSERT(temporary->size() >= kBlockSize);
    block->size = temporary->size() - kBlockSize;

    _assignBlock(block);
  }
}

void Zone::reset(uint32_t resetPolicy) noexcept {
  Block* cur = _block;

  // Can't be altered.
  if (cur == &_zeroBlock)
    return;

  if (resetPolicy == Globals::kResetHard) {
    Block* initial = const_cast<Zone::Block*>(&_zeroBlock);
    _ptr = initial->data();
    _end = initial->data();
    _block = initial;

    // Since cur can be in the middle of the double-linked list, we have to
    // traverse both directions (`prev` and `next`) separately to visit all.
    Block* next = cur->next;
    do {
      Block* prev = cur->prev;

      // If this is the first block and this ZoneTmp is temporary then the
      // first block is statically allocated. We cannot free it and it makes
      // sense to keep it even when this is hard reset.
      if (prev == nullptr && _isTemporary) {
        cur->prev = nullptr;
        cur->next = nullptr;
        _assignBlock(cur);
        break;
      }

      ::free(cur);
      cur = prev;
    } while (cur);

    cur = next;
    while (cur) {
      next = cur->next;
      ::free(cur);
      cur = next;
    }
  }
  else {
    while (cur->prev)
      cur = cur->prev;
    _assignBlock(cur);
  }
}

// ============================================================================
// [asmjit::Zone - Alloc]
// ============================================================================

void* Zone::_alloc(size_t size, size_t alignment) noexcept {
  Block* curBlock = _block;
  Block* next = curBlock->next;

  size_t rawBlockAlignment = blockAlignment();
  size_t minimumAlignment = Support::max<size_t>(alignment, rawBlockAlignment);

  // If the `Zone` has been cleared the current block doesn't have to be the
  // last one. Check if there is a block that can be used instead of allocating
  // a new one. If there is a `next` block it's completely unused, we don't have
  // to check for remaining bytes in that case.
  if (next) {
    uint8_t* ptr = Support::alignUp(next->data(), minimumAlignment);
    uint8_t* end = Support::alignDown(next->data() + next->size, rawBlockAlignment);

    if (size <= (size_t)(end - ptr)) {
      _block = next;
      _ptr = ptr + size;
      _end = Support::alignDown(next->data() + next->size, rawBlockAlignment);
      return static_cast<void*>(ptr);
    }
  }

  size_t blockAlignmentOverhead = alignment - Support::min<size_t>(alignment, Globals::kAllocAlignment);
  size_t newSize = Support::max(blockSize(), size);

  // Prevent arithmetic overflow.
  if (ASMJIT_UNLIKELY(newSize > SIZE_MAX - kBlockSize - blockAlignmentOverhead))
    return nullptr;

  // Allocate new block - we add alignment overhead to `newSize`, which becomes the
  // new block size, and we also add `kBlockOverhead` to the allocator as it includes
  // members of `Zone::Block` structure.
  newSize += blockAlignmentOverhead;
  Block* newBlock = static_cast<Block*>(::malloc(newSize + kBlockSize));

  if (ASMJIT_UNLIKELY(!newBlock))
    return nullptr;

  // Align the pointer to `minimumAlignment` and adjust the size of this block
  // accordingly. It's the same as using `minimumAlignment - Support::alignUpDiff()`,
  // just written differently.
  {
    newBlock->prev = nullptr;
    newBlock->next = nullptr;
    newBlock->size = newSize;

    if (curBlock != &_zeroBlock) {
      newBlock->prev = curBlock;
      curBlock->next = newBlock;

      // Does only happen if there is a next block, but the requested memory
      // can't fit into it. In this case a new buffer is allocated and inserted
      // between the current block and the next one.
      if (next) {
        newBlock->next = next;
        next->prev = newBlock;
      }
    }

    uint8_t* ptr = Support::alignUp(newBlock->data(), minimumAlignment);
    uint8_t* end = Support::alignDown(newBlock->data() + newSize, rawBlockAlignment);

    _ptr = ptr + size;
    _end = end;
    _block = newBlock;

    ASMJIT_ASSERT(_ptr <= _end);
    return static_cast<void*>(ptr);
  }
}

void* Zone::allocZeroed(size_t size, size_t alignment) noexcept {
  void* p = alloc(size, alignment);
  if (ASMJIT_UNLIKELY(!p))
    return p;
  return memset(p, 0, size);
}

void* Zone::dup(const void* data, size_t size, bool nullTerminate) noexcept {
  if (ASMJIT_UNLIKELY(!data || !size))
    return nullptr;

  ASMJIT_ASSERT(size != SIZE_MAX);
  uint8_t* m = allocT<uint8_t>(size + nullTerminate);
  if (ASMJIT_UNLIKELY(!m)) return nullptr;

  memcpy(m, data, size);
  if (nullTerminate) m[size] = '\0';

  return static_cast<void*>(m);
}

char* Zone::sformat(const char* fmt, ...) noexcept {
  if (ASMJIT_UNLIKELY(!fmt))
    return nullptr;

  char buf[512];
  size_t size;
  va_list ap;

  va_start(ap, fmt);
  size = unsigned(vsnprintf(buf, ASMJIT_ARRAY_SIZE(buf) - 1, fmt, ap));
  va_end(ap);

  buf[size++] = 0;
  return static_cast<char*>(dup(buf, size));
}

// ============================================================================
// [asmjit::ZoneAllocator - Helpers]
// ============================================================================

#if defined(ASMJIT_BUILD_DEBUG)
static bool ZoneAllocator_hasDynamicBlock(ZoneAllocator* self, ZoneAllocator::DynamicBlock* block) noexcept {
  ZoneAllocator::DynamicBlock* cur = self->_dynamicBlocks;
  while (cur) {
    if (cur == block)
      return true;
    cur = cur->next;
  }
  return false;
}
#endif

// ============================================================================
// [asmjit::ZoneAllocator - Init / Reset]
// ============================================================================

void ZoneAllocator::reset(Zone* zone) noexcept {
  // Free dynamic blocks.
  DynamicBlock* block = _dynamicBlocks;
  while (block) {
    DynamicBlock* next = block->next;
    ::free(block);
    block = next;
  }

  // Zero the entire class and initialize to the given `zone`.
  memset(this, 0, sizeof(*this));
  _zone = zone;
}

// ============================================================================
// [asmjit::ZoneAllocator - Alloc / Release]
// ============================================================================

void* ZoneAllocator::_alloc(size_t size, size_t& allocatedSize) noexcept {
  ASMJIT_ASSERT(isInitialized());

  // Use the memory pool only if the requested block has a reasonable size.
  uint32_t slot;
  if (_getSlotIndex(size, slot, allocatedSize)) {
    // Slot reuse.
    uint8_t* p = reinterpret_cast<uint8_t*>(_slots[slot]);
    size = allocatedSize;

    if (p) {
      _slots[slot] = reinterpret_cast<Slot*>(p)->next;
      return p;
    }

    _zone->align(kBlockAlignment);
    p = _zone->ptr();
    size_t remain = (size_t)(_zone->end() - p);

    if (ASMJIT_LIKELY(remain >= size)) {
      _zone->setPtr(p + size);
      return p;
    }
    else {
      // Distribute the remaining memory to suitable slots, if possible.
      if (remain >= kLoGranularity) {
        do {
          size_t distSize = Support::min<size_t>(remain, kLoMaxSize);
          uint32_t distSlot = uint32_t((distSize - kLoGranularity) / kLoGranularity);
          ASMJIT_ASSERT(distSlot < kLoCount);

          reinterpret_cast<Slot*>(p)->next = _slots[distSlot];
          _slots[distSlot] = reinterpret_cast<Slot*>(p);

          p += distSize;
          remain -= distSize;
        } while (remain >= kLoGranularity);
        _zone->setPtr(p);
      }

      p = static_cast<uint8_t*>(_zone->_alloc(size, kBlockAlignment));
      if (ASMJIT_UNLIKELY(!p)) {
        allocatedSize = 0;
        return nullptr;
      }

      return p;
    }
  }
  else {
    // Allocate a dynamic block.
    size_t kBlockOverhead = sizeof(DynamicBlock) + sizeof(DynamicBlock*) + kBlockAlignment;

    // Handle a possible overflow.
    if (ASMJIT_UNLIKELY(kBlockOverhead >= SIZE_MAX - size))
      return nullptr;

    void* p = ::malloc(size + kBlockOverhead);
    if (ASMJIT_UNLIKELY(!p)) {
      allocatedSize = 0;
      return nullptr;
    }

    // Link as first in `_dynamicBlocks` double-linked list.
    DynamicBlock* block = static_cast<DynamicBlock*>(p);
    DynamicBlock* next = _dynamicBlocks;

    if (next)
      next->prev = block;

    block->prev = nullptr;
    block->next = next;
    _dynamicBlocks = block;

    // Align the pointer to the guaranteed alignment and store `DynamicBlock`
    // at the beginning of the memory block, so `_releaseDynamic()` can find it.
    p = Support::alignUp(static_cast<uint8_t*>(p) + sizeof(DynamicBlock) + sizeof(DynamicBlock*), kBlockAlignment);
    reinterpret_cast<DynamicBlock**>(p)[-1] = block;

    allocatedSize = size;
    return p;
  }
}

void* ZoneAllocator::_allocZeroed(size_t size, size_t& allocatedSize) noexcept {
  ASMJIT_ASSERT(isInitialized());

  void* p = _alloc(size, allocatedSize);
  if (ASMJIT_UNLIKELY(!p)) return p;
  return memset(p, 0, allocatedSize);
}

void ZoneAllocator::_releaseDynamic(void* p, size_t size) noexcept {
  DebugUtils::unused(size);
  ASMJIT_ASSERT(isInitialized());

  // Pointer to `DynamicBlock` is stored at [-1].
  DynamicBlock* block = reinterpret_cast<DynamicBlock**>(p)[-1];
  ASMJIT_ASSERT(ZoneAllocator_hasDynamicBlock(this, block));

  // Unlink and free.
  DynamicBlock* prev = block->prev;
  DynamicBlock* next = block->next;

  if (prev)
    prev->next = next;
  else
    _dynamicBlocks = next;

  if (next)
    next->prev = prev;

  ::free(block);
}

ASMJIT_END_NAMESPACE
