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

#ifndef ASMJIT_CORE_RASTACK_P_H_INCLUDED
#define ASMJIT_CORE_RASTACK_P_H_INCLUDED

#include "../core/api-config.h"
#ifndef ASMJIT_NO_COMPILER

#include "../core/radefs_p.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_ra
//! \{

// ============================================================================
// [asmjit::RAStackSlot]
// ============================================================================

//! Stack slot.
struct RAStackSlot {
  //! Stack slot flags.
  //!
  //! TODO: kFlagStackArg is not used by the current implementation, do we need to keep it?
  enum Flags : uint32_t {
    //! Stack slot is register home slot.
    kFlagRegHome = 0x0001u,
    //! Stack slot position matches argument passed via stack.
    kFlagStackArg = 0x0002u  
  };

  enum ArgIndex : uint32_t {
    kNoArgIndex = 0xFF
  };

  //! Base register used to address the stack.
  uint8_t _baseRegId;
  //! Minimum alignment required by the slot.
  uint8_t _alignment;
  //! Reserved for future use.
  uint16_t _flags;
  //! Size of memory required by the slot.
  uint32_t _size;

  //! Usage counter (one unit equals one memory access).
  uint32_t _useCount;
  //! Weight of the slot, calculated by \ref RAStackAllocator::calculateStackFrame().
  uint32_t _weight;
  //! Stack offset, calculated by \ref RAStackAllocator::calculateStackFrame().
  int32_t _offset;

  //! \name Accessors
  //! \{

  inline uint32_t baseRegId() const noexcept { return _baseRegId; }
  inline void setBaseRegId(uint32_t id) noexcept { _baseRegId = uint8_t(id); }

  inline uint32_t size() const noexcept { return _size; }
  inline uint32_t alignment() const noexcept { return _alignment; }

  inline uint32_t flags() const noexcept { return _flags; }
  inline bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }
  inline void addFlags(uint32_t flags) noexcept { _flags = uint16_t(_flags | flags); }

  inline bool isRegHome() const noexcept { return hasFlag(kFlagRegHome); }
  inline bool isStackArg() const noexcept { return hasFlag(kFlagStackArg); }

  inline uint32_t useCount() const noexcept { return _useCount; }
  inline void addUseCount(uint32_t n = 1) noexcept { _useCount += n; }

  inline uint32_t weight() const noexcept { return _weight; }
  inline void setWeight(uint32_t weight) noexcept { _weight = weight; }

  inline int32_t offset() const noexcept { return _offset; }
  inline void setOffset(int32_t offset) noexcept { _offset = offset; }

  //! \}
};

typedef ZoneVector<RAStackSlot*> RAStackSlots;

// ============================================================================
// [asmjit::RAStackAllocator]
// ============================================================================

//! Stack allocator.
class RAStackAllocator {
public:
  ASMJIT_NONCOPYABLE(RAStackAllocator)

  enum Size : uint32_t {
    kSize1     = 0,
    kSize2     = 1,
    kSize4     = 2,
    kSize8     = 3,
    kSize16    = 4,
    kSize32    = 5,
    kSize64    = 6,
    kSizeCount = 7
  };

  //! Allocator used to allocate internal data.
  ZoneAllocator* _allocator;
  //! Count of bytes used by all slots.
  uint32_t _bytesUsed;
  //! Calculated stack size (can be a bit greater than `_bytesUsed`).
  uint32_t _stackSize;
  //! Minimum stack alignment.
  uint32_t _alignment;
  //! Stack slots vector.
  RAStackSlots _slots;

  //! \name Construction / Destruction
  //! \{

  inline RAStackAllocator() noexcept
    : _allocator(nullptr),
      _bytesUsed(0),
      _stackSize(0),
      _alignment(1),
      _slots() {}

  inline void reset(ZoneAllocator* allocator) noexcept {
    _allocator = allocator;
    _bytesUsed = 0;
    _stackSize = 0;
    _alignment = 1;
    _slots.reset();
  }

  //! \}

  //! \name Accessors
  //! \{

  inline ZoneAllocator* allocator() const noexcept { return _allocator; }

  inline uint32_t bytesUsed() const noexcept { return _bytesUsed; }
  inline uint32_t stackSize() const noexcept { return _stackSize; }
  inline uint32_t alignment() const noexcept { return _alignment; }

  inline RAStackSlots& slots() noexcept { return _slots; }
  inline const RAStackSlots& slots() const noexcept { return _slots; }
  inline uint32_t slotCount() const noexcept { return _slots.size(); }

  //! \}

  //! \name Utilities
  //! \{

  RAStackSlot* newSlot(uint32_t baseRegId, uint32_t size, uint32_t alignment, uint32_t flags = 0) noexcept;

  Error calculateStackFrame() noexcept;
  Error adjustSlotOffsets(int32_t offset) noexcept;

  //! \}
};

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_COMPILER
#endif // ASMJIT_CORE_RASTACK_P_H_INCLUDED
