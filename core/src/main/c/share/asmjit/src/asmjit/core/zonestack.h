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

#ifndef ASMJIT_CORE_ZONESTACK_H_INCLUDED
#define ASMJIT_CORE_ZONESTACK_H_INCLUDED

#include "../core/zone.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_zone
//! \{

// ============================================================================
// [asmjit::ZoneStackBase]
// ============================================================================

//! Base class used by \ref ZoneStack.
class ZoneStackBase {
public:
  ASMJIT_NONCOPYABLE(ZoneStackBase)

  static constexpr uint32_t kBlockSize = ZoneAllocator::kHiMaxSize;

  struct Block {
    inline bool empty() const noexcept { return _start == _end; }
    inline Block* prev() const noexcept { return _link[Globals::kLinkLeft]; }
    inline Block* next() const noexcept { return _link[Globals::kLinkRight]; }

    inline void setPrev(Block* block) noexcept { _link[Globals::kLinkLeft] = block; }
    inline void setNext(Block* block) noexcept { _link[Globals::kLinkRight] = block; }

    template<typename T>
    inline T* start() const noexcept { return static_cast<T*>(_start); }
    template<typename T>
    inline void setStart(T* start) noexcept { _start = static_cast<void*>(start); }

    template<typename T>
    inline T* end() const noexcept { return (T*)_end; }
    template<typename T>
    inline void setEnd(T* end) noexcept { _end = (void*)end; }

    template<typename T>
    inline T* data() const noexcept { return (T*)((uint8_t*)(this) + sizeof(Block)); }

    template<typename T>
    inline bool canPrepend() const noexcept { return _start > data<void>(); }

    template<typename T>
    inline bool canAppend() const noexcept {
      size_t kNumBlockItems = (kBlockSize - sizeof(Block)) / sizeof(T);
      size_t kStartBlockIndex = sizeof(Block);
      size_t kEndBlockIndex = kStartBlockIndex + kNumBlockItems * sizeof(T);

      return (uintptr_t)_end <= ((uintptr_t)this + kEndBlockIndex - sizeof(T));
    }

    Block* _link[Globals::kLinkCount];   //!< Next and previous blocks.
    void* _start;                        //!< Pointer to the start of the array.
    void* _end;                          //!< Pointer to the end of the array.
  };

  //! Allocator used to allocate data.
  ZoneAllocator* _allocator;
  //! First and last blocks.
  Block* _block[Globals::kLinkCount];

  //! \name Construction / Destruction
  //! \{

  inline ZoneStackBase() noexcept {
    _allocator = nullptr;
    _block[0] = nullptr;
    _block[1] = nullptr;
  }
  inline ~ZoneStackBase() noexcept { reset(); }

  inline bool isInitialized() const noexcept { return _allocator != nullptr; }
  ASMJIT_API Error _init(ZoneAllocator* allocator, size_t middleIndex) noexcept;
  inline Error reset() noexcept { return _init(nullptr, 0); }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns `ZoneAllocator` attached to this container.
  inline ZoneAllocator* allocator() const noexcept { return _allocator; }

  inline bool empty() const noexcept {
    ASMJIT_ASSERT(isInitialized());
    return _block[0]->start<void>() == _block[1]->end<void>();
  }

  //! \}

  //! \cond INTERNAL
  //! \name Internal
  //! \{

  ASMJIT_API Error _prepareBlock(uint32_t side, size_t initialIndex) noexcept;
  ASMJIT_API void _cleanupBlock(uint32_t side, size_t middleIndex) noexcept;

  //! \}
  //! \endcond
};

// ============================================================================
// [asmjit::ZoneStack<T>]
// ============================================================================

//! Zone allocated stack container.
template<typename T>
class ZoneStack : public ZoneStackBase {
public:
  ASMJIT_NONCOPYABLE(ZoneStack)

  enum : uint32_t {
    kNumBlockItems   = uint32_t((kBlockSize - sizeof(Block)) / sizeof(T)),
    kStartBlockIndex = uint32_t(sizeof(Block)),
    kMidBlockIndex   = uint32_t(kStartBlockIndex + (kNumBlockItems / 2) * sizeof(T)),
    kEndBlockIndex   = uint32_t(kStartBlockIndex + (kNumBlockItems    ) * sizeof(T))
  };

  //! \name Construction / Destruction
  //! \{

  inline ZoneStack() noexcept {}
  inline ~ZoneStack() noexcept {}

  inline Error init(ZoneAllocator* allocator) noexcept { return _init(allocator, kMidBlockIndex); }

  //! \}

  //! \name Utilities
  //! \{

  ASMJIT_INLINE Error prepend(T item) noexcept {
    ASMJIT_ASSERT(isInitialized());
    Block* block = _block[Globals::kLinkFirst];

    if (!block->canPrepend<T>()) {
      ASMJIT_PROPAGATE(_prepareBlock(Globals::kLinkFirst, kEndBlockIndex));
      block = _block[Globals::kLinkFirst];
    }

    T* ptr = block->start<T>() - 1;
    ASMJIT_ASSERT(ptr >= block->data<T>() && ptr <= block->data<T>() + (kNumBlockItems - 1));
    *ptr = item;
    block->setStart<T>(ptr);
    return kErrorOk;
  }

  ASMJIT_INLINE Error append(T item) noexcept {
    ASMJIT_ASSERT(isInitialized());
    Block* block = _block[Globals::kLinkLast];

    if (!block->canAppend<T>()) {
      ASMJIT_PROPAGATE(_prepareBlock(Globals::kLinkLast, kStartBlockIndex));
      block = _block[Globals::kLinkLast];
    }

    T* ptr = block->end<T>();
    ASMJIT_ASSERT(ptr >= block->data<T>() && ptr <= block->data<T>() + (kNumBlockItems - 1));

    *ptr++ = item;
    block->setEnd(ptr);
    return kErrorOk;
  }

  ASMJIT_INLINE T popFirst() noexcept {
    ASMJIT_ASSERT(isInitialized());
    ASMJIT_ASSERT(!empty());

    Block* block = _block[Globals::kLinkFirst];
    ASMJIT_ASSERT(!block->empty());

    T* ptr = block->start<T>();
    T item = *ptr++;

    block->setStart(ptr);
    if (block->empty())
      _cleanupBlock(Globals::kLinkFirst, kMidBlockIndex);

    return item;
  }

  ASMJIT_INLINE T pop() noexcept {
    ASMJIT_ASSERT(isInitialized());
    ASMJIT_ASSERT(!empty());

    Block* block = _block[Globals::kLinkLast];
    ASMJIT_ASSERT(!block->empty());

    T* ptr = block->end<T>();
    T item = *--ptr;
    ASMJIT_ASSERT(ptr >= block->data<T>());
    ASMJIT_ASSERT(ptr >= block->start<T>());

    block->setEnd(ptr);
    if (block->empty())
      _cleanupBlock(Globals::kLinkLast, kMidBlockIndex);

    return item;
  }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ZONESTACK_H_INCLUDED
