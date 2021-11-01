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

#ifndef ASMJIT_CORE_CODEBUFFER_H_INCLUDED
#define ASMJIT_CORE_CODEBUFFER_H_INCLUDED

#include "../core/globals.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::CodeBuffer]
// ============================================================================

//! Code or data buffer.
struct CodeBuffer {
  //! The content of the buffer (data).
  uint8_t* _data;
  //! Number of bytes of `data` used.
  size_t _size;
  //! Buffer capacity (in bytes).
  size_t _capacity;
  //! Buffer flags.
  uint32_t _flags;

  //! Code buffer flags.
  enum Flags : uint32_t {
    //! Buffer is external (not allocated by asmjit).
    kFlagIsExternal = 0x00000001u,
    //! Buffer is fixed (cannot be reallocated).
    kFlagIsFixed = 0x00000002u
  };

  //! \name Overloaded Operators
  //! \{

  //! Returns a referebce to the byte at the given `index`.
  inline uint8_t& operator[](size_t index) noexcept {
    ASMJIT_ASSERT(index < _size);
    return _data[index];
  }
  //! \overload
  inline const uint8_t& operator[](size_t index) const noexcept {
    ASMJIT_ASSERT(index < _size);
    return _data[index];
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns code buffer flags, see \ref Flags.
  inline uint32_t flags() const noexcept { return _flags; }
  //! Tests whether the code buffer has the given `flag` set.
  inline bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }

  //! Tests whether this code buffer has a fixed size.
  //!
  //! Fixed size means that the code buffer is fixed and cannot grow.
  inline bool isFixed() const noexcept { return hasFlag(kFlagIsFixed); }

  //! Tests whether the data in this code buffer is external.
  //!
  //! External data can only be provided by users, it's never used by AsmJit.
  inline bool isExternal() const noexcept { return hasFlag(kFlagIsExternal); }

  //! Tests whether the data in this code buffer is allocated (non-null).
  inline bool isAllocated() const noexcept { return _data != nullptr; }

  //! Tests whether the code buffer is empty.
  inline bool empty() const noexcept { return !_size; }

  //! Returns the size of the data.
  inline size_t size() const noexcept { return _size; }
  //! Returns the capacity of the data.
  inline size_t capacity() const noexcept { return _capacity; }

  //! Returns the pointer to the data the buffer references.
  inline uint8_t* data() noexcept { return _data; }
  //! \overload
  inline const uint8_t* data() const noexcept { return _data; }

  //! \}

  //! \name Iterators
  //! \{

  inline uint8_t* begin() noexcept { return _data; }
  inline const uint8_t* begin() const noexcept { return _data; }

  inline uint8_t* end() noexcept { return _data + _size; }
  inline const uint8_t* end() const noexcept { return _data + _size; }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_CODEBUFFER_H_INCLUDED

