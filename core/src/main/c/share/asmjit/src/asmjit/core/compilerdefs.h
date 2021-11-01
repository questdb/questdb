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

#ifndef ASMJIT_CORE_COMPILERDEFS_H_INCLUDED
#define ASMJIT_CORE_COMPILERDEFS_H_INCLUDED

#include "../core/api-config.h"
#include "../core/operand.h"
#include "../core/zonestring.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [Forward Declarations]
// ============================================================================

class RAWorkReg;

//! \addtogroup asmjit_compiler
//! \{

// ============================================================================
// [asmjit::VirtReg]
// ============================================================================

//! Virtual register data, managed by \ref BaseCompiler.
class VirtReg {
public:
  ASMJIT_NONCOPYABLE(VirtReg)

  //! Virtual register id.
  uint32_t _id = 0;
  //! Virtual register info (signature).
  RegInfo _info = {};
  //! Virtual register size (can be smaller than `regInfo._size`).
  uint32_t _virtSize = 0;
  //! Virtual register alignment (for spilling).
  uint8_t _alignment = 0;
  //! Type-id.
  uint8_t _typeId = 0;
  //! Virtual register weight for alloc/spill decisions.
  uint8_t _weight = 1;
  //! True if this is a fixed register, never reallocated.
  uint8_t _isFixed : 1;
  //! True if the virtual register is only used as a stack (never accessed as register).
  uint8_t _isStack : 1;
  uint8_t _reserved : 6;

  //! Virtual register name (user provided or automatically generated).
  ZoneString<16> _name {};

  // -------------------------------------------------------------------------
  // The following members are used exclusively by RAPass. They are initialized
  // when the VirtReg is created to NULL pointers and then changed during RAPass
  // execution. RAPass sets them back to NULL before it returns.
  // -------------------------------------------------------------------------

  //! Reference to `RAWorkReg`, used during register allocation.
  RAWorkReg* _workReg = nullptr;

  //! \name Construction & Destruction
  //! \{

  inline VirtReg(uint32_t id, uint32_t signature, uint32_t virtSize, uint32_t alignment, uint32_t typeId) noexcept
    : _id(id),
      _info { signature },
      _virtSize(virtSize),
      _alignment(uint8_t(alignment)),
      _typeId(uint8_t(typeId)),
      _isFixed(false),
      _isStack(false),
      _reserved(0) {}

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the virtual register id.
  inline uint32_t id() const noexcept { return _id; }

  //! Returns the virtual register name.
  inline const char* name() const noexcept { return _name.data(); }
  //! Returns the size of the virtual register name.
  inline uint32_t nameSize() const noexcept { return _name.size(); }

  //! Returns a register information that wraps the register signature.
  inline const RegInfo& info() const noexcept { return _info; }
  //! Returns a virtual register type (maps to the physical register type as well).
  inline uint32_t type() const noexcept { return _info.type(); }
  //! Returns a virtual register group (maps to the physical register group as well).
  inline uint32_t group() const noexcept { return _info.group(); }

  //! Returns a real size of the register this virtual register maps to.
  //!
  //! For example if this is a 128-bit SIMD register used for a scalar single
  //! precision floating point value then its virtSize would be 4, however, the
  //! `regSize` would still say 16 (128-bits), because it's the smallest size
  //! of that register type.
  inline uint32_t regSize() const noexcept { return _info.size(); }

  //! Returns a register signature of this virtual register.
  inline uint32_t signature() const noexcept { return _info.signature(); }

  //! Returns the virtual register size.
  //!
  //! The virtual register size describes how many bytes the virtual register
  //! needs to store its content. It can be smaller than the physical register
  //! size, see `regSize()`.
  inline uint32_t virtSize() const noexcept { return _virtSize; }

  //! Returns the virtual register alignment.
  inline uint32_t alignment() const noexcept { return _alignment; }

  //! Returns the virtual register type id, see `Type::Id`.
  inline uint32_t typeId() const noexcept { return _typeId; }

  //! Returns the virtual register weight - the register allocator can use it
  //! as explicit hint for alloc/spill decisions.
  inline uint32_t weight() const noexcept { return _weight; }
  //! Sets the virtual register weight (0 to 255) - the register allocator can
  //! use it as explicit hint for alloc/spill decisions and initial bin-packing.
  inline void setWeight(uint32_t weight) noexcept { _weight = uint8_t(weight); }

  //! Returns whether the virtual register is always allocated to a fixed
  //! physical register (and never reallocated).
  //!
  //! \note This is only used for special purposes and it's mostly internal.
  inline bool isFixed() const noexcept { return bool(_isFixed); }

  //! Returns whether the virtual register is indeed a stack that only uses
  //! the virtual register id for making it accessible.
  //!
  //! \note It's an error if a stack is accessed as a register.
  inline bool isStack() const noexcept { return bool(_isStack); }

  inline bool hasWorkReg() const noexcept { return _workReg != nullptr; }
  inline RAWorkReg* workReg() const noexcept { return _workReg; }
  inline void setWorkReg(RAWorkReg* workReg) noexcept { _workReg = workReg; }
  inline void resetWorkReg() noexcept { _workReg = nullptr; }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_COMPILERDEFS_H_INCLUDED

