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

#ifndef ASMJIT_CORE_ASSEMBLER_H_INCLUDED
#define ASMJIT_CORE_ASSEMBLER_H_INCLUDED

#include "../core/codeholder.h"
#include "../core/datatypes.h"
#include "../core/emitter.h"
#include "../core/operand.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_assembler
//! \{

// ============================================================================
// [asmjit::BaseAssembler]
// ============================================================================

//! Base assembler.
//!
//! This is a base class that provides interface used by architecture specific
//! assembler implementations. Assembler doesn't hold any data, instead it's
//! attached to \ref CodeHolder, which provides all the data that Assembler
//! needs and which can be altered by it.
//!
//! Check out architecture specific assemblers for more details and examples:
//!
//!   - \ref x86::Assembler - X86/X64 assembler implementation.
class ASMJIT_VIRTAPI BaseAssembler : public BaseEmitter {
public:
  ASMJIT_NONCOPYABLE(BaseAssembler)
  typedef BaseEmitter Base;

  //! Current section where the assembling happens.
  Section* _section = nullptr;
  //! Start of the CodeBuffer of the current section.
  uint8_t* _bufferData = nullptr;
  //! End (first invalid byte) of the current section.
  uint8_t* _bufferEnd = nullptr;
  //! Pointer in the CodeBuffer of the current section.
  uint8_t* _bufferPtr = nullptr;

  //! \name Construction & Destruction
  //! \{

  //! Creates a new `BaseAssembler` instance.
  ASMJIT_API BaseAssembler() noexcept;
  //! Destroys the `BaseAssembler` instance.
  ASMJIT_API virtual ~BaseAssembler() noexcept;

  //! \}

  //! \name Code-Buffer Management
  //! \{

  //! Returns the capacity of the current CodeBuffer.
  inline size_t bufferCapacity() const noexcept { return (size_t)(_bufferEnd - _bufferData); }
  //! Returns the number of remaining bytes in the current CodeBuffer.
  inline size_t remainingSpace() const noexcept { return (size_t)(_bufferEnd - _bufferPtr); }

  //! Returns the current position in the CodeBuffer.
  inline size_t offset() const noexcept { return (size_t)(_bufferPtr - _bufferData); }

  //! Sets the current position in the CodeBuffer to `offset`.
  //!
  //! \note The `offset` cannot be greater than buffer size even if it's
  //! within the buffer's capacity.
  ASMJIT_API Error setOffset(size_t offset);

  //! Returns the start of the CodeBuffer in the current section.
  inline uint8_t* bufferData() const noexcept { return _bufferData; }
  //! Returns the end (first invalid byte) in the current section.
  inline uint8_t* bufferEnd() const noexcept { return _bufferEnd; }
  //! Returns the current pointer in the CodeBuffer in the current section.
  inline uint8_t* bufferPtr() const noexcept { return _bufferPtr; }

  //! \}

  //! \name Section Management
  //! \{

  //! Returns the current section.
  inline Section* currentSection() const noexcept { return _section; }

  ASMJIT_API Error section(Section* section) override;

  //! \}

  //! \name Label Management
  //! \{

  ASMJIT_API Label newLabel() override;
  ASMJIT_API Label newNamedLabel(const char* name, size_t nameSize = SIZE_MAX, uint32_t type = Label::kTypeGlobal, uint32_t parentId = Globals::kInvalidId) override;
  ASMJIT_API Error bind(const Label& label) override;

  //! \}

  //! \name Embed
  //! \{

  ASMJIT_API Error embed(const void* data, size_t dataSize) override;
  ASMJIT_API Error embedDataArray(uint32_t typeId, const void* data, size_t itemCount, size_t repeatCount = 1) override;
  ASMJIT_API Error embedConstPool(const Label& label, const ConstPool& pool) override;

  ASMJIT_API Error embedLabel(const Label& label, size_t dataSize = 0) override;
  ASMJIT_API Error embedLabelDelta(const Label& label, const Label& base, size_t dataSize = 0) override;

  //! \}

  //! \name Comment
  //! \{

  ASMJIT_API Error comment(const char* data, size_t size = SIZE_MAX) override;

  //! \}

  //! \name Events
  //! \{

  ASMJIT_API Error onAttach(CodeHolder* code) noexcept override;
  ASMJIT_API Error onDetach(CodeHolder* code) noexcept override;

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ASSEMBLER_H_INCLUDED
