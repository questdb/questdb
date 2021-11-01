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

#ifndef ASMJIT_CORE_FORMATTER_H_INCLUDED
#define ASMJIT_CORE_FORMATTER_H_INCLUDED

#include "../core/inst.h"
#include "../core/string.h"

#ifndef ASMJIT_NO_LOGGING

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_logging
//! \{

// ============================================================================
// [Forward Declarations]
// ============================================================================

class BaseEmitter;
struct Operand_;

#ifndef ASMJIT_NO_BUILDER
class BaseBuilder;
class BaseNode;
#endif

#ifndef ASMJIT_NO_COMPILER
class BaseCompiler;
#endif

// ============================================================================
// [asmjit::FormatOptions]
// ============================================================================

//! Formatting options used by \ref Logger and \ref Formatter.
class FormatOptions {
public:
  //! Format flags, see \ref Flags.
  uint32_t _flags;
  //! Indentation by type, see \ref IndentationType.
  uint8_t _indentation[4];

  //! Flags can enable a logging feature.
  enum Flags : uint32_t {
    //! No flags.
    kNoFlags = 0u,

    //! Show also binary form of each logged instruction (Assembler).
    kFlagMachineCode = 0x00000001u,
    //! Show a text explanation of some immediate values.
    kFlagExplainImms = 0x00000002u,
    //! Use hexadecimal notation of immediate values.
    kFlagHexImms = 0x00000004u,
    //! Use hexadecimal notation of address offsets.
    kFlagHexOffsets = 0x00000008u,
    //! Show casts between virtual register types (Compiler).
    kFlagRegCasts = 0x00000010u,
    //! Show positions associated with nodes (Compiler).
    kFlagPositions = 0x00000020u,
    //! Annotate nodes that are lowered by passes.
    kFlagAnnotations = 0x00000040u,

    // TODO: These must go, keep this only for formatting.
    //! Show an additional output from passes.
    kFlagDebugPasses = 0x00000080u,
    //! Show an additional output from RA.
    kFlagDebugRA = 0x00000100u
  };

  //! Describes indentation type of code, label, or comment in logger output.
  enum IndentationType : uint32_t {
    //! Indentation used for instructions and directives.
    kIndentationCode = 0u,
    //! Indentation used for labels and function nodes.
    kIndentationLabel = 1u,
    //! Indentation used for comments (not inline comments).
    kIndentationComment = 2u,
    //! \cond INTERNAL
    //! Reserved for future use.
    kIndentationReserved = 3u
    //! \endcond
  };

  //! \name Construction & Destruction
  //! \{

  //! Creates a default-initialized FormatOptions.
  constexpr FormatOptions() noexcept
    : _flags(0),
      _indentation { 0, 0, 0, 0 } {}

  constexpr FormatOptions(const FormatOptions& other) noexcept = default;
  inline FormatOptions& operator=(const FormatOptions& other) noexcept = default;

  //! Resets FormatOptions to its default initialized state.
  inline void reset() noexcept {
    _flags = 0;
    _indentation[0] = 0;
    _indentation[1] = 0;
    _indentation[2] = 0;
    _indentation[3] = 0;
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns format flags.
  constexpr uint32_t flags() const noexcept { return _flags; }
  //! Tests whether the given `flag` is set in format flags.
  constexpr bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }
  //! Resets all format flags to `flags`.
  inline void setFlags(uint32_t flags) noexcept { _flags = flags; }
  //! Adds `flags` to format flags.
  inline void addFlags(uint32_t flags) noexcept { _flags |= flags; }
  //! Removes `flags` from format flags.
  inline void clearFlags(uint32_t flags) noexcept { _flags &= ~flags; }

  //! Returns indentation for the given `type`, see \ref IndentationType.
  constexpr uint8_t indentation(uint32_t type) const noexcept { return _indentation[type]; }
  //! Sets indentation for the given `type`, see \ref IndentationType.
  inline void setIndentation(uint32_t type, uint32_t n) noexcept { _indentation[type] = uint8_t(n); }
  //! Resets indentation for the given `type` to zero.
  inline void resetIndentation(uint32_t type) noexcept { _indentation[type] = uint8_t(0); }

  //! \}
};

// ============================================================================
// [asmjit::Formatter]
// ============================================================================

//! Provides formatting functionality to format operands, instructions, and nodes.
namespace Formatter {

//! Appends a formatted `typeId` to the output string `sb`.
ASMJIT_API Error formatTypeId(
  String& sb,
  uint32_t typeId) noexcept;

//! Appends a formatted `featureId` to the output string `sb`.
//!
//! See \ref BaseFeatures.
ASMJIT_API Error formatFeature(
  String& sb,
  uint32_t arch,
  uint32_t featureId) noexcept;

//! Appends a formatted register to the output string `sb`.
//!
//! \note Emitter is optional, but it's required to format virtual registers,
//! which won't be formatted properly if the `emitter` is not provided.
ASMJIT_API Error formatRegister(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t arch,
  uint32_t regType,
  uint32_t regId) noexcept;

//! Appends a formatted label to the output string `sb`.
//!
//! \note Emitter is optional, but it's required to format named labels
//! properly, otherwise the formatted as it is an anonymous label.
ASMJIT_API Error formatLabel(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t labelId) noexcept;

//! Appends a formatted operand to the output string `sb`.
//!
//! \note Emitter is optional, but it's required to format named labels and
//! virtual registers. See \ref formatRegister() and \ref formatLabel() for
//! more details.
ASMJIT_API Error formatOperand(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t arch,
  const Operand_& op) noexcept;

//! Appends a formatted data-type to the output string `sb`.
ASMJIT_API Error formatDataType(
  String& sb,
  uint32_t formatFlags,
  uint32_t arch,
  uint32_t typeId) noexcept;

//! Appends a formatted data to the output string `sb`.
ASMJIT_API Error formatData(
  String& sb,
  uint32_t formatFlags,
  uint32_t arch,
  uint32_t typeId, const void* data, size_t itemCount, size_t repeatCount = 1) noexcept;

//! Appends a formatted instruction to the output string `sb`.
//!
//! \note Emitter is optional, but it's required to format named labels and
//! virtual registers. See \ref formatRegister() and \ref formatLabel() for
//! more details.
ASMJIT_API Error formatInstruction(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t arch,
  const BaseInst& inst, const Operand_* operands, size_t opCount) noexcept;

#ifndef ASMJIT_NO_BUILDER
//! Appends a formatted node to the output string `sb`.
//!
//! The `node` must belong to the provided `builder`.
ASMJIT_API Error formatNode(
  String& sb,
  uint32_t formatFlags,
  const BaseBuilder* builder,
  const BaseNode* node) noexcept;

//! Appends formatted nodes to the output string `sb`.
//!
//! All nodes that are part of the given `builder` will be appended.
ASMJIT_API Error formatNodeList(
  String& sb,
  uint32_t formatFlags,
  const BaseBuilder* builder) noexcept;

//! Appends formatted nodes to the output string `sb`.
//!
//! This function works the same as \ref formatNode(), but appends more nodes
//! to the output string, separating each node with a newline '\n' character.
ASMJIT_API Error formatNodeList(
  String& sb,
  uint32_t formatFlags,
  const BaseBuilder* builder,
  const BaseNode* begin,
  const BaseNode* end) noexcept;
#endif

} // {Formatter}

//! \}

ASMJIT_END_NAMESPACE

#endif

#endif // ASMJIT_CORE_FORMATTER_H_INCLUDED
