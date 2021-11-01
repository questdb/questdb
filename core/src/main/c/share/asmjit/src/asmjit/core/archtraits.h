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

#ifndef ASMJIT_CORE_ARCHTRAITS_H_INCLUDED
#define ASMJIT_CORE_ARCHTRAITS_H_INCLUDED

#include "../core/environment.h"
#include "../core/operand.h"
#include "../core/type.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

//! Identifier used to represent names of different data types across architectures.
enum class ISAWordNameId : uint8_t {
  //! Describes 'db' (X86/X86_64 convention, always 8-bit quantity).
  kDB = 0,
  //! Describes 'dw' (X86/X86_64 convention, always 16-bit word).
  kDW,
  //! Describes 'dd' (X86/X86_64 convention, always 32-bit word).
  kDD,
  //! Describes 'dq' (X86/X86_64 convention, always 64-bit word).
  kDQ,
  //! Describes 'byte' (always 8-bit quantity).
  kByte,
  //! Describes 'half' (most likely 16-bit word).
  kHalf,
  //! Describes 'word' (either 16-bit or 32-bit word).
  kWord,
  //! Describes 'hword' (most likely 16-bit word).
  kHWord,
  //! Describes 'dword' (either 32-bit or 64-bit word).
  kDWord,
  //! Describes 'qword' (64-bit word).
  kQWord,
  //! Describes 'xword' (64-bit word).
  kXWord,
  //! Describes 'short' (always 16-bit word).
  kShort,
  //! Describes 'long' (most likely 32-bit word).
  kLong,
  //! Describes 'quad' (64-bit word).
  kQuad,

  //! Maximum value.
  kMaxValue = kQuad
};

// ============================================================================
// [asmjit::ArchTraits]
// ============================================================================

//! Architecture traits used by Function API and Compiler's register allocator.
struct ArchTraits {
  //! ISA features for each register group.
  enum IsaFeatures : uint32_t {
    //! ISA features a register swap by using a single instruction.
    kIsaFeatureSwap = 0x01u,
    //! ISA features a push/pop like instruction for this register group.
    kIsaFeaturePushPop = 0x02u,
  };

  //! Stack pointer register id.
  uint8_t _spRegId;
  //! Frame pointer register id.
  uint8_t _fpRegId;
  //! Link register id.
  uint8_t _linkRegId;
  //! Instruction pointer (or program counter) register id, if accessible.
  uint8_t _ipRegId;

  // Reserved.
  uint8_t _reserved[3];
  //! Hardware stack alignment requirement.
  uint8_t _hwStackAlignment;

  //! Minimum addressable offset on stack guaranteed for all instructions.
  uint32_t _minStackOffset;
  //! Maximum addressable offset on stack depending on specific instruction.
  uint32_t _maxStackOffset;

  //! Flags for each virtual register group (always covers GP and Vec groups).
  uint8_t _isaFlags[BaseReg::kGroupVirt];

  //! Maps register type into a signature, that provides group, size and can
  //! be used to construct register operands.
  RegInfo _regInfo[BaseReg::kTypeMax + 1];
  //! Maps a register to type-id, see \ref Type::Id.
  uint8_t _regTypeToTypeId[BaseReg::kTypeMax + 1];
  //! Maps base TypeId values (from TypeId::_kIdBaseStart) to register types, see \ref Type::Id.
  uint8_t _typeIdToRegType[32];

  //! Word name identifiers of 8-bit, 16-bit, 32-biit, and 64-bit quantities that appear in formatted text.
  ISAWordNameId _isaWordNameIdTable[4];

  //! Resets all members to zeros.
  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! \name Accessors
  //! \{

  //! Returns stack pointer register id.
  inline constexpr uint32_t spRegId() const noexcept { return _spRegId; }
  //! Returns stack frame register id.
  inline constexpr uint32_t fpRegId() const noexcept { return _fpRegId; }
  //! Returns link register id, if the architecture provides it.
  inline constexpr uint32_t linkRegId() const noexcept { return _linkRegId; }
  //! Returns instruction pointer register id, if the architecture provides it.
  inline constexpr uint32_t ipRegId() const noexcept { return _ipRegId; }

  //! Returns a hardware stack alignment requirement.
  //!
  //! \note This is a hardware constraint. Architectures that don't constrain
  //! it would return the lowest alignment (1), however, some architectures may
  //! constrain the alignment, for example AArch64 requires 16-byte alignment.
  inline constexpr uint32_t hwStackAlignment() const noexcept { return _hwStackAlignment; }

  //! Tests whether the architecture provides link register, which is used across
  //! function calls. If the link register is not provided then a function call
  //! pushes the return address on stack (X86/X64).
  inline constexpr bool hasLinkReg() const noexcept { return _linkRegId != BaseReg::kIdBad; }

  //! Returns minimum addressable offset on stack guaranteed for all instructions.
  inline constexpr uint32_t minStackOffset() const noexcept { return _minStackOffset; }
  //! Returns maximum addressable offset on stack depending on specific instruction.
  inline constexpr uint32_t maxStackOffset() const noexcept { return _maxStackOffset; }

  //! Returns ISA flags of the given register `group`.
  inline constexpr uint32_t isaFlags(uint32_t group) const noexcept { return _isaFlags[group]; }
  //! Tests whether the given register `group` has the given `flag` set.
  inline constexpr bool hasIsaFlag(uint32_t group, uint32_t flag) const noexcept { return (_isaFlags[group] & flag) != 0; }
  //! Tests whether the ISA provides register swap instruction for the given register `group`.
  inline constexpr bool hasSwap(uint32_t group) const noexcept { return hasIsaFlag(group, kIsaFeatureSwap); }
  //! Tests whether the ISA provides push/pop instructions for the given register `group`.
  inline constexpr bool hasPushPop(uint32_t group) const noexcept { return hasIsaFlag(group, kIsaFeaturePushPop); }

  inline uint32_t hasRegType(uint32_t rType) const noexcept {
    return rType <= BaseReg::kTypeMax && _regInfo[rType].signature() != 0;
  }

  inline uint32_t regTypeToSignature(uint32_t rType) const noexcept {
    ASMJIT_ASSERT(rType <= BaseReg::kTypeMax);
    return _regInfo[rType].signature();
  }

  inline uint32_t regTypeToGroup(uint32_t rType) const noexcept {
    ASMJIT_ASSERT(rType <= BaseReg::kTypeMax);
    return _regInfo[rType].group();
  }

  inline uint32_t regTypeToSize(uint32_t rType) const noexcept {
    ASMJIT_ASSERT(rType <= BaseReg::kTypeMax);
    return _regInfo[rType].size();
  }

  inline uint32_t regTypeToTypeId(uint32_t rType) const noexcept {
    ASMJIT_ASSERT(rType <= BaseReg::kTypeMax);
    return _regTypeToTypeId[rType];
  }

  //! Returns a table of ISA word names that appear in formatted text. Word names are ISA dependent.
  //!
  //! The index of this table is log2 of the size:
  //!   - [0] 8-bits
  //!   - [1] 16-bits
  //!   - [2] 32-bits
  //!   - [3] 64-bits
  inline const ISAWordNameId* isaWordNameIdTable() const noexcept { return _isaWordNameIdTable; }

  //! Returns an ISA word name identifier of the given `index`, see \ref isaWordNameIdTable() for more details.
  inline ISAWordNameId isaWordNameId(uint32_t index) const noexcept { return _isaWordNameIdTable[index]; }

  //! \}

  //! \name Statics
  //! \{

  //! Returns a const reference to `ArchTraits` for the given architecture `arch`.
  static inline const ArchTraits& byArch(uint32_t arch) noexcept;

  //! \}
};

ASMJIT_VARAPI const ArchTraits _archTraits[Environment::kArchCount];

inline const ArchTraits& ArchTraits::byArch(uint32_t arch) noexcept { return _archTraits[arch & ~Environment::kArchBigEndianMask]; }

// ============================================================================
// [asmjit::ArchUtils]
// ============================================================================

//! Architecture utilities.
namespace ArchUtils {

ASMJIT_API Error typeIdToRegInfo(uint32_t arch, uint32_t typeId, uint32_t* typeIdOut, RegInfo* regInfo) noexcept;

} // {ArchUtils}

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ARCHTRAITS_H_INCLUDED
