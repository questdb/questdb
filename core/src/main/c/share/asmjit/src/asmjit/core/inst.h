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

#ifndef ASMJIT_CORE_INST_H_INCLUDED
#define ASMJIT_CORE_INST_H_INCLUDED

#include "../core/cpuinfo.h"
#include "../core/operand.h"
#include "../core/string.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_instruction_db
//! \{

// ============================================================================
// [asmjit::BaseInst]
// ============================================================================

//! Instruction id, options, and extraReg in a single structure. This structure
//! exists mainly to simplify analysis and validation API that requires `BaseInst`
//! and `Operand[]` array.
class BaseInst {
public:
  //! Instruction id, see \ref BaseInst::Id or {arch-specific}::Inst::Id.
  uint32_t _id;
  //! Instruction options, see \ref BaseInst::Options or {arch-specific}::Inst::Options.
  uint32_t _options;
  //! Extra register used by instruction (either REP register or AVX-512 selector).
  RegOnly _extraReg;

  enum Id : uint32_t {
    //! Invalid or uninitialized instruction id.
    kIdNone = 0x00000000u,
    //! Abstract instruction (BaseBuilder and BaseCompiler).
    kIdAbstract = 0x80000000u
  };

  enum Options : uint32_t {
    //! Used internally by emitters for handling errors and rare cases.
    kOptionReserved = 0x00000001u,

    //! Prevents following a jump during compilation (BaseCompiler).
    kOptionUnfollow = 0x00000002u,

    //! Overwrite the destination operand(s) (BaseCompiler).
    //!
    //! Hint that is important for register liveness analysis. It tells the
    //! compiler that the destination operand will be overwritten now or by
    //! adjacent instructions. BaseCompiler knows when a register is completely
    //! overwritten by a single instruction, for example you don't have to
    //! mark "movaps" or "pxor x, x", however, if a pair of instructions is
    //! used and the first of them doesn't completely overwrite the content
    //! of the destination, BaseCompiler fails to mark that register as dead.
    //!
    //! X86 Specific
    //! ------------
    //!
    //!   - All instructions that always overwrite at least the size of the
    //!     register the virtual-register uses , for example "mov", "movq",
    //!     "movaps" don't need the overwrite option to be used - conversion,
    //!     shuffle, and other miscellaneous instructions included.
    //!
    //!   - All instructions that clear the destination register if all operands
    //!     are the same, for example "xor x, x", "pcmpeqb x x", etc...
    //!
    //!   - Consecutive instructions that partially overwrite the variable until
    //!     there is no old content require `BaseCompiler::overwrite()` to be used.
    //!     Some examples (not always the best use cases thought):
    //!
    //!     - `movlps xmm0, ?` followed by `movhps xmm0, ?` and vice versa
    //!     - `movlpd xmm0, ?` followed by `movhpd xmm0, ?` and vice versa
    //!     - `mov al, ?` followed by `and ax, 0xFF`
    //!     - `mov al, ?` followed by `mov ah, al`
    //!     - `pinsrq xmm0, ?, 0` followed by `pinsrq xmm0, ?, 1`
    //!
    //!   - If allocated variable is used temporarily for scalar operations. For
    //!     example if you allocate a full vector like `x86::Compiler::newXmm()`
    //!     and then use that vector for scalar operations you should use
    //!     `overwrite()` directive:
    //!
    //!     - `sqrtss x, y` - only LO element of `x` is changed, if you don't
    //!       use HI elements, use `compiler.overwrite().sqrtss(x, y)`.
    kOptionOverwrite = 0x00000004u,

    //! Emit short-form of the instruction.
    kOptionShortForm = 0x00000010u,
    //! Emit long-form of the instruction.
    kOptionLongForm = 0x00000020u,

    //! Conditional jump is likely to be taken.
    kOptionTaken = 0x00000040u,
    //! Conditional jump is unlikely to be taken.
    kOptionNotTaken = 0x00000080u
  };

  //! Control type.
  enum ControlType : uint32_t {
    //! No control type (doesn't jump).
    kControlNone = 0u,
    //! Unconditional jump.
    kControlJump = 1u,
    //! Conditional jump (branch).
    kControlBranch = 2u,
    //! Function call.
    kControlCall = 3u,
    //! Function return.
    kControlReturn = 4u
  };

  //! \name Construction & Destruction
  //! \{

  //! Creates a new BaseInst instance with `id` and `options` set.
  //!
  //! Default values of `id` and `options` are zero, which means none instruciton.
  //! Such instruction is guaranteed to never exist for any architecture supported
  //! by AsmJit.
  inline explicit BaseInst(uint32_t id = 0, uint32_t options = 0) noexcept
    : _id(id),
      _options(options),
      _extraReg() {}

  inline BaseInst(uint32_t id, uint32_t options, const RegOnly& extraReg) noexcept
    : _id(id),
      _options(options),
      _extraReg(extraReg) {}

  inline BaseInst(uint32_t id, uint32_t options, const BaseReg& extraReg) noexcept
    : _id(id),
      _options(options),
      _extraReg { extraReg.signature(), extraReg.id() } {}

  //! \}

  //! \name Instruction ID
  //! \{

  //! Returns the instruction id.
  inline uint32_t id() const noexcept { return _id; }
  //! Sets the instruction id to the given `id`.
  inline void setId(uint32_t id) noexcept { _id = id; }
  //! Resets the instruction id to zero, see \ref kIdNone.
  inline void resetId() noexcept { _id = 0; }

  //! \}

  //! \name Instruction Options
  //! \{

  inline uint32_t options() const noexcept { return _options; }
  inline bool hasOption(uint32_t option) const noexcept { return (_options & option) != 0; }
  inline void setOptions(uint32_t options) noexcept { _options = options; }
  inline void addOptions(uint32_t options) noexcept { _options |= options; }
  inline void clearOptions(uint32_t options) noexcept { _options &= ~options; }
  inline void resetOptions() noexcept { _options = 0; }

  //! \}

  //! \name Extra Register
  //! \{

  inline bool hasExtraReg() const noexcept { return _extraReg.isReg(); }
  inline RegOnly& extraReg() noexcept { return _extraReg; }
  inline const RegOnly& extraReg() const noexcept { return _extraReg; }
  inline void setExtraReg(const BaseReg& reg) noexcept { _extraReg.init(reg); }
  inline void setExtraReg(const RegOnly& reg) noexcept { _extraReg.init(reg); }
  inline void resetExtraReg() noexcept { _extraReg.reset(); }

  //! \}
};

// ============================================================================
// [asmjit::OpRWInfo]
// ============================================================================

//! Read/Write information related to a single operand, used by \ref InstRWInfo.
struct OpRWInfo {
  //! Read/Write flags, see \ref OpRWInfo::Flags.
  uint32_t _opFlags;
  //! Physical register index, if required.
  uint8_t _physId;
  //! Size of a possible memory operand that can replace a register operand.
  uint8_t _rmSize;
  //! Reserved for future use.
  uint8_t _reserved[2];
  //! Read bit-mask where each bit represents one byte read from Reg/Mem.
  uint64_t _readByteMask;
  //! Write bit-mask where each bit represents one byte written to Reg/Mem.
  uint64_t _writeByteMask;
  //! Zero/Sign extend bit-mask where each bit represents one byte written to Reg/Mem.
  uint64_t _extendByteMask;

  //! Flags describe how the operand is accessed and some additional information.
  enum Flags : uint32_t {
    //! Operand is read.
    kRead = 0x00000001u,

    //! Operand is written.
    kWrite = 0x00000002u,

    //! Operand is both read and written.
    kRW = 0x00000003u,

    //! Register operand can be replaced by a memory operand.
    kRegMem = 0x00000004u,

    //! The `extendByteMask()` represents a zero extension.
    kZExt = 0x00000010u,

    //! Register operand must use \ref physId().
    kRegPhysId = 0x00000100u,
    //! Base register of a memory operand must use \ref physId().
    kMemPhysId = 0x00000200u,

    //! This memory operand is only used to encode registers and doesn't access memory.
    //!
    //! X86 Specific
    //! ------------
    //!
    //! Instructions that use such feature include BNDLDX, BNDSTX, and LEA.
    kMemFake = 0x000000400u,

    //! Base register of the memory operand will be read.
    kMemBaseRead = 0x00001000u,
    //! Base register of the memory operand will be written.
    kMemBaseWrite = 0x00002000u,
    //! Base register of the memory operand will be read & written.
    kMemBaseRW = 0x00003000u,

    //! Index register of the memory operand will be read.
    kMemIndexRead = 0x00004000u,
    //! Index register of the memory operand will be written.
    kMemIndexWrite = 0x00008000u,
    //! Index register of the memory operand will be read & written.
    kMemIndexRW = 0x0000C000u,

    //! Base register of the memory operand will be modified before the operation.
    kMemBasePreModify = 0x00010000u,
    //! Base register of the memory operand will be modified after the operation.
    kMemBasePostModify = 0x00020000u
  };

  // Don't remove these asserts. Read/Write flags are used extensively
  // by Compiler and they must always be compatible with constants below.
  static_assert(kRead  == 0x1, "OpRWInfo::kRead flag must be 0x1");
  static_assert(kWrite == 0x2, "OpRWInfo::kWrite flag must be 0x2");
  static_assert(kRegMem == 0x4, "OpRWInfo::kRegMem flag must be 0x4");

  //! \name Reset
  //! \{

  //! Resets this operand information to all zeros.
  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! Resets this operand info (resets all members) and set common information
  //! to the given `opFlags`, `regSize`, and possibly `physId`.
  inline void reset(uint32_t opFlags, uint32_t regSize, uint32_t physId = BaseReg::kIdBad) noexcept {
    _opFlags = opFlags;
    _physId = uint8_t(physId);
    _rmSize = uint8_t((opFlags & kRegMem) ? regSize : uint32_t(0));
    _resetReserved();

    uint64_t mask = Support::lsbMask<uint64_t>(regSize);
    _readByteMask = opFlags & kRead ? mask : uint64_t(0);
    _writeByteMask = opFlags & kWrite ? mask : uint64_t(0);
    _extendByteMask = 0;
  }

  inline void _resetReserved() noexcept {
    memset(_reserved, 0, sizeof(_reserved));
  }

  //! \}

  //! \name Operand Flags
  //! \{

  //! Returns operand flags, see \ref Flags.
  inline uint32_t opFlags() const noexcept { return _opFlags; }
  //! Tests whether operand flags contain the given `flag`.
  inline bool hasOpFlag(uint32_t flag) const noexcept { return (_opFlags & flag) != 0; }

  //! Adds the given `flags` to operand flags.
  inline void addOpFlags(uint32_t flags) noexcept { _opFlags |= flags; }
  //! Removes the given `flags` from operand flags.
  inline void clearOpFlags(uint32_t flags) noexcept { _opFlags &= ~flags; }

  //! Tests whether this operand is read from.
  inline bool isRead() const noexcept { return hasOpFlag(kRead); }
  //! Tests whether this operand is written to.
  inline bool isWrite() const noexcept { return hasOpFlag(kWrite); }
  //! Tests whether this operand is both read and write.
  inline bool isReadWrite() const noexcept { return (_opFlags & kRW) == kRW; }
  //! Tests whether this operand is read only.
  inline bool isReadOnly() const noexcept { return (_opFlags & kRW) == kRead; }
  //! Tests whether this operand is write only.
  inline bool isWriteOnly() const noexcept { return (_opFlags & kRW) == kWrite; }

  //! Tests whether this operand is Reg/Mem
  //!
  //! Reg/Mem operands can use either register or memory.
  inline bool isRm() const noexcept { return hasOpFlag(kRegMem); }

  //! Tests whether the operand will be zero extended.
  inline bool isZExt() const noexcept { return hasOpFlag(kZExt); }

  //! \}

  //! \name Memory Flags
  //! \{

  //! Tests whether this is a fake memory operand, which is only used, because
  //! of encoding. Fake memory operands do not access any memory, they are only
  //! used to encode registers.
  inline bool isMemFake() const noexcept { return hasOpFlag(kMemFake); }

  //! Tests whether the instruction's memory BASE register is used.
  inline bool isMemBaseUsed() const noexcept { return (_opFlags & kMemBaseRW) != 0; }
  //! Tests whether the instruction reads from its BASE registers.
  inline bool isMemBaseRead() const noexcept { return hasOpFlag(kMemBaseRead); }
  //! Tests whether the instruction writes to its BASE registers.
  inline bool isMemBaseWrite() const noexcept { return hasOpFlag(kMemBaseWrite); }
  //! Tests whether the instruction reads and writes from/to its BASE registers.
  inline bool isMemBaseReadWrite() const noexcept { return (_opFlags & kMemBaseRW) == kMemBaseRW; }
  //! Tests whether the instruction only reads from its BASE registers.
  inline bool isMemBaseReadOnly() const noexcept { return (_opFlags & kMemBaseRW) == kMemBaseRead; }
  //! Tests whether the instruction only writes to its BASE registers.
  inline bool isMemBaseWriteOnly() const noexcept { return (_opFlags & kMemBaseRW) == kMemBaseWrite; }

  //! Tests whether the instruction modifies the BASE register before it uses
  //! it to calculate the target address.
  inline bool isMemBasePreModify() const noexcept { return hasOpFlag(kMemBasePreModify); }
  //! Tests whether the instruction modifies the BASE register after it uses
  //! it to calculate the target address.
  inline bool isMemBasePostModify() const noexcept { return hasOpFlag(kMemBasePostModify); }

  //! Tests whether the instruction's memory INDEX register is used.
  inline bool isMemIndexUsed() const noexcept { return (_opFlags & kMemIndexRW) != 0; }
  //! Tests whether the instruction reads the INDEX registers.
  inline bool isMemIndexRead() const noexcept { return hasOpFlag(kMemIndexRead); }
  //! Tests whether the instruction writes to its INDEX registers.
  inline bool isMemIndexWrite() const noexcept { return hasOpFlag(kMemIndexWrite); }
  //! Tests whether the instruction reads and writes from/to its INDEX registers.
  inline bool isMemIndexReadWrite() const noexcept { return (_opFlags & kMemIndexRW) == kMemIndexRW; }
  //! Tests whether the instruction only reads from its INDEX registers.
  inline bool isMemIndexReadOnly() const noexcept { return (_opFlags & kMemIndexRW) == kMemIndexRead; }
  //! Tests whether the instruction only writes to its INDEX registers.
  inline bool isMemIndexWriteOnly() const noexcept { return (_opFlags & kMemIndexRW) == kMemIndexWrite; }

  //! \}

  //! \name Physical Register ID
  //! \{

  //! Returns a physical id of the register that is fixed for this operand.
  //!
  //! Returns \ref BaseReg::kIdBad if any register can be used.
  inline uint32_t physId() const noexcept { return _physId; }
  //! Tests whether \ref physId() would return a valid physical register id.
  inline bool hasPhysId() const noexcept { return _physId != BaseReg::kIdBad; }
  //! Sets physical register id, which would be fixed for this operand.
  inline void setPhysId(uint32_t physId) noexcept { _physId = uint8_t(physId); }

  //! \}

  //! \name Reg/Mem Information
  //! \{

  //! Returns Reg/Mem size of the operand.
  inline uint32_t rmSize() const noexcept { return _rmSize; }
  //! Sets Reg/Mem size of the operand.
  inline void setRmSize(uint32_t rmSize) noexcept { _rmSize = uint8_t(rmSize); }

  //! \}

  //! \name Read & Write Masks
  //! \{

  //! Returns read mask.
  inline uint64_t readByteMask() const noexcept { return _readByteMask; }
  //! Returns write mask.
  inline uint64_t writeByteMask() const noexcept { return _writeByteMask; }
  //! Returns extend mask.
  inline uint64_t extendByteMask() const noexcept { return _extendByteMask; }

  //! Sets read mask.
  inline void setReadByteMask(uint64_t mask) noexcept { _readByteMask = mask; }
  //! Sets write mask.
  inline void setWriteByteMask(uint64_t mask) noexcept { _writeByteMask = mask; }
  //! Sets externd mask.
  inline void setExtendByteMask(uint64_t mask) noexcept { _extendByteMask = mask; }

  //! \}
};

// ============================================================================
// [asmjit::InstRWInfo]
// ============================================================================

//! Read/Write information of an instruction.
struct InstRWInfo {
  //! Instruction flags (there are no flags at the moment, this field is reserved).
  uint32_t _instFlags;
  //! Mask of CPU flags read.
  uint32_t _readFlags;
  //! Mask of CPU flags written.
  uint32_t _writeFlags;
  //! Count of operands.
  uint8_t _opCount;
  //! CPU feature required for replacing register operand with memory operand.
  uint8_t _rmFeature;
  //! Reserved for future use.
  uint8_t _reserved[18];
  //! Read/Write onfo of extra register (rep{} or kz{}).
  OpRWInfo _extraReg;
  //! Read/Write info of instruction operands.
  OpRWInfo _operands[Globals::kMaxOpCount];

  //! \name Commons
  //! \{

  //! Resets this RW information to all zeros.
  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! \}

  //! \name Instruction Flags
  //!
  //! \{

  inline uint32_t instFlags() const noexcept { return _instFlags; }
  inline bool hasInstFlag(uint32_t flag) const noexcept { return (_instFlags & flag) != 0; }

  //! }

  //! \name CPU Flags Read/Write Information
  //! \{

  //! Returns read flags of the instruction.
  inline uint32_t readFlags() const noexcept { return _readFlags; }
  //! Returns write flags of the instruction.
  inline uint32_t writeFlags() const noexcept { return _writeFlags; }

  //! \}

  //! \name Reg/Mem Information
  //! \{

  //! Returns the CPU feature required to replace a register operand with memory
  //! operand. If the returned feature is zero (none) then this instruction
  //! either doesn't provide memory operand combination or there is no extra
  //! CPU feature required.
  //!
  //! X86 Specific
  //! ------------
  //!
  //! Some AVX+ instructions may require extra features for replacing registers
  //! with memory operands, for example VPSLLDQ instruction only supports
  //! 'reg/reg/imm' combination on AVX/AVX2 capable CPUs and requires AVX-512 for
  //! 'reg/mem/imm' combination.
  inline uint32_t rmFeature() const noexcept { return _rmFeature; }

  //! \}

  //! \name Operand Read/Write Information
  //! \{

  //! Returns RW information of extra register operand (extraReg).
  inline const OpRWInfo& extraReg() const noexcept { return _extraReg; }

  //! Returns RW information of all instruction's operands.
  inline const OpRWInfo* operands() const noexcept { return _operands; }

  //! Returns RW information of the operand at the given `index`.
  inline const OpRWInfo& operand(size_t index) const noexcept {
    ASMJIT_ASSERT(index < Globals::kMaxOpCount);
    return _operands[index];
  }

  //! Returns the number of operands this instruction has.
  inline uint32_t opCount() const noexcept { return _opCount; }

  //! \}
};

// ============================================================================
// [asmjit::InstAPI]
// ============================================================================

//! Instruction API.
namespace InstAPI {

//! Validation flags that can be used with \ref InstAPI::validate().
enum ValidationFlags : uint32_t {
  //! Allow virtual registers in the instruction.
  kValidationFlagVirtRegs = 0x01u
};

#ifndef ASMJIT_NO_TEXT
//! Appends the name of the instruction specified by `instId` and `instOptions`
//! into the `output` string.
//!
//! \note Instruction options would only affect instruction prefix & suffix,
//! other options would be ignored. If `instOptions` is zero then only raw
//! instruction name (without any additional text) will be appended.
ASMJIT_API Error instIdToString(uint32_t arch, uint32_t instId, String& output) noexcept;

//! Parses an instruction name in the given string `s`. Length is specified
//! by `len` argument, which can be `SIZE_MAX` if `s` is known to be null
//! terminated.
//!
//! Returns the parsed instruction id or \ref BaseInst::kIdNone if no such
//! instruction exists.
ASMJIT_API uint32_t stringToInstId(uint32_t arch, const char* s, size_t len) noexcept;
#endif // !ASMJIT_NO_TEXT

#ifndef ASMJIT_NO_VALIDATION
//! Validates the given instruction considering the validation `flags`, see
//! \ref ValidationFlags.
ASMJIT_API Error validate(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, uint32_t validationFlags = 0) noexcept;
#endif // !ASMJIT_NO_VALIDATION

#ifndef ASMJIT_NO_INTROSPECTION
//! Gets Read/Write information of the given instruction.
ASMJIT_API Error queryRWInfo(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, InstRWInfo* out) noexcept;

//! Gets CPU features required by the given instruction.
ASMJIT_API Error queryFeatures(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, BaseFeatures* out) noexcept;
#endif // !ASMJIT_NO_INTROSPECTION

} // {InstAPI}

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_INST_H_INCLUDED
