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

#ifndef ASMJIT_X86_X86INSTDB_H_INCLUDED
#define ASMJIT_X86_X86INSTDB_H_INCLUDED

#include "../x86/x86globals.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \addtogroup asmjit_x86
//! \{

//! Instruction database (X86).
namespace InstDB {

// ============================================================================
// [asmjit::x86::InstDB::Mode]
// ============================================================================

//! Describes which mode is supported by an instruction or instruction signature.
enum Mode : uint32_t {
  //! Invalid mode.
  kModeNone = 0x00u,
  //! X86 mode supported.
  kModeX86 = 0x01u,
  //! X64 mode supported.
  kModeX64 = 0x02u,
  //! Both X86 and X64 modes supported.
  kModeAny = 0x03u
};

static constexpr uint32_t modeFromArch(uint32_t arch) noexcept {
  return arch == Environment::kArchX86 ? kModeX86 :
         arch == Environment::kArchX64 ? kModeX64 : kModeNone;
}

// ============================================================================
// [asmjit::x86::InstDB::OpFlags]
// ============================================================================

//! Operand flags (X86).
enum OpFlags : uint32_t {
  kOpNone                  = 0x00000000u, //!< No flags.

  kOpGpbLo                 = 0x00000001u, //!< Operand can be low 8-bit GPB register.
  kOpGpbHi                 = 0x00000002u, //!< Operand can be high 8-bit GPB register.
  kOpGpw                   = 0x00000004u, //!< Operand can be 16-bit GPW register.
  kOpGpd                   = 0x00000008u, //!< Operand can be 32-bit GPD register.
  kOpGpq                   = 0x00000010u, //!< Operand can be 64-bit GPQ register.
  kOpXmm                   = 0x00000020u, //!< Operand can be 128-bit XMM register.
  kOpYmm                   = 0x00000040u, //!< Operand can be 256-bit YMM register.
  kOpZmm                   = 0x00000080u, //!< Operand can be 512-bit ZMM register.
  kOpMm                    = 0x00000100u, //!< Operand can be 64-bit MM register.
  kOpKReg                  = 0x00000200u, //!< Operand can be 64-bit K register.
  kOpSReg                  = 0x00000400u, //!< Operand can be SReg (segment register).
  kOpCReg                  = 0x00000800u, //!< Operand can be CReg (control register).
  kOpDReg                  = 0x00001000u, //!< Operand can be DReg (debug register).
  kOpSt                    = 0x00002000u, //!< Operand can be 80-bit ST register (X87).
  kOpBnd                   = 0x00004000u, //!< Operand can be 128-bit BND register.
  kOpTmm                   = 0x00008000u, //!< Operand can be 0..8192-bit TMM register.
  kOpAllRegs               = 0x0000FFFFu, //!< Combination of all possible registers.

  kOpI4                    = 0x00010000u, //!< Operand can be unsigned 4-bit immediate.
  kOpU4                    = 0x00020000u, //!< Operand can be unsigned 4-bit immediate.
  kOpI8                    = 0x00040000u, //!< Operand can be signed 8-bit immediate.
  kOpU8                    = 0x00080000u, //!< Operand can be unsigned 8-bit immediate.
  kOpI16                   = 0x00100000u, //!< Operand can be signed 16-bit immediate.
  kOpU16                   = 0x00200000u, //!< Operand can be unsigned 16-bit immediate.
  kOpI32                   = 0x00400000u, //!< Operand can be signed 32-bit immediate.
  kOpU32                   = 0x00800000u, //!< Operand can be unsigned 32-bit immediate.
  kOpI64                   = 0x01000000u, //!< Operand can be signed 64-bit immediate.
  kOpU64                   = 0x02000000u, //!< Operand can be unsigned 64-bit immediate.
  kOpAllImm                = 0x03FF0000u, //!< Operand can be any immediate.

  kOpMem                   = 0x04000000u, //!< Operand can be a scalar memory pointer.
  kOpVm                    = 0x08000000u, //!< Operand can be a vector memory pointer.

  kOpRel8                  = 0x10000000u, //!< Operand can be relative 8-bit  displacement.
  kOpRel32                 = 0x20000000u, //!< Operand can be relative 32-bit displacement.

  kOpImplicit              = 0x80000000u  //!< Operand is implicit.
};

// ============================================================================
// [asmjit::x86::InstDB::MemFlags]
// ============================================================================

//! Memory operand flags (X86).
enum MemFlags : uint32_t {
  // NOTE: Instruction uses either scalar or vector memory operands, they never
  // collide. This allows us to share bits between "M" and "Vm" enums.

  kMemOpAny                = 0x0001u,     //!< Operand can be any scalar memory pointer.
  kMemOpM8                 = 0x0002u,     //!< Operand can be an 8-bit memory pointer.
  kMemOpM16                = 0x0004u,     //!< Operand can be a 16-bit memory pointer.
  kMemOpM32                = 0x0008u,     //!< Operand can be a 32-bit memory pointer.
  kMemOpM48                = 0x0010u,     //!< Operand can be a 48-bit memory pointer (FAR pointers only).
  kMemOpM64                = 0x0020u,     //!< Operand can be a 64-bit memory pointer.
  kMemOpM80                = 0x0040u,     //!< Operand can be an 80-bit memory pointer.
  kMemOpM128               = 0x0080u,     //!< Operand can be a 128-bit memory pointer.
  kMemOpM256               = 0x0100u,     //!< Operand can be a 256-bit memory pointer.
  kMemOpM512               = 0x0200u,     //!< Operand can be a 512-bit memory pointer.
  kMemOpM1024              = 0x0400u,     //!< Operand can be a 1024-bit memory pointer.

  kMemOpVm32x              = 0x0002u,     //!< Operand can be a vm32x (vector) pointer.
  kMemOpVm32y              = 0x0004u,     //!< Operand can be a vm32y (vector) pointer.
  kMemOpVm32z              = 0x0008u,     //!< Operand can be a vm32z (vector) pointer.
  kMemOpVm64x              = 0x0020u,     //!< Operand can be a vm64x (vector) pointer.
  kMemOpVm64y              = 0x0040u,     //!< Operand can be a vm64y (vector) pointer.
  kMemOpVm64z              = 0x0080u,     //!< Operand can be a vm64z (vector) pointer.

  kMemOpBaseOnly           = 0x0800u,     //!< Only memory base is allowed (no index, no offset).
  kMemOpDs                 = 0x1000u,     //!< Implicit memory operand's DS segment.
  kMemOpEs                 = 0x2000u,     //!< Implicit memory operand's ES segment.

  kMemOpMib                = 0x4000u,     //!< Operand must be MIB (base+index) pointer.
  kMemOpTMem               = 0x8000u      //!< Operand is a sib_mem (ADX memory operand).
};

// ============================================================================
// [asmjit::x86::InstDB::Flags]
// ============================================================================

//! Instruction flags (X86).
//!
//! Details about instruction encoding, operation, features, and some limitations.
enum Flags : uint32_t {
  kFlagNone                = 0x00000000u, //!< No flags.

  // Instruction Family
  // ------------------
  //
  // Instruction family information.

  kFlagFpu                 = 0x00000100u, //!< Instruction that accesses FPU registers.
  kFlagMmx                 = 0x00000200u, //!< Instruction that accesses MMX registers (including 3DNOW and GEODE) and EMMS.
  kFlagVec                 = 0x00000400u, //!< Instruction that accesses XMM registers (SSE, AVX, AVX512).

  // FPU Flags
  // ---------
  //
  // Used to tell the encoder which memory operand sizes are encodable.

  kFlagFpuM16              = 0x00000800u, //!< FPU instruction can address `word_ptr` (shared with M80).
  kFlagFpuM32              = 0x00001000u, //!< FPU instruction can address `dword_ptr`.
  kFlagFpuM64              = 0x00002000u, //!< FPU instruction can address `qword_ptr`.
  kFlagFpuM80              = 0x00000800u, //!< FPU instruction can address `tword_ptr` (shared with M16).

  // Prefixes and Encoding Flags
  // ---------------------------
  //
  // These describe optional X86 prefixes that can be used to change the instruction's operation.

  kFlagTsib                = 0x00004000u, //!< Instruction uses TSIB (or SIB_MEM) encoding (MODRM followed by SIB).
  kFlagRep                 = 0x00008000u, //!< Instruction can be prefixed with using the REP(REPE) or REPNE prefix.
  kFlagRepIgnored          = 0x00010000u, //!< Instruction ignores REP|REPNE prefixes, but they are accepted.
  kFlagLock                = 0x00020000u, //!< Instruction can be prefixed with using the LOCK prefix.
  kFlagXAcquire            = 0x00040000u, //!< Instruction can be prefixed with using the XACQUIRE prefix.
  kFlagXRelease            = 0x00080000u, //!< Instruction can be prefixed with using the XRELEASE prefix.
  kFlagMib                 = 0x00100000u, //!< Instruction uses MIB (BNDLDX|BNDSTX) to encode two registers.
  kFlagVsib                = 0x00200000u, //!< Instruction uses VSIB instead of legacy SIB.

  // If both `kFlagPrefixVex` and `kFlagPrefixEvex` flags are specified it
  // means that the instructions can be encoded by either VEX or EVEX prefix.
  // In that case AsmJit checks global options and also instruction options
  // to decide whether to emit VEX or EVEX prefix.

  kFlagVex                 = 0x00400000u, //!< Instruction can be encoded by VEX|XOP (AVX|AVX2|BMI|XOP|...).
  kFlagEvex                = 0x00800000u, //!< Instruction can be encoded by EVEX (AVX512).
  kFlagPreferEvex          = 0x01000000u, //!< EVEX encoding is preferred over VEX encoding (AVX515_VNNI vs AVX_VNNI).
  kFlagEvexCompat          = 0x02000000u, //!< EVEX and VEX signatures are compatible.
  kFlagEvexKReg            = 0x04000000u, //!< EVEX instruction requires K register in the first operand (compare instructions).
  kFlagEvexTwoOp           = 0x08000000u, //!< EVEX instruction requires two operands and K register as a selector (gather instructions).
  kFlagEvexTransformable   = 0x10000000u  //!< VEX instruction that can be transformed to a compatible EVEX instruction.
};

// ============================================================================
// [asmjit::x86::InstDB::Avx512Flags]
// ============================================================================

//! AVX512 flags.
enum Avx512Flags : uint32_t {
  kAvx512Flag_             = 0x00000000u, //!< Internally used in tables, has no meaning.
  kAvx512FlagK             = 0x00000001u, //!< Supports masking {k1..k7}.
  kAvx512FlagZ             = 0x00000002u, //!< Supports zeroing {z}, must be used together with `kAvx512k`.
  kAvx512FlagER            = 0x00000004u, //!< Supports 'embedded-rounding' {er} with implicit {sae},
  kAvx512FlagSAE           = 0x00000008u, //!< Supports 'suppress-all-exceptions' {sae}.
  kAvx512FlagB32           = 0x00000010u, //!< Supports 32-bit broadcast 'b32'.
  kAvx512FlagB64           = 0x00000020u, //!< Supports 64-bit broadcast 'b64'.
  kAvx512FlagT4X           = 0x00000080u  //!< Operates on a vector of consecutive registers (AVX512_4FMAPS and AVX512_4VNNIW).
};

// ============================================================================
// [asmjit::x86::InstDB::SingleRegCase]
// ============================================================================

enum SingleRegCase : uint32_t {
  //! No special handling.
  kSingleRegNone = 0,
  //! Operands become read-only  - `REG & REG` and similar.
  kSingleRegRO = 1,
  //! Operands become write-only - `REG ^ REG` and similar.
  kSingleRegWO = 2
};

// ============================================================================
// [asmjit::x86::InstDB::InstSignature / OpSignature]
// ============================================================================

//! Operand signature (X86).
//!
//! Contains all possible operand combinations, memory size information, and
//! a fixed register id (or `BaseReg::kIdBad` if fixed id isn't required).
struct OpSignature {
  //! Operand flags.
  uint32_t opFlags;
  //! Memory flags.
  uint16_t memFlags;
  //! Extra flags.
  uint8_t extFlags;
  //! Mask of possible register IDs.
  uint8_t regMask;
};

ASMJIT_VARAPI const OpSignature _opSignatureTable[];

//! Instruction signature (X86).
//!
//! Contains a sequence of operands' combinations and other metadata that defines
//! a single instruction. This data is used by instruction validator.
struct InstSignature {
  //! Count of operands in `opIndex` (0..6).
  uint8_t opCount : 3;
  //! Architecture modes supported (X86 / X64).
  uint8_t modes : 2;
  //! Number of implicit operands.
  uint8_t implicit : 3;
  //! Reserved for future use.
  uint8_t reserved;
  //! Indexes to `OpSignature` table.
  uint8_t operands[Globals::kMaxOpCount];
};

ASMJIT_VARAPI const InstSignature _instSignatureTable[];

// ============================================================================
// [asmjit::x86::InstDB::CommonInfo]
// ============================================================================

//! Instruction common information (X86)
//!
//! Aggregated information shared across one or more instruction.
struct CommonInfo {
  //! Instruction flags.
  uint32_t _flags;
  //! Reserved for future use.
  uint32_t _avx512Flags : 11;
  //! First `InstSignature` entry in the database.
  uint32_t _iSignatureIndex : 11;
  //! Number of relevant `ISignature` entries.
  uint32_t _iSignatureCount : 5;
  //! Control type, see `ControlType`.
  uint32_t _controlType : 3;
  //! Specifies what happens if all source operands share the same register.
  uint32_t _singleRegCase : 2;

  // --------------------------------------------------------------------------
  // [Accessors]
  // --------------------------------------------------------------------------

  //! Returns instruction flags, see \ref Flags.
  inline uint32_t flags() const noexcept { return _flags; }
  //! Tests whether the instruction has a `flag`, see \ref Flags.
  inline bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }

  //! Returns instruction AVX-512 flags, see \ref Avx512Flags.
  inline uint32_t avx512Flags() const noexcept { return _avx512Flags; }
  //! Tests whether the instruction has an AVX-512 `flag`, see \ref Avx512Flags.
  inline bool hasAvx512Flag(uint32_t flag) const noexcept { return (_avx512Flags & flag) != 0; }

  //! Tests whether the instruction is FPU instruction.
  inline bool isFpu() const noexcept { return hasFlag(kFlagFpu); }
  //! Tests whether the instruction is MMX/3DNOW instruction that accesses MMX registers (includes EMMS and FEMMS).
  inline bool isMmx() const noexcept { return hasFlag(kFlagMmx); }
  //! Tests whether the instruction is SSE|AVX|AVX512 instruction that accesses XMM|YMM|ZMM registers.
  inline bool isVec() const noexcept { return hasFlag(kFlagVec); }
  //! Tests whether the instruction is SSE+ (SSE4.2, AES, SHA included) instruction that accesses XMM registers.
  inline bool isSse() const noexcept { return (flags() & (kFlagVec | kFlagVex | kFlagEvex)) == kFlagVec; }
  //! Tests whether the instruction is AVX+ (FMA included) instruction that accesses XMM|YMM|ZMM registers.
  inline bool isAvx() const noexcept { return isVec() && isVexOrEvex(); }

  //! Tests whether the instruction can be prefixed with LOCK prefix.
  inline bool hasLockPrefix() const noexcept { return hasFlag(kFlagLock); }
  //! Tests whether the instruction can be prefixed with REP (REPE|REPZ) prefix.
  inline bool hasRepPrefix() const noexcept { return hasFlag(kFlagRep); }
  //! Tests whether the instruction can be prefixed with XACQUIRE prefix.
  inline bool hasXAcquirePrefix() const noexcept { return hasFlag(kFlagXAcquire); }
  //! Tests whether the instruction can be prefixed with XRELEASE prefix.
  inline bool hasXReleasePrefix() const noexcept { return hasFlag(kFlagXRelease); }

  //! Tests whether the rep prefix is supported by the instruction, but ignored (has no effect).
  inline bool isRepIgnored() const noexcept { return hasFlag(kFlagRepIgnored); }
  //! Tests whether the instruction uses MIB.
  inline bool isMibOp() const noexcept { return hasFlag(kFlagMib); }
  //! Tests whether the instruction uses VSIB.
  inline bool isVsibOp() const noexcept { return hasFlag(kFlagVsib); }
  //! Tests whether the instruction uses TSIB (AMX, instruction requires MOD+SIB).
  inline bool isTsibOp() const noexcept { return hasFlag(kFlagTsib); }
  //! Tests whether the instruction uses VEX (can be set together with EVEX if both are encodable).
  inline bool isVex() const noexcept { return hasFlag(kFlagVex); }
  //! Tests whether the instruction uses EVEX (can be set together with VEX if both are encodable).
  inline bool isEvex() const noexcept { return hasFlag(kFlagEvex); }
  //! Tests whether the instruction uses EVEX (can be set together with VEX if both are encodable).
  inline bool isVexOrEvex() const noexcept { return hasFlag(kFlagVex | kFlagEvex); }

  //! Tests whether the instruction should prefer EVEX prefix instead of VEX prefix.
  inline bool preferEvex() const noexcept { return hasFlag(kFlagPreferEvex); }

  inline bool isEvexCompatible() const noexcept { return hasFlag(kFlagEvexCompat); }
  inline bool isEvexKRegOnly() const noexcept { return hasFlag(kFlagEvexKReg); }
  inline bool isEvexTwoOpOnly() const noexcept { return hasFlag(kFlagEvexTwoOp); }
  inline bool isEvexTransformable() const noexcept { return hasFlag(kFlagEvexTransformable); }

  //! Tests whether the instruction supports AVX512 masking {k}.
  inline bool hasAvx512K() const noexcept { return hasAvx512Flag(kAvx512FlagK); }
  //! Tests whether the instruction supports AVX512 zeroing {k}{z}.
  inline bool hasAvx512Z() const noexcept { return hasAvx512Flag(kAvx512FlagZ); }
  //! Tests whether the instruction supports AVX512 embedded-rounding {er}.
  inline bool hasAvx512ER() const noexcept { return hasAvx512Flag(kAvx512FlagER); }
  //! Tests whether the instruction supports AVX512 suppress-all-exceptions {sae}.
  inline bool hasAvx512SAE() const noexcept { return hasAvx512Flag(kAvx512FlagSAE); }
  //! Tests whether the instruction supports AVX512 broadcast (either 32-bit or 64-bit).
  inline bool hasAvx512B() const noexcept { return hasAvx512Flag(kAvx512FlagB32 | kAvx512FlagB64); }
  //! Tests whether the instruction supports AVX512 broadcast (32-bit).
  inline bool hasAvx512B32() const noexcept { return hasAvx512Flag(kAvx512FlagB32); }
  //! Tests whether the instruction supports AVX512 broadcast (64-bit).
  inline bool hasAvx512B64() const noexcept { return hasAvx512Flag(kAvx512FlagB64); }

  inline uint32_t signatureIndex() const noexcept { return _iSignatureIndex; }
  inline uint32_t signatureCount() const noexcept { return _iSignatureCount; }

  inline const InstSignature* signatureData() const noexcept { return _instSignatureTable + _iSignatureIndex; }
  inline const InstSignature* signatureEnd() const noexcept { return _instSignatureTable + _iSignatureIndex + _iSignatureCount; }

  //! Returns the control-flow type of the instruction.
  inline uint32_t controlType() const noexcept { return _controlType; }

  inline uint32_t singleRegCase() const noexcept { return _singleRegCase; }
};

ASMJIT_VARAPI const CommonInfo _commonInfoTable[];

// ============================================================================
// [asmjit::x86::InstDB::InstInfo]
// ============================================================================

//! Instruction information (X86).
struct InstInfo {
  //! Index to \ref _nameData.
  uint32_t _nameDataIndex : 14;
  //! Index to \ref _commonInfoTable.
  uint32_t _commonInfoIndex : 10;
  //! Index to \ref _commonInfoTableB.
  uint32_t _commonInfoIndexB : 8;

  //! Instruction encoding (internal encoding identifier used by \ref Assembler).
  uint8_t _encoding;
  //! Main opcode value (0..255).
  uint8_t _mainOpcodeValue;
  //! Index to \ref _mainOpcodeTable` that is combined with \ref _mainOpcodeValue
  //! to form the final opcode.
  uint8_t _mainOpcodeIndex;
  //! Index to \ref _altOpcodeTable that contains a full alternative opcode.
  uint8_t _altOpcodeIndex;

  // --------------------------------------------------------------------------
  // [Accessors]
  // --------------------------------------------------------------------------

  //! Returns common information, see `CommonInfo`.
  inline const CommonInfo& commonInfo() const noexcept { return _commonInfoTable[_commonInfoIndex]; }

  //! Returns instruction flags, see \ref Flags.
  inline uint32_t flags() const noexcept { return commonInfo().flags(); }
  //! Tests whether the instruction has flag `flag`, see \ref Flags.
  inline bool hasFlag(uint32_t flag) const noexcept { return commonInfo().hasFlag(flag); }

  //! Returns instruction AVX-512 flags, see \ref Avx512Flags.
  inline uint32_t avx512Flags() const noexcept { return commonInfo().avx512Flags(); }
  //! Tests whether the instruction has an AVX-512 `flag`, see \ref Avx512Flags.
  inline bool hasAvx512Flag(uint32_t flag) const noexcept { return commonInfo().hasAvx512Flag(flag); }

  //! Tests whether the instruction is FPU instruction.
  inline bool isFpu() const noexcept { return commonInfo().isFpu(); }
  //! Tests whether the instruction is MMX/3DNOW instruction that accesses MMX registers (includes EMMS and FEMMS).
  inline bool isMmx() const noexcept { return commonInfo().isMmx(); }
  //! Tests whether the instruction is SSE|AVX|AVX512 instruction that accesses XMM|YMM|ZMM registers.
  inline bool isVec() const noexcept { return commonInfo().isVec(); }
  //! Tests whether the instruction is SSE+ (SSE4.2, AES, SHA included) instruction that accesses XMM registers.
  inline bool isSse() const noexcept { return commonInfo().isSse(); }
  //! Tests whether the instruction is AVX+ (FMA included) instruction that accesses XMM|YMM|ZMM registers.
  inline bool isAvx() const noexcept { return commonInfo().isAvx(); }

  //! Tests whether the instruction can be prefixed with LOCK prefix.
  inline bool hasLockPrefix() const noexcept { return commonInfo().hasLockPrefix(); }
  //! Tests whether the instruction can be prefixed with REP (REPE|REPZ) prefix.
  inline bool hasRepPrefix() const noexcept { return commonInfo().hasRepPrefix(); }
  //! Tests whether the instruction can be prefixed with XACQUIRE prefix.
  inline bool hasXAcquirePrefix() const noexcept { return commonInfo().hasXAcquirePrefix(); }
  //! Tests whether the instruction can be prefixed with XRELEASE prefix.
  inline bool hasXReleasePrefix() const noexcept { return commonInfo().hasXReleasePrefix(); }

  //! Tests whether the rep prefix is supported by the instruction, but ignored (has no effect).
  inline bool isRepIgnored() const noexcept { return commonInfo().isRepIgnored(); }
  //! Tests whether the instruction uses MIB.
  inline bool isMibOp() const noexcept { return hasFlag(kFlagMib); }
  //! Tests whether the instruction uses VSIB.
  inline bool isVsibOp() const noexcept { return hasFlag(kFlagVsib); }
  //! Tests whether the instruction uses VEX (can be set together with EVEX if both are encodable).
  inline bool isVex() const noexcept { return hasFlag(kFlagVex); }
  //! Tests whether the instruction uses EVEX (can be set together with VEX if both are encodable).
  inline bool isEvex() const noexcept { return hasFlag(kFlagEvex); }
  //! Tests whether the instruction uses EVEX (can be set together with VEX if both are encodable).
  inline bool isVexOrEvex() const noexcept { return hasFlag(kFlagVex | kFlagEvex); }

  inline bool isEvexCompatible() const noexcept { return hasFlag(kFlagEvexCompat); }
  inline bool isEvexKRegOnly() const noexcept { return hasFlag(kFlagEvexKReg); }
  inline bool isEvexTwoOpOnly() const noexcept { return hasFlag(kFlagEvexTwoOp); }
  inline bool isEvexTransformable() const noexcept { return hasFlag(kFlagEvexTransformable); }

  //! Tests whether the instruction supports AVX512 masking {k}.
  inline bool hasAvx512K() const noexcept { return hasAvx512Flag(kAvx512FlagK); }
  //! Tests whether the instruction supports AVX512 zeroing {k}{z}.
  inline bool hasAvx512Z() const noexcept { return hasAvx512Flag(kAvx512FlagZ); }
  //! Tests whether the instruction supports AVX512 embedded-rounding {er}.
  inline bool hasAvx512ER() const noexcept { return hasAvx512Flag(kAvx512FlagER); }
  //! Tests whether the instruction supports AVX512 suppress-all-exceptions {sae}.
  inline bool hasAvx512SAE() const noexcept { return hasAvx512Flag(kAvx512FlagSAE); }
  //! Tests whether the instruction supports AVX512 broadcast (either 32-bit or 64-bit).
  inline bool hasAvx512B() const noexcept { return hasAvx512Flag(kAvx512FlagB32 | kAvx512FlagB64); }
  //! Tests whether the instruction supports AVX512 broadcast (32-bit).
  inline bool hasAvx512B32() const noexcept { return hasAvx512Flag(kAvx512FlagB32); }
  //! Tests whether the instruction supports AVX512 broadcast (64-bit).
  inline bool hasAvx512B64() const noexcept { return hasAvx512Flag(kAvx512FlagB64); }

  //! Gets the control-flow type of the instruction.
  inline uint32_t controlType() const noexcept { return commonInfo().controlType(); }
  inline uint32_t singleRegCase() const noexcept { return commonInfo().singleRegCase(); }

  inline uint32_t signatureIndex() const noexcept { return commonInfo().signatureIndex(); }
  inline uint32_t signatureCount() const noexcept { return commonInfo().signatureCount(); }

  inline const InstSignature* signatureData() const noexcept { return commonInfo().signatureData(); }
  inline const InstSignature* signatureEnd() const noexcept { return commonInfo().signatureEnd(); }
};

ASMJIT_VARAPI const InstInfo _instInfoTable[];

static inline const InstInfo& infoById(uint32_t instId) noexcept {
  ASMJIT_ASSERT(Inst::isDefinedId(instId));
  return _instInfoTable[instId];
}

} // {InstDB}

//! \}

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86INSTDB_H_INCLUDED
