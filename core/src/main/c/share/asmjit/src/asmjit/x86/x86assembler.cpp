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
#if !defined(ASMJIT_NO_X86)

#include "../core/assembler.h"
#include "../core/codewriter_p.h"
#include "../core/cpuinfo.h"
#include "../core/emitterutils_p.h"
#include "../core/formatter.h"
#include "../core/logger.h"
#include "../core/misc_p.h"
#include "../core/support.h"
#include "../x86/x86assembler.h"
#include "../x86/x86instdb_p.h"
#include "../x86/x86formatter_p.h"
#include "../x86/x86opcode_p.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

// ============================================================================
// [TypeDefs]
// ============================================================================

typedef Support::FastUInt8 FastUInt8;

// ============================================================================
// [Constants]
// ============================================================================

//! X86 bytes used to encode important prefixes.
enum X86Byte : uint32_t {
  //! 1-byte REX prefix mask.
  kX86ByteRex = 0x40,

  //! 1-byte REX.W component.
  kX86ByteRexW = 0x08,

  kX86ByteInvalidRex = 0x80,

  //! 2-byte VEX prefix:
  //!   - `[0]` - `0xC5`.
  //!   - `[1]` - `RvvvvLpp`.
  kX86ByteVex2 = 0xC5,

  //! 3-byte VEX prefix:
  //!   - `[0]` - `0xC4`.
  //!   - `[1]` - `RXBmmmmm`.
  //!   - `[2]` - `WvvvvLpp`.
  kX86ByteVex3 = 0xC4,

  //! 3-byte XOP prefix:
  //!   - `[0]` - `0x8F`.
  //!   - `[1]` - `RXBmmmmm`.
  //!   - `[2]` - `WvvvvLpp`.
  kX86ByteXop3 = 0x8F,

  //! 4-byte EVEX prefix:
  //!   - `[0]` - `0x62`.
  //!   - `[1]` - Payload0 or `P[ 7: 0]` - `[R  X  B  R' 0  0  m  m]`.
  //!   - `[2]` - Payload1 or `P[15: 8]` - `[W  v  v  v  v  1  p  p]`.
  //!   - `[3]` - Payload2 or `P[23:16]` - `[z  L' L  b  V' a  a  a]`.
  //!
  //! Payload:
  //!   - `P[ 1: 0]` - OPCODE: EVEX.mmmmm, only lowest 2 bits [1:0] used.
  //!   - `P[ 3: 2]` - ______: Must be 0.
  //!   - `P[    4]` - REG-ID: EVEX.R' - 5th bit of 'RRRRR'.
  //!   - `P[    5]` - REG-ID: EVEX.B  - 4th bit of 'BBBBB'.
  //!   - `P[    6]` - REG-ID: EVEX.X  - 5th bit of 'BBBBB' or 4th bit of 'XXXX' (with SIB).
  //!   - `P[    7]` - REG-ID: EVEX.R  - 4th bit of 'RRRRR'.
  //!   - `P[ 9: 8]` - OPCODE: EVEX.pp.
  //!   - `P[   10]` - ______: Must be 1.
  //!   - `P[14:11]` - REG-ID: 4 bits of 'VVVV'.
  //!   - `P[   15]` - OPCODE: EVEX.W.
  //!   - `P[18:16]` - REG-ID: K register k0...k7 (Merging/Zeroing Vector Ops).
  //!   - `P[   19]` - REG-ID: 5th bit of 'VVVVV'.
  //!   - `P[   20]` - OPCODE: Broadcast/Rounding Control/SAE bit.
  //!   - `P[22.21]` - OPCODE: Vector Length (L' and  L) / Rounding Control.
  //!   - `P[   23]` - OPCODE: Zeroing/Merging.
  kX86ByteEvex = 0x62
};

// AsmJit specific (used to encode VVVVV field in XOP/VEX/EVEX).
enum VexVVVVV : uint32_t {
  kVexVVVVVShift = 7,
  kVexVVVVVMask = 0x1F << kVexVVVVVShift
};

//! Instruction 2-byte/3-byte opcode prefix definition.
struct X86OpcodeMM {
  uint8_t size;
  uint8_t data[3];
};

//! Mandatory prefixes used to encode legacy [66, F3, F2] or [9B] byte.
static const uint8_t x86OpcodePP[8] = { 0x00, 0x66, 0xF3, 0xF2, 0x00, 0x00, 0x00, 0x9B };

//! Instruction 2-byte/3-byte opcode prefix data.
static const X86OpcodeMM x86OpcodeMM[] = {
  { 0, { 0x00, 0x00, 0 } }, // #00 (0b0000).
  { 1, { 0x0F, 0x00, 0 } }, // #01 (0b0001).
  { 2, { 0x0F, 0x38, 0 } }, // #02 (0b0010).
  { 2, { 0x0F, 0x3A, 0 } }, // #03 (0b0011).
  { 2, { 0x0F, 0x01, 0 } }, // #04 (0b0100).
  { 0, { 0x00, 0x00, 0 } }, // #05 (0b0101).
  { 0, { 0x00, 0x00, 0 } }, // #06 (0b0110).
  { 0, { 0x00, 0x00, 0 } }, // #07 (0b0111).
  { 0, { 0x00, 0x00, 0 } }, // #08 (0b1000).
  { 0, { 0x00, 0x00, 0 } }, // #09 (0b1001).
  { 0, { 0x00, 0x00, 0 } }, // #0A (0b1010).
  { 0, { 0x00, 0x00, 0 } }, // #0B (0b1011).
  { 0, { 0x00, 0x00, 0 } }, // #0C (0b1100).
  { 0, { 0x00, 0x00, 0 } }, // #0D (0b1101).
  { 0, { 0x00, 0x00, 0 } }, // #0E (0b1110).
  { 0, { 0x00, 0x00, 0 } }  // #0F (0b1111).
};

static const uint8_t x86SegmentPrefix[8] = {
  0x00, // None.
  0x26, // ES.
  0x2E, // CS.
  0x36, // SS.
  0x3E, // DS.
  0x64, // FS.
  0x65  // GS.
};

static const uint32_t x86OpcodePushSReg[8] = {
  Opcode::k000000 | 0x00, // None.
  Opcode::k000000 | 0x06, // Push ES.
  Opcode::k000000 | 0x0E, // Push CS.
  Opcode::k000000 | 0x16, // Push SS.
  Opcode::k000000 | 0x1E, // Push DS.
  Opcode::k000F00 | 0xA0, // Push FS.
  Opcode::k000F00 | 0xA8  // Push GS.
};

static const uint32_t x86OpcodePopSReg[8]  = {
  Opcode::k000000 | 0x00, // None.
  Opcode::k000000 | 0x07, // Pop ES.
  Opcode::k000000 | 0x00, // Pop CS.
  Opcode::k000000 | 0x17, // Pop SS.
  Opcode::k000000 | 0x1F, // Pop DS.
  Opcode::k000F00 | 0xA1, // Pop FS.
  Opcode::k000F00 | 0xA9  // Pop GS.
};

// ============================================================================
// [asmjit::X86MemInfo | X86VEXPrefix | X86LLByRegType | X86CDisp8Table]
// ============================================================================

//! Memory operand's info bits.
//!
//! A lookup table that contains various information based on the BASE and INDEX
//! information of a memory operand. This is much better and safer than playing
//! with IFs in the code and can check for errors must faster and better.
enum X86MemInfo_Enum {
  kX86MemInfo_0         = 0x00,

  kX86MemInfo_BaseGp    = 0x01, //!< Has BASE reg, REX.B can be 1, compatible with REX.B byte.
  kX86MemInfo_Index     = 0x02, //!< Has INDEX reg, REX.X can be 1, compatible with REX.X byte.

  kX86MemInfo_BaseLabel = 0x10, //!< Base is Label.
  kX86MemInfo_BaseRip   = 0x20, //!< Base is RIP.

  kX86MemInfo_67H_X86   = 0x40, //!< Address-size override in 32-bit mode.
  kX86MemInfo_67H_X64   = 0x80, //!< Address-size override in 64-bit mode.
  kX86MemInfo_67H_Mask  = 0xC0  //!< Contains all address-size override bits.
};

template<uint32_t X>
struct X86MemInfo_T {
  enum {
    B = (X     ) & 0x1F,
    I = (X >> 5) & 0x1F,

    kBase  = (B >= Reg::kTypeGpw    && B <= Reg::kTypeGpq ) ? kX86MemInfo_BaseGp    :
             (B == Reg::kTypeRip                          ) ? kX86MemInfo_BaseRip   :
             (B == Label::kLabelTag                       ) ? kX86MemInfo_BaseLabel : 0,

    kIndex = (I >= Reg::kTypeGpw    && I <= Reg::kTypeGpq ) ? kX86MemInfo_Index     :
             (I >= Reg::kTypeXmm    && I <= Reg::kTypeZmm ) ? kX86MemInfo_Index     : 0,

    k67H   = (B == Reg::kTypeGpw    && I == Reg::kTypeNone) ? kX86MemInfo_67H_X86   :
             (B == Reg::kTypeGpd    && I == Reg::kTypeNone) ? kX86MemInfo_67H_X64   :
             (B == Reg::kTypeNone   && I == Reg::kTypeGpw ) ? kX86MemInfo_67H_X86   :
             (B == Reg::kTypeNone   && I == Reg::kTypeGpd ) ? kX86MemInfo_67H_X64   :
             (B == Reg::kTypeGpw    && I == Reg::kTypeGpw ) ? kX86MemInfo_67H_X86   :
             (B == Reg::kTypeGpd    && I == Reg::kTypeGpd ) ? kX86MemInfo_67H_X64   :
             (B == Reg::kTypeGpw    && I == Reg::kTypeXmm ) ? kX86MemInfo_67H_X86   :
             (B == Reg::kTypeGpd    && I == Reg::kTypeXmm ) ? kX86MemInfo_67H_X64   :
             (B == Reg::kTypeGpw    && I == Reg::kTypeYmm ) ? kX86MemInfo_67H_X86   :
             (B == Reg::kTypeGpd    && I == Reg::kTypeYmm ) ? kX86MemInfo_67H_X64   :
             (B == Reg::kTypeGpw    && I == Reg::kTypeZmm ) ? kX86MemInfo_67H_X86   :
             (B == Reg::kTypeGpd    && I == Reg::kTypeZmm ) ? kX86MemInfo_67H_X64   :
             (B == Label::kLabelTag && I == Reg::kTypeGpw ) ? kX86MemInfo_67H_X86   :
             (B == Label::kLabelTag && I == Reg::kTypeGpd ) ? kX86MemInfo_67H_X64   : 0,

    kValue = kBase | kIndex | k67H | 0x04 | 0x08
  };
};

// The result stored in the LUT is a combination of
//   - 67H - Address override prefix - depends on BASE+INDEX register types and
//           the target architecture.
//   - REX - A possible combination of REX.[B|X|R|W] bits in REX prefix where
//           REX.B and REX.X are possibly masked out, but REX.R and REX.W are
//           kept as is.
#define VALUE(x) X86MemInfo_T<x>::kValue
static const uint8_t x86MemInfo[] = { ASMJIT_LOOKUP_TABLE_1024(VALUE, 0) };
#undef VALUE

// VEX3 or XOP xor bits applied to the opcode before emitted. The index to this
// table is 'mmmmm' value, which contains all we need. This is only used by a
// 3 BYTE VEX and XOP prefixes, 2 BYTE VEX prefix is handled differently. The
// idea is to minimize the difference between VEX3 vs XOP when encoding VEX
// or XOP instruction. This should minimize the code required to emit such
// instructions and should also make it faster as we don't need any branch to
// decide between VEX3 vs XOP.
//            ____    ___
// [_OPCODE_|WvvvvLpp|RXBmmmmm|VEX3_XOP]
#define VALUE(x) ((x & 0x08) ? kX86ByteXop3 : kX86ByteVex3) | (0xF << 19) | (0x7 << 13)
static const uint32_t x86VEXPrefix[] = { ASMJIT_LOOKUP_TABLE_16(VALUE, 0) };
#undef VALUE

// Table that contains LL opcode field addressed by a register size / 16. It's
// used to propagate L.256 or L.512 when YMM or ZMM registers are used,
// respectively.
#define VALUE(x) (x & (64 >> 4)) ? Opcode::kLL_2 : \
                 (x & (32 >> 4)) ? Opcode::kLL_1 : Opcode::kLL_0
static const uint32_t x86LLBySizeDiv16[] = { ASMJIT_LOOKUP_TABLE_16(VALUE, 0) };
#undef VALUE

// Table that contains LL opcode field addressed by a register size / 16. It's
// used to propagate L.256 or L.512 when YMM or ZMM registers are used,
// respectively.
#define VALUE(x) x == Reg::kTypeZmm ? Opcode::kLL_2 : \
                 x == Reg::kTypeYmm ? Opcode::kLL_1 : Opcode::kLL_0
static const uint32_t x86LLByRegType[] = { ASMJIT_LOOKUP_TABLE_16(VALUE, 0) };
#undef VALUE

// Table that contains a scale (shift left) based on 'TTWLL' field and
// the instruction's tuple-type (TT) field. The scale is then applied to
// the BASE-N stored in each opcode to calculate the final compressed
// displacement used by all EVEX encoded instructions.
template<uint32_t X>
struct X86CDisp8SHL_T {
  enum {
    TT = (X >> 3) << Opcode::kCDTT_Shift,
    LL = (X >> 0) & 0x3,
    W  = (X >> 2) & 0x1,

    kValue = (TT == Opcode::kCDTT_None ? ((LL==0) ? 0 : (LL==1) ? 0   : 0  ) :
              TT == Opcode::kCDTT_ByLL ? ((LL==0) ? 0 : (LL==1) ? 1   : 2  ) :
              TT == Opcode::kCDTT_T1W  ? ((LL==0) ? W : (LL==1) ? 1+W : 2+W) :
              TT == Opcode::kCDTT_DUP  ? ((LL==0) ? 0 : (LL==1) ? 2   : 3  ) : 0) << Opcode::kCDSHL_Shift
  };
};

#define VALUE(x) X86CDisp8SHL_T<x>::kValue
static const uint32_t x86CDisp8SHL[] = { ASMJIT_LOOKUP_TABLE_32(VALUE, 0) };
#undef VALUE

// Table that contains MOD byte of a 16-bit [BASE + disp] address.
//   0xFF == Invalid.
static const uint8_t x86Mod16BaseTable[8] = {
  0xFF, // AX -> N/A.
  0xFF, // CX -> N/A.
  0xFF, // DX -> N/A.
  0x07, // BX -> 111.
  0xFF, // SP -> N/A.
  0x06, // BP -> 110.
  0x04, // SI -> 100.
  0x05  // DI -> 101.
};

// Table that contains MOD byte of a 16-bit [BASE + INDEX + disp] combination.
//   0xFF == Invalid.
template<uint32_t X>
struct X86Mod16BaseIndexTable_T {
  enum {
    B = X >> 3,
    I = X & 0x7,

    kValue = ((B == Gp::kIdBx && I == Gp::kIdSi) || (B == Gp::kIdSi && I == Gp::kIdBx)) ? 0x00 :
             ((B == Gp::kIdBx && I == Gp::kIdDi) || (B == Gp::kIdDi && I == Gp::kIdBx)) ? 0x01 :
             ((B == Gp::kIdBp && I == Gp::kIdSi) || (B == Gp::kIdSi && I == Gp::kIdBp)) ? 0x02 :
             ((B == Gp::kIdBp && I == Gp::kIdDi) || (B == Gp::kIdDi && I == Gp::kIdBp)) ? 0x03 : 0xFF
  };
};

#define VALUE(x) X86Mod16BaseIndexTable_T<x>::kValue
static const uint8_t x86Mod16BaseIndexTable[] = { ASMJIT_LOOKUP_TABLE_64(VALUE, 0) };
#undef VALUE

// ============================================================================
// [asmjit::x86::Assembler - Helpers]
// ============================================================================

static ASMJIT_INLINE bool x86IsJmpOrCall(uint32_t instId) noexcept {
  return instId == Inst::kIdJmp || instId == Inst::kIdCall;
}

static ASMJIT_INLINE bool x86IsImplicitMem(const Operand_& op, uint32_t base) noexcept {
  return op.isMem() && op.as<Mem>().baseId() == base && !op.as<Mem>().hasOffset();
}

//! Combine `regId` and `vvvvvId` into a single value (used by AVX and AVX-512).
static ASMJIT_INLINE uint32_t x86PackRegAndVvvvv(uint32_t regId, uint32_t vvvvvId) noexcept {
  return regId + (vvvvvId << kVexVVVVVShift);
}

static ASMJIT_INLINE uint32_t x86OpcodeLByVMem(const Operand_& op) noexcept {
  return x86LLByRegType[op.as<Mem>().indexType()];
}

static ASMJIT_INLINE uint32_t x86OpcodeLBySize(uint32_t size) noexcept {
  return x86LLBySizeDiv16[size / 16];
}

//! Encode MOD byte.
static ASMJIT_INLINE uint32_t x86EncodeMod(uint32_t m, uint32_t o, uint32_t rm) noexcept {
  ASMJIT_ASSERT(m <= 3);
  ASMJIT_ASSERT(o <= 7);
  ASMJIT_ASSERT(rm <= 7);
  return (m << 6) + (o << 3) + rm;
}

//! Encode SIB byte.
static ASMJIT_INLINE uint32_t x86EncodeSib(uint32_t s, uint32_t i, uint32_t b) noexcept {
  ASMJIT_ASSERT(s <= 3);
  ASMJIT_ASSERT(i <= 7);
  ASMJIT_ASSERT(b <= 7);
  return (s << 6) + (i << 3) + b;
}

static ASMJIT_INLINE bool x86IsRexInvalid(uint32_t rex) noexcept {
  // Validates the following possibilities:
  //   REX == 0x00      -> OKAY (X86_32 / X86_64).
  //   REX == 0x40-0x4F -> OKAY (X86_64).
  //   REX == 0x80      -> OKAY (X86_32 mode, rex prefix not used).
  //   REX == 0x81-0xCF -> BAD  (X86_32 mode, rex prefix used).
  return rex > kX86ByteInvalidRex;
}

template<typename T>
static constexpr T x86SignExtendI32(T imm) noexcept { return T(int64_t(int32_t(imm & T(0xFFFFFFFF)))); }

static ASMJIT_INLINE uint32_t x86AltOpcodeOf(const InstDB::InstInfo* info) noexcept {
  return InstDB::_altOpcodeTable[info->_altOpcodeIndex];
}

// ============================================================================
// [asmjit::X86BufferWriter]
// ============================================================================

class X86BufferWriter : public CodeWriter {
public:
  ASMJIT_INLINE explicit X86BufferWriter(Assembler* a) noexcept
    : CodeWriter(a) {}

  ASMJIT_INLINE void emitPP(uint32_t opcode) noexcept {
    uint32_t ppIndex = (opcode              >> Opcode::kPP_Shift) &
                       (Opcode::kPP_FPUMask >> Opcode::kPP_Shift) ;
    emit8If(x86OpcodePP[ppIndex], ppIndex != 0);
  }

  ASMJIT_INLINE void emitMMAndOpcode(uint32_t opcode) noexcept {
    uint32_t mmIndex = (opcode & Opcode::kMM_Mask) >> Opcode::kMM_Shift;
    const X86OpcodeMM& mmCode = x86OpcodeMM[mmIndex];

    emit8If(mmCode.data[0], mmCode.size > 0);
    emit8If(mmCode.data[1], mmCode.size > 1);
    emit8(opcode);
  }

  ASMJIT_INLINE void emitSegmentOverride(uint32_t segmentId) noexcept {
    ASMJIT_ASSERT(segmentId < ASMJIT_ARRAY_SIZE(x86SegmentPrefix));

    FastUInt8 prefix = x86SegmentPrefix[segmentId];
    emit8If(prefix, prefix != 0);
  }

  template<typename CondT>
  ASMJIT_INLINE void emitAddressOverride(CondT condition) noexcept {
    emit8If(0x67, condition);
  }

  ASMJIT_INLINE void emitImmByteOrDWord(uint64_t immValue, FastUInt8 immSize) noexcept {
    if (!immSize)
      return;

    ASMJIT_ASSERT(immSize == 1 || immSize == 4);

#if ASMJIT_ARCH_BITS >= 64
    uint64_t imm = uint64_t(immValue);
#else
    uint32_t imm = uint32_t(immValue & 0xFFFFFFFFu);
#endif

    // Many instructions just use a single byte immediate, so make it fast.
    emit8(imm & 0xFFu);
    if (immSize == 1) return;

    imm >>= 8;
    emit8(imm & 0xFFu);
    imm >>= 8;
    emit8(imm & 0xFFu);
    imm >>= 8;
    emit8(imm & 0xFFu);
  }

  ASMJIT_INLINE void emitImmediate(uint64_t immValue, FastUInt8 immSize) noexcept {
#if ASMJIT_ARCH_BITS >= 64
    uint64_t imm = immValue;
    if (immSize >= 4) {
      emit32uLE(imm & 0xFFFFFFFFu);
      imm >>= 32;
      immSize = FastUInt8(immSize - 4u);
    }
#else
    uint32_t imm = uint32_t(immValue & 0xFFFFFFFFu);
    if (immSize >= 4) {
      emit32uLE(imm);
      imm = uint32_t(immValue >> 32);
      immSize = FastUInt8(immSize - 4u);
    }
#endif

    if (!immSize)
      return;
    emit8(imm & 0xFFu);
    imm >>= 8;

    if (--immSize == 0)
      return;
    emit8(imm & 0xFFu);
    imm >>= 8;

    if (--immSize == 0)
      return;
    emit8(imm & 0xFFu);
    imm >>= 8;

    if (--immSize == 0)
      return;
    emit8(imm & 0xFFu);
  }
};

// If the operand is BPL|SPL|SIL|DIL|R8B-15B
//   - Force REX prefix
// If the operand is AH|BH|CH|DH
//   - patch its index from 0..3 to 4..7 as encoded by X86.
//   - Disallow REX prefix.
#define FIXUP_GPB(REG_OP, REG_ID)                           \
  do {                                                      \
    if (!static_cast<const Gp&>(REG_OP).isGpbHi()) {        \
      options |= (REG_ID >= 4) ? uint32_t(Inst::kOptionRex) \
                               : uint32_t(0);               \
    }                                                       \
    else {                                                  \
      options |= Inst::_kOptionInvalidRex;                  \
      REG_ID += 4;                                          \
    }                                                       \
  } while (0)

#define ENC_OPS1(OP0)                ((Operand::kOp##OP0))
#define ENC_OPS2(OP0, OP1)           ((Operand::kOp##OP0) + ((Operand::kOp##OP1) << 3))
#define ENC_OPS3(OP0, OP1, OP2)      ((Operand::kOp##OP0) + ((Operand::kOp##OP1) << 3) + ((Operand::kOp##OP2) << 6))
#define ENC_OPS4(OP0, OP1, OP2, OP3) ((Operand::kOp##OP0) + ((Operand::kOp##OP1) << 3) + ((Operand::kOp##OP2) << 6) + ((Operand::kOp##OP3) << 9))

// ============================================================================
// [asmjit::x86::Assembler - Movabs Heuristics]
// ============================================================================

static ASMJIT_INLINE uint32_t x86GetMovAbsInstSize64Bit(uint32_t regSize, uint32_t options, const Mem& rmRel) noexcept {
  uint32_t segmentPrefixSize = rmRel.segmentId() != 0;
  uint32_t _66hPrefixSize = regSize == 2;
  uint32_t rexPrefixSize = (regSize == 8) || ((options & Inst::kOptionRex) != 0);
  uint32_t opCodeByteSize = 1;
  uint32_t immediateSize = 8;

  return segmentPrefixSize + _66hPrefixSize + rexPrefixSize + opCodeByteSize + immediateSize;
}

static ASMJIT_INLINE bool x86ShouldUseMovabs(Assembler* self, X86BufferWriter& writer, uint32_t regSize, uint32_t options, const Mem& rmRel) noexcept {
  if (self->is32Bit()) {
    // There is no relative addressing, just decide whether to use MOV encoded with MOD R/M or absolute.
    return !(options & Inst::kOptionModMR);
  }
  else {
    // If the addressing type is REL or MOD R/M was specified then absolute mov won't be used.
    if (rmRel.addrType() == Mem::kAddrTypeRel || (options & Inst::kOptionModMR) != 0)
      return false;

    int64_t addrValue = rmRel.offset();
    uint64_t baseAddress = self->code()->baseAddress();

    // If the address type is default, it means to basically check whether relative addressing is possible. However,
    // this is only possible when the base address is known - relative encoding uses RIP+N it has to be calculated.
    if (rmRel.addrType() == Mem::kAddrTypeDefault && baseAddress != Globals::kNoBaseAddress && !rmRel.hasSegment()) {
      uint32_t instructionSize = x86GetMovAbsInstSize64Bit(regSize, options, rmRel);
      uint64_t virtualOffset = uint64_t(writer.offsetFrom(self->_bufferData));
      uint64_t rip64 = baseAddress + self->_section->offset() + virtualOffset + instructionSize;
      uint64_t rel64 = uint64_t(addrValue) - rip64;

      if (Support::isInt32(int64_t(rel64)))
        return false;
    }
    else {
      if (Support::isInt32(addrValue))
        return false;
    }

    return uint64_t(addrValue) > 0xFFFFFFFFu;
  }
}

// ============================================================================
// [asmjit::x86::Assembler - Construction / Destruction]
// ============================================================================

Assembler::Assembler(CodeHolder* code) noexcept : BaseAssembler() {
  if (code)
    code->attach(this);
}
Assembler::~Assembler() noexcept {}

// ============================================================================
// [asmjit::x86::Assembler - Emit (Low-Level)]
// ============================================================================

ASMJIT_FAVOR_SPEED Error Assembler::_emit(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt) {
  constexpr uint32_t kVSHR_W     = Opcode::kW_Shift  - 23;
  constexpr uint32_t kVSHR_PP    = Opcode::kPP_Shift - 16;
  constexpr uint32_t kVSHR_PP_EW = Opcode::kPP_Shift - 16;

  constexpr uint32_t kRequiresSpecialHandling =
    uint32_t(Inst::kOptionReserved) | // Logging/Validation/Error.
    uint32_t(Inst::kOptionRep     ) | // REP/REPE prefix.
    uint32_t(Inst::kOptionRepne   ) | // REPNE prefix.
    uint32_t(Inst::kOptionLock    ) | // LOCK prefix.
    uint32_t(Inst::kOptionXAcquire) | // XACQUIRE prefix.
    uint32_t(Inst::kOptionXRelease) ; // XRELEASE prefix.

  Error err;

  Opcode opcode;                   // Instruction opcode.
  uint32_t options;                // Instruction options.
  uint32_t isign3;                 // A combined signature of first 3 operands.

  const Operand_* rmRel;           // Memory operand or operand that holds Label|Imm.
  uint32_t rmInfo;                 // Memory operand's info based on x86MemInfo.
  uint32_t rbReg;                  // Memory base or modRM register.
  uint32_t rxReg;                  // Memory index register.
  uint32_t opReg;                  // ModR/M opcode or register id.

  LabelEntry* label;               // Label entry.
  RelocEntry* re = nullptr;        // Relocation entry.
  int32_t relOffset;               // Relative offset
  FastUInt8 relSize = 0;           // Relative size.
  uint8_t* memOpAOMark = nullptr;  // Marker that points before 'address-override prefix' is emitted.

  int64_t immValue = 0;            // Immediate value (must be 64-bit).
  FastUInt8 immSize = 0;           // Immediate size.

  X86BufferWriter writer(this);

  if (instId >= Inst::_kIdCount)
    instId = 0;

  const InstDB::InstInfo* instInfo = &InstDB::_instInfoTable[instId];
  const InstDB::CommonInfo* commonInfo = &instInfo->commonInfo();

  // Signature of the first 3 operands.
  isign3 = o0.opType() + (o1.opType() << 3) + (o2.opType() << 6);

  // Combine all instruction options and also check whether the instruction
  // is valid. All options that require special handling (including invalid
  // instruction) are handled by the next branch.
  options  = uint32_t(instId == 0);
  options |= uint32_t((size_t)(_bufferEnd - writer.cursor()) < 16);
  options |= uint32_t(instOptions() | forcedInstOptions());

  // Handle failure and rare cases first.
  if (ASMJIT_UNLIKELY(options & kRequiresSpecialHandling)) {
    if (ASMJIT_UNLIKELY(!_code))
      return reportError(DebugUtils::errored(kErrorNotInitialized));

    // Unknown instruction.
    if (ASMJIT_UNLIKELY(instId == 0))
      goto InvalidInstruction;

    // Grow request, happens rarely.
    err = writer.ensureSpace(this, 16);
    if (ASMJIT_UNLIKELY(err))
      goto Failed;

#ifndef ASMJIT_NO_VALIDATION
    // Strict validation.
    if (hasValidationOption(kValidationOptionAssembler)) {
      Operand_ opArray[Globals::kMaxOpCount];
      EmitterUtils::opArrayFromEmitArgs(opArray, o0, o1, o2, opExt);

      err = InstAPI::validate(arch(), BaseInst(instId, options, _extraReg), opArray, Globals::kMaxOpCount);
      if (ASMJIT_UNLIKELY(err))
        goto Failed;
    }
#endif

    uint32_t iFlags = instInfo->flags();

    // LOCK, XACQUIRE, and XRELEASE prefixes.
    if (options & Inst::kOptionLock) {
      bool xAcqRel = (options & (Inst::kOptionXAcquire | Inst::kOptionXRelease)) != 0;

      if (ASMJIT_UNLIKELY(!(iFlags & (InstDB::kFlagLock)) && !xAcqRel))
        goto InvalidLockPrefix;

      if (xAcqRel) {
        if (ASMJIT_UNLIKELY((options & Inst::kOptionXAcquire) && !(iFlags & InstDB::kFlagXAcquire)))
          goto InvalidXAcquirePrefix;

        if (ASMJIT_UNLIKELY((options & Inst::kOptionXRelease) && !(iFlags & InstDB::kFlagXRelease)))
          goto InvalidXReleasePrefix;

        writer.emit8((options & Inst::kOptionXAcquire) ? 0xF2 : 0xF3);
      }

      writer.emit8(0xF0);
    }

    // REP and REPNE prefixes.
    if (options & (Inst::kOptionRep | Inst::kOptionRepne)) {
      if (ASMJIT_UNLIKELY(!(iFlags & InstDB::kFlagRep)))
        goto InvalidRepPrefix;

      if (_extraReg.isReg() && ASMJIT_UNLIKELY(_extraReg.group() != Reg::kGroupGp || _extraReg.id() != Gp::kIdCx))
        goto InvalidRepPrefix;

      writer.emit8((options & Inst::kOptionRepne) ? 0xF2 : 0xF3);
    }
  }

  // This sequence seems to be the fastest.
  opcode = InstDB::_mainOpcodeTable[instInfo->_mainOpcodeIndex];
  opReg = opcode.extractModO();
  rbReg = 0;
  opcode |= instInfo->_mainOpcodeValue;

  // --------------------------------------------------------------------------
  // [Encoding Scope]
  // --------------------------------------------------------------------------

  // How it works? Each case here represents a unique encoding of a group of
  // instructions, which is handled separately. The handlers check instruction
  // signature, possibly register types, etc, and process this information by
  // writing some bits to opcode, opReg/rbReg, immValue/immSize, etc, and then
  // at the end of the sequence it uses goto to jump into a lower level handler,
  // that actually encodes the instruction.

  switch (instInfo->_encoding) {
    case InstDB::kEncodingNone:
      goto EmitDone;

    // ------------------------------------------------------------------------
    // [X86]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingX86Op:
      goto EmitX86Op;

    case InstDB::kEncodingX86Op_Mod11RM:
      rbReg = opcode.extractModRM();
      goto EmitX86R;

    case InstDB::kEncodingX86Op_Mod11RM_I8:
      // The first operand must be immediate, we don't care of other operands as they could be implicit.
      if (!o0.isImm())
        goto InvalidInstruction;

      rbReg = opcode.extractModRM();
      immValue = o0.as<Imm>().valueAs<uint8_t>();
      immSize = 1;
      goto EmitX86R;

    case InstDB::kEncodingX86Op_xAddr:
      if (ASMJIT_UNLIKELY(!o0.isReg()))
        goto InvalidInstruction;

      rmInfo = x86MemInfo[o0.as<Reg>().type()];
      writer.emitAddressOverride((rmInfo & _addressOverrideMask()) != 0);
      goto EmitX86Op;

    case InstDB::kEncodingX86Op_xAX:
      if (isign3 == 0)
        goto EmitX86Op;

      if (isign3 == ENC_OPS1(Reg) && o0.id() == Gp::kIdAx)
        goto EmitX86Op;
      break;

    case InstDB::kEncodingX86Op_xDX_xAX:
      if (isign3 == 0)
        goto EmitX86Op;

      if (isign3 == ENC_OPS2(Reg, Reg) && o0.id() == Gp::kIdDx && o1.id() == Gp::kIdAx)
        goto EmitX86Op;
      break;

    case InstDB::kEncodingX86Op_MemZAX:
      if (isign3 == 0)
        goto EmitX86Op;

      rmRel = &o0;
      if (isign3 == ENC_OPS1(Mem) && x86IsImplicitMem(o0, Gp::kIdAx))
        goto EmitX86OpImplicitMem;

      break;

    case InstDB::kEncodingX86I_xAX:
      // Implicit form.
      if (isign3 == ENC_OPS1(Imm)) {
        immValue = o0.as<Imm>().valueAs<uint8_t>();
        immSize = 1;
        goto EmitX86Op;
      }

      // Explicit form.
      if (isign3 == ENC_OPS2(Reg, Imm) && o0.id() == Gp::kIdAx) {
        immValue = o1.as<Imm>().valueAs<uint8_t>();
        immSize = 1;
        goto EmitX86Op;
      }
      break;

    case InstDB::kEncodingX86M_NoMemSize:
      if (o0.isReg())
        opcode.addPrefixBySize(o0.size());
      goto CaseX86M_NoSize;

    case InstDB::kEncodingX86M:
      opcode.addPrefixBySize(o0.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86M_NoSize:
CaseX86M_NoSize:
      rbReg = o0.id();
      if (isign3 == ENC_OPS1(Reg))
        goto EmitX86R;

      rmRel = &o0;
      if (isign3 == ENC_OPS1(Mem))
        goto EmitX86M;
      break;

    case InstDB::kEncodingX86M_GPB_MulDiv:
CaseX86M_GPB_MulDiv:
      // Explicit form?
      if (isign3 > 0x7) {
        // [AX] <- [AX] div|mul r8.
        if (isign3 == ENC_OPS2(Reg, Reg)) {
          if (ASMJIT_UNLIKELY(!Reg::isGpw(o0, Gp::kIdAx) || !Reg::isGpb(o1)))
            goto InvalidInstruction;

          rbReg = o1.id();
          FIXUP_GPB(o1, rbReg);
          goto EmitX86R;
        }

        // [AX] <- [AX] div|mul m8.
        if (isign3 == ENC_OPS2(Reg, Mem)) {
          if (ASMJIT_UNLIKELY(!Reg::isGpw(o0, Gp::kIdAx)))
            goto InvalidInstruction;

          rmRel = &o1;
          goto EmitX86M;
        }

        // [?DX:?AX] <- [?DX:?AX] div|mul r16|r32|r64
        if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
          if (ASMJIT_UNLIKELY(o0.size() != o1.size()))
            goto InvalidInstruction;

          opcode.addArithBySize(o0.size());
          rbReg = o2.id();
          goto EmitX86R;
        }

        // [?DX:?AX] <- [?DX:?AX] div|mul m16|m32|m64
        if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
          if (ASMJIT_UNLIKELY(o0.size() != o1.size()))
            goto InvalidInstruction;

          opcode.addArithBySize(o0.size());
          rmRel = &o2;
          goto EmitX86M;
        }

        goto InvalidInstruction;
      }

      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86M_GPB:
      if (isign3 == ENC_OPS1(Reg)) {
        opcode.addArithBySize(o0.size());
        rbReg = o0.id();

        if (o0.size() != 1)
          goto EmitX86R;

        FIXUP_GPB(o0, rbReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS1(Mem)) {
        if (ASMJIT_UNLIKELY(o0.size() == 0))
          goto AmbiguousOperandSize;

        opcode.addArithBySize(o0.size());
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86M_Only_EDX_EAX:
      if (isign3 == ENC_OPS3(Mem, Reg, Reg) && Reg::isGpd(o1, Gp::kIdDx) && Reg::isGpd(o2, Gp::kIdAx)) {
        rmRel = &o0;
        goto EmitX86M;
      }
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86M_Only:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86M_Nop:
      if (isign3 == ENC_OPS1(None))
        goto EmitX86Op;

      // Single operand NOP instruction "0F 1F /0".
      opcode = Opcode::k000F00 | 0x1F;
      opReg = 0;

      if (isign3 == ENC_OPS1(Reg)) {
        opcode.addPrefixBySize(o0.size());
        rbReg = o0.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS1(Mem)) {
        opcode.addPrefixBySize(o0.size());
        rmRel = &o0;
        goto EmitX86M;
      }

      // Two operand NOP instruction "0F 1F /r".
      opReg = o1.id();
      opcode.addPrefixBySize(o1.size());

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        rbReg = o0.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86R_FromM:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        rbReg = o0.id();
        goto EmitX86RFromM;
      }
      break;

    case InstDB::kEncodingX86R32_EDX_EAX:
      // Explicit form: R32, EDX, EAX.
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        if (!Reg::isGpd(o1, Gp::kIdDx) || !Reg::isGpd(o2, Gp::kIdAx))
          goto InvalidInstruction;
        rbReg = o0.id();
        goto EmitX86R;
      }

      // Implicit form: R32.
      if (isign3 == ENC_OPS1(Reg)) {
        if (!Reg::isGpd(o0))
          goto InvalidInstruction;
        rbReg = o0.id();
        goto EmitX86R;
      }
      break;

    case InstDB::kEncodingX86R_Native:
      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();
        goto EmitX86R;
      }
      break;

    case InstDB::kEncodingX86Rm:
      opcode.addPrefixBySize(o0.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86Rm_NoSize:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Rm_Raw66H:
      // We normally emit either [66|F2|F3], this instruction requires 66+[F2|F3].
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        if (o0.size() == 2)
          writer.emit8(0x66);
        else
          opcode.addWBySize(o0.size());
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;

        if (o0.size() == 2)
          writer.emit8(0x66);
        else
          opcode.addWBySize(o0.size());
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Mr:
      opcode.addPrefixBySize(o0.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86Mr_NoSize:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        rbReg = o0.id();
        opReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        rmRel = &o0;
        opReg = o1.id();
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Arith:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opcode.addArithBySize(o0.size());

        if (o0.size() != o1.size())
          goto OperandSizeMismatch;

        rbReg = o0.id();
        opReg = o1.id();

        if (o0.size() == 1) {
          FIXUP_GPB(o0, rbReg);
          FIXUP_GPB(o1, opReg);
        }

        // MOD/MR: The default encoding used if not instructed otherwise..
        if (!(options & Inst::kOptionModRM))
          goto EmitX86R;

        // MOD/RM: Alternative encoding selected via instruction options.
        opcode += 2;
        std::swap(opReg, rbReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode += 2;
        opcode.addArithBySize(o0.size());

        opReg = o0.id();
        rmRel = &o1;

        if (o0.size() != 1)
          goto EmitX86M;

        FIXUP_GPB(o0, opReg);
        goto EmitX86M;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.addArithBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;

        if (o1.size() != 1)
          goto EmitX86M;

        FIXUP_GPB(o1, opReg);
        goto EmitX86M;
      }

      // The remaining instructions use 0x80 opcode.
      opcode = 0x80;

      if (isign3 == ENC_OPS2(Reg, Imm)) {
        uint32_t size = o0.size();

        rbReg = o0.id();
        immValue = o1.as<Imm>().value();

        if (size == 1) {
          FIXUP_GPB(o0, rbReg);
          immSize = 1;
        }
        else {
          if (size == 2) {
            opcode |= Opcode::kPP_66;
          }
          else if (size == 4) {
            // Sign extend so isInt8 returns the right result.
            immValue = x86SignExtendI32<int64_t>(immValue);
          }
          else if (size == 8) {
            bool canTransformTo32Bit = instId == Inst::kIdAnd && Support::isUInt32(immValue);

            if (!Support::isInt32(immValue)) {
              // We would do this by default when `kOptionOptimizedForSize` is
              // enabled, however, in this case we just force this as otherwise
              // we would have to fail.
              if (canTransformTo32Bit)
                size = 4;
              else
                goto InvalidImmediate;
            }
            else if (canTransformTo32Bit && hasEncodingOption(kEncodingOptionOptimizeForSize)) {
              size = 4;
            }

            opcode.addWBySize(size);
          }

          immSize = FastUInt8(Support::min<uint32_t>(size, 4));
          if (Support::isInt8(immValue) && !(options & Inst::kOptionLongForm))
            immSize = 1;
        }

        // Short form - AL, AX, EAX, RAX.
        if (rbReg == 0 && (size == 1 || immSize != 1) && !(options & Inst::kOptionLongForm)) {
          opcode &= Opcode::kPP_66 | Opcode::kW;
          opcode |= ((opReg << 3) | (0x04 + (size != 1)));
          immSize = FastUInt8(Support::min<uint32_t>(size, 4));
          goto EmitX86Op;
        }

        opcode += size != 1 ? (immSize != 1 ? 1 : 3) : 0;
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Imm)) {
        uint32_t memSize = o0.size();

        if (ASMJIT_UNLIKELY(memSize == 0))
          goto AmbiguousOperandSize;

        immValue = o1.as<Imm>().value();
        immSize = FastUInt8(Support::min<uint32_t>(memSize, 4));

        // Sign extend so isInt8 returns the right result.
        if (memSize == 4)
          immValue = x86SignExtendI32<int64_t>(immValue);

        if (Support::isInt8(immValue) && !(options & Inst::kOptionLongForm))
          immSize = 1;

        opcode += memSize != 1 ? (immSize != 1 ? 1 : 3) : 0;
        opcode.addPrefixBySize(memSize);

        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Bswap:
      if (isign3 == ENC_OPS1(Reg)) {
        if (ASMJIT_UNLIKELY(o0.size() == 1))
          goto InvalidInstruction;

        opReg = o0.id();
        opcode.addPrefixBySize(o0.size());
        goto EmitX86OpReg;
      }
      break;

    case InstDB::kEncodingX86Bt:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opcode.addPrefixBySize(o1.size());
        opReg = o1.id();
        rbReg = o0.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.addPrefixBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }

      // The remaining instructions use the secondary opcode/r.
      immValue = o1.as<Imm>().value();
      immSize = 1;

      opcode = x86AltOpcodeOf(instInfo);
      opcode.addPrefixBySize(o0.size());
      opReg = opcode.extractModO();

      if (isign3 == ENC_OPS2(Reg, Imm)) {
        rbReg = o0.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Imm)) {
        if (ASMJIT_UNLIKELY(o0.size() == 0))
          goto AmbiguousOperandSize;

        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Call:
      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();
        goto EmitX86R;
      }

      rmRel = &o0;
      if (isign3 == ENC_OPS1(Mem))
        goto EmitX86M;

      // Call with 32-bit displacement use 0xE8 opcode. Call with 8-bit
      // displacement is not encodable so the alternative opcode field
      // in X86DB must be zero.
      opcode = 0xE8;
      opReg = 0;
      goto EmitJmpCall;

    case InstDB::kEncodingX86Cmpxchg: {
      // Convert explicit to implicit.
      if (isign3 & (0x7 << 6)) {
        if (!Reg::isGp(o2) || o2.id() != Gp::kIdAx)
          goto InvalidInstruction;
        isign3 &= 0x3F;
      }

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        if (o0.size() != o1.size())
          goto OperandSizeMismatch;

        opcode.addArithBySize(o0.size());
        rbReg = o0.id();
        opReg = o1.id();

        if (o0.size() != 1)
          goto EmitX86R;

        FIXUP_GPB(o0, rbReg);
        FIXUP_GPB(o1, opReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.addArithBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;

        if (o1.size() != 1)
          goto EmitX86M;

        FIXUP_GPB(o1, opReg);
        goto EmitX86M;
      }
      break;
    }

    case InstDB::kEncodingX86Cmpxchg8b_16b: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const Operand_& o4 = opExt[EmitterUtils::kOp4];

      if (isign3 == ENC_OPS3(Mem, Reg, Reg)) {
        if (o3.isReg() && o4.isReg()) {
          rmRel = &o0;
          goto EmitX86M;
        }
      }

      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        goto EmitX86M;
      }
      break;
    }

    case InstDB::kEncodingX86Crc:
      opReg = o0.id();
      opcode.addWBySize(o0.size());

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        rbReg = o1.id();

        if (o1.size() == 1) {
          FIXUP_GPB(o1, rbReg);
          goto EmitX86R;
        }
        else {
          // This seems to be the only exception of encoding '66F2' prefix.
          if (o1.size() == 2) writer.emit8(0x66);

          opcode.add(1);
          goto EmitX86R;
        }
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        rmRel = &o1;
        if (o1.size() == 0)
          goto AmbiguousOperandSize;

        // This seems to be the only exception of encoding '66F2' prefix.
        if (o1.size() == 2) writer.emit8(0x66);

        opcode += o1.size() != 1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Enter:
      if (isign3 == ENC_OPS2(Imm, Imm)) {
        uint32_t iw = o0.as<Imm>().valueAs<uint16_t>();
        uint32_t ib = o1.as<Imm>().valueAs<uint8_t>();

        immValue = iw | (ib << 16);
        immSize = 3;
        goto EmitX86Op;
      }
      break;

    case InstDB::kEncodingX86Imul:
      // First process all forms distinct of `kEncodingX86M_OptB_MulDiv`.
      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opcode = 0x6B;
        opcode.addPrefixBySize(o0.size());

        immValue = o2.as<Imm>().value();
        immSize = 1;

        if (!Support::isInt8(immValue) || (options & Inst::kOptionLongForm)) {
          opcode -= 2;
          immSize = o0.size() == 2 ? 2 : 4;
        }

        opReg = o0.id();
        rbReg = o1.id();

        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opcode = 0x6B;
        opcode.addPrefixBySize(o0.size());

        immValue = o2.as<Imm>().value();
        immSize = 1;

        // Sign extend so isInt8 returns the right result.
        if (o0.size() == 4)
          immValue = x86SignExtendI32<int64_t>(immValue);

        if (!Support::isInt8(immValue) || (options & Inst::kOptionLongForm)) {
          opcode -= 2;
          immSize = o0.size() == 2 ? 2 : 4;
        }

        opReg = o0.id();
        rmRel = &o1;

        goto EmitX86M;
      }

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        // Must be explicit 'ax, r8' form.
        if (o1.size() == 1)
          goto CaseX86M_GPB_MulDiv;

        if (o0.size() != o1.size())
          goto OperandSizeMismatch;

        opReg = o0.id();
        rbReg = o1.id();

        opcode = Opcode::k000F00 | 0xAF;
        opcode.addPrefixBySize(o0.size());
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        // Must be explicit 'ax, m8' form.
        if (o1.size() == 1)
          goto CaseX86M_GPB_MulDiv;

        opReg = o0.id();
        rmRel = &o1;

        opcode = Opcode::k000F00 | 0xAF;
        opcode.addPrefixBySize(o0.size());
        goto EmitX86M;
      }

      // Shorthand to imul 'reg, reg, imm'.
      if (isign3 == ENC_OPS2(Reg, Imm)) {
        opcode = 0x6B;
        opcode.addPrefixBySize(o0.size());

        immValue = o1.as<Imm>().value();
        immSize = 1;

        // Sign extend so isInt8 returns the right result.
        if (o0.size() == 4)
          immValue = x86SignExtendI32<int64_t>(immValue);

        if (!Support::isInt8(immValue) || (options & Inst::kOptionLongForm)) {
          opcode -= 2;
          immSize = o0.size() == 2 ? 2 : 4;
        }

        opReg = rbReg = o0.id();
        goto EmitX86R;
      }

      // Try implicit form.
      goto CaseX86M_GPB_MulDiv;

    case InstDB::kEncodingX86In:
      if (isign3 == ENC_OPS2(Reg, Imm)) {
        if (ASMJIT_UNLIKELY(o0.id() != Gp::kIdAx))
          goto InvalidInstruction;

        immValue = o1.as<Imm>().valueAs<uint8_t>();
        immSize = 1;

        opcode = x86AltOpcodeOf(instInfo) + (o0.size() != 1);
        opcode.add66hBySize(o0.size());
        goto EmitX86Op;
      }

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        if (ASMJIT_UNLIKELY(o0.id() != Gp::kIdAx || o1.id() != Gp::kIdDx))
          goto InvalidInstruction;

        opcode += o0.size() != 1;
        opcode.add66hBySize(o0.size());
        goto EmitX86Op;
      }
      break;

    case InstDB::kEncodingX86Ins:
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        if (ASMJIT_UNLIKELY(!x86IsImplicitMem(o0, Gp::kIdDi) || o1.id() != Gp::kIdDx))
          goto InvalidInstruction;

        uint32_t size = o0.size();
        if (ASMJIT_UNLIKELY(size == 0))
          goto AmbiguousOperandSize;

        rmRel = &o0;
        opcode += (size != 1);

        opcode.add66hBySize(size);
        goto EmitX86OpImplicitMem;
      }
      break;

    case InstDB::kEncodingX86IncDec:
      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();

        if (o0.size() == 1) {
          FIXUP_GPB(o0, rbReg);
          goto EmitX86R;
        }

        if (is32Bit()) {
          // INC r16|r32 is only encodable in 32-bit mode (collides with REX).
          opcode = x86AltOpcodeOf(instInfo) + (rbReg & 0x07);
          opcode.add66hBySize(o0.size());
          goto EmitX86Op;
        }
        else {
          opcode.addArithBySize(o0.size());
          goto EmitX86R;
        }
      }

      if (isign3 == ENC_OPS1(Mem)) {
        if (!o0.size())
          goto AmbiguousOperandSize;
        opcode.addArithBySize(o0.size());
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Int:
      if (isign3 == ENC_OPS1(Imm)) {
        immValue = o0.as<Imm>().value();
        immSize = 1;
        goto EmitX86Op;
      }
      break;

    case InstDB::kEncodingX86Jcc:
      if ((options & (Inst::kOptionTaken | Inst::kOptionNotTaken)) && hasEncodingOption(kEncodingOptionPredictedJumps)) {
        uint8_t prefix = (options & Inst::kOptionTaken) ? uint8_t(0x3E) : uint8_t(0x2E);
        writer.emit8(prefix);
      }

      rmRel = &o0;
      opReg = 0;
      goto EmitJmpCall;

    case InstDB::kEncodingX86JecxzLoop:
      rmRel = &o0;
      // Explicit jecxz|loop [r|e]cx, dst
      if (o0.isReg()) {
        if (ASMJIT_UNLIKELY(!Reg::isGp(o0, Gp::kIdCx)))
          goto InvalidInstruction;

        writer.emitAddressOverride((is32Bit() && o0.size() == 2) || (is64Bit() && o0.size() == 4));
        rmRel = &o1;
      }

      opReg = 0;
      goto EmitJmpCall;

    case InstDB::kEncodingX86Jmp:
      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();
        goto EmitX86R;
      }

      rmRel = &o0;
      if (isign3 == ENC_OPS1(Mem))
        goto EmitX86M;

      // Jump encoded with 32-bit displacement use 0xE9 opcode. Jump encoded
      // with 8-bit displacement's opcode is stored as an alternative opcode.
      opcode = 0xE9;
      opReg = 0;
      goto EmitJmpCall;

    case InstDB::kEncodingX86JmpRel:
      rmRel = &o0;
      goto EmitJmpCall;

    case InstDB::kEncodingX86LcallLjmp:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        uint32_t mSize = rmRel->size();
        if (mSize == 0) {
          mSize = registerSize();
        }
        else {
          mSize -= 2;
          if (mSize != 2 && mSize != 4 && mSize != registerSize())
            goto InvalidAddress;
        }
        opcode.addPrefixBySize(mSize);
        goto EmitX86M;
      }

      if (isign3 == ENC_OPS2(Imm, Imm)) {
        if (!is32Bit())
          goto InvalidInstruction;

        const Imm& imm0 = o0.as<Imm>();
        const Imm& imm1 = o1.as<Imm>();

        if (imm0.value() > 0xFFFFu || imm1.value() > 0xFFFFFFFFu)
          goto InvalidImmediate;

        opcode = x86AltOpcodeOf(instInfo);
        immValue = imm1.value() | (imm0.value() << 32);
        immSize = 6;
        goto EmitX86Op;
      }
      break;

    case InstDB::kEncodingX86Lea:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode.addPrefixBySize(o0.size());
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Mov:
      // Reg <- Reg
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        // Asmjit uses segment registers indexed from 1 to 6, leaving zero as
        // "no segment register used". We have to fix this (decrement the index
        // of the register) when emitting MOV instructions which move to/from
        // a segment register. The segment register is always `opReg`, because
        // the MOV instruction uses either RM or MR encoding.

        // GP <- ??
        if (Reg::isGp(o0)) {
          rbReg = o0.id();
          opReg = o1.id();

          // GP <- GP
          if (Reg::isGp(o1)) {
            uint32_t opSize = o0.size();
            if (opSize != o1.size()) {
              // TODO: [X86 Assembler] This is a non-standard extension, which should be removed.
              // We allow 'mov r64, r32' as it's basically zero-extend.
              if (opSize == 8 && o1.size() == 4)
                opSize = 4; // Zero extend, don't promote to 64-bit.
              else
                goto InvalidInstruction;
            }

            if (opSize == 1) {
              FIXUP_GPB(o0, rbReg);
              FIXUP_GPB(o1, opReg);
              opcode = 0x88;

              if (!(options & Inst::kOptionModRM))
                goto EmitX86R;

              opcode += 2;
              std::swap(opReg, rbReg);
              goto EmitX86R;
            }
            else {
              opcode = 0x89;
              opcode.addPrefixBySize(opSize);

              if (!(options & Inst::kOptionModRM))
                goto EmitX86R;

              opcode += 2;
              std::swap(opReg, rbReg);
              goto EmitX86R;
            }
          }

          // GP <- SReg
          if (Reg::isSReg(o1)) {
            opcode = 0x8C;
            opcode.addPrefixBySize(o0.size());
            opReg--;
            goto EmitX86R;
          }

          // GP <- CReg
          if (Reg::isCReg(o1)) {
            opcode = Opcode::k000F00 | 0x20;

            // Use `LOCK MOV` in 32-bit mode if CR8+ register is accessed (AMD extension).
            if ((opReg & 0x8) && is32Bit()) {
              writer.emit8(0xF0);
              opReg &= 0x7;
            }
            goto EmitX86R;
          }

          // GP <- DReg
          if (Reg::isDReg(o1)) {
            opcode = Opcode::k000F00 | 0x21;
            goto EmitX86R;
          }
        }
        else {
          opReg = o0.id();
          rbReg = o1.id();

          // ?? <- GP
          if (!Reg::isGp(o1))
            goto InvalidInstruction;

          // SReg <- GP
          if (Reg::isSReg(o0)) {
            opcode = 0x8E;
            opcode.addPrefixBySize(o1.size());
            opReg--;
            goto EmitX86R;
          }

          // CReg <- GP
          if (Reg::isCReg(o0)) {
            opcode = Opcode::k000F00 | 0x22;

            // Use `LOCK MOV` in 32-bit mode if CR8+ register is accessed (AMD extension).
            if ((opReg & 0x8) && is32Bit()) {
              writer.emit8(0xF0);
              opReg &= 0x7;
            }
            goto EmitX86R;
          }

          // DReg <- GP
          if (Reg::isDReg(o0)) {
            opcode = Opcode::k000F00 | 0x23;
            goto EmitX86R;
          }
        }

        goto InvalidInstruction;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;

        // SReg <- Mem
        if (Reg::isSReg(o0)) {
          opcode = 0x8E;
          opcode.addPrefixBySize(o1.size());
          opReg--;
          goto EmitX86M;
        }
        // Reg <- Mem
        else {
          opcode = 0;
          opcode.addArithBySize(o0.size());

          // Handle a special form of `mov al|ax|eax|rax, [ptr64]` that doesn't use MOD.
          if (opReg == Gp::kIdAx && !rmRel->as<Mem>().hasBaseOrIndex()) {
            if (x86ShouldUseMovabs(this, writer, o0.size(), options, rmRel->as<Mem>())) {
              opcode += 0xA0;
              immValue = rmRel->as<Mem>().offset();
              goto EmitX86OpMovAbs;
            }
          }

          if (o0.size() == 1)
            FIXUP_GPB(o0, opReg);

          opcode += 0x8A;
          goto EmitX86M;
        }
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;

        // Mem <- SReg
        if (Reg::isSReg(o1)) {
          opcode = 0x8C;
          opcode.addPrefixBySize(o0.size());
          opReg--;
          goto EmitX86M;
        }
        // Mem <- Reg
        else {
          opcode = 0;
          opcode.addArithBySize(o1.size());

          // Handle a special form of `mov [ptr64], al|ax|eax|rax` that doesn't use MOD.
          if (opReg == Gp::kIdAx && !rmRel->as<Mem>().hasBaseOrIndex()) {
            if (x86ShouldUseMovabs(this, writer, o1.size(), options, rmRel->as<Mem>())) {
              opcode += 0xA2;
              immValue = rmRel->as<Mem>().offset();
              goto EmitX86OpMovAbs;
            }
          }

          if (o1.size() == 1)
            FIXUP_GPB(o1, opReg);

          opcode += 0x88;
          goto EmitX86M;
        }
      }

      if (isign3 == ENC_OPS2(Reg, Imm)) {
        opReg = o0.id();
        immSize = FastUInt8(o0.size());

        if (immSize == 1) {
          FIXUP_GPB(o0, opReg);

          opcode = 0xB0;
          immValue = o1.as<Imm>().valueAs<uint8_t>();
          goto EmitX86OpReg;
        }
        else {
          // 64-bit immediate in 64-bit mode is allowed.
          immValue = o1.as<Imm>().value();

          // Optimize the instruction size by using a 32-bit immediate if possible.
          if (immSize == 8 && !(options & Inst::kOptionLongForm)) {
            if (Support::isUInt32(immValue) && hasEncodingOption(kEncodingOptionOptimizeForSize)) {
              // Zero-extend by using a 32-bit GPD destination instead of a 64-bit GPQ.
              immSize = 4;
            }
            else if (Support::isInt32(immValue)) {
              // Sign-extend, uses 'C7 /0' opcode.
              rbReg = opReg;

              opcode = Opcode::kW | 0xC7;
              opReg = 0;

              immSize = 4;
              goto EmitX86R;
            }
          }

          opcode = 0xB8;
          opcode.addPrefixBySize(immSize);
          goto EmitX86OpReg;
        }
      }

      if (isign3 == ENC_OPS2(Mem, Imm)) {
        uint32_t memSize = o0.size();
        if (ASMJIT_UNLIKELY(memSize == 0))
          goto AmbiguousOperandSize;

        opcode = 0xC6 + (memSize != 1);
        opcode.addPrefixBySize(memSize);
        opReg = 0;
        rmRel = &o0;

        immValue = o1.as<Imm>().value();
        immSize = FastUInt8(Support::min<uint32_t>(memSize, 4));
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Movabs:
      // Reg <- Mem
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;

        opcode = 0xA0;
        opcode.addArithBySize(o0.size());

        if (ASMJIT_UNLIKELY(!o0.as<Reg>().isGp()) || opReg != Gp::kIdAx)
          goto InvalidInstruction;

        if (ASMJIT_UNLIKELY(rmRel->as<Mem>().hasBaseOrIndex()))
          goto InvalidAddress;

        if (ASMJIT_UNLIKELY(rmRel->as<Mem>().addrType() == Mem::kAddrTypeRel))
          goto InvalidAddress;

        immValue = rmRel->as<Mem>().offset();
        goto EmitX86OpMovAbs;
      }

      // Mem <- Reg
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;

        opcode = 0xA2;
        opcode.addArithBySize(o1.size());

        if (ASMJIT_UNLIKELY(!o1.as<Reg>().isGp()) || opReg != Gp::kIdAx)
          goto InvalidInstruction;

        if (ASMJIT_UNLIKELY(rmRel->as<Mem>().hasBaseOrIndex()))
          goto InvalidAddress;

        immValue = rmRel->as<Mem>().offset();
        goto EmitX86OpMovAbs;
      }

      // Reg <- Imm.
      if (isign3 == ENC_OPS2(Reg, Imm)) {
        if (ASMJIT_UNLIKELY(!o0.as<Reg>().isGpq()))
          goto InvalidInstruction;

        opReg = o0.id();
        opcode = 0xB8;

        immSize = 8;
        immValue = o1.as<Imm>().value();

        opcode.addPrefixBySize(8);
        goto EmitX86OpReg;
      }
      break;

    case InstDB::kEncodingX86MovsxMovzx:
      opcode.add(o1.size() != 1);
      opcode.addPrefixBySize(o0.size());

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        if (o1.size() != 1)
          goto EmitX86R;

        FIXUP_GPB(o1, rbReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86MovntiMovdiri:
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.addWIf(Reg::isGpq(o1));

        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86EnqcmdMovdir64b:
      if (isign3 == ENC_OPS2(Mem, Mem)) {
        const Mem& m0 = o0.as<Mem>();
        // This is the only required validation, the rest is handled afterwards.
        if (ASMJIT_UNLIKELY(m0.baseType() != o1.as<Mem>().baseType() ||
                            m0.hasIndex() ||
                            m0.hasOffset() ||
                            (m0.hasSegment() && m0.segmentId() != SReg::kIdEs)))
          goto InvalidInstruction;

        // The first memory operand is passed via register, the second memory operand is RM.
        opReg = o0.as<Mem>().baseId();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Out:
      if (isign3 == ENC_OPS2(Imm, Reg)) {
        if (ASMJIT_UNLIKELY(o1.id() != Gp::kIdAx))
          goto InvalidInstruction;

        opcode = x86AltOpcodeOf(instInfo) + (o1.size() != 1);
        opcode.add66hBySize(o1.size());

        immValue = o0.as<Imm>().valueAs<uint8_t>();
        immSize = 1;
        goto EmitX86Op;
      }

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        if (ASMJIT_UNLIKELY(o0.id() != Gp::kIdDx || o1.id() != Gp::kIdAx))
          goto InvalidInstruction;

        opcode.add(o1.size() != 1);
        opcode.add66hBySize(o1.size());
        goto EmitX86Op;
      }
      break;

    case InstDB::kEncodingX86Outs:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        if (ASMJIT_UNLIKELY(o0.id() != Gp::kIdDx || !x86IsImplicitMem(o1, Gp::kIdSi)))
          goto InvalidInstruction;

        uint32_t size = o1.size();
        if (ASMJIT_UNLIKELY(size == 0))
          goto AmbiguousOperandSize;

        rmRel = &o1;
        opcode.add(size != 1);
        opcode.add66hBySize(size);
        goto EmitX86OpImplicitMem;
      }
      break;

    case InstDB::kEncodingX86Push:
      if (isign3 == ENC_OPS1(Reg)) {
        if (Reg::isSReg(o0)) {
          uint32_t segment = o0.id();
          if (ASMJIT_UNLIKELY(segment >= SReg::kIdCount))
            goto InvalidSegment;

          opcode = x86OpcodePushSReg[segment];
          goto EmitX86Op;
        }
        else {
          goto CaseX86PushPop_Gp;
        }
      }

      if (isign3 == ENC_OPS1(Imm)) {
        immValue = o0.as<Imm>().value();
        immSize = 4;

        if (Support::isInt8(immValue) && !(options & Inst::kOptionLongForm))
          immSize = 1;

        opcode = immSize == 1 ? 0x6A : 0x68;
        goto EmitX86Op;
      }
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86Pop:
      if (isign3 == ENC_OPS1(Reg)) {
        if (Reg::isSReg(o0)) {
          uint32_t segment = o0.id();
          if (ASMJIT_UNLIKELY(segment == SReg::kIdCs || segment >= SReg::kIdCount))
            goto InvalidSegment;

          opcode = x86OpcodePopSReg[segment];
          goto EmitX86Op;
        }
        else {
CaseX86PushPop_Gp:
          // We allow 2 byte, 4 byte, and 8 byte register sizes, although PUSH
          // and POP only allow 2 bytes or native size. On 64-bit we simply
          // PUSH/POP 64-bit register even if 32-bit register was given.
          if (ASMJIT_UNLIKELY(o0.size() < 2))
            goto InvalidInstruction;

          opcode = x86AltOpcodeOf(instInfo);
          opcode.add66hBySize(o0.size());
          opReg = o0.id();
          goto EmitX86OpReg;
        }
      }

      if (isign3 == ENC_OPS1(Mem)) {
        if (ASMJIT_UNLIKELY(o0.size() == 0))
          goto AmbiguousOperandSize;

        if (ASMJIT_UNLIKELY(o0.size() != 2 && o0.size() != registerSize()))
          goto InvalidInstruction;

        opcode.add66hBySize(o0.size());
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Ret:
      if (isign3 == 0) {
        // 'ret' without immediate, change C2 to C3.
        opcode.add(1);
        goto EmitX86Op;
      }

      if (isign3 == ENC_OPS1(Imm)) {
        immValue = o0.as<Imm>().value();
        if (immValue == 0 && !(options & Inst::kOptionLongForm)) {
          // 'ret' without immediate, change C2 to C3.
          opcode.add(1);
          goto EmitX86Op;
        }
        else {
          immSize = 2;
          goto EmitX86Op;
        }
      }
      break;

    case InstDB::kEncodingX86Rot:
      if (o0.isReg()) {
        opcode.addArithBySize(o0.size());
        rbReg = o0.id();

        if (o0.size() == 1)
          FIXUP_GPB(o0, rbReg);

        if (isign3 == ENC_OPS2(Reg, Reg)) {
          if (ASMJIT_UNLIKELY(o1.id() != Gp::kIdCx))
            goto InvalidInstruction;

          opcode += 2;
          goto EmitX86R;
        }

        if (isign3 == ENC_OPS2(Reg, Imm)) {
          immValue = o1.as<Imm>().value() & 0xFF;
          immSize = 0;

          if (immValue == 1 && !(options & Inst::kOptionLongForm))
            goto EmitX86R;

          opcode -= 0x10;
          immSize = 1;
          goto EmitX86R;
        }
      }
      else {
        if (ASMJIT_UNLIKELY(o0.size() == 0))
          goto AmbiguousOperandSize;
        opcode.addArithBySize(o0.size());

        if (isign3 == ENC_OPS2(Mem, Reg)) {
          if (ASMJIT_UNLIKELY(o1.id() != Gp::kIdCx))
            goto InvalidInstruction;

          opcode += 2;
          rmRel = &o0;
          goto EmitX86M;
        }

        if (isign3 == ENC_OPS2(Mem, Imm)) {
          rmRel = &o0;
          immValue = o1.as<Imm>().value() & 0xFF;
          immSize = 0;

          if (immValue == 1 && !(options & Inst::kOptionLongForm))
            goto EmitX86M;

          opcode -= 0x10;
          immSize = 1;
          goto EmitX86M;
        }
      }
      break;

    case InstDB::kEncodingX86Set:
      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();
        FIXUP_GPB(o0, rbReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86ShldShrd:
      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opcode.addPrefixBySize(o0.size());
        opReg = o1.id();
        rbReg = o0.id();

        immValue = o2.as<Imm>().value();
        immSize = 1;
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Mem, Reg, Imm)) {
        opcode.addPrefixBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;

        immValue = o2.as<Imm>().value();
        immSize = 1;
        goto EmitX86M;
      }

      // The following instructions use opcode + 1.
      opcode.add(1);

      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        if (ASMJIT_UNLIKELY(o2.id() != Gp::kIdCx))
          goto InvalidInstruction;

        opcode.addPrefixBySize(o0.size());
        opReg = o1.id();
        rbReg = o0.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Mem, Reg, Reg)) {
        if (ASMJIT_UNLIKELY(o2.id() != Gp::kIdCx))
          goto InvalidInstruction;

        opcode.addPrefixBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86StrRm:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        rmRel = &o1;
        if (ASMJIT_UNLIKELY(rmRel->as<Mem>().offsetLo32() || !Reg::isGp(o0.as<Reg>(), Gp::kIdAx)))
          goto InvalidInstruction;

        uint32_t size = o0.size();
        if (o1.hasSize() && ASMJIT_UNLIKELY(o1.size() != size))
          goto OperandSizeMismatch;

        opcode.addArithBySize(size);
        goto EmitX86OpImplicitMem;
      }
      break;

    case InstDB::kEncodingX86StrMr:
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        rmRel = &o0;
        if (ASMJIT_UNLIKELY(rmRel->as<Mem>().offsetLo32() || !Reg::isGp(o1.as<Reg>(), Gp::kIdAx)))
          goto InvalidInstruction;

        uint32_t size = o1.size();
        if (o0.hasSize() && ASMJIT_UNLIKELY(o0.size() != size))
          goto OperandSizeMismatch;

        opcode.addArithBySize(size);
        goto EmitX86OpImplicitMem;
      }
      break;

    case InstDB::kEncodingX86StrMm:
      if (isign3 == ENC_OPS2(Mem, Mem)) {
        if (ASMJIT_UNLIKELY(o0.as<Mem>().baseAndIndexTypes() !=
                            o1.as<Mem>().baseAndIndexTypes()))
          goto InvalidInstruction;

        rmRel = &o1;
        if (ASMJIT_UNLIKELY(o0.as<Mem>().hasOffset()))
          goto InvalidInstruction;

        uint32_t size = o1.size();
        if (ASMJIT_UNLIKELY(size == 0))
          goto AmbiguousOperandSize;

        if (ASMJIT_UNLIKELY(o0.size() != size))
          goto OperandSizeMismatch;

        opcode.addArithBySize(size);
        goto EmitX86OpImplicitMem;
      }
      break;

    case InstDB::kEncodingX86Test:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        if (o0.size() != o1.size())
          goto OperandSizeMismatch;

        opcode.addArithBySize(o0.size());
        rbReg = o0.id();
        opReg = o1.id();

        if (o0.size() != 1)
          goto EmitX86R;

        FIXUP_GPB(o0, rbReg);
        FIXUP_GPB(o1, opReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.addArithBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;

        if (o1.size() != 1)
          goto EmitX86M;

        FIXUP_GPB(o1, opReg);
        goto EmitX86M;
      }

      // The following instructions use the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);
      opReg = opcode.extractModO();

      if (isign3 == ENC_OPS2(Reg, Imm)) {
        opcode.addArithBySize(o0.size());
        rbReg = o0.id();

        if (o0.size() == 1) {
          FIXUP_GPB(o0, rbReg);
          immValue = o1.as<Imm>().valueAs<uint8_t>();
          immSize = 1;
        }
        else {
          immValue = o1.as<Imm>().value();
          immSize = FastUInt8(Support::min<uint32_t>(o0.size(), 4));
        }

        // Short form - AL, AX, EAX, RAX.
        if (rbReg == 0 && !(options & Inst::kOptionLongForm)) {
          opcode &= Opcode::kPP_66 | Opcode::kW;
          opcode |= 0xA8 + (o0.size() != 1);
          goto EmitX86Op;
        }

        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Imm)) {
        if (ASMJIT_UNLIKELY(o0.size() == 0))
          goto AmbiguousOperandSize;

        opcode.addArithBySize(o0.size());
        rmRel = &o0;

        immValue = o1.as<Imm>().value();
        immSize = FastUInt8(Support::min<uint32_t>(o0.size(), 4));
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Xchg:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode.addArithBySize(o0.size());
        opReg = o0.id();
        rmRel = &o1;

        if (o0.size() != 1)
          goto EmitX86M;

        FIXUP_GPB(o0, opReg);
        goto EmitX86M;
      }
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingX86Xadd:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        rbReg = o0.id();
        opReg = o1.id();

        uint32_t opSize = o0.size();
        if (opSize != o1.size())
          goto OperandSizeMismatch;

        if (opSize == 1) {
          FIXUP_GPB(o0, rbReg);
          FIXUP_GPB(o1, opReg);
          goto EmitX86R;
        }

        // Special cases for 'xchg ?ax, reg'.
        if (instId == Inst::kIdXchg && (opReg == 0 || rbReg == 0)) {
          if (is64Bit() && opReg == rbReg && opSize >= 4) {
            if (opSize == 8) {
              // Encode 'xchg rax, rax' as '90' (REX and other prefixes are optional).
              opcode &= Opcode::kW;
              opcode |= 0x90;
              goto EmitX86OpReg;
            }
            else {
              // Encode 'xchg eax, eax' by by using a generic path.
            }
          }
          else if (!(options & Inst::kOptionLongForm)) {
            // The special encoding encodes only one register, which is non-zero.
            opReg += rbReg;

            opcode.addArithBySize(opSize);
            opcode &= Opcode::kW | Opcode::kPP_66;
            opcode |= 0x90;
            goto EmitX86OpReg;
          }
        }

        opcode.addArithBySize(opSize);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.addArithBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;

        if (o1.size() == 1) {
          FIXUP_GPB(o1, opReg);
        }

        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingX86Fence:
      rbReg = 0;
      goto EmitX86R;

    case InstDB::kEncodingX86Bndmov:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        // ModRM encoding:
        if (!(options & Inst::kOptionModMR))
          goto EmitX86R;

        // ModMR encoding:
        opcode = x86AltOpcodeOf(instInfo);
        std::swap(opReg, rbReg);
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode = x86AltOpcodeOf(instInfo);

        rmRel = &o0;
        opReg = o1.id();
        goto EmitX86M;
      }
      break;

    // ------------------------------------------------------------------------
    // [FPU]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingFpuOp:
      goto EmitFpuOp;

    case InstDB::kEncodingFpuArith:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        // We switch to the alternative opcode if the first operand is zero.
        if (opReg == 0) {
CaseFpuArith_Reg:
          opcode = ((0xD8   << Opcode::kFPU_2B_Shift)       ) +
                   ((opcode >> Opcode::kFPU_2B_Shift) & 0xFF) + rbReg;
          goto EmitFpuOp;
        }
        else if (rbReg == 0) {
          rbReg = opReg;
          opcode = ((0xDC   << Opcode::kFPU_2B_Shift)       ) +
                   ((opcode                         ) & 0xFF) + rbReg;
          goto EmitFpuOp;
        }
        else {
          goto InvalidInstruction;
        }
      }

      if (isign3 == ENC_OPS1(Mem)) {
CaseFpuArith_Mem:
        // 0xD8/0xDC, depends on the size of the memory operand; opReg is valid.
        opcode = (o0.size() == 4) ? 0xD8 : 0xDC;
        // Clear compressed displacement before going to EmitX86M.
        opcode &= ~uint32_t(Opcode::kCDSHL_Mask);

        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingFpuCom:
      if (isign3 == 0) {
        rbReg = 1;
        goto CaseFpuArith_Reg;
      }

      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();
        goto CaseFpuArith_Reg;
      }

      if (isign3 == ENC_OPS1(Mem)) {
        goto CaseFpuArith_Mem;
      }
      break;

    case InstDB::kEncodingFpuFldFst:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;

        if (o0.size() == 4 && commonInfo->hasFlag(InstDB::kFlagFpuM32)) {
          goto EmitX86M;
        }

        if (o0.size() == 8 && commonInfo->hasFlag(InstDB::kFlagFpuM64)) {
          opcode += 4;
          goto EmitX86M;
        }

        if (o0.size() == 10 && commonInfo->hasFlag(InstDB::kFlagFpuM80)) {
          opcode = x86AltOpcodeOf(instInfo);
          opReg  = opcode.extractModO();
          goto EmitX86M;
        }
      }

      if (isign3 == ENC_OPS1(Reg)) {
        if (instId == Inst::kIdFld ) { opcode = (0xD9 << Opcode::kFPU_2B_Shift) + 0xC0 + o0.id(); goto EmitFpuOp; }
        if (instId == Inst::kIdFst ) { opcode = (0xDD << Opcode::kFPU_2B_Shift) + 0xD0 + o0.id(); goto EmitFpuOp; }
        if (instId == Inst::kIdFstp) { opcode = (0xDD << Opcode::kFPU_2B_Shift) + 0xD8 + o0.id(); goto EmitFpuOp; }
      }
      break;

    case InstDB::kEncodingFpuM:
      if (isign3 == ENC_OPS1(Mem)) {
        // Clear compressed displacement before going to EmitX86M.
        opcode &= ~uint32_t(Opcode::kCDSHL_Mask);

        rmRel = &o0;
        if (o0.size() == 2 && commonInfo->hasFlag(InstDB::kFlagFpuM16)) {
          opcode += 4;
          goto EmitX86M;
        }

        if (o0.size() == 4 && commonInfo->hasFlag(InstDB::kFlagFpuM32)) {
          goto EmitX86M;
        }

        if (o0.size() == 8 && commonInfo->hasFlag(InstDB::kFlagFpuM64)) {
          opcode = x86AltOpcodeOf(instInfo) & ~uint32_t(Opcode::kCDSHL_Mask);
          opReg  = opcode.extractModO();
          goto EmitX86M;
        }
      }
      break;

    case InstDB::kEncodingFpuRDef:
      if (isign3 == 0) {
        opcode += 1;
        goto EmitFpuOp;
      }
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingFpuR:
      if (isign3 == ENC_OPS1(Reg)) {
        opcode += o0.id();
        goto EmitFpuOp;
      }
      break;

    case InstDB::kEncodingFpuStsw:
      if (isign3 == ENC_OPS1(Reg)) {
        if (ASMJIT_UNLIKELY(o0.id() != Gp::kIdAx))
          goto InvalidInstruction;

        opcode = x86AltOpcodeOf(instInfo);
        goto EmitFpuOp;
      }

      if (isign3 == ENC_OPS1(Mem)) {
        // Clear compressed displacement before going to EmitX86M.
        opcode &= ~uint32_t(Opcode::kCDSHL_Mask);

        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    // ------------------------------------------------------------------------
    // [Ext]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingExtPextrw:
      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opcode.add66hIf(Reg::isXmm(o1));

        immValue = o2.as<Imm>().value();
        immSize = 1;

        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Mem, Reg, Imm)) {
        // Secondary opcode of 'pextrw' instruction (SSE4.1).
        opcode = x86AltOpcodeOf(instInfo);
        opcode.add66hIf(Reg::isXmm(o1));

        immValue = o2.as<Imm>().value();
        immSize = 1;

        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtExtract:
      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opcode.add66hIf(Reg::isXmm(o1));

        immValue = o2.as<Imm>().value();
        immSize = 1;

        opReg = o1.id();
        rbReg = o0.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Mem, Reg, Imm)) {
        opcode.add66hIf(Reg::isXmm(o1));

        immValue = o2.as<Imm>().value();
        immSize = 1;

        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtMov:
      // GP|MM|XMM <- GP|MM|XMM
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        if (!(options & Inst::kOptionModMR) || !instInfo->_altOpcodeIndex)
          goto EmitX86R;

        opcode = x86AltOpcodeOf(instInfo);
        std::swap(opReg, rbReg);
        goto EmitX86R;
      }

      // GP|MM|XMM <- Mem
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }

      // The following instruction uses opcode[1].
      opcode = x86AltOpcodeOf(instInfo);

      // Mem <- GP|MM|XMM
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtMovbe:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        if (o0.size() == 1)
          goto InvalidInstruction;

        opcode.addPrefixBySize(o0.size());
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }

      // The following instruction uses the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        if (o1.size() == 1)
          goto InvalidInstruction;

        opcode.addPrefixBySize(o1.size());
        opReg = o1.id();
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtMovd:
CaseExtMovd:
      opReg = o0.id();
      opcode.add66hIf(Reg::isXmm(o0));

      // MM/XMM <- Gp
      if (isign3 == ENC_OPS2(Reg, Reg) && Reg::isGp(o1)) {
        rbReg = o1.id();
        goto EmitX86R;
      }

      // MM/XMM <- Mem
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        rmRel = &o1;
        goto EmitX86M;
      }

      // The following instructions use the secondary opcode.
      opcode &= Opcode::kW;
      opcode |= x86AltOpcodeOf(instInfo);
      opReg = o1.id();
      opcode.add66hIf(Reg::isXmm(o1));

      // GP <- MM/XMM
      if (isign3 == ENC_OPS2(Reg, Reg) && Reg::isGp(o0)) {
        rbReg = o0.id();
        goto EmitX86R;
      }

      // Mem <- MM/XMM
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        rmRel = &o0;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtMovq:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        // MM <- MM
        if (Reg::isMm(o0) && Reg::isMm(o1)) {
          opcode = Opcode::k000F00 | 0x6F;

          if (!(options & Inst::kOptionModMR))
            goto EmitX86R;

          opcode += 0x10;
          std::swap(opReg, rbReg);
          goto EmitX86R;
        }

        // XMM <- XMM
        if (Reg::isXmm(o0) && Reg::isXmm(o1)) {
          opcode = Opcode::kF30F00 | 0x7E;

          if (!(options & Inst::kOptionModMR))
            goto EmitX86R;

          opcode = Opcode::k660F00 | 0xD6;
          std::swap(opReg, rbReg);
          goto EmitX86R;
        }
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;

        // MM <- Mem
        if (Reg::isMm(o0)) {
          opcode = Opcode::k000F00 | 0x6F;
          goto EmitX86M;
        }

        // XMM <- Mem
        if (Reg::isXmm(o0)) {
          opcode = Opcode::kF30F00 | 0x7E;
          goto EmitX86M;
        }
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;

        // Mem <- MM
        if (Reg::isMm(o1)) {
          opcode = Opcode::k000F00 | 0x7F;
          goto EmitX86M;
        }

        // Mem <- XMM
        if (Reg::isXmm(o1)) {
          opcode = Opcode::k660F00 | 0xD6;
          goto EmitX86M;
        }
      }

      // MOVQ in other case is simply a MOVD instruction promoted to 64-bit.
      opcode |= Opcode::kW;
      goto CaseExtMovd;

    case InstDB::kEncodingExtRm_XMM0:
      if (ASMJIT_UNLIKELY(!o2.isNone() && !Reg::isXmm(o2, 0)))
        goto InvalidInstruction;

      isign3 &= 0x3F;
      goto CaseExtRm;

    case InstDB::kEncodingExtRm_ZDI:
      if (ASMJIT_UNLIKELY(!o2.isNone() && !x86IsImplicitMem(o2, Gp::kIdDi)))
        goto InvalidInstruction;

      isign3 &= 0x3F;
      goto CaseExtRm;

    case InstDB::kEncodingExtRm_Wx:
      opcode.addWIf(o1.size() == 8);
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingExtRm_Wx_GpqOnly:
      opcode.addWIf(Reg::isGpq(o0));
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingExtRm:
CaseExtRm:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtRm_P:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opcode.add66hIf(Reg::isXmm(o0) | Reg::isXmm(o1));

        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode.add66hIf(Reg::isXmm(o0));

        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtRmRi:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }

      // The following instruction uses the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);
      opReg  = opcode.extractModO();

      if (isign3 == ENC_OPS2(Reg, Imm)) {
        immValue = o1.as<Imm>().value();
        immSize = 1;

        rbReg = o0.id();
        goto EmitX86R;
      }
      break;

    case InstDB::kEncodingExtRmRi_P:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opcode.add66hIf(Reg::isXmm(o0) | Reg::isXmm(o1));

        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode.add66hIf(Reg::isXmm(o0));

        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }

      // The following instruction uses the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);
      opReg  = opcode.extractModO();

      if (isign3 == ENC_OPS2(Reg, Imm)) {
        opcode.add66hIf(Reg::isXmm(o0));

        immValue = o1.as<Imm>().value();
        immSize = 1;

        rbReg = o0.id();
        goto EmitX86R;
      }
      break;

    case InstDB::kEncodingExtRmi:
      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    case InstDB::kEncodingExtRmi_P:
      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opcode.add66hIf(Reg::isXmm(o0) | Reg::isXmm(o1));

        opReg = o0.id();
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opcode.add66hIf(Reg::isXmm(o0));

        opReg = o0.id();
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    // ------------------------------------------------------------------------
    // [Extrq / Insertq (SSE4A)]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingExtExtrq:
      opReg = o0.id();
      rbReg = o1.id();

      if (isign3 == ENC_OPS2(Reg, Reg))
        goto EmitX86R;

      if (isign3 == ENC_OPS3(Reg, Imm, Imm)) {
        // This variant of the instruction uses the secondary opcode.
        opcode = x86AltOpcodeOf(instInfo);
        rbReg = opReg;
        opReg = opcode.extractModO();

        immValue = (uint32_t(o1.as<Imm>().valueAs<uint8_t>())     ) +
                   (uint32_t(o2.as<Imm>().valueAs<uint8_t>()) << 8) ;
        immSize = 2;
        goto EmitX86R;
      }
      break;

    case InstDB::kEncodingExtInsertq: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const uint32_t isign4 = isign3 + (o3.opType() << 9);

      opReg = o0.id();
      rbReg = o1.id();

      if (isign4 == ENC_OPS2(Reg, Reg))
        goto EmitX86R;

      if (isign4 == ENC_OPS4(Reg, Reg, Imm, Imm)) {
        // This variant of the instruction uses the secondary opcode.
        opcode = x86AltOpcodeOf(instInfo);

        immValue = (uint32_t(o2.as<Imm>().valueAs<uint8_t>())     ) +
                   (uint32_t(o3.as<Imm>().valueAs<uint8_t>()) << 8) ;
        immSize = 2;
        goto EmitX86R;
      }
      break;
    }

    // ------------------------------------------------------------------------
    // [3dNow]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingExt3dNow:
      // Every 3dNow instruction starts with 0x0F0F and the actual opcode is
      // stored as 8-bit immediate.
      immValue = opcode.v & 0xFFu;
      immSize = 1;

      opcode = Opcode::k000F00 | 0x0F;
      opReg = o0.id();

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        rbReg = o1.id();
        goto EmitX86R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        rmRel = &o1;
        goto EmitX86M;
      }
      break;

    // ------------------------------------------------------------------------
    // [VEX/EVEX]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingVexOp:
      goto EmitVexEvexOp;

    case InstDB::kEncodingVexOpMod:
      rbReg = 0;
      goto EmitVexEvexR;

    case InstDB::kEncodingVexKmov:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();

        // Form 'k, reg'.
        if (Reg::isGp(o1)) {
          opcode = x86AltOpcodeOf(instInfo);
          goto EmitVexEvexR;
        }

        // Form 'reg, k'.
        if (Reg::isGp(o0)) {
          opcode = x86AltOpcodeOf(instInfo) + 1;
          goto EmitVexEvexR;
        }

        // Form 'k, k'.
        if (!(options & Inst::kOptionModMR))
          goto EmitVexEvexR;

        opcode.add(1);
        std::swap(opReg, rbReg);
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;

        goto EmitVexEvexM;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode.add(1);
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexR_Wx:
      if (isign3 == ENC_OPS1(Reg)) {
        rbReg = o0.id();
        opcode.addWIf(o0.as<Reg>().isGpq());
        goto EmitVexEvexR;
      }
      break;

    case InstDB::kEncodingVexM:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexM_VM:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexMr_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o1.id();
        rbReg = o0.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexMr_VM:
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode |= Support::max(x86OpcodeLByVMem(o0), x86OpcodeLBySize(o1.size()));

        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexMri_Vpextrw:
      // Use 'vpextrw reg, xmm1, i8' when possible.
      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opcode = Opcode::k660F00 | 0xC5;

        opReg = o0.id();
        rbReg = o1.id();

        immValue = o2.as<Imm>().value();
        immSize = 1;
        goto EmitVexEvexR;
      }

      goto CaseVexMri;

    case InstDB::kEncodingVexMri_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexMri:
CaseVexMri:
      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = o1.id();
        rbReg = o0.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Mem, Reg, Imm)) {
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRm_ZDI:
      if (ASMJIT_UNLIKELY(!o2.isNone() && !x86IsImplicitMem(o2, Gp::kIdDi)))
        goto InvalidInstruction;

      isign3 &= 0x3F;
      goto CaseVexRm;

    case InstDB::kEncodingVexRm_Wx:
      opcode.addWIf(Reg::isGpq(o0) | Reg::isGpq(o1));
      goto CaseVexRm;

    case InstDB::kEncodingVexRm_Lx_Narrow:
      if (o1.size())
        opcode |= x86OpcodeLBySize(o1.size());
      else if (o0.size() == 32)
        opcode |= Opcode::kLL_2;
      goto CaseVexRm;

    case InstDB::kEncodingVexRm_Lx_Bcst:
      if (isign3 == ENC_OPS2(Reg, Reg) && Reg::isGp(o1.as<Reg>())) {
        opcode = x86AltOpcodeOf(instInfo) | x86OpcodeLBySize(o0.size() | o1.size());
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitVexEvexR;
      }
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRm_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRm:
CaseVexRm:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRm_VM:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode |= Support::max(x86OpcodeLByVMem(o1), x86OpcodeLBySize(o0.size()));
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRm_T1_4X: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const Operand_& o4 = opExt[EmitterUtils::kOp4];
      const Operand_& o5 = opExt[EmitterUtils::kOp5];

      if (Reg::isVec(o0) && Reg::isVec(o1) && Reg::isVec(o2) && Reg::isVec(o3) && Reg::isVec(o4) && o5.isMem()) {
        // Registers [o1, o2, o3, o4] must start aligned and must be consecutive.
        uint32_t i1 = o1.id();
        uint32_t i2 = o2.id();
        uint32_t i3 = o3.id();
        uint32_t i4 = o4.id();

        if (ASMJIT_UNLIKELY((i1 & 0x3) != 0 || i2 != i1 + 1 || i3 != i1 + 2 || i4 != i1 + 3))
          goto NotConsecutiveRegs;

        opReg = x86PackRegAndVvvvv(o0.id(), i1);
        rmRel = &o5;
        goto EmitVexEvexM;
      }
      break;
    }

    case InstDB::kEncodingVexRmi_Wx:
      opcode.addWIf(Reg::isGpq(o0) | Reg::isGpq(o1));
      goto CaseVexRmi;

    case InstDB::kEncodingVexRmi_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRmi:
CaseVexRmi:
      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvm:
CaseVexRvm:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
CaseVexRvm_R:
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvm_ZDX_Wx: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      if (ASMJIT_UNLIKELY(!o3.isNone() && !Reg::isGp(o3, Gp::kIdDx)))
        goto InvalidInstruction;
      ASMJIT_FALLTHROUGH;
    }

    case InstDB::kEncodingVexRvm_Wx: {
      opcode.addWIf(Reg::isGpq(o0) | (o2.size() == 8));
      goto CaseVexRvm;
    }

    case InstDB::kEncodingVexRvm_Lx_KEvex: {
      opcode.forceEvexIf(Reg::isKReg(o0));
      ASMJIT_FALLTHROUGH;
    }

    case InstDB::kEncodingVexRvm_Lx: {
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      goto CaseVexRvm;
    }

    case InstDB::kEncodingVexRvm_Lx_2xK: {
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        // Two registers are encoded as a single register.
        //   - First K register must be even.
        //   - Second K register must be first+1.
        if ((o0.id() & 1) != 0 || o0.id() + 1 != o1.id())
          goto InvalidPhysId;

        const Operand_& o3 = opExt[EmitterUtils::kOp3];

        opcode |= x86OpcodeLBySize(o2.size());
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());

        if (o3.isReg()) {
          rbReg = o3.id();
          goto EmitVexEvexR;
        }

        if (o3.isMem()) {
          rmRel = &o3;
          goto EmitVexEvexM;
        }
      }
      break;
    }

    case InstDB::kEncodingVexRvmr_Lx: {
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;
    }

    case InstDB::kEncodingVexRvmr: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const uint32_t isign4 = isign3 + (o3.opType() << 9);

      immValue = o3.id() << 4;
      immSize = 1;

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }
      break;
    }

    case InstDB::kEncodingVexRvmi_KEvex:
      opcode.forceEvexIf(Reg::isKReg(o0));
      goto VexRvmi;

    case InstDB::kEncodingVexRvmi_Lx_KEvex:
      opcode.forceEvexIf(Reg::isKReg(o0));
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvmi_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvmi:
VexRvmi:
    {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const uint32_t isign4 = isign3 + (o3.opType() << 9);

      immValue = o3.as<Imm>().value();
      immSize = 1;

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Imm)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Mem, Imm)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }
      break;
    }

    case InstDB::kEncodingVexRmv_Wx:
      opcode.addWIf(Reg::isGpq(o0) | Reg::isGpq(o2));
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRmv:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRmvRm_VM:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opcode  = x86AltOpcodeOf(instInfo);
        opcode |= Support::max(x86OpcodeLByVMem(o1), x86OpcodeLBySize(o0.size()));

        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRmv_VM:
      if (isign3 == ENC_OPS3(Reg, Mem, Reg)) {
        opcode |= Support::max(x86OpcodeLByVMem(o1), x86OpcodeLBySize(o0.size() | o2.size()));

        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;


    case InstDB::kEncodingVexRmvi: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const uint32_t isign4 = isign3 + (o3.opType() << 9);

      immValue = o3.as<Imm>().value();
      immSize = 1;

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Imm)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign4 == ENC_OPS4(Reg, Mem, Reg, Imm)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;
    }

    case InstDB::kEncodingVexMovdMovq:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        if (Reg::isGp(o0)) {
          opcode = x86AltOpcodeOf(instInfo);
          opcode.addWBySize(o0.size());
          opReg = o1.id();
          rbReg = o0.id();
          goto EmitVexEvexR;
        }

        if (Reg::isGp(o1)) {
          opcode.addWBySize(o1.size());
          opReg = o0.id();
          rbReg = o1.id();
          goto EmitVexEvexR;
        }

        // If this is a 'W' version (movq) then allow also vmovq 'xmm|xmm' form.
        if (opcode & Opcode::kEvex_W_1) {
          opcode &= ~(Opcode::kPP_VEXMask | Opcode::kMM_Mask | 0xFF);
          opcode |=  (Opcode::kF30F00 | 0x7E);

          opReg = o0.id();
          rbReg = o1.id();
          goto EmitVexEvexR;
        }
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        if (opcode & Opcode::kEvex_W_1) {
          opcode &= ~(Opcode::kPP_VEXMask | Opcode::kMM_Mask | 0xFF);
          opcode |=  (Opcode::kF30F00 | 0x7E);
        }

        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }

      // The following instruction uses the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        if (opcode & Opcode::kEvex_W_1) {
          opcode &= ~(Opcode::kPP_VEXMask | Opcode::kMM_Mask | 0xFF);
          opcode |=  (Opcode::k660F00 | 0xD6);
        }

        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRmMr_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRmMr:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }

      // The following instruction uses the secondary opcode.
      opcode &= Opcode::kLL_Mask;
      opcode |= x86AltOpcodeOf(instInfo);

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvmRmv:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rbReg = o1.id();

        if (!(options & Inst::kOptionModMR))
          goto EmitVexEvexR;

        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvmRmi_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvmRmi:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }

      // The following instructions use the secondary opcode.
      opcode &= Opcode::kLL_Mask;
      opcode |= x86AltOpcodeOf(instInfo);

      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvmRmvRmi:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rbReg = o1.id();

        if (!(options & Inst::kOptionModMR))
          goto EmitVexEvexR;

        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }

      // The following instructions use the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);

      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = o0.id();
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvmMr:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }

      // The following instructions use the secondary opcode.
      opcode = x86AltOpcodeOf(instInfo);

      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = o1.id();
        rbReg = o0.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvmMvr_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvmMvr:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }

      // The following instruction uses the secondary opcode.
      opcode &= Opcode::kLL_Mask;
      opcode |= x86AltOpcodeOf(instInfo);

      if (isign3 == ENC_OPS3(Mem, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o2.id(), o1.id());
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexRvmVmi_Lx_MEvex:
      opcode.forceEvexIf(o1.isMem());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvmVmi_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvmVmi:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Reg, Mem)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;
        goto EmitVexEvexM;
      }

      // The following instruction uses the secondary opcode.
      opcode &= Opcode::kLL_Mask | Opcode::kMM_ForceEvex;
      opcode |= x86AltOpcodeOf(instInfo);
      opReg = opcode.extractModO();

      immValue = o2.as<Imm>().value();
      immSize = 1;

      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = x86PackRegAndVvvvv(opReg, o0.id());
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opReg = x86PackRegAndVvvvv(opReg, o0.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexVm_Wx:
      opcode.addWIf(Reg::isGpq(o0) | Reg::isGpq(o1));
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexVm:
      if (isign3 == ENC_OPS2(Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(opReg, o0.id());
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = x86PackRegAndVvvvv(opReg, o0.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexVmi_Lx_MEvex:
      if (isign3 == ENC_OPS3(Reg, Mem, Imm))
        opcode.forceEvex();
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexVmi_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexVmi:
      immValue = o2.as<Imm>().value();
      immSize = 1;

CaseVexVmi_AfterImm:
      if (isign3 == ENC_OPS3(Reg, Reg, Imm)) {
        opReg = x86PackRegAndVvvvv(opReg, o0.id());
        rbReg = o1.id();
        goto EmitVexEvexR;
      }

      if (isign3 == ENC_OPS3(Reg, Mem, Imm)) {
        opReg = x86PackRegAndVvvvv(opReg, o0.id());
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingVexVmi4_Wx:
      opcode.addWIf(Reg::isGpq(o0) || o1.size() == 8);
      immValue = o2.as<Imm>().value();
      immSize = 4;
      goto CaseVexVmi_AfterImm;

    case InstDB::kEncodingVexRvrmRvmr_Lx:
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingVexRvrmRvmr: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const uint32_t isign4 = isign3 + (o3.opType() << 9);

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();

        immValue = o3.id() << 4;
        immSize = 1;
        goto EmitVexEvexR;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Mem)) {
        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o3;

        immValue = o2.id() << 4;
        immSize = 1;
        goto EmitVexEvexM;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;

        immValue = o3.id() << 4;
        immSize = 1;
        goto EmitVexEvexM;
      }
      break;
    }

    case InstDB::kEncodingVexRvrmiRvmri_Lx: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const Operand_& o4 = opExt[EmitterUtils::kOp4];

      if (ASMJIT_UNLIKELY(!o4.isImm()))
        goto InvalidInstruction;

      const uint32_t isign4 = isign3 + (o3.opType() << 9);
      opcode |= x86OpcodeLBySize(o0.size() | o1.size() | o2.size() | o3.size());

      immValue = o4.as<Imm>().valueAs<uint8_t>() & 0x0F;
      immSize = 1;

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rbReg = o2.id();

        immValue |= o3.id() << 4;
        goto EmitVexEvexR;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Mem)) {
        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o3;

        immValue |= o2.id() << 4;
        goto EmitVexEvexM;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;

        immValue |= o3.id() << 4;
        goto EmitVexEvexM;
      }
      break;
    }

    case InstDB::kEncodingVexMovssMovsd:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        goto CaseVexRvm_R;
      }

      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }

      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opcode = x86AltOpcodeOf(instInfo);
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    // ------------------------------------------------------------------------
    // [FMA4]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingFma4_Lx:
      // It's fine to just check the first operand, second is just for sanity.
      opcode |= x86OpcodeLBySize(o0.size() | o1.size());
      ASMJIT_FALLTHROUGH;

    case InstDB::kEncodingFma4: {
      const Operand_& o3 = opExt[EmitterUtils::kOp3];
      const uint32_t isign4 = isign3 + (o3.opType() << 9);

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());

        if (!(options & Inst::kOptionModMR)) {
          // MOD/RM - Encoding preferred by LLVM.
          opcode.addW();
          rbReg = o3.id();

          immValue = o2.id() << 4;
          immSize = 1;
          goto EmitVexEvexR;
        }
        else {
          // MOD/MR - Alternative encoding.
          rbReg = o2.id();

          immValue = o3.id() << 4;
          immSize = 1;
          goto EmitVexEvexR;
        }
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Reg, Mem)) {
        opcode.addW();
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o3;

        immValue = o2.id() << 4;
        immSize = 1;
        goto EmitVexEvexM;
      }

      if (isign4 == ENC_OPS4(Reg, Reg, Mem, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o1.id());
        rmRel = &o2;

        immValue = o3.id() << 4;
        immSize = 1;
        goto EmitVexEvexM;
      }
      break;
    }

    // ------------------------------------------------------------------------
    // [AMX]
    // ------------------------------------------------------------------------

    case InstDB::kEncodingAmxCfg:
      if (isign3 == ENC_OPS1(Mem)) {
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingAmxR:
      if (isign3 == ENC_OPS1(Reg)) {
        opReg = o0.id();
        rbReg = 0;
        goto EmitVexEvexR;
      }
      break;

    case InstDB::kEncodingAmxRm:
      if (isign3 == ENC_OPS2(Reg, Mem)) {
        opReg = o0.id();
        rmRel = &o1;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingAmxMr:
      if (isign3 == ENC_OPS2(Mem, Reg)) {
        opReg = o1.id();
        rmRel = &o0;
        goto EmitVexEvexM;
      }
      break;

    case InstDB::kEncodingAmxRmv:
      if (isign3 == ENC_OPS3(Reg, Reg, Reg)) {
        opReg = x86PackRegAndVvvvv(o0.id(), o2.id());
        rbReg = o1.id();
        goto EmitVexEvexR;
      }
      break;
  }

  goto InvalidInstruction;

  // --------------------------------------------------------------------------
  // [Emit - X86]
  // --------------------------------------------------------------------------

EmitX86OpMovAbs:
  immSize = FastUInt8(registerSize());
  writer.emitSegmentOverride(rmRel->as<Mem>().segmentId());

EmitX86Op:
  // Emit mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // Emit REX prefix (64-bit only).
  {
    uint32_t rex = opcode.extractRex(options);
    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);
  }

  // Emit instruction opcodes.
  writer.emitMMAndOpcode(opcode.v);
  writer.emitImmediate(uint64_t(immValue), immSize);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - X86 - Opcode + Reg]
  // --------------------------------------------------------------------------

EmitX86OpReg:
  // Emit mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // Emit REX prefix (64-bit only).
  {
    uint32_t rex = opcode.extractRex(options) | (opReg >> 3); // Rex.B (0x01).
    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);

    opReg &= 0x7;
  }

  // Emit instruction opcodes.
  opcode += opReg;
  writer.emitMMAndOpcode(opcode.v);
  writer.emitImmediate(uint64_t(immValue), immSize);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - X86 - Opcode with implicit <mem> operand]
  // --------------------------------------------------------------------------

EmitX86OpImplicitMem:
  rmInfo = x86MemInfo[rmRel->as<Mem>().baseAndIndexTypes()];
  if (ASMJIT_UNLIKELY(rmRel->as<Mem>().hasOffset() || (rmInfo & kX86MemInfo_Index)))
    goto InvalidInstruction;

  // Emit mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // Emit REX prefix (64-bit only).
  {
    uint32_t rex = opcode.extractRex(options);
    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);
  }

  // Emit override prefixes.
  writer.emitSegmentOverride(rmRel->as<Mem>().segmentId());
  writer.emitAddressOverride((rmInfo & _addressOverrideMask()) != 0);

  // Emit instruction opcodes.
  writer.emitMMAndOpcode(opcode.v);

  // Emit immediate value.
  writer.emitImmediate(uint64_t(immValue), immSize);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - X86 - Opcode /r - register]
  // --------------------------------------------------------------------------

EmitX86R:
  // Mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // Emit REX prefix (64-bit only).
  {
    uint32_t rex = opcode.extractRex(options) |
                   ((opReg & 0x08) >> 1) | // REX.R (0x04).
                   ((rbReg & 0x08) >> 3) ; // REX.B (0x01).

    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);

    opReg &= 0x07;
    rbReg &= 0x07;
  }

  // Emit instruction opcodes.
  writer.emitMMAndOpcode(opcode.v);

  // Emit ModR.
  writer.emit8(x86EncodeMod(3, opReg, rbReg));

  // Emit immediate value.
  writer.emitImmediate(uint64_t(immValue), immSize);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - X86 - Opcode /r - memory base]
  // --------------------------------------------------------------------------

EmitX86RFromM:
  rmInfo = x86MemInfo[rmRel->as<Mem>().baseAndIndexTypes()];
  if (ASMJIT_UNLIKELY(rmRel->as<Mem>().hasOffset() || (rmInfo & kX86MemInfo_Index)))
    goto InvalidInstruction;

  // Emit mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // Emit REX prefix (64-bit only).
  {
    uint32_t rex = opcode.extractRex(options) |
                   ((opReg & 0x08) >> 1) | // REX.R (0x04).
                   ((rbReg       ) >> 3) ; // REX.B (0x01).

    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);

    opReg &= 0x07;
    rbReg &= 0x07;
  }

  // Emit override prefixes.
  writer.emitSegmentOverride(rmRel->as<Mem>().segmentId());
  writer.emitAddressOverride((rmInfo & _addressOverrideMask()) != 0);

  // Emit instruction opcodes.
  writer.emitMMAndOpcode(opcode.v);

  // Emit ModR/M.
  writer.emit8(x86EncodeMod(3, opReg, rbReg));

  // Emit immediate value.
  writer.emitImmediate(uint64_t(immValue), immSize);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - X86 - Opcode /r - memory operand]
  // --------------------------------------------------------------------------

EmitX86M:
  // `rmRel` operand must be memory.
  ASMJIT_ASSERT(rmRel != nullptr);
  ASMJIT_ASSERT(rmRel->opType() == Operand::kOpMem);
  ASMJIT_ASSERT((opcode & Opcode::kCDSHL_Mask) == 0);

  // Emit override prefixes.
  rmInfo = x86MemInfo[rmRel->as<Mem>().baseAndIndexTypes()];
  writer.emitSegmentOverride(rmRel->as<Mem>().segmentId());

  memOpAOMark = writer.cursor();
  writer.emitAddressOverride((rmInfo & _addressOverrideMask()) != 0);

  // Emit mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // Emit REX prefix (64-bit only).
  rbReg = rmRel->as<Mem>().baseId();
  rxReg = rmRel->as<Mem>().indexId();
  {
    uint32_t rex;

    rex  = (rbReg >> 3) & 0x01; // REX.B (0x01).
    rex |= (rxReg >> 2) & 0x02; // REX.X (0x02).
    rex |= (opReg >> 1) & 0x04; // REX.R (0x04).

    rex &= rmInfo;
    rex |= opcode.extractRex(options);

    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);

    opReg &= 0x07;
  }

  // Emit instruction opcodes.
  writer.emitMMAndOpcode(opcode.v);

  // ... Fall through ...

  // --------------------------------------------------------------------------
  // [Emit - MOD/SIB]
  // --------------------------------------------------------------------------

EmitModSib:
  if (!(rmInfo & (kX86MemInfo_Index | kX86MemInfo_67H_X86))) {
    // ==========|> [BASE + DISP8|DISP32].
    if (rmInfo & kX86MemInfo_BaseGp) {
      rbReg &= 0x7;
      relOffset = rmRel->as<Mem>().offsetLo32();

      uint32_t mod = x86EncodeMod(0, opReg, rbReg);
      bool forceSIB = commonInfo->isTsibOp();

      if (rbReg == Gp::kIdSp || forceSIB) {
        // TSIB or [XSP|R12].
        mod = (mod & 0xF8u) | 0x04u;
        if (rbReg != Gp::kIdBp && relOffset == 0) {
          writer.emit8(mod);
          writer.emit8(x86EncodeSib(0, 4, rbReg));
        }
        // TSIB or [XSP|R12 + DISP8|DISP32].
        else {
          uint32_t cdShift = (opcode & Opcode::kCDSHL_Mask) >> Opcode::kCDSHL_Shift;
          int32_t cdOffset = relOffset >> cdShift;

          if (Support::isInt8(cdOffset) && relOffset == int32_t(uint32_t(cdOffset) << cdShift)) {
            writer.emit8(mod + 0x40); // <- MOD(1, opReg, rbReg).
            writer.emit8(x86EncodeSib(0, 4, rbReg));
            writer.emit8(cdOffset & 0xFF);
          }
          else {
            writer.emit8(mod + 0x80); // <- MOD(2, opReg, rbReg).
            writer.emit8(x86EncodeSib(0, 4, rbReg));
            writer.emit32uLE(uint32_t(relOffset));
          }
        }
      }
      else if (rbReg != Gp::kIdBp && relOffset == 0) {
        // [BASE].
        writer.emit8(mod);
      }
      else {
        // [BASE + DISP8|DISP32].
        uint32_t cdShift = (opcode & Opcode::kCDSHL_Mask) >> Opcode::kCDSHL_Shift;
        int32_t cdOffset = relOffset >> cdShift;

        if (Support::isInt8(cdOffset) && relOffset == int32_t(uint32_t(cdOffset) << cdShift)) {
          writer.emit8(mod + 0x40);
          writer.emit8(cdOffset & 0xFF);
        }
        else {
          writer.emit8(mod + 0x80);
          writer.emit32uLE(uint32_t(relOffset));
        }
      }
    }
    // ==========|> [ABSOLUTE | DISP32].
    else if (!(rmInfo & (kX86MemInfo_BaseLabel | kX86MemInfo_BaseRip))) {
      uint32_t addrType = rmRel->as<Mem>().addrType();
      relOffset = rmRel->as<Mem>().offsetLo32();

      if (is32Bit()) {
        // Explicit relative addressing doesn't work in 32-bit mode.
        if (ASMJIT_UNLIKELY(addrType == Mem::kAddrTypeRel))
          goto InvalidAddress;

        writer.emit8(x86EncodeMod(0, opReg, 5));
        writer.emit32uLE(uint32_t(relOffset));
      }
      else {
        bool isOffsetI32 = rmRel->as<Mem>().offsetHi32() == (relOffset >> 31);
        bool isOffsetU32 = rmRel->as<Mem>().offsetHi32() == 0;
        uint64_t baseAddress = code()->baseAddress();

        // If relative addressing was not explicitly set then we can try to guess.
        // By guessing we check some properties of the memory operand and try to
        // base the decision on the segment prefix and the address type.
        if (addrType == Mem::kAddrTypeDefault) {
          if (baseAddress == Globals::kNoBaseAddress) {
            // Prefer absolute addressing mode if the offset is 32-bit.
            addrType = isOffsetI32 || isOffsetU32 ? Mem::kAddrTypeAbs
                                                  : Mem::kAddrTypeRel;
          }
          else {
            // Prefer absolute addressing mode if FS|GS segment override is present.
            bool hasFsGs = rmRel->as<Mem>().segmentId() >= SReg::kIdFs;
            // Prefer absolute addressing mode if this is LEA with 32-bit immediate.
            bool isLea32 = (instId == Inst::kIdLea) && (isOffsetI32 || isOffsetU32);

            addrType = hasFsGs || isLea32 ? Mem::kAddrTypeAbs
                                          : Mem::kAddrTypeRel;
          }
        }

        if (addrType == Mem::kAddrTypeRel) {
          uint32_t kModRel32Size = 5;
          uint64_t virtualOffset = uint64_t(writer.offsetFrom(_bufferData)) + immSize + kModRel32Size;

          if (baseAddress == Globals::kNoBaseAddress || _section->id() != 0) {
            // Create a new RelocEntry as we cannot calculate the offset right now.
            err = _code->newRelocEntry(&re, RelocEntry::kTypeAbsToRel);
            if (ASMJIT_UNLIKELY(err))
              goto Failed;

            writer.emit8(x86EncodeMod(0, opReg, 5));

            re->_sourceSectionId = _section->id();
            re->_sourceOffset = offset();
            re->_format.resetToDataValue(4);
            re->_format.setLeadingAndTrailingSize(writer.offsetFrom(_bufferPtr), immSize);
            re->_payload = uint64_t(rmRel->as<Mem>().offset());

            writer.emit32uLE(0);
            writer.emitImmediate(uint64_t(immValue), immSize);
            goto EmitDone;
          }
          else {
            uint64_t rip64 = baseAddress + _section->offset() + virtualOffset;
            uint64_t rel64 = uint64_t(rmRel->as<Mem>().offset()) - rip64;

            if (Support::isInt32(int64_t(rel64))) {
              writer.emit8(x86EncodeMod(0, opReg, 5));
              writer.emit32uLE(uint32_t(rel64 & 0xFFFFFFFFu));
              writer.emitImmediate(uint64_t(immValue), immSize);
              goto EmitDone;
            }
            else {
              // We must check the original address type as we have modified
              // `addrType`. We failed if the original address type is 'rel'.
              if (ASMJIT_UNLIKELY(rmRel->as<Mem>().isRel()))
                goto InvalidAddress;
            }
          }
        }

        // Handle unsigned 32-bit address that doesn't work with sign extension.
        // Consider the following instructions:
        //
        //   1. lea rax, [-1]         - Sign extended to 0xFFFFFFFFFFFFFFFF
        //   2. lea rax, [0xFFFFFFFF] - Zero extended to 0x00000000FFFFFFFF
        //   3. add rax, [-1]         - Sign extended to 0xFFFFFFFFFFFFFFFF
        //   4. add rax, [0xFFFFFFFF] - Zero extended to 0x00000000FFFFFFFF
        //
        // Sign extension is naturally performed by the CPU so we don't have to
        // bother, however, zero extension requires address-size override prefix,
        // which we probably don't have at this moment. So to make the address
        // valid we need to insert it at `memOpAOMark` if it's not already there.
        //
        // If this is 'lea' instruction then it's possible to remove REX.W part
        // from REX prefix (if it's there), which would be one-byte shorter than
        // inserting address-size override.
        //
        // NOTE: If we don't do this then these instructions are unencodable.
        if (!isOffsetI32) {
          // 64-bit absolute address is unencodable.
          if (ASMJIT_UNLIKELY(!isOffsetU32))
            goto InvalidAddress64Bit;

          // We only patch the existing code if we don't have address-size override.
          if (*memOpAOMark != 0x67) {
            if (instId == Inst::kIdLea) {
              // LEA: Remove REX.W, if present. This is easy as we know that 'lea'
              // doesn't use any PP prefix so if REX prefix was emitted it would be
              // at `memOpAOMark`.
              uint32_t rex = *memOpAOMark;
              if (rex & kX86ByteRex) {
                rex &= (~kX86ByteRexW) & 0xFF;
                *memOpAOMark = uint8_t(rex);

                // We can remove the REX prefix completely if it was not forced.
                if (rex == kX86ByteRex && !(options & Inst::kOptionRex))
                  writer.remove8(memOpAOMark);
              }
            }
            else {
              // Any other instruction: Insert address-size override prefix.
              writer.insert8(memOpAOMark, 0x67);
            }
          }
        }

        // Emit 32-bit absolute address.
        writer.emit8(x86EncodeMod(0, opReg, 4));
        writer.emit8(x86EncodeSib(0, 4, 5));
        writer.emit32uLE(uint32_t(relOffset));
      }
    }
    // ==========|> [LABEL|RIP + DISP32]
    else {
      writer.emit8(x86EncodeMod(0, opReg, 5));

      if (is32Bit()) {
EmitModSib_LabelRip_X86:
        if (ASMJIT_UNLIKELY(_code->_relocations.willGrow(_code->allocator()) != kErrorOk))
          goto OutOfMemory;

        relOffset = rmRel->as<Mem>().offsetLo32();
        if (rmInfo & kX86MemInfo_BaseLabel) {
          // [LABEL->ABS].
          label = _code->labelEntry(rmRel->as<Mem>().baseId());
          if (ASMJIT_UNLIKELY(!label))
            goto InvalidLabel;

          err = _code->newRelocEntry(&re, RelocEntry::kTypeRelToAbs);
          if (ASMJIT_UNLIKELY(err))
            goto Failed;

          re->_sourceSectionId = _section->id();
          re->_sourceOffset = offset();
          re->_format.resetToDataValue(4);
          re->_format.setLeadingAndTrailingSize(writer.offsetFrom(_bufferPtr), immSize);
          re->_payload = uint64_t(int64_t(relOffset));

          if (label->isBound()) {
            // Label bound to the current section.
            re->_payload += label->offset();
            re->_targetSectionId = label->section()->id();
            writer.emit32uLE(0);
          }
          else {
            // Non-bound label or label bound to a different section.
            relOffset = -4 - immSize;
            relSize = 4;
            goto EmitRel;
          }
        }
        else {
          // [RIP->ABS].
          err = _code->newRelocEntry(&re, RelocEntry::kTypeRelToAbs);
          if (ASMJIT_UNLIKELY(err))
            goto Failed;

          re->_sourceSectionId = _section->id();
          re->_targetSectionId = _section->id();
          re->_format.resetToDataValue(4);
          re->_format.setLeadingAndTrailingSize(writer.offsetFrom(_bufferPtr), immSize);
          re->_sourceOffset = offset();
          re->_payload = re->_sourceOffset + re->_format.regionSize() + uint64_t(int64_t(relOffset));

          writer.emit32uLE(0);
        }
      }
      else {
        relOffset = rmRel->as<Mem>().offsetLo32();
        if (rmInfo & kX86MemInfo_BaseLabel) {
          // [RIP].
          label = _code->labelEntry(rmRel->as<Mem>().baseId());
          if (ASMJIT_UNLIKELY(!label))
            goto InvalidLabel;

          relOffset -= (4 + immSize);
          if (label->isBoundTo(_section)) {
            // Label bound to the current section.
            relOffset += int32_t(label->offset() - writer.offsetFrom(_bufferData));
            writer.emit32uLE(uint32_t(relOffset));
          }
          else {
            // Non-bound label or label bound to a different section.
            relSize = 4;
            goto EmitRel;
          }
        }
        else {
          // [RIP].
          writer.emit32uLE(uint32_t(relOffset));
        }
      }
    }
  }
  else if (!(rmInfo & kX86MemInfo_67H_X86)) {
    // ESP|RSP can't be used as INDEX in pure SIB mode, however, VSIB mode
    // allows XMM4|YMM4|ZMM4 (that's why the check is before the label).
    if (ASMJIT_UNLIKELY(rxReg == Gp::kIdSp))
      goto InvalidAddressIndex;

EmitModVSib:
    rxReg &= 0x7;

    // ==========|> [BASE + INDEX + DISP8|DISP32].
    if (rmInfo & kX86MemInfo_BaseGp) {
      rbReg &= 0x7;
      relOffset = rmRel->as<Mem>().offsetLo32();

      uint32_t mod = x86EncodeMod(0, opReg, 4);
      uint32_t sib = x86EncodeSib(rmRel->as<Mem>().shift(), rxReg, rbReg);

      if (relOffset == 0 && rbReg != Gp::kIdBp) {
        // [BASE + INDEX << SHIFT].
        writer.emit8(mod);
        writer.emit8(sib);
      }
      else {
        uint32_t cdShift = (opcode & Opcode::kCDSHL_Mask) >> Opcode::kCDSHL_Shift;
        int32_t cdOffset = relOffset >> cdShift;

        if (Support::isInt8(cdOffset) && relOffset == int32_t(uint32_t(cdOffset) << cdShift)) {
          // [BASE + INDEX << SHIFT + DISP8].
          writer.emit8(mod + 0x40); // <- MOD(1, opReg, 4).
          writer.emit8(sib);
          writer.emit8(uint32_t(cdOffset));
        }
        else {
          // [BASE + INDEX << SHIFT + DISP32].
          writer.emit8(mod + 0x80); // <- MOD(2, opReg, 4).
          writer.emit8(sib);
          writer.emit32uLE(uint32_t(relOffset));
        }
      }
    }
    // ==========|> [INDEX + DISP32].
    else if (!(rmInfo & (kX86MemInfo_BaseLabel | kX86MemInfo_BaseRip))) {
      // [INDEX << SHIFT + DISP32].
      writer.emit8(x86EncodeMod(0, opReg, 4));
      writer.emit8(x86EncodeSib(rmRel->as<Mem>().shift(), rxReg, 5));

      relOffset = rmRel->as<Mem>().offsetLo32();
      writer.emit32uLE(uint32_t(relOffset));
    }
    // ==========|> [LABEL|RIP + INDEX + DISP32].
    else {
      if (is32Bit()) {
        writer.emit8(x86EncodeMod(0, opReg, 4));
        writer.emit8(x86EncodeSib(rmRel->as<Mem>().shift(), rxReg, 5));
        goto EmitModSib_LabelRip_X86;
      }
      else {
        // NOTE: This also handles VSIB+RIP, which is not allowed in 64-bit mode.
        goto InvalidAddress;
      }
    }
  }
  else {
    // 16-bit address mode (32-bit mode with 67 override prefix).
    relOffset = (int32_t(rmRel->as<Mem>().offsetLo32()) << 16) >> 16;

    // NOTE: 16-bit addresses don't use SIB byte and their encoding differs. We
    // use a table-based approach to calculate the proper MOD byte as it's easier.
    // Also, not all BASE [+ INDEX] combinations are supported in 16-bit mode, so
    // this may fail.
    const uint32_t kBaseGpIdx = (kX86MemInfo_BaseGp | kX86MemInfo_Index);

    if (rmInfo & kBaseGpIdx) {
      // ==========|> [BASE + INDEX + DISP16].
      uint32_t mod;

      rbReg &= 0x7;
      rxReg &= 0x7;

      if ((rmInfo & kBaseGpIdx) == kBaseGpIdx) {
        uint32_t shf = rmRel->as<Mem>().shift();
        if (ASMJIT_UNLIKELY(shf != 0))
          goto InvalidAddress;
        mod = x86Mod16BaseIndexTable[(rbReg << 3) + rxReg];
      }
      else {
        if (rmInfo & kX86MemInfo_Index)
          rbReg = rxReg;
        mod = x86Mod16BaseTable[rbReg];
      }

      if (ASMJIT_UNLIKELY(mod == 0xFF))
        goto InvalidAddress;

      mod += opReg << 3;
      if (relOffset == 0 && mod != 0x06) {
        writer.emit8(mod);
      }
      else if (Support::isInt8(relOffset)) {
        writer.emit8(mod + 0x40);
        writer.emit8(uint32_t(relOffset));
      }
      else {
        writer.emit8(mod + 0x80);
        writer.emit16uLE(uint32_t(relOffset));
      }
    }
    else {
      // Not supported in 16-bit addresses.
      if (rmInfo & (kX86MemInfo_BaseRip | kX86MemInfo_BaseLabel))
        goto InvalidAddress;

      // ==========|> [DISP16].
      writer.emit8(opReg | 0x06);
      writer.emit16uLE(uint32_t(relOffset));
    }
  }

  writer.emitImmediate(uint64_t(immValue), immSize);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - FPU]
  // --------------------------------------------------------------------------

EmitFpuOp:
  // Mandatory instruction prefix.
  writer.emitPP(opcode.v);

  // FPU instructions consist of two opcodes.
  writer.emit8(opcode.v >> Opcode::kFPU_2B_Shift);
  writer.emit8(opcode.v);
  goto EmitDone;

  // --------------------------------------------------------------------------
  // [Emit - VEX|EVEX]
  // --------------------------------------------------------------------------

EmitVexEvexOp:
  {
    // These don't use immediate.
    ASMJIT_ASSERT(immSize == 0);

    // Only 'vzeroall' and 'vzeroupper' instructions use this encoding, they
    // don't define 'W' to be '1' so we can just check the 'mmmmm' field. Both
    // functions can encode by using VEX2 prefix so VEX3 is basically only used
    // when specified as instruction option.
    ASMJIT_ASSERT((opcode & Opcode::kW) == 0);

    uint32_t x = ((opcode  & Opcode::kMM_Mask    ) >> (Opcode::kMM_Shift     )) |
                 ((opcode  & Opcode::kLL_Mask    ) >> (Opcode::kLL_Shift - 10)) |
                 ((opcode  & Opcode::kPP_VEXMask ) >> (Opcode::kPP_Shift -  8)) |
                 ((options & Inst::kOptionVex3   ) >> (Opcode::kMM_Shift     )) ;
    if (x & 0x04u) {
      x  = (x & (0x4 ^ 0xFFFF)) << 8;                    // [00000000|00000Lpp|0000m0mm|00000000].
      x ^= (kX86ByteVex3) |                              // [........|00000Lpp|0000m0mm|__VEX3__].
           (0x07u  << 13) |                              // [........|00000Lpp|1110m0mm|__VEX3__].
           (0x0Fu  << 19) |                              // [........|01111Lpp|1110m0mm|__VEX3__].
           (opcode << 24) ;                              // [_OPCODE_|01111Lpp|1110m0mm|__VEX3__].

      writer.emit32uLE(x);
      goto EmitDone;
    }
    else {
      x = ((x >> 8) ^ x) ^ 0xF9;
      writer.emit8(kX86ByteVex2);
      writer.emit8(x);
      writer.emit8(opcode.v);
      goto EmitDone;
    }
  }

  // --------------------------------------------------------------------------
  // [Emit - VEX|EVEX - /r (Register)]
  // --------------------------------------------------------------------------

EmitVexEvexR:
  {
    // Construct `x` - a complete EVEX|VEX prefix.
    uint32_t x = ((opReg << 4) & 0xF980u) |              // [........|........|Vvvvv..R|R.......].
                 ((rbReg << 2) & 0x0060u) |              // [........|........|........|.BB.....].
                 (opcode.extractLLMM(options)) |         // [........|.LL.....|Vvvvv..R|RBBmmmmm].
                 (_extraReg.id() << 16);                 // [........|.LL..aaa|Vvvvv..R|RBBmmmmm].
    opReg &= 0x7;

    // Handle AVX512 options by a single branch.
    const uint32_t kAvx512Options = Inst::kOptionZMask | Inst::kOptionER | Inst::kOptionSAE;
    if (options & kAvx512Options) {
      uint32_t kBcstMask = 0x1 << 20;
      uint32_t kLLMask10 = 0x2 << 21;
      uint32_t kLLMask11 = 0x3 << 21;

      // Designed to be easily encodable so the position must be exact.
      // The {rz-sae} is encoded as {11}, so it should match the mask.
      ASMJIT_ASSERT(Inst::kOptionRZ_SAE == kLLMask11);

      x |= options & Inst::kOptionZMask;                 // [........|zLLb.aaa|Vvvvv..R|RBBmmmmm].

      // Support embedded-rounding {er} and suppress-all-exceptions {sae}.
      if (options & (Inst::kOptionER | Inst::kOptionSAE)) {
        // Embedded rounding is only encodable if the instruction is either
        // scalar or it's a 512-bit operation as the {er} rounding predicate
        // collides with LL part of the instruction.
        if ((x & kLLMask11) != kLLMask10) {
          // Ok, so LL is not 10, thus the instruction must be scalar.
          // Scalar instructions don't support broadcast so if this
          // instruction supports it {er} nor {sae} would be encodable.
          if (ASMJIT_UNLIKELY(commonInfo->hasAvx512B()))
            goto InvalidEROrSAE;
        }

        if (options & Inst::kOptionER) {
          if (ASMJIT_UNLIKELY(!commonInfo->hasAvx512ER()))
            goto InvalidEROrSAE;

          x &=~kLLMask11;                                // [........|.00..aaa|Vvvvv..R|RBBmmmmm].
          x |= kBcstMask | (options & kLLMask11);        // [........|.LLb.aaa|Vvvvv..R|RBBmmmmm].
        }
        else {
          if (ASMJIT_UNLIKELY(!commonInfo->hasAvx512SAE()))
            goto InvalidEROrSAE;

          x |= kBcstMask;                                // [........|.LLb.aaa|Vvvvv..R|RBBmmmmm].
        }
      }
    }

    // If these bits are used then EVEX prefix is required.
    constexpr uint32_t kEvexBits = 0x00D78150u;          // [........|xx.x.xxx|x......x|.x.x....].

    // Force EVEX prefix even in case the instruction has VEX encoding, because EVEX encoding is preferred. At the
    // moment this is only required for AVX_VNNI instructions, which were added after AVX512_VNNI instructions. If
    // such instruction doesn't specify prefix, EVEX (AVX512_VNNI) would be used by default,
    if (commonInfo->preferEvex()) {
      if ((x & kEvexBits) == 0 && (options & (Inst::kOptionVex | Inst::kOptionVex3)) == 0) {
        x |= (Opcode::kMM_ForceEvex) >> Opcode::kMM_Shift;
      }
    }

    // Check if EVEX is required by checking bits in `x` :  [........|xx.x.xxx|x......x|.x.x....].
    if (x & kEvexBits) {
      uint32_t y = ((x << 4) & 0x00080000u) |            // [........|...bV...|........|........].
                   ((x >> 4) & 0x00000010u) ;            // [........|...bV...|........|...R....].
      x  = (x & 0x00FF78E3u) | y;                        // [........|zLLbVaaa|0vvvv000|RBBR00mm].
      x  = x << 8;                                       // [zLLbVaaa|0vvvv000|RBBR00mm|00000000].
      x |= (opcode >> kVSHR_W    ) & 0x00800000u;        // [zLLbVaaa|Wvvvv000|RBBR00mm|00000000].
      x |= (opcode >> kVSHR_PP_EW) & 0x00830000u;        // [zLLbVaaa|Wvvvv0pp|RBBR00mm|00000000] (added PP and EVEX.W).
                                                         //      _     ____    ____
      x ^= 0x087CF000u | kX86ByteEvex;                   // [zLLbVaaa|Wvvvv1pp|RBBR00mm|01100010].

      writer.emit32uLE(x);
      writer.emit8(opcode.v);

      rbReg &= 0x7;
      writer.emit8(x86EncodeMod(3, opReg, rbReg));
      writer.emitImmByteOrDWord(uint64_t(immValue), immSize);
      goto EmitDone;
    }

    // Not EVEX, prepare `x` for VEX2 or VEX3:          x = [........|00L00000|0vvvv000|R0B0mmmm].
    x |= ((opcode >> (kVSHR_W  + 8)) & 0x8000u) |        // [00000000|00L00000|Wvvvv000|R0B0mmmm].
         ((opcode >> (kVSHR_PP + 8)) & 0x0300u) |        // [00000000|00L00000|0vvvv0pp|R0B0mmmm].
         ((x      >> 11            ) & 0x0400u) ;        // [00000000|00L00000|WvvvvLpp|R0B0mmmm].

    // Check if VEX3 is required / forced:                  [........|........|x.......|..x..x..].
    if (x & 0x0008024u) {
      uint32_t xorMsk = x86VEXPrefix[x & 0xF] | (opcode << 24);

      // Clear 'FORCE-VEX3' bit and all high bits.
      x  = (x & (0x4 ^ 0xFFFF)) << 8;                    // [00000000|WvvvvLpp|R0B0m0mm|00000000].
                                                         //            ____    _ _
      x ^= xorMsk;                                       // [_OPCODE_|WvvvvLpp|R1Bmmmmm|VEX3|XOP].
      writer.emit32uLE(x);

      rbReg &= 0x7;
      writer.emit8(x86EncodeMod(3, opReg, rbReg));
      writer.emitImmByteOrDWord(uint64_t(immValue), immSize);
      goto EmitDone;
    }
    else {
      // 'mmmmm' must be '00001'.
      ASMJIT_ASSERT((x & 0x1F) == 0x01);

      x = ((x >> 8) ^ x) ^ 0xF9;
      writer.emit8(kX86ByteVex2);
      writer.emit8(x);
      writer.emit8(opcode.v);

      rbReg &= 0x7;
      writer.emit8(x86EncodeMod(3, opReg, rbReg));
      writer.emitImmByteOrDWord(uint64_t(immValue), immSize);
      goto EmitDone;
    }
  }

  // --------------------------------------------------------------------------
  // [Emit - VEX|EVEX - /r (Memory)]
  // --------------------------------------------------------------------------

EmitVexEvexM:
  ASMJIT_ASSERT(rmRel != nullptr);
  ASMJIT_ASSERT(rmRel->opType() == Operand::kOpMem);

  rmInfo = x86MemInfo[rmRel->as<Mem>().baseAndIndexTypes()];
  writer.emitSegmentOverride(rmRel->as<Mem>().segmentId());

  memOpAOMark = writer.cursor();
  writer.emitAddressOverride((rmInfo & _addressOverrideMask()) != 0);

  rbReg = rmRel->as<Mem>().hasBaseReg()  ? rmRel->as<Mem>().baseId()  : uint32_t(0);
  rxReg = rmRel->as<Mem>().hasIndexReg() ? rmRel->as<Mem>().indexId() : uint32_t(0);

  {
    uint32_t broadcastBit = uint32_t(rmRel->as<Mem>().hasBroadcast());

    // Construct `x` - a complete EVEX|VEX prefix.
    uint32_t x = ((opReg <<  4) & 0x0000F980u) |         // [........|........|Vvvvv..R|R.......].
                 ((rxReg <<  3) & 0x00000040u) |         // [........|........|........|.X......].
                 ((rxReg << 15) & 0x00080000u) |         // [........|....X...|........|........].
                 ((rbReg <<  2) & 0x00000020u) |         // [........|........|........|..B.....].
                 opcode.extractLLMM(options)   |         // [........|.LL.X...|Vvvvv..R|RXBmmmmm].
                 (_extraReg.id()    << 16)     |         // [........|.LL.Xaaa|Vvvvv..R|RXBmmmmm].
                 (broadcastBit      << 20)     ;         // [........|.LLbXaaa|Vvvvv..R|RXBmmmmm].
    opReg &= 0x07u;

    // Mark invalid VEX (force EVEX) case:               // [@.......|.LLbXaaa|Vvvvv..R|RXBmmmmm].
    x |= (~commonInfo->flags() & InstDB::kFlagVex) << (31 - Support::constCtz(InstDB::kFlagVex));

    // Handle AVX512 options by a single branch.
    const uint32_t kAvx512Options = Inst::kOptionZMask   |
                                    Inst::kOptionER      |
                                    Inst::kOptionSAE     ;
    if (options & kAvx512Options) {
      // {er} and {sae} are both invalid if memory operand is used.
      if (ASMJIT_UNLIKELY(options & (Inst::kOptionER | Inst::kOptionSAE)))
        goto InvalidEROrSAE;

      x |= options & (Inst::kOptionZMask);               // [@.......|zLLbXaaa|Vvvvv..R|RXBmmmmm].
    }

    // If these bits are used then EVEX prefix is required.
    constexpr uint32_t kEvexBits = 0x80DF8110u;          // [@.......|xx.xxxxx|x......x|...x....].

    // Force EVEX prefix even in case the instruction has VEX encoding, because EVEX encoding is preferred. At the
    // moment this is only required for AVX_VNNI instructions, which were added after AVX512_VNNI instructions. If
    // such instruction doesn't specify prefix, EVEX (AVX512_VNNI) would be used by default,
    if (commonInfo->preferEvex()) {
      if ((x & kEvexBits) == 0 && (options & (Inst::kOptionVex | Inst::kOptionVex3)) == 0) {
        x |= (Opcode::kMM_ForceEvex) >> Opcode::kMM_Shift;
      }
    }

    // Check if EVEX is required by checking bits in `x` :  [@.......|xx.xxxxx|x......x|...x....].
    if (x & kEvexBits) {
      uint32_t y = ((x << 4) & 0x00080000u) |            // [@.......|....V...|........|........].
                   ((x >> 4) & 0x00000010u) ;            // [@.......|....V...|........|...R....].
      x  = (x & 0x00FF78E3u) | y;                        // [........|zLLbVaaa|0vvvv000|RXBR00mm].
      x  = x << 8;                                       // [zLLbVaaa|0vvvv000|RBBR00mm|00000000].
      x |= (opcode >> kVSHR_W    ) & 0x00800000u;        // [zLLbVaaa|Wvvvv000|RBBR00mm|00000000].
      x |= (opcode >> kVSHR_PP_EW) & 0x00830000u;        // [zLLbVaaa|Wvvvv0pp|RBBR00mm|00000000] (added PP and EVEX.W).
                                                         //      _     ____    ____
      x ^= 0x087CF000u | kX86ByteEvex;                   // [zLLbVaaa|Wvvvv1pp|RBBR00mm|01100010].

      writer.emit32uLE(x);
      writer.emit8(opcode.v);

      if (x & 0x10000000u) {
        // Broadcast, change the compressed displacement scale to either x4 (SHL 2) or x8 (SHL 3)
        // depending on instruction's W. If 'W' is 1 'SHL' must be 3, otherwise it must be 2.
        opcode &=~uint32_t(Opcode::kCDSHL_Mask);
        opcode |= ((x & 0x00800000u) ? 3u : 2u) << Opcode::kCDSHL_Shift;
      }
      else {
        // Add the compressed displacement 'SHF' to the opcode based on 'TTWLL'.
        // The index to `x86CDisp8SHL` is composed as `CDTT[4:3] | W[2] | LL[1:0]`.
        uint32_t TTWLL = ((opcode >> (Opcode::kCDTT_Shift - 3)) & 0x18) +
                         ((opcode >> (Opcode::kW_Shift    - 2)) & 0x04) +
                         ((x >> 29) & 0x3);
        opcode += x86CDisp8SHL[TTWLL];
      }
    }
    else {
      // Not EVEX, prepare `x` for VEX2 or VEX3:        x = [........|00L00000|0vvvv000|RXB0mmmm].
      x |= ((opcode >> (kVSHR_W  + 8)) & 0x8000u) |      // [00000000|00L00000|Wvvvv000|RXB0mmmm].
           ((opcode >> (kVSHR_PP + 8)) & 0x0300u) |      // [00000000|00L00000|Wvvvv0pp|RXB0mmmm].
           ((x      >> 11            ) & 0x0400u) ;      // [00000000|00L00000|WvvvvLpp|RXB0mmmm].

      // Clear a possible CDisp specified by EVEX.
      opcode &= ~Opcode::kCDSHL_Mask;

      // Check if VEX3 is required / forced:                [........|........|x.......|.xx..x..].
      if (x & 0x0008064u) {
        uint32_t xorMsk = x86VEXPrefix[x & 0xF] | (opcode << 24);

        // Clear 'FORCE-VEX3' bit and all high bits.
        x  = (x & (0x4 ^ 0xFFFF)) << 8;                  // [00000000|WvvvvLpp|RXB0m0mm|00000000].
                                                         //            ____    ___
        x ^= xorMsk;                                     // [_OPCODE_|WvvvvLpp|RXBmmmmm|VEX3_XOP].
        writer.emit32uLE(x);
      }
      else {
        // 'mmmmm' must be '00001'.
        ASMJIT_ASSERT((x & 0x1F) == 0x01);

        x = ((x >> 8) ^ x) ^ 0xF9;
        writer.emit8(kX86ByteVex2);
        writer.emit8(x);
        writer.emit8(opcode.v);
      }
    }
  }

  // MOD|SIB address.
  if (!commonInfo->hasFlag(InstDB::kFlagVsib))
    goto EmitModSib;

  // MOD|VSIB address without INDEX is invalid.
  if (rmInfo & kX86MemInfo_Index)
    goto EmitModVSib;
  goto InvalidInstruction;

  // --------------------------------------------------------------------------
  // [Emit - Jmp/Jcc/Call]
  // --------------------------------------------------------------------------

EmitJmpCall:
  {
    // Emit REX prefix if asked for (64-bit only).
    uint32_t rex = opcode.extractRex(options);
    if (ASMJIT_UNLIKELY(x86IsRexInvalid(rex)))
      goto InvalidRexPrefix;
    rex &= ~kX86ByteInvalidRex & 0xFF;
    writer.emit8If(rex | kX86ByteRex, rex != 0);

    uint64_t ip = uint64_t(writer.offsetFrom(_bufferData));
    uint32_t rel32 = 0;
    uint32_t opCode8 = x86AltOpcodeOf(instInfo);

    uint32_t inst8Size  = 1 + 1; //          OPCODE + REL8 .
    uint32_t inst32Size = 1 + 4; // [PREFIX] OPCODE + REL32.

    // Jcc instructions with 32-bit displacement use 0x0F prefix,
    // other instructions don't. No other prefixes are used by X86.
    ASMJIT_ASSERT((opCode8 & Opcode::kMM_Mask) == 0);
    ASMJIT_ASSERT((opcode  & Opcode::kMM_Mask) == 0 ||
                  (opcode  & Opcode::kMM_Mask) == Opcode::kMM_0F);

    // Only one of these should be used at the same time.
    inst32Size += uint32_t(opReg != 0);
    inst32Size += uint32_t((opcode & Opcode::kMM_Mask) == Opcode::kMM_0F);

    if (rmRel->isLabel()) {
      label = _code->labelEntry(rmRel->as<Label>());
      if (ASMJIT_UNLIKELY(!label))
        goto InvalidLabel;

      if (label->isBoundTo(_section)) {
        // Label bound to the current section.
        rel32 = uint32_t((label->offset() - ip - inst32Size) & 0xFFFFFFFFu);
        goto EmitJmpCallRel;
      }
      else {
        // Non-bound label or label bound to a different section.
        if (opCode8 && (!opcode.v || (options & Inst::kOptionShortForm))) {
          writer.emit8(opCode8);

          // Record DISP8 (non-bound label).
          relOffset = -1;
          relSize = 1;
          goto EmitRel;
        }
        else {
          // Refuse also 'short' prefix, if specified.
          if (ASMJIT_UNLIKELY(!opcode.v || (options & Inst::kOptionShortForm) != 0))
            goto InvalidDisplacement;

          writer.emit8If(0x0F, (opcode & Opcode::kMM_Mask) != 0);// Emit 0F prefix.
          writer.emit8(opcode.v);                                // Emit opcode.
          writer.emit8If(x86EncodeMod(3, opReg, 0), opReg != 0); // Emit MOD.

          // Record DISP32 (non-bound label).
          relOffset = -4;
          relSize = 4;
          goto EmitRel;
        }
      }
    }

    if (rmRel->isImm()) {
      uint64_t baseAddress = code()->baseAddress();
      uint64_t jumpAddress = rmRel->as<Imm>().valueAs<uint64_t>();

      // If the base-address is known calculate a relative displacement and
      // check if it fits in 32 bits (which is always true in 32-bit mode).
      // Emit relative displacement as it was a bound label if all checks are ok.
      if (baseAddress != Globals::kNoBaseAddress) {
        uint64_t rel64 = jumpAddress - (ip + baseAddress) - inst32Size;
        if (Environment::is32Bit(arch()) || Support::isInt32(int64_t(rel64))) {
          rel32 = uint32_t(rel64 & 0xFFFFFFFFu);
          goto EmitJmpCallRel;
        }
        else {
          // Relative displacement exceeds 32-bits - relocator can only
          // insert trampoline for jmp/call, but not for jcc/jecxz.
          if (ASMJIT_UNLIKELY(!x86IsJmpOrCall(instId)))
            goto InvalidDisplacement;
        }
      }

      err = _code->newRelocEntry(&re, RelocEntry::kTypeAbsToRel);
      if (ASMJIT_UNLIKELY(err))
        goto Failed;

      re->_sourceOffset = offset();
      re->_sourceSectionId = _section->id();
      re->_payload = jumpAddress;

      if (ASMJIT_LIKELY(opcode.v)) {
        // 64-bit: Emit REX prefix so the instruction can be patched later.
        // REX prefix does nothing if not patched, but allows to patch the
        // instruction to use MOD/M and to point to a memory where the final
        // 64-bit address is stored.
        if (Environment::is64Bit(arch()) && x86IsJmpOrCall(instId)) {
          if (!rex)
            writer.emit8(kX86ByteRex);

          err = _code->addAddressToAddressTable(jumpAddress);
          if (ASMJIT_UNLIKELY(err))
            goto Failed;

          re->_relocType = RelocEntry::kTypeX64AddressEntry;
        }

        writer.emit8If(0x0F, (opcode & Opcode::kMM_Mask) != 0);  // Emit 0F prefix.
        writer.emit8(opcode.v);                                  // Emit opcode.
        writer.emit8If(x86EncodeMod(3, opReg, 0), opReg != 0);   // Emit MOD.
        re->_format.resetToDataValue(4);
        re->_format.setLeadingAndTrailingSize(writer.offsetFrom(_bufferPtr), immSize);
        writer.emit32uLE(0);                                     // Emit DISP32.
      }
      else {
        writer.emit8(opCode8);                                   // Emit opcode.
        re->_format.resetToDataValue(4);
        re->_format.setLeadingAndTrailingSize(writer.offsetFrom(_bufferPtr), immSize);
        writer.emit8(0);                                         // Emit DISP8 (zero).
      }
      goto EmitDone;
    }

    // Not Label|Imm -> Invalid.
    goto InvalidInstruction;

    // Emit jmp/call with relative displacement known at assembly-time. Decide
    // between 8-bit and 32-bit displacement encoding. Some instructions only
    // allow either 8-bit or 32-bit encoding, others allow both encodings.
EmitJmpCallRel:
    if (Support::isInt8(int32_t(rel32 + inst32Size - inst8Size)) && opCode8 && !(options & Inst::kOptionLongForm)) {
      options |= Inst::kOptionShortForm;
      writer.emit8(opCode8);                                     // Emit opcode
      writer.emit8(rel32 + inst32Size - inst8Size);              // Emit DISP8.
      goto EmitDone;
    }
    else {
      if (ASMJIT_UNLIKELY(!opcode.v || (options & Inst::kOptionShortForm) != 0))
        goto InvalidDisplacement;

      options &= ~Inst::kOptionShortForm;
      writer.emit8If(0x0F, (opcode & Opcode::kMM_Mask) != 0);    // Emit 0x0F prefix.
      writer.emit8(opcode.v);                                    // Emit Opcode.
      writer.emit8If(x86EncodeMod(3, opReg, 0), opReg != 0);     // Emit MOD.
      writer.emit32uLE(rel32);                                   // Emit DISP32.
      goto EmitDone;
    }
  }

  // --------------------------------------------------------------------------
  // [Emit - Relative]
  // --------------------------------------------------------------------------

EmitRel:
  {
    ASMJIT_ASSERT(relSize == 1 || relSize == 4);

    // Chain with label.
    size_t offset = size_t(writer.offsetFrom(_bufferData));
    OffsetFormat of;
    of.resetToDataValue(relSize);

    LabelLink* link = _code->newLabelLink(label, _section->id(), offset, relOffset, of);
    if (ASMJIT_UNLIKELY(!link))
      goto OutOfMemory;

    if (re)
      link->relocId = re->id();

    // Emit dummy zeros, must be patched later when the reference becomes known.
    writer.emitZeros(relSize);
  }
  writer.emitImmediate(uint64_t(immValue), immSize);

  // --------------------------------------------------------------------------
  // [Done]
  // --------------------------------------------------------------------------

EmitDone:
  if (ASMJIT_UNLIKELY(options & Inst::kOptionReserved)) {
#ifndef ASMJIT_NO_LOGGING
    if (_logger)
      EmitterUtils::logInstructionEmitted(this, instId, options, o0, o1, o2, opExt, relSize, immSize, writer.cursor());
#endif
  }

  resetExtraReg();
  resetInstOptions();
  resetInlineComment();

  writer.done(this);
  return kErrorOk;

  // --------------------------------------------------------------------------
  // [Error Handler]
  // --------------------------------------------------------------------------

#define ERROR_HANDLER(ERR) ERR: err = DebugUtils::errored(kError##ERR); goto Failed;
  ERROR_HANDLER(OutOfMemory)
  ERROR_HANDLER(InvalidLabel)
  ERROR_HANDLER(InvalidInstruction)
  ERROR_HANDLER(InvalidLockPrefix)
  ERROR_HANDLER(InvalidXAcquirePrefix)
  ERROR_HANDLER(InvalidXReleasePrefix)
  ERROR_HANDLER(InvalidRepPrefix)
  ERROR_HANDLER(InvalidRexPrefix)
  ERROR_HANDLER(InvalidEROrSAE)
  ERROR_HANDLER(InvalidAddress)
  ERROR_HANDLER(InvalidAddressIndex)
  ERROR_HANDLER(InvalidAddress64Bit)
  ERROR_HANDLER(InvalidDisplacement)
  ERROR_HANDLER(InvalidPhysId)
  ERROR_HANDLER(InvalidSegment)
  ERROR_HANDLER(InvalidImmediate)
  ERROR_HANDLER(OperandSizeMismatch)
  ERROR_HANDLER(AmbiguousOperandSize)
  ERROR_HANDLER(NotConsecutiveRegs)
#undef ERROR_HANDLER

Failed:
#ifndef ASMJIT_NO_LOGGING
  return EmitterUtils::logInstructionFailed(this, err, instId, options, o0, o1, o2, opExt);
#else
  resetExtraReg();
  resetInstOptions();
  resetInlineComment();
  return reportError(err);
#endif
}

// ============================================================================
// [asmjit::x86::Assembler - Align]
// ============================================================================

Error Assembler::align(uint32_t alignMode, uint32_t alignment) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  if (ASMJIT_UNLIKELY(alignMode >= kAlignCount))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  if (alignment <= 1)
    return kErrorOk;

  if (ASMJIT_UNLIKELY(!Support::isPowerOf2(alignment) || alignment > Globals::kMaxAlignment))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  uint32_t i = uint32_t(Support::alignUpDiff<size_t>(offset(), alignment));
  if (i > 0) {
    CodeWriter writer(this);
    ASMJIT_PROPAGATE(writer.ensureSpace(this, i));

    uint8_t pattern = 0x00;
    switch (alignMode) {
      case kAlignCode: {
        if (hasEncodingOption(kEncodingOptionOptimizedAlign)) {
          // Intel 64 and IA-32 Architectures Software Developer's Manual - Volume 2B (NOP).
          enum { kMaxNopSize = 9 };

          static const uint8_t nopData[kMaxNopSize][kMaxNopSize] = {
            { 0x90 },
            { 0x66, 0x90 },
            { 0x0F, 0x1F, 0x00 },
            { 0x0F, 0x1F, 0x40, 0x00 },
            { 0x0F, 0x1F, 0x44, 0x00, 0x00 },
            { 0x66, 0x0F, 0x1F, 0x44, 0x00, 0x00 },
            { 0x0F, 0x1F, 0x80, 0x00, 0x00, 0x00, 0x00 },
            { 0x0F, 0x1F, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00 },
            { 0x66, 0x0F, 0x1F, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00 }
          };

          do {
            uint32_t n = Support::min<uint32_t>(i, kMaxNopSize);
            const uint8_t* src = nopData[n - 1];

            i -= n;
            do {
              writer.emit8(*src++);
            } while (--n);
          } while (i);
        }

        pattern = 0x90;
        break;
      }

      case kAlignData:
        pattern = 0xCC;
        break;

      case kAlignZero:
        // Pattern already set to zero.
        break;
    }

    while (i) {
      writer.emit8(pattern);
      i--;
    }

    writer.done(this);
  }

#ifndef ASMJIT_NO_LOGGING
  if (_logger) {
    StringTmp<128> sb;
    sb.appendChars(' ', _logger->indentation(FormatOptions::kIndentationCode));
    sb.appendFormat("align %u\n", alignment);
    _logger->log(sb);
  }
#endif

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::Assembler - Events]
// ============================================================================

Error Assembler::onAttach(CodeHolder* code) noexcept {
  uint32_t arch = code->arch();
  if (!Environment::isFamilyX86(arch))
    return DebugUtils::errored(kErrorInvalidArch);

  ASMJIT_PROPAGATE(Base::onAttach(code));

  if (Environment::is32Bit(arch)) {
    // 32 bit architecture - X86.
    _forcedInstOptions |= Inst::_kOptionInvalidRex;
    _setAddressOverrideMask(kX86MemInfo_67H_X86);
  }
  else {
    // 64 bit architecture - X64.
    _forcedInstOptions &= ~Inst::_kOptionInvalidRex;
    _setAddressOverrideMask(kX86MemInfo_67H_X64);
  }

  return kErrorOk;
}

Error Assembler::onDetach(CodeHolder* code) noexcept {
  _forcedInstOptions &= ~Inst::_kOptionInvalidRex;
  _setAddressOverrideMask(0);

  return Base::onDetach(code);
}

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_X86
