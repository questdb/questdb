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

#ifndef ASMJIT_X86_X86OPERAND_H_INCLUDED
#define ASMJIT_X86_X86OPERAND_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/operand.h"
#include "../core/type.h"
#include "../x86/x86globals.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

#define ASMJIT_MEM_PTR(FUNC, SIZE)                                                    \
  static constexpr Mem FUNC(const Gp& base, int32_t offset = 0) noexcept {            \
    return Mem(base, offset, SIZE);                                                   \
  }                                                                                   \
  static constexpr Mem FUNC(const Gp& base, const Gp& index, uint32_t shift = 0, int32_t offset = 0) noexcept { \
    return Mem(base, index, shift, offset, SIZE);                                     \
  }                                                                                   \
  static constexpr Mem FUNC(const Gp& base, const Vec& index, uint32_t shift = 0, int32_t offset = 0) noexcept { \
    return Mem(base, index, shift, offset, SIZE);                                     \
  }                                                                                   \
  static constexpr Mem FUNC(const Label& base, int32_t offset = 0) noexcept {         \
    return Mem(base, offset, SIZE);                                                   \
  }                                                                                   \
  static constexpr Mem FUNC(const Label& base, const Gp& index, uint32_t shift = 0, int32_t offset = 0) noexcept { \
    return Mem(base, index, shift, offset, SIZE);                                     \
  }                                                                                   \
  static constexpr Mem FUNC(const Rip& rip_, int32_t offset = 0) noexcept {           \
    return Mem(rip_, offset, SIZE);                                                   \
  }                                                                                   \
  static constexpr Mem FUNC(uint64_t base) noexcept {                                 \
    return Mem(base, SIZE);                                                           \
  }                                                                                   \
  static constexpr Mem FUNC(uint64_t base, const Gp& index, uint32_t shift = 0) noexcept { \
    return Mem(base, index, shift, SIZE);                                             \
  }                                                                                   \
  static constexpr Mem FUNC(uint64_t base, const Vec& index, uint32_t shift = 0) noexcept { \
    return Mem(base, index, shift, SIZE);                                             \
  }                                                                                   \
                                                                                      \
  static constexpr Mem FUNC##_abs(uint64_t base) noexcept {                           \
    return Mem(base, SIZE, Mem::kSignatureMemAbs);                                    \
  }                                                                                   \
  static constexpr Mem FUNC##_abs(uint64_t base, const Gp& index, uint32_t shift = 0) noexcept { \
    return Mem(base, index, shift, SIZE, Mem::kSignatureMemAbs);                      \
  }                                                                                   \
  static constexpr Mem FUNC##_abs(uint64_t base, const Vec& index, uint32_t shift = 0) noexcept { \
    return Mem(base, index, shift, SIZE, Mem::kSignatureMemAbs);                      \
  }                                                                                   \
                                                                                      \
  static constexpr Mem FUNC##_rel(uint64_t base) noexcept {                           \
    return Mem(base, SIZE, Mem::kSignatureMemRel);                                    \
  }                                                                                   \
  static constexpr Mem FUNC##_rel(uint64_t base, const Gp& index, uint32_t shift = 0) noexcept { \
    return Mem(base, index, shift, SIZE, Mem::kSignatureMemRel);                      \
  }                                                                                   \
  static constexpr Mem FUNC##_rel(uint64_t base, const Vec& index, uint32_t shift = 0) noexcept { \
    return Mem(base, index, shift, SIZE, Mem::kSignatureMemRel);                      \
  }

//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [Forward Declarations]
// ============================================================================

class Reg;
class Mem;

class Gp;
class Gpb;
class GpbLo;
class GpbHi;
class Gpw;
class Gpd;
class Gpq;
class Vec;
class Xmm;
class Ymm;
class Zmm;
class Mm;
class KReg;
class SReg;
class CReg;
class DReg;
class St;
class Bnd;
class Tmm;
class Rip;

// ============================================================================
// [asmjit::x86::Reg]
// ============================================================================

//! Register traits (X86).
//!
//! Register traits contains information about a particular register type. It's
//! used by asmjit to setup register information on-the-fly and to populate
//! tables that contain register information (this way it's possible to change
//! register types and groups without having to reorder these tables).
template<uint32_t REG_TYPE>
struct RegTraits : public BaseRegTraits {};

//! \cond
// <--------------------+-----+-------------------------+------------------------+---+---+----------------+
//                      | Reg |        Reg-Type         |        Reg-Group       |Sz |Cnt|     TypeId     |
// <--------------------+-----+-------------------------+------------------------+---+---+----------------+
ASMJIT_DEFINE_REG_TRAITS(GpbLo, BaseReg::kTypeGp8Lo     , BaseReg::kGroupGp      , 1 , 16, Type::kIdI8    );
ASMJIT_DEFINE_REG_TRAITS(GpbHi, BaseReg::kTypeGp8Hi     , BaseReg::kGroupGp      , 1 , 4 , Type::kIdI8    );
ASMJIT_DEFINE_REG_TRAITS(Gpw  , BaseReg::kTypeGp16      , BaseReg::kGroupGp      , 2 , 16, Type::kIdI16   );
ASMJIT_DEFINE_REG_TRAITS(Gpd  , BaseReg::kTypeGp32      , BaseReg::kGroupGp      , 4 , 16, Type::kIdI32   );
ASMJIT_DEFINE_REG_TRAITS(Gpq  , BaseReg::kTypeGp64      , BaseReg::kGroupGp      , 8 , 16, Type::kIdI64   );
ASMJIT_DEFINE_REG_TRAITS(Xmm  , BaseReg::kTypeVec128    , BaseReg::kGroupVec     , 16, 32, Type::kIdI32x4 );
ASMJIT_DEFINE_REG_TRAITS(Ymm  , BaseReg::kTypeVec256    , BaseReg::kGroupVec     , 32, 32, Type::kIdI32x8 );
ASMJIT_DEFINE_REG_TRAITS(Zmm  , BaseReg::kTypeVec512    , BaseReg::kGroupVec     , 64, 32, Type::kIdI32x16);
ASMJIT_DEFINE_REG_TRAITS(Mm   , BaseReg::kTypeOther0    , BaseReg::kGroupOther0  , 8 , 8 , Type::kIdMmx64 );
ASMJIT_DEFINE_REG_TRAITS(KReg , BaseReg::kTypeOther1    , BaseReg::kGroupOther1  , 0 , 8 , Type::kIdVoid  );
ASMJIT_DEFINE_REG_TRAITS(SReg , BaseReg::kTypeCustom + 0, BaseReg::kGroupVirt + 0, 2 , 7 , Type::kIdVoid  );
ASMJIT_DEFINE_REG_TRAITS(CReg , BaseReg::kTypeCustom + 1, BaseReg::kGroupVirt + 1, 0 , 16, Type::kIdVoid  );
ASMJIT_DEFINE_REG_TRAITS(DReg , BaseReg::kTypeCustom + 2, BaseReg::kGroupVirt + 2, 0 , 16, Type::kIdVoid  );
ASMJIT_DEFINE_REG_TRAITS(St   , BaseReg::kTypeCustom + 3, BaseReg::kGroupVirt + 3, 10, 8 , Type::kIdF80   );
ASMJIT_DEFINE_REG_TRAITS(Bnd  , BaseReg::kTypeCustom + 4, BaseReg::kGroupVirt + 4, 16, 4 , Type::kIdVoid  );
ASMJIT_DEFINE_REG_TRAITS(Tmm  , BaseReg::kTypeCustom + 5, BaseReg::kGroupVirt + 5, 0 , 8 , Type::kIdVoid  );
ASMJIT_DEFINE_REG_TRAITS(Rip  , BaseReg::kTypeIP        , BaseReg::kGroupVirt + 6, 0 , 1 , Type::kIdVoid  );
//! \endcond

//! Register (X86).
class Reg : public BaseReg {
public:
  ASMJIT_DEFINE_ABSTRACT_REG(Reg, BaseReg)

  //! Register type.
  enum RegType : uint32_t {
    //! No register type or invalid register.
    kTypeNone = BaseReg::kTypeNone,

    //! Low GPB register (AL, BL, CL, DL, ...).
    kTypeGpbLo = BaseReg::kTypeGp8Lo,
    //! High GPB register (AH, BH, CH, DH only).
    kTypeGpbHi = BaseReg::kTypeGp8Hi,
    //! GPW register.
    kTypeGpw = BaseReg::kTypeGp16,
    //! GPD register.
    kTypeGpd = BaseReg::kTypeGp32,
    //! GPQ register (64-bit).
    kTypeGpq = BaseReg::kTypeGp64,
    //! XMM register (SSE+).
    kTypeXmm = BaseReg::kTypeVec128,
    //! YMM register (AVX+).
    kTypeYmm = BaseReg::kTypeVec256,
    //! ZMM register (AVX512+).
    kTypeZmm = BaseReg::kTypeVec512,
    //! MMX register.
    kTypeMm = BaseReg::kTypeOther0,
    //! K register (AVX512+).
    kTypeKReg = BaseReg::kTypeOther1,
    //! Instruction pointer (EIP, RIP).
    kTypeRip = BaseReg::kTypeIP,
    //! Segment register (None, ES, CS, SS, DS, FS, GS).
    kTypeSReg = BaseReg::kTypeCustom + 0,
    //! Control register (CR).
    kTypeCReg = BaseReg::kTypeCustom + 1,
    //! Debug register (DR).
    kTypeDReg = BaseReg::kTypeCustom + 2,
    //! FPU (x87) register.
    kTypeSt = BaseReg::kTypeCustom + 3,
    //! Bound register (BND).
    kTypeBnd = BaseReg::kTypeCustom + 4,
    //! TMM register (AMX_TILE)
    kTypeTmm = BaseReg::kTypeCustom + 5,

    //! Count of register types.
    kTypeCount = BaseReg::kTypeCustom + 6
  };

  //! Register group.
  enum RegGroup : uint32_t {
    //! GP register group or none (universal).
    kGroupGp   = BaseReg::kGroupGp,
    //! XMM|YMM|ZMM register group (universal).
    kGroupVec  = BaseReg::kGroupVec,
    //! MMX register group (legacy).
    kGroupMm   = BaseReg::kGroupOther0,
    //! K register group.
    kGroupKReg = BaseReg::kGroupOther1,

    // These are not managed by Compiler nor used by Func-API:

    //! Segment register group.
    kGroupSReg = BaseReg::kGroupVirt+0,
    //! Control register group.
    kGroupCReg = BaseReg::kGroupVirt+1,
    //! Debug register group.
    kGroupDReg = BaseReg::kGroupVirt+2,
    //! FPU register group.
    kGroupSt   = BaseReg::kGroupVirt+3,
    //! Bound register group.
    kGroupBnd  = BaseReg::kGroupVirt+4,
    //! TMM register group.
    kGroupTmm  = BaseReg::kGroupVirt+5,
    //! Instrucion pointer (IP).
    kGroupRip  = BaseReg::kGroupVirt+6,

    //! Count of all register groups.
    kGroupCount
  };

  //! Tests whether the register is a GPB register (8-bit).
  constexpr bool isGpb() const noexcept { return size() == 1; }
  //! Tests whether the register is a low GPB register (8-bit).
  constexpr bool isGpbLo() const noexcept { return hasBaseSignature(RegTraits<kTypeGpbLo>::kSignature); }
  //! Tests whether the register is a high GPB register (8-bit).
  constexpr bool isGpbHi() const noexcept { return hasBaseSignature(RegTraits<kTypeGpbHi>::kSignature); }
  //! Tests whether the register is a GPW register (16-bit).
  constexpr bool isGpw() const noexcept { return hasBaseSignature(RegTraits<kTypeGpw>::kSignature); }
  //! Tests whether the register is a GPD register (32-bit).
  constexpr bool isGpd() const noexcept { return hasBaseSignature(RegTraits<kTypeGpd>::kSignature); }
  //! Tests whether the register is a GPQ register (64-bit).
  constexpr bool isGpq() const noexcept { return hasBaseSignature(RegTraits<kTypeGpq>::kSignature); }
  //! Tests whether the register is an XMM register (128-bit).
  constexpr bool isXmm() const noexcept { return hasBaseSignature(RegTraits<kTypeXmm>::kSignature); }
  //! Tests whether the register is a YMM register (256-bit).
  constexpr bool isYmm() const noexcept { return hasBaseSignature(RegTraits<kTypeYmm>::kSignature); }
  //! Tests whether the register is a ZMM register (512-bit).
  constexpr bool isZmm() const noexcept { return hasBaseSignature(RegTraits<kTypeZmm>::kSignature); }
  //! Tests whether the register is an MMX register (64-bit).
  constexpr bool isMm() const noexcept { return hasBaseSignature(RegTraits<kTypeMm>::kSignature); }
  //! Tests whether the register is a K register (64-bit).
  constexpr bool isKReg() const noexcept { return hasBaseSignature(RegTraits<kTypeKReg>::kSignature); }
  //! Tests whether the register is a segment register.
  constexpr bool isSReg() const noexcept { return hasBaseSignature(RegTraits<kTypeSReg>::kSignature); }
  //! Tests whether the register is a control register.
  constexpr bool isCReg() const noexcept { return hasBaseSignature(RegTraits<kTypeCReg>::kSignature); }
  //! Tests whether the register is a debug register.
  constexpr bool isDReg() const noexcept { return hasBaseSignature(RegTraits<kTypeDReg>::kSignature); }
  //! Tests whether the register is an FPU register (80-bit).
  constexpr bool isSt() const noexcept { return hasBaseSignature(RegTraits<kTypeSt>::kSignature); }
  //! Tests whether the register is a bound register.
  constexpr bool isBnd() const noexcept { return hasBaseSignature(RegTraits<kTypeBnd>::kSignature); }
  //! Tests whether the register is a TMM register.
  constexpr bool isTmm() const noexcept { return hasBaseSignature(RegTraits<kTypeTmm>::kSignature); }
  //! Tests whether the register is RIP.
  constexpr bool isRip() const noexcept { return hasBaseSignature(RegTraits<kTypeRip>::kSignature); }

  template<uint32_t REG_TYPE>
  inline void setRegT(uint32_t rId) noexcept {
    setSignature(RegTraits<REG_TYPE>::kSignature);
    setId(rId);
  }

  inline void setTypeAndId(uint32_t rType, uint32_t rId) noexcept {
    ASMJIT_ASSERT(rType < kTypeCount);
    setSignature(signatureOf(rType));
    setId(rId);
  }

  static inline uint32_t groupOf(uint32_t rType) noexcept { return _archTraits[Environment::kArchX86].regTypeToGroup(rType); }
  static inline uint32_t typeIdOf(uint32_t rType) noexcept { return _archTraits[Environment::kArchX86].regTypeToTypeId(rType); }
  static inline uint32_t signatureOf(uint32_t rType) noexcept { return _archTraits[Environment::kArchX86].regTypeToSignature(rType); }

  template<uint32_t REG_TYPE>
  static inline uint32_t groupOfT() noexcept { return RegTraits<REG_TYPE>::kGroup; }

  template<uint32_t REG_TYPE>
  static inline uint32_t typeIdOfT() noexcept { return RegTraits<REG_TYPE>::kTypeId; }

  template<uint32_t REG_TYPE>
  static inline uint32_t signatureOfT() noexcept { return RegTraits<REG_TYPE>::kSignature; }

  static inline uint32_t signatureOfVecByType(uint32_t typeId) noexcept {
    return typeId <= Type::_kIdVec128End ? RegTraits<kTypeXmm>::kSignature :
           typeId <= Type::_kIdVec256End ? RegTraits<kTypeYmm>::kSignature : RegTraits<kTypeZmm>::kSignature;
  }

  static inline uint32_t signatureOfVecBySize(uint32_t size) noexcept {
    return size <= 16 ? RegTraits<kTypeXmm>::kSignature :
           size <= 32 ? RegTraits<kTypeYmm>::kSignature : RegTraits<kTypeZmm>::kSignature;
  }

  //! Tests whether the `op` operand is either a low or high 8-bit GPB register.
  static inline bool isGpb(const Operand_& op) noexcept {
    // Check operand type, register group, and size. Not interested in register type.
    const uint32_t kSgn = (Operand::kOpReg << kSignatureOpTypeShift) |
                          (1               << kSignatureSizeShift  ) ;
    return (op.signature() & (kSignatureOpTypeMask | kSignatureSizeMask)) == kSgn;
  }

  static inline bool isGpbLo(const Operand_& op) noexcept { return op.as<Reg>().isGpbLo(); }
  static inline bool isGpbHi(const Operand_& op) noexcept { return op.as<Reg>().isGpbHi(); }
  static inline bool isGpw(const Operand_& op) noexcept { return op.as<Reg>().isGpw(); }
  static inline bool isGpd(const Operand_& op) noexcept { return op.as<Reg>().isGpd(); }
  static inline bool isGpq(const Operand_& op) noexcept { return op.as<Reg>().isGpq(); }
  static inline bool isXmm(const Operand_& op) noexcept { return op.as<Reg>().isXmm(); }
  static inline bool isYmm(const Operand_& op) noexcept { return op.as<Reg>().isYmm(); }
  static inline bool isZmm(const Operand_& op) noexcept { return op.as<Reg>().isZmm(); }
  static inline bool isMm(const Operand_& op) noexcept { return op.as<Reg>().isMm(); }
  static inline bool isKReg(const Operand_& op) noexcept { return op.as<Reg>().isKReg(); }
  static inline bool isSReg(const Operand_& op) noexcept { return op.as<Reg>().isSReg(); }
  static inline bool isCReg(const Operand_& op) noexcept { return op.as<Reg>().isCReg(); }
  static inline bool isDReg(const Operand_& op) noexcept { return op.as<Reg>().isDReg(); }
  static inline bool isSt(const Operand_& op) noexcept { return op.as<Reg>().isSt(); }
  static inline bool isBnd(const Operand_& op) noexcept { return op.as<Reg>().isBnd(); }
  static inline bool isTmm(const Operand_& op) noexcept { return op.as<Reg>().isTmm(); }
  static inline bool isRip(const Operand_& op) noexcept { return op.as<Reg>().isRip(); }

  static inline bool isGpb(const Operand_& op, uint32_t rId) noexcept { return isGpb(op) & (op.id() == rId); }
  static inline bool isGpbLo(const Operand_& op, uint32_t rId) noexcept { return isGpbLo(op) & (op.id() == rId); }
  static inline bool isGpbHi(const Operand_& op, uint32_t rId) noexcept { return isGpbHi(op) & (op.id() == rId); }
  static inline bool isGpw(const Operand_& op, uint32_t rId) noexcept { return isGpw(op) & (op.id() == rId); }
  static inline bool isGpd(const Operand_& op, uint32_t rId) noexcept { return isGpd(op) & (op.id() == rId); }
  static inline bool isGpq(const Operand_& op, uint32_t rId) noexcept { return isGpq(op) & (op.id() == rId); }
  static inline bool isXmm(const Operand_& op, uint32_t rId) noexcept { return isXmm(op) & (op.id() == rId); }
  static inline bool isYmm(const Operand_& op, uint32_t rId) noexcept { return isYmm(op) & (op.id() == rId); }
  static inline bool isZmm(const Operand_& op, uint32_t rId) noexcept { return isZmm(op) & (op.id() == rId); }
  static inline bool isMm(const Operand_& op, uint32_t rId) noexcept { return isMm(op) & (op.id() == rId); }
  static inline bool isKReg(const Operand_& op, uint32_t rId) noexcept { return isKReg(op) & (op.id() == rId); }
  static inline bool isSReg(const Operand_& op, uint32_t rId) noexcept { return isSReg(op) & (op.id() == rId); }
  static inline bool isCReg(const Operand_& op, uint32_t rId) noexcept { return isCReg(op) & (op.id() == rId); }
  static inline bool isDReg(const Operand_& op, uint32_t rId) noexcept { return isDReg(op) & (op.id() == rId); }
  static inline bool isSt(const Operand_& op, uint32_t rId) noexcept { return isSt(op) & (op.id() == rId); }
  static inline bool isBnd(const Operand_& op, uint32_t rId) noexcept { return isBnd(op) & (op.id() == rId); }
  static inline bool isTmm(const Operand_& op, uint32_t rId) noexcept { return isTmm(op) & (op.id() == rId); }
  static inline bool isRip(const Operand_& op, uint32_t rId) noexcept { return isRip(op) & (op.id() == rId); }
};

//! General purpose register (X86).
class Gp : public Reg {
public:
  ASMJIT_DEFINE_ABSTRACT_REG(Gp, Reg)

  //! Physical id (X86).
  //!
  //! \note Register indexes have been reduced to only support general purpose
  //! registers. There is no need to have enumerations with number suffix that
  //! expands to the exactly same value as the suffix value itself.
  enum Id : uint32_t {
    kIdAx  = 0,  //!< Physical id of AL|AH|AX|EAX|RAX registers.
    kIdCx  = 1,  //!< Physical id of CL|CH|CX|ECX|RCX registers.
    kIdDx  = 2,  //!< Physical id of DL|DH|DX|EDX|RDX registers.
    kIdBx  = 3,  //!< Physical id of BL|BH|BX|EBX|RBX registers.
    kIdSp  = 4,  //!< Physical id of SPL|SP|ESP|RSP registers.
    kIdBp  = 5,  //!< Physical id of BPL|BP|EBP|RBP registers.
    kIdSi  = 6,  //!< Physical id of SIL|SI|ESI|RSI registers.
    kIdDi  = 7,  //!< Physical id of DIL|DI|EDI|RDI registers.
    kIdR8  = 8,  //!< Physical id of R8B|R8W|R8D|R8 registers (64-bit only).
    kIdR9  = 9,  //!< Physical id of R9B|R9W|R9D|R9 registers (64-bit only).
    kIdR10 = 10, //!< Physical id of R10B|R10W|R10D|R10 registers (64-bit only).
    kIdR11 = 11, //!< Physical id of R11B|R11W|R11D|R11 registers (64-bit only).
    kIdR12 = 12, //!< Physical id of R12B|R12W|R12D|R12 registers (64-bit only).
    kIdR13 = 13, //!< Physical id of R13B|R13W|R13D|R13 registers (64-bit only).
    kIdR14 = 14, //!< Physical id of R14B|R14W|R14D|R14 registers (64-bit only).
    kIdR15 = 15  //!< Physical id of R15B|R15W|R15D|R15 registers (64-bit only).
  };

  //! Casts this register to 8-bit (LO) part.
  inline GpbLo r8() const noexcept;
  //! Casts this register to 8-bit (LO) part.
  inline GpbLo r8Lo() const noexcept;
  //! Casts this register to 8-bit (HI) part.
  inline GpbHi r8Hi() const noexcept;
  //! Casts this register to 16-bit.
  inline Gpw r16() const noexcept;
  //! Casts this register to 32-bit.
  inline Gpd r32() const noexcept;
  //! Casts this register to 64-bit.
  inline Gpq r64() const noexcept;
};

//! Vector register (XMM|YMM|ZMM) (X86).
class Vec : public Reg {
  ASMJIT_DEFINE_ABSTRACT_REG(Vec, Reg)

  //! Casts this register to XMM (clone).
  inline Xmm xmm() const noexcept;
  //! Casts this register to YMM.
  inline Ymm ymm() const noexcept;
  //! Casts this register to ZMM.
  inline Zmm zmm() const noexcept;

  //! Casts this register to a register that has half the size (or XMM if it's already XMM).
  inline Vec half() const noexcept {
    return Vec::fromSignatureAndId(type() == kTypeZmm ? signatureOfT<kTypeYmm>() : signatureOfT<kTypeXmm>(), id());
  }
};

//! Segment register (X86).
class SReg : public Reg {
  ASMJIT_DEFINE_FINAL_REG(SReg, Reg, RegTraits<kTypeSReg>)

  //! X86 segment id.
  enum Id : uint32_t {
    //! No segment (default).
    kIdNone = 0,
    //! ES segment.
    kIdEs = 1,
    //! CS segment.
    kIdCs = 2,
    //! SS segment.
    kIdSs = 3,
    //! DS segment.
    kIdDs = 4,
    //! FS segment.
    kIdFs = 5,
    //! GS segment.
    kIdGs = 6,

    //! Count of X86 segment registers supported by AsmJit.
    //!
    //! \note X86 architecture has 6 segment registers - ES, CS, SS, DS, FS, GS.
    //! X64 architecture lowers them down to just FS and GS. AsmJit supports 7
    //! segment registers - all addressable in both X86 and X64 modes and one
    //! extra called `SReg::kIdNone`, which is AsmJit specific and means that
    //! there is no segment register specified.
    kIdCount = 7
  };
};

//! GPB low or high register (X86).
class Gpb : public Gp { ASMJIT_DEFINE_ABSTRACT_REG(Gpb, Gp) };
//! GPB low register (X86).
class GpbLo : public Gpb { ASMJIT_DEFINE_FINAL_REG(GpbLo, Gpb, RegTraits<kTypeGpbLo>) };
//! GPB high register (X86).
class GpbHi : public Gpb { ASMJIT_DEFINE_FINAL_REG(GpbHi, Gpb, RegTraits<kTypeGpbHi>) };
//! GPW register (X86).
class Gpw : public Gp { ASMJIT_DEFINE_FINAL_REG(Gpw, Gp, RegTraits<kTypeGpw>) };
//! GPD register (X86).
class Gpd : public Gp { ASMJIT_DEFINE_FINAL_REG(Gpd, Gp, RegTraits<kTypeGpd>) };
//! GPQ register (X86_64).
class Gpq : public Gp { ASMJIT_DEFINE_FINAL_REG(Gpq, Gp, RegTraits<kTypeGpq>) };

//! 128-bit XMM register (SSE+).
class Xmm : public Vec {
  ASMJIT_DEFINE_FINAL_REG(Xmm, Vec, RegTraits<kTypeXmm>)
  //! Casts this register to a register that has half the size (XMM).
  inline Xmm half() const noexcept { return Xmm(id()); }
};

//! 256-bit YMM register (AVX+).
class Ymm : public Vec {
  ASMJIT_DEFINE_FINAL_REG(Ymm, Vec, RegTraits<kTypeYmm>)
  //! Casts this register to a register that has half the size (XMM).
  inline Xmm half() const noexcept { return Xmm(id()); }
};

//! 512-bit ZMM register (AVX512+).
class Zmm : public Vec {
  ASMJIT_DEFINE_FINAL_REG(Zmm, Vec, RegTraits<kTypeZmm>)
  //! Casts this register to a register that has half the size (YMM).
  inline Ymm half() const noexcept { return Ymm(id()); }
};

//! 64-bit MMX register (MMX+).
class Mm : public Reg { ASMJIT_DEFINE_FINAL_REG(Mm, Reg, RegTraits<kTypeMm>) };
//! 64-bit K register (AVX512+).
class KReg : public Reg { ASMJIT_DEFINE_FINAL_REG(KReg, Reg, RegTraits<kTypeKReg>) };
//! 32-bit or 64-bit control register (X86).
class CReg : public Reg { ASMJIT_DEFINE_FINAL_REG(CReg, Reg, RegTraits<kTypeCReg>) };
//! 32-bit or 64-bit debug register (X86).
class DReg : public Reg { ASMJIT_DEFINE_FINAL_REG(DReg, Reg, RegTraits<kTypeDReg>) };
//! 80-bit FPU register (X86).
class St : public Reg { ASMJIT_DEFINE_FINAL_REG(St, Reg, RegTraits<kTypeSt>) };
//! 128-bit BND register (BND+).
class Bnd : public Reg { ASMJIT_DEFINE_FINAL_REG(Bnd, Reg, RegTraits<kTypeBnd>) };
//! 8192-bit TMM register (AMX).
class Tmm : public Reg { ASMJIT_DEFINE_FINAL_REG(Tmm, Reg, RegTraits<kTypeTmm>) };
//! RIP register (X86).
class Rip : public Reg { ASMJIT_DEFINE_FINAL_REG(Rip, Reg, RegTraits<kTypeRip>) };

//! \cond
inline GpbLo Gp::r8() const noexcept { return GpbLo(id()); }
inline GpbLo Gp::r8Lo() const noexcept { return GpbLo(id()); }
inline GpbHi Gp::r8Hi() const noexcept { return GpbHi(id()); }
inline Gpw Gp::r16() const noexcept { return Gpw(id()); }
inline Gpd Gp::r32() const noexcept { return Gpd(id()); }
inline Gpq Gp::r64() const noexcept { return Gpq(id()); }
inline Xmm Vec::xmm() const noexcept { return Xmm(id()); }
inline Ymm Vec::ymm() const noexcept { return Ymm(id()); }
inline Zmm Vec::zmm() const noexcept { return Zmm(id()); }
//! \endcond

//! \namespace asmjit::x86::regs
//!
//! Registers provided by X86 and X64 ISAs are in both `asmjit::x86` and
//! `asmjit::x86::regs` namespaces so they can be included with using directive.
//! For example `using namespace asmjit::x86::regs` would include all registers,
//! but not other X86-specific API, whereas `using namespace asmjit::x86` would
//! include everything X86-specific.
#ifndef _DOXYGEN
namespace regs {
#endif

//! Creates an 8-bit low GPB register operand.
static constexpr GpbLo gpb(uint32_t rId) noexcept { return GpbLo(rId); }
//! Creates an 8-bit low GPB register operand.
static constexpr GpbLo gpb_lo(uint32_t rId) noexcept { return GpbLo(rId); }
//! Creates an 8-bit high GPB register operand.
static constexpr GpbHi gpb_hi(uint32_t rId) noexcept { return GpbHi(rId); }
//! Creates a 16-bit GPW register operand.
static constexpr Gpw gpw(uint32_t rId) noexcept { return Gpw(rId); }
//! Creates a 32-bit GPD register operand.
static constexpr Gpd gpd(uint32_t rId) noexcept { return Gpd(rId); }
//! Creates a 64-bit GPQ register operand (64-bit).
static constexpr Gpq gpq(uint32_t rId) noexcept { return Gpq(rId); }
//! Creates a 128-bit XMM register operand.
static constexpr Xmm xmm(uint32_t rId) noexcept { return Xmm(rId); }
//! Creates a 256-bit YMM register operand.
static constexpr Ymm ymm(uint32_t rId) noexcept { return Ymm(rId); }
//! Creates a 512-bit ZMM register operand.
static constexpr Zmm zmm(uint32_t rId) noexcept { return Zmm(rId); }
//! Creates a 64-bit Mm register operand.
static constexpr Mm mm(uint32_t rId) noexcept { return Mm(rId); }
//! Creates a 64-bit K register operand.
static constexpr KReg k(uint32_t rId) noexcept { return KReg(rId); }
//! Creates a 32-bit or 64-bit control register operand.
static constexpr CReg cr(uint32_t rId) noexcept { return CReg(rId); }
//! Creates a 32-bit or 64-bit debug register operand.
static constexpr DReg dr(uint32_t rId) noexcept { return DReg(rId); }
//! Creates an 80-bit st register operand.
static constexpr St st(uint32_t rId) noexcept { return St(rId); }
//! Creates a 128-bit bound register operand.
static constexpr Bnd bnd(uint32_t rId) noexcept { return Bnd(rId); }
//! Creates a TMM register operand.
static constexpr Tmm tmm(uint32_t rId) noexcept { return Tmm(rId); }

static constexpr GpbLo al = GpbLo(Gp::kIdAx);
static constexpr GpbLo bl = GpbLo(Gp::kIdBx);
static constexpr GpbLo cl = GpbLo(Gp::kIdCx);
static constexpr GpbLo dl = GpbLo(Gp::kIdDx);
static constexpr GpbLo spl = GpbLo(Gp::kIdSp);
static constexpr GpbLo bpl = GpbLo(Gp::kIdBp);
static constexpr GpbLo sil = GpbLo(Gp::kIdSi);
static constexpr GpbLo dil = GpbLo(Gp::kIdDi);
static constexpr GpbLo r8b = GpbLo(Gp::kIdR8);
static constexpr GpbLo r9b = GpbLo(Gp::kIdR9);
static constexpr GpbLo r10b = GpbLo(Gp::kIdR10);
static constexpr GpbLo r11b = GpbLo(Gp::kIdR11);
static constexpr GpbLo r12b = GpbLo(Gp::kIdR12);
static constexpr GpbLo r13b = GpbLo(Gp::kIdR13);
static constexpr GpbLo r14b = GpbLo(Gp::kIdR14);
static constexpr GpbLo r15b = GpbLo(Gp::kIdR15);

static constexpr GpbHi ah = GpbHi(Gp::kIdAx);
static constexpr GpbHi bh = GpbHi(Gp::kIdBx);
static constexpr GpbHi ch = GpbHi(Gp::kIdCx);
static constexpr GpbHi dh = GpbHi(Gp::kIdDx);

static constexpr Gpw ax = Gpw(Gp::kIdAx);
static constexpr Gpw bx = Gpw(Gp::kIdBx);
static constexpr Gpw cx = Gpw(Gp::kIdCx);
static constexpr Gpw dx = Gpw(Gp::kIdDx);
static constexpr Gpw sp = Gpw(Gp::kIdSp);
static constexpr Gpw bp = Gpw(Gp::kIdBp);
static constexpr Gpw si = Gpw(Gp::kIdSi);
static constexpr Gpw di = Gpw(Gp::kIdDi);
static constexpr Gpw r8w = Gpw(Gp::kIdR8);
static constexpr Gpw r9w = Gpw(Gp::kIdR9);
static constexpr Gpw r10w = Gpw(Gp::kIdR10);
static constexpr Gpw r11w = Gpw(Gp::kIdR11);
static constexpr Gpw r12w = Gpw(Gp::kIdR12);
static constexpr Gpw r13w = Gpw(Gp::kIdR13);
static constexpr Gpw r14w = Gpw(Gp::kIdR14);
static constexpr Gpw r15w = Gpw(Gp::kIdR15);

static constexpr Gpd eax = Gpd(Gp::kIdAx);
static constexpr Gpd ebx = Gpd(Gp::kIdBx);
static constexpr Gpd ecx = Gpd(Gp::kIdCx);
static constexpr Gpd edx = Gpd(Gp::kIdDx);
static constexpr Gpd esp = Gpd(Gp::kIdSp);
static constexpr Gpd ebp = Gpd(Gp::kIdBp);
static constexpr Gpd esi = Gpd(Gp::kIdSi);
static constexpr Gpd edi = Gpd(Gp::kIdDi);
static constexpr Gpd r8d = Gpd(Gp::kIdR8);
static constexpr Gpd r9d = Gpd(Gp::kIdR9);
static constexpr Gpd r10d = Gpd(Gp::kIdR10);
static constexpr Gpd r11d = Gpd(Gp::kIdR11);
static constexpr Gpd r12d = Gpd(Gp::kIdR12);
static constexpr Gpd r13d = Gpd(Gp::kIdR13);
static constexpr Gpd r14d = Gpd(Gp::kIdR14);
static constexpr Gpd r15d = Gpd(Gp::kIdR15);

static constexpr Gpq rax = Gpq(Gp::kIdAx);
static constexpr Gpq rbx = Gpq(Gp::kIdBx);
static constexpr Gpq rcx = Gpq(Gp::kIdCx);
static constexpr Gpq rdx = Gpq(Gp::kIdDx);
static constexpr Gpq rsp = Gpq(Gp::kIdSp);
static constexpr Gpq rbp = Gpq(Gp::kIdBp);
static constexpr Gpq rsi = Gpq(Gp::kIdSi);
static constexpr Gpq rdi = Gpq(Gp::kIdDi);
static constexpr Gpq r8 = Gpq(Gp::kIdR8);
static constexpr Gpq r9 = Gpq(Gp::kIdR9);
static constexpr Gpq r10 = Gpq(Gp::kIdR10);
static constexpr Gpq r11 = Gpq(Gp::kIdR11);
static constexpr Gpq r12 = Gpq(Gp::kIdR12);
static constexpr Gpq r13 = Gpq(Gp::kIdR13);
static constexpr Gpq r14 = Gpq(Gp::kIdR14);
static constexpr Gpq r15 = Gpq(Gp::kIdR15);

static constexpr Xmm xmm0 = Xmm(0);
static constexpr Xmm xmm1 = Xmm(1);
static constexpr Xmm xmm2 = Xmm(2);
static constexpr Xmm xmm3 = Xmm(3);
static constexpr Xmm xmm4 = Xmm(4);
static constexpr Xmm xmm5 = Xmm(5);
static constexpr Xmm xmm6 = Xmm(6);
static constexpr Xmm xmm7 = Xmm(7);
static constexpr Xmm xmm8 = Xmm(8);
static constexpr Xmm xmm9 = Xmm(9);
static constexpr Xmm xmm10 = Xmm(10);
static constexpr Xmm xmm11 = Xmm(11);
static constexpr Xmm xmm12 = Xmm(12);
static constexpr Xmm xmm13 = Xmm(13);
static constexpr Xmm xmm14 = Xmm(14);
static constexpr Xmm xmm15 = Xmm(15);
static constexpr Xmm xmm16 = Xmm(16);
static constexpr Xmm xmm17 = Xmm(17);
static constexpr Xmm xmm18 = Xmm(18);
static constexpr Xmm xmm19 = Xmm(19);
static constexpr Xmm xmm20 = Xmm(20);
static constexpr Xmm xmm21 = Xmm(21);
static constexpr Xmm xmm22 = Xmm(22);
static constexpr Xmm xmm23 = Xmm(23);
static constexpr Xmm xmm24 = Xmm(24);
static constexpr Xmm xmm25 = Xmm(25);
static constexpr Xmm xmm26 = Xmm(26);
static constexpr Xmm xmm27 = Xmm(27);
static constexpr Xmm xmm28 = Xmm(28);
static constexpr Xmm xmm29 = Xmm(29);
static constexpr Xmm xmm30 = Xmm(30);
static constexpr Xmm xmm31 = Xmm(31);

static constexpr Ymm ymm0 = Ymm(0);
static constexpr Ymm ymm1 = Ymm(1);
static constexpr Ymm ymm2 = Ymm(2);
static constexpr Ymm ymm3 = Ymm(3);
static constexpr Ymm ymm4 = Ymm(4);
static constexpr Ymm ymm5 = Ymm(5);
static constexpr Ymm ymm6 = Ymm(6);
static constexpr Ymm ymm7 = Ymm(7);
static constexpr Ymm ymm8 = Ymm(8);
static constexpr Ymm ymm9 = Ymm(9);
static constexpr Ymm ymm10 = Ymm(10);
static constexpr Ymm ymm11 = Ymm(11);
static constexpr Ymm ymm12 = Ymm(12);
static constexpr Ymm ymm13 = Ymm(13);
static constexpr Ymm ymm14 = Ymm(14);
static constexpr Ymm ymm15 = Ymm(15);
static constexpr Ymm ymm16 = Ymm(16);
static constexpr Ymm ymm17 = Ymm(17);
static constexpr Ymm ymm18 = Ymm(18);
static constexpr Ymm ymm19 = Ymm(19);
static constexpr Ymm ymm20 = Ymm(20);
static constexpr Ymm ymm21 = Ymm(21);
static constexpr Ymm ymm22 = Ymm(22);
static constexpr Ymm ymm23 = Ymm(23);
static constexpr Ymm ymm24 = Ymm(24);
static constexpr Ymm ymm25 = Ymm(25);
static constexpr Ymm ymm26 = Ymm(26);
static constexpr Ymm ymm27 = Ymm(27);
static constexpr Ymm ymm28 = Ymm(28);
static constexpr Ymm ymm29 = Ymm(29);
static constexpr Ymm ymm30 = Ymm(30);
static constexpr Ymm ymm31 = Ymm(31);

static constexpr Zmm zmm0 = Zmm(0);
static constexpr Zmm zmm1 = Zmm(1);
static constexpr Zmm zmm2 = Zmm(2);
static constexpr Zmm zmm3 = Zmm(3);
static constexpr Zmm zmm4 = Zmm(4);
static constexpr Zmm zmm5 = Zmm(5);
static constexpr Zmm zmm6 = Zmm(6);
static constexpr Zmm zmm7 = Zmm(7);
static constexpr Zmm zmm8 = Zmm(8);
static constexpr Zmm zmm9 = Zmm(9);
static constexpr Zmm zmm10 = Zmm(10);
static constexpr Zmm zmm11 = Zmm(11);
static constexpr Zmm zmm12 = Zmm(12);
static constexpr Zmm zmm13 = Zmm(13);
static constexpr Zmm zmm14 = Zmm(14);
static constexpr Zmm zmm15 = Zmm(15);
static constexpr Zmm zmm16 = Zmm(16);
static constexpr Zmm zmm17 = Zmm(17);
static constexpr Zmm zmm18 = Zmm(18);
static constexpr Zmm zmm19 = Zmm(19);
static constexpr Zmm zmm20 = Zmm(20);
static constexpr Zmm zmm21 = Zmm(21);
static constexpr Zmm zmm22 = Zmm(22);
static constexpr Zmm zmm23 = Zmm(23);
static constexpr Zmm zmm24 = Zmm(24);
static constexpr Zmm zmm25 = Zmm(25);
static constexpr Zmm zmm26 = Zmm(26);
static constexpr Zmm zmm27 = Zmm(27);
static constexpr Zmm zmm28 = Zmm(28);
static constexpr Zmm zmm29 = Zmm(29);
static constexpr Zmm zmm30 = Zmm(30);
static constexpr Zmm zmm31 = Zmm(31);

static constexpr Mm mm0 = Mm(0);
static constexpr Mm mm1 = Mm(1);
static constexpr Mm mm2 = Mm(2);
static constexpr Mm mm3 = Mm(3);
static constexpr Mm mm4 = Mm(4);
static constexpr Mm mm5 = Mm(5);
static constexpr Mm mm6 = Mm(6);
static constexpr Mm mm7 = Mm(7);

static constexpr KReg k0 = KReg(0);
static constexpr KReg k1 = KReg(1);
static constexpr KReg k2 = KReg(2);
static constexpr KReg k3 = KReg(3);
static constexpr KReg k4 = KReg(4);
static constexpr KReg k5 = KReg(5);
static constexpr KReg k6 = KReg(6);
static constexpr KReg k7 = KReg(7);

static constexpr SReg no_seg = SReg(SReg::kIdNone);
static constexpr SReg es = SReg(SReg::kIdEs);
static constexpr SReg cs = SReg(SReg::kIdCs);
static constexpr SReg ss = SReg(SReg::kIdSs);
static constexpr SReg ds = SReg(SReg::kIdDs);
static constexpr SReg fs = SReg(SReg::kIdFs);
static constexpr SReg gs = SReg(SReg::kIdGs);

static constexpr CReg cr0 = CReg(0);
static constexpr CReg cr1 = CReg(1);
static constexpr CReg cr2 = CReg(2);
static constexpr CReg cr3 = CReg(3);
static constexpr CReg cr4 = CReg(4);
static constexpr CReg cr5 = CReg(5);
static constexpr CReg cr6 = CReg(6);
static constexpr CReg cr7 = CReg(7);
static constexpr CReg cr8 = CReg(8);
static constexpr CReg cr9 = CReg(9);
static constexpr CReg cr10 = CReg(10);
static constexpr CReg cr11 = CReg(11);
static constexpr CReg cr12 = CReg(12);
static constexpr CReg cr13 = CReg(13);
static constexpr CReg cr14 = CReg(14);
static constexpr CReg cr15 = CReg(15);

static constexpr DReg dr0 = DReg(0);
static constexpr DReg dr1 = DReg(1);
static constexpr DReg dr2 = DReg(2);
static constexpr DReg dr3 = DReg(3);
static constexpr DReg dr4 = DReg(4);
static constexpr DReg dr5 = DReg(5);
static constexpr DReg dr6 = DReg(6);
static constexpr DReg dr7 = DReg(7);
static constexpr DReg dr8 = DReg(8);
static constexpr DReg dr9 = DReg(9);
static constexpr DReg dr10 = DReg(10);
static constexpr DReg dr11 = DReg(11);
static constexpr DReg dr12 = DReg(12);
static constexpr DReg dr13 = DReg(13);
static constexpr DReg dr14 = DReg(14);
static constexpr DReg dr15 = DReg(15);

static constexpr St st0 = St(0);
static constexpr St st1 = St(1);
static constexpr St st2 = St(2);
static constexpr St st3 = St(3);
static constexpr St st4 = St(4);
static constexpr St st5 = St(5);
static constexpr St st6 = St(6);
static constexpr St st7 = St(7);

static constexpr Bnd bnd0 = Bnd(0);
static constexpr Bnd bnd1 = Bnd(1);
static constexpr Bnd bnd2 = Bnd(2);
static constexpr Bnd bnd3 = Bnd(3);

static constexpr Tmm tmm0 = Tmm(0);
static constexpr Tmm tmm1 = Tmm(1);
static constexpr Tmm tmm2 = Tmm(2);
static constexpr Tmm tmm3 = Tmm(3);
static constexpr Tmm tmm4 = Tmm(4);
static constexpr Tmm tmm5 = Tmm(5);
static constexpr Tmm tmm6 = Tmm(6);
static constexpr Tmm tmm7 = Tmm(7);

static constexpr Rip rip = Rip(0);

#ifndef _DOXYGEN
} // {regs}

// Make `x86::regs` accessible through `x86` namespace as well.
using namespace regs;
#endif

// ============================================================================
// [asmjit::x86::Mem]
// ============================================================================

//! Memory operand.
class Mem : public BaseMem {
public:
  //! Additional bits of operand's signature used by `x86::Mem`.
  enum AdditionalBits : uint32_t {
    // Memory address type (2 bits).
    // |........|........|XX......|........|
    kSignatureMemAddrTypeShift = 14,
    kSignatureMemAddrTypeMask = 0x03u << kSignatureMemAddrTypeShift,

    // Memory shift amount (2 bits).
    // |........|......XX|........|........|
    kSignatureMemShiftValueShift = 16,
    kSignatureMemShiftValueMask = 0x03u << kSignatureMemShiftValueShift,

    // Memory segment reg (3 bits).
    // |........|...XXX..|........|........|
    kSignatureMemSegmentShift = 18,
    kSignatureMemSegmentMask = 0x07u << kSignatureMemSegmentShift,

    // Memory broadcast type (3 bits).
    // |........|XXX.....|........|........|
    kSignatureMemBroadcastShift = 21,
    kSignatureMemBroadcastMask = 0x7u << kSignatureMemBroadcastShift
  };

  //! Address type.
  enum AddrType : uint32_t {
    //! Default address type, Assembler will select the best type when necessary.
    kAddrTypeDefault = 0,
    //! Absolute address type.
    kAddrTypeAbs = 1,
    //! Relative address type.
    kAddrTypeRel = 2
  };

  //! Memory broadcast type.
  enum Broadcast : uint32_t {
    //! Broadcast {1to1}.
    kBroadcast1To1 = 0,
    //! Broadcast {1to2}.
    kBroadcast1To2 = 1,
    //! Broadcast {1to4}.
    kBroadcast1To4 = 2,
    //! Broadcast {1to8}.
    kBroadcast1To8 = 3,
    //! Broadcast {1to16}.
    kBroadcast1To16 = 4,
    //! Broadcast {1to32}.
    kBroadcast1To32 = 5,
    //! Broadcast {1to64}.
    kBroadcast1To64 = 6
  };

  //! \cond
  //! Shortcuts.
  enum SignatureMem : uint32_t {
    kSignatureMemAbs = kAddrTypeAbs << kSignatureMemAddrTypeShift,
    kSignatureMemRel = kAddrTypeRel << kSignatureMemAddrTypeShift
  };
  //! \endcond

  // --------------------------------------------------------------------------
  // [Construction / Destruction]
  // --------------------------------------------------------------------------

  //! Creates a default `Mem` operand that points to [0].
  constexpr Mem() noexcept
    : BaseMem() {}

  constexpr Mem(const Mem& other) noexcept
    : BaseMem(other) {}

  //! \cond INTERNAL
  //!
  //! A constructor used internally to create `Mem` operand from `Decomposed` data.
  constexpr explicit Mem(const Decomposed& d) noexcept
    : BaseMem(d) {}
  //! \endcond

  constexpr Mem(const Label& base, int32_t off, uint32_t size = 0, uint32_t flags = 0) noexcept
    : BaseMem(Decomposed { Label::kLabelTag, base.id(), 0, 0, off, size, flags }) {}

  constexpr Mem(const Label& base, const BaseReg& index, uint32_t shift, int32_t off, uint32_t size = 0, uint32_t flags = 0) noexcept
    : BaseMem(Decomposed { Label::kLabelTag, base.id(), index.type(), index.id(), off, size, flags | (shift << kSignatureMemShiftValueShift) }) {}

  constexpr Mem(const BaseReg& base, int32_t off, uint32_t size = 0, uint32_t flags = 0) noexcept
    : BaseMem(Decomposed { base.type(), base.id(), 0, 0, off, size, flags }) {}

  constexpr Mem(const BaseReg& base, const BaseReg& index, uint32_t shift, int32_t off, uint32_t size = 0, uint32_t flags = 0) noexcept
    : BaseMem(Decomposed { base.type(), base.id(), index.type(), index.id(), off, size, flags | (shift << kSignatureMemShiftValueShift) }) {}

  constexpr explicit Mem(uint64_t base, uint32_t size = 0, uint32_t flags = 0) noexcept
    : BaseMem(Decomposed { 0, uint32_t(base >> 32), 0, 0, int32_t(uint32_t(base & 0xFFFFFFFFu)), size, flags }) {}

  constexpr Mem(uint64_t base, const BaseReg& index, uint32_t shift = 0, uint32_t size = 0, uint32_t flags = 0) noexcept
    : BaseMem(Decomposed { 0, uint32_t(base >> 32), index.type(), index.id(), int32_t(uint32_t(base & 0xFFFFFFFFu)), size, flags | (shift << kSignatureMemShiftValueShift) }) {}

  constexpr Mem(Globals::Init_, uint32_t u0, uint32_t u1, uint32_t u2, uint32_t u3) noexcept
    : BaseMem(Globals::Init, u0, u1, u2, u3) {}

  inline explicit Mem(Globals::NoInit_) noexcept
    : BaseMem(Globals::NoInit) {}

  //! Clones the memory operand.
  constexpr Mem clone() const noexcept { return Mem(*this); }

  //! Creates a new copy of this memory operand adjusted by `off`.
  inline Mem cloneAdjusted(int64_t off) const noexcept {
    Mem result(*this);
    result.addOffset(off);
    return result;
  }

  //! Converts memory `baseType` and `baseId` to `x86::Reg` instance.
  //!
  //! The memory must have a valid base register otherwise the result will be wrong.
  inline Reg baseReg() const noexcept { return Reg::fromTypeAndId(baseType(), baseId()); }

  //! Converts memory `indexType` and `indexId` to `x86::Reg` instance.
  //!
  //! The memory must have a valid index register otherwise the result will be wrong.
  inline Reg indexReg() const noexcept { return Reg::fromTypeAndId(indexType(), indexId()); }

  constexpr Mem _1to1() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To1 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }
  constexpr Mem _1to2() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To2 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }
  constexpr Mem _1to4() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To4 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }
  constexpr Mem _1to8() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To8 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }
  constexpr Mem _1to16() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To16 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }
  constexpr Mem _1to32() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To32 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }
  constexpr Mem _1to64() const noexcept { return Mem(Globals::Init, (_signature & ~kSignatureMemBroadcastMask) | (kBroadcast1To64 << kSignatureMemBroadcastShift), _baseId, _data[0], _data[1]); }

  // --------------------------------------------------------------------------
  // [Mem]
  // --------------------------------------------------------------------------

  using BaseMem::setIndex;

  inline void setIndex(const BaseReg& index, uint32_t shift) noexcept {
    setIndex(index);
    setShift(shift);
  }

  //! Returns the address type (see \ref AddrType) of the memory operand.
  //!
  //! By default, address type of newly created memory operands is always \ref kAddrTypeDefault.
  constexpr uint32_t addrType() const noexcept { return _getSignaturePart<kSignatureMemAddrTypeMask>(); }
  //! Sets the address type to `addrType`, see \ref AddrType.
  inline void setAddrType(uint32_t addrType) noexcept { _setSignaturePart<kSignatureMemAddrTypeMask>(addrType); }
  //! Resets the address type to \ref kAddrTypeDefault.
  inline void resetAddrType() noexcept { _setSignaturePart<kSignatureMemAddrTypeMask>(0); }

  //! Tests whether the address type is \ref kAddrTypeAbs.
  constexpr bool isAbs() const noexcept { return addrType() == kAddrTypeAbs; }
  //! Sets the address type to \ref kAddrTypeAbs.
  inline void setAbs() noexcept { setAddrType(kAddrTypeAbs); }

  //! Tests whether the address type is \ref kAddrTypeRel.
  constexpr bool isRel() const noexcept { return addrType() == kAddrTypeRel; }
  //! Sets the address type to \ref kAddrTypeRel.
  inline void setRel() noexcept { setAddrType(kAddrTypeRel); }

  //! Tests whether the memory operand has a segment override.
  constexpr bool hasSegment() const noexcept { return _hasSignaturePart<kSignatureMemSegmentMask>(); }
  //! Returns the associated segment override as `SReg` operand.
  constexpr SReg segment() const noexcept { return SReg(segmentId()); }
  //! Returns segment override register id, see `SReg::Id`.
  constexpr uint32_t segmentId() const noexcept { return _getSignaturePart<kSignatureMemSegmentMask>(); }

  //! Sets the segment override to `seg`.
  inline void setSegment(const SReg& seg) noexcept { setSegment(seg.id()); }
  //! Sets the segment override to `id`.
  inline void setSegment(uint32_t rId) noexcept { _setSignaturePart<kSignatureMemSegmentMask>(rId); }
  //! Resets the segment override.
  inline void resetSegment() noexcept { _setSignaturePart<kSignatureMemSegmentMask>(0); }

  //! Tests whether the memory operand has shift (aka scale) value.
  constexpr bool hasShift() const noexcept { return _hasSignaturePart<kSignatureMemShiftValueMask>(); }
  //! Returns the memory operand's shift (aka scale) value.
  constexpr uint32_t shift() const noexcept { return _getSignaturePart<kSignatureMemShiftValueMask>(); }
  //! Sets the memory operand's shift (aka scale) value.
  inline void setShift(uint32_t shift) noexcept { _setSignaturePart<kSignatureMemShiftValueMask>(shift); }
  //! Resets the memory operand's shift (aka scale) value to zero.
  inline void resetShift() noexcept { _setSignaturePart<kSignatureMemShiftValueMask>(0); }

  //! Tests whether the memory operand has broadcast {1tox}.
  constexpr bool hasBroadcast() const noexcept { return _hasSignaturePart<kSignatureMemBroadcastMask>(); }
  //! Returns the memory operand's broadcast.
  constexpr uint32_t getBroadcast() const noexcept { return _getSignaturePart<kSignatureMemBroadcastMask>(); }
  //! Sets the memory operand's broadcast.
  inline void setBroadcast(uint32_t bcst) noexcept { _setSignaturePart<kSignatureMemBroadcastMask>(bcst); }
  //! Resets the memory operand's broadcast to none.
  inline void resetBroadcast() noexcept { _setSignaturePart<kSignatureMemBroadcastMask>(0); }

  // --------------------------------------------------------------------------
  // [Operator Overload]
  // --------------------------------------------------------------------------

  inline Mem& operator=(const Mem& other) noexcept = default;
};

//! Creates `[base.reg + offset]` memory operand.
static constexpr Mem ptr(const Gp& base, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(base, offset, size);
}
//! Creates `[base.reg + (index << shift) + offset]` memory operand (scalar index).
static constexpr Mem ptr(const Gp& base, const Gp& index, uint32_t shift = 0, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, offset, size);
}
//! Creates `[base.reg + (index << shift) + offset]` memory operand (vector index).
static constexpr Mem ptr(const Gp& base, const Vec& index, uint32_t shift = 0, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, offset, size);
}

//! Creates `[base + offset]` memory operand.
static constexpr Mem ptr(const Label& base, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(base, offset, size);
}
//! Creates `[base + (index << shift) + offset]` memory operand.
static constexpr Mem ptr(const Label& base, const Gp& index, uint32_t shift = 0, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, offset, size);
}
//! Creates `[base + (index << shift) + offset]` memory operand.
static constexpr Mem ptr(const Label& base, const Vec& index, uint32_t shift = 0, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, offset, size);
}

//! Creates `[rip + offset]` memory operand.
static constexpr Mem ptr(const Rip& rip_, int32_t offset = 0, uint32_t size = 0) noexcept {
  return Mem(rip_, offset, size);
}

//! Creates `[base]` absolute memory operand.
static constexpr Mem ptr(uint64_t base, uint32_t size = 0) noexcept {
  return Mem(base, size);
}
//! Creates `[base + (index.reg << shift)]` absolute memory operand.
static constexpr Mem ptr(uint64_t base, const Reg& index, uint32_t shift = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, size);
}
//! Creates `[base + (index.reg << shift)]` absolute memory operand.
static constexpr Mem ptr(uint64_t base, const Vec& index, uint32_t shift = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, size);
}

//! Creates `[base]` absolute memory operand (absolute).
static constexpr Mem ptr_abs(uint64_t base, uint32_t size = 0) noexcept {
  return Mem(base, size, Mem::kSignatureMemAbs);
}
//! Creates `[base + (index.reg << shift)]` absolute memory operand (absolute).
static constexpr Mem ptr_abs(uint64_t base, const Reg& index, uint32_t shift = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, size, Mem::kSignatureMemAbs);
}
//! Creates `[base + (index.reg << shift)]` absolute memory operand (absolute).
static constexpr Mem ptr_abs(uint64_t base, const Vec& index, uint32_t shift = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, size, Mem::kSignatureMemAbs);
}

//! Creates `[base]` relative memory operand (relative).
static constexpr Mem ptr_rel(uint64_t base, uint32_t size = 0) noexcept {
  return Mem(base, size, Mem::kSignatureMemRel);
}
//! Creates `[base + (index.reg << shift)]` relative memory operand (relative).
static constexpr Mem ptr_rel(uint64_t base, const Reg& index, uint32_t shift = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, size, Mem::kSignatureMemRel);
}
//! Creates `[base + (index.reg << shift)]` relative memory operand (relative).
static constexpr Mem ptr_rel(uint64_t base, const Vec& index, uint32_t shift = 0, uint32_t size = 0) noexcept {
  return Mem(base, index, shift, size, Mem::kSignatureMemRel);
}

// Definition of memory operand constructors that use platform independent naming.
ASMJIT_MEM_PTR(ptr_8, 1)
ASMJIT_MEM_PTR(ptr_16, 2)
ASMJIT_MEM_PTR(ptr_32, 4)
ASMJIT_MEM_PTR(ptr_48, 6)
ASMJIT_MEM_PTR(ptr_64, 8)
ASMJIT_MEM_PTR(ptr_80, 10)
ASMJIT_MEM_PTR(ptr_128, 16)
ASMJIT_MEM_PTR(ptr_256, 32)
ASMJIT_MEM_PTR(ptr_512, 64)

// Definition of memory operand constructors that use X86-specific convention.
ASMJIT_MEM_PTR(byte_ptr, 1)
ASMJIT_MEM_PTR(word_ptr, 2)
ASMJIT_MEM_PTR(dword_ptr, 4)
ASMJIT_MEM_PTR(fword_ptr, 6)
ASMJIT_MEM_PTR(qword_ptr, 8)
ASMJIT_MEM_PTR(tbyte_ptr, 10)
ASMJIT_MEM_PTR(tword_ptr, 10)
ASMJIT_MEM_PTR(oword_ptr, 16)
ASMJIT_MEM_PTR(dqword_ptr, 16)
ASMJIT_MEM_PTR(qqword_ptr, 32)
ASMJIT_MEM_PTR(xmmword_ptr, 16)
ASMJIT_MEM_PTR(ymmword_ptr, 32)
ASMJIT_MEM_PTR(zmmword_ptr, 64)

//! \}

ASMJIT_END_SUB_NAMESPACE

// ============================================================================
// [asmjit::Type::IdOfT<x86::Reg>]
// ============================================================================

//! \cond INTERNAL
ASMJIT_BEGIN_NAMESPACE
ASMJIT_DEFINE_TYPE_ID(x86::Gpb, kIdI8);
ASMJIT_DEFINE_TYPE_ID(x86::Gpw, kIdI16);
ASMJIT_DEFINE_TYPE_ID(x86::Gpd, kIdI32);
ASMJIT_DEFINE_TYPE_ID(x86::Gpq, kIdI64);
ASMJIT_DEFINE_TYPE_ID(x86::Mm , kIdMmx64);
ASMJIT_DEFINE_TYPE_ID(x86::Xmm, kIdI32x4);
ASMJIT_DEFINE_TYPE_ID(x86::Ymm, kIdI32x8);
ASMJIT_DEFINE_TYPE_ID(x86::Zmm, kIdI32x16);
ASMJIT_END_NAMESPACE
//! \endcond

#undef ASMJIT_MEM_PTR

#endif // ASMJIT_X86_X86OPERAND_H_INCLUDED
