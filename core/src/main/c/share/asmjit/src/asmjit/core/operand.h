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

#ifndef ASMJIT_CORE_OPERAND_H_INCLUDED
#define ASMJIT_CORE_OPERAND_H_INCLUDED

#include "../core/archcommons.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [Macros]
// ============================================================================

//! Adds a template specialization for `REG_TYPE` into the local `RegTraits`.
#define ASMJIT_DEFINE_REG_TRAITS(REG, REG_TYPE, GROUP, SIZE, COUNT, TYPE_ID)  \
template<>                                                                    \
struct RegTraits<REG_TYPE> {                                                  \
  typedef REG RegT;                                                           \
                                                                              \
  static constexpr uint32_t kValid = 1;                                       \
  static constexpr uint32_t kCount = COUNT;                                   \
  static constexpr uint32_t kTypeId = TYPE_ID;                                \
                                                                              \
  static constexpr uint32_t kType = REG_TYPE;                                 \
  static constexpr uint32_t kGroup = GROUP;                                   \
  static constexpr uint32_t kSize = SIZE;                                     \
                                                                              \
  static constexpr uint32_t kSignature =                                      \
    (Operand::kOpReg << Operand::kSignatureOpTypeShift  ) |                   \
    (kType           << Operand::kSignatureRegTypeShift ) |                   \
    (kGroup          << Operand::kSignatureRegGroupShift) |                   \
    (kSize           << Operand::kSignatureSizeShift    ) ;                   \
}

//! Adds constructors and member functions to a class that implements abstract
//! register. Abstract register is register that doesn't have type or signature
//! yet, it's a base class like `x86::Reg` or `arm::Reg`.
#define ASMJIT_DEFINE_ABSTRACT_REG(REG, BASE)                                 \
public:                                                                       \
  /*! Default constructor that only setups basics. */                         \
  constexpr REG() noexcept                                                    \
    : BASE(SignatureAndId(kSignature, kIdBad)) {}                             \
                                                                              \
  /*! Makes a copy of the `other` register operand. */                        \
  constexpr REG(const REG& other) noexcept                                    \
    : BASE(other) {}                                                          \
                                                                              \
  /*! Makes a copy of the `other` register having id set to `rId` */          \
  constexpr REG(const BaseReg& other, uint32_t rId) noexcept                  \
    : BASE(other, rId) {}                                                     \
                                                                              \
  /*! Creates a register based on `signature` and `rId`. */                   \
  constexpr explicit REG(const SignatureAndId& sid) noexcept                  \
    : BASE(sid) {}                                                            \
                                                                              \
  /*! Creates a completely uninitialized REG register operand (garbage). */   \
  inline explicit REG(Globals::NoInit_) noexcept                              \
    : BASE(Globals::NoInit) {}                                                \
                                                                              \
  /*! Creates a new register from register type and id. */                    \
  static inline REG fromTypeAndId(uint32_t rType, uint32_t rId) noexcept {    \
    return REG(SignatureAndId(signatureOf(rType), rId));                      \
  }                                                                           \
                                                                              \
  /*! Creates a new register from register signature and id. */               \
  static inline REG fromSignatureAndId(uint32_t rSgn, uint32_t rId) noexcept {\
    return REG(SignatureAndId(rSgn, rId));                                    \
  }                                                                           \
                                                                              \
  /*! Clones the register operand. */                                         \
  constexpr REG clone() const noexcept { return REG(*this); }                 \
                                                                              \
  inline REG& operator=(const REG& other) noexcept = default;

//! Adds constructors and member functions to a class that implements final
//! register. Final registers MUST HAVE a valid signature.
#define ASMJIT_DEFINE_FINAL_REG(REG, BASE, TRAITS)                            \
public:                                                                       \
  static constexpr uint32_t kThisType  = TRAITS::kType;                       \
  static constexpr uint32_t kThisGroup = TRAITS::kGroup;                      \
  static constexpr uint32_t kThisSize  = TRAITS::kSize;                       \
  static constexpr uint32_t kSignature = TRAITS::kSignature;                  \
                                                                              \
  ASMJIT_DEFINE_ABSTRACT_REG(REG, BASE)                                       \
                                                                              \
  /*! Creates a register operand having its id set to `rId`. */               \
  constexpr explicit REG(uint32_t rId) noexcept                               \
    : BASE(SignatureAndId(kSignature, rId)) {}

//! \addtogroup asmjit_assembler
//! \{

// ============================================================================
// [asmjit::Operand_]
// ============================================================================

//! Constructor-less `Operand`.
//!
//! Contains no initialization code and can be used safely to define an array
//! of operands that won't be initialized. This is an `Operand` compatible
//! data structure designed to be statically initialized, static const, or to
//! be used by the user to define an array of operands without having them
//! default initialized.
//!
//! The key difference between `Operand` and `Operand_`:
//!
//! ```
//! Operand_ xArray[10]; // Not initialized, contains garbage.
//! Operand  yArray[10]; // All operands initialized to none.
//! ```
struct Operand_ {
  //! Operand's signature that provides operand type and additional information.
  uint32_t _signature;
  //! Either base id as used by memory operand or any id as used by others.
  uint32_t _baseId;

  //! Data specific to the operand type.
  //!
  //! The reason we don't use union is that we have `constexpr` constructors that
  //! construct operands and other `constexpr` functions that return wither another
  //! Operand or something else. These cannot generally work with unions so we also
  //! cannot use `union` if we want to be standard compliant.
  uint32_t _data[2];

  //! Indexes to `_data` array.
  enum DataIndex : uint32_t {
    kDataMemIndexId  = 0,
    kDataMemOffsetLo = 1,

    kDataImmValueLo = ASMJIT_ARCH_LE ? 0 : 1,
    kDataImmValueHi = ASMJIT_ARCH_LE ? 1 : 0
  };

  //! Operand types that can be encoded in `Operand`.
  enum OpType : uint32_t {
    //! Not an operand or not initialized.
    kOpNone = 0,
    //! Operand is a register.
    kOpReg = 1,
    //! Operand is a memory.
    kOpMem = 2,
    //! Operand is an immediate value.
    kOpImm = 3,
    //! Operand is a label.
    kOpLabel = 4
  };
  static_assert(kOpMem == kOpReg + 1, "asmjit::Operand requires `kOpMem` to be `kOpReg+1`.");

  //! Label tag.
  enum LabelTag {
    //! Label tag is used as a sub-type, forming a unique signature across all
    //! operand types as 0x1 is never associated with any register type. This
    //! means that a memory operand's BASE register can be constructed from
    //! virtually any operand (register vs. label) by just assigning its type
    //! (register type or label-tag) and operand id.
    kLabelTag = 0x1
  };

  // \cond INTERNAL
  enum SignatureBits : uint32_t {
    // Operand type (3 least significant bits).
    // |........|........|........|.....XXX|
    kSignatureOpTypeShift = 0,
    kSignatureOpTypeMask = 0x07u << kSignatureOpTypeShift,

    // Register type (5 bits).
    // |........|........|........|XXXXX...|
    kSignatureRegTypeShift = 3,
    kSignatureRegTypeMask = 0x1Fu << kSignatureRegTypeShift,

    // Register group (4 bits).
    // |........|........|....XXXX|........|
    kSignatureRegGroupShift = 8,
    kSignatureRegGroupMask = 0x0Fu << kSignatureRegGroupShift,

    // Memory base type (5 bits).
    // |........|........|........|XXXXX...|
    kSignatureMemBaseTypeShift = 3,
    kSignatureMemBaseTypeMask = 0x1Fu << kSignatureMemBaseTypeShift,

    // Memory index type (5 bits).
    // |........|........|...XXXXX|........|
    kSignatureMemIndexTypeShift = 8,
    kSignatureMemIndexTypeMask = 0x1Fu << kSignatureMemIndexTypeShift,

    // Memory base+index combined (10 bits).
    // |........|........|...XXXXX|XXXXX...|
    kSignatureMemBaseIndexShift = 3,
    kSignatureMemBaseIndexMask = 0x3FFu << kSignatureMemBaseIndexShift,

    // This memory operand represents a home-slot or stack (Compiler) (1 bit).
    // |........|........|..X.....|........|
    kSignatureMemRegHomeShift = 13,
    kSignatureMemRegHomeFlag = 0x01u << kSignatureMemRegHomeShift,

    // Immediate type (1 bit).
    // |........|........|........|....X...|
    kSignatureImmTypeShift = 4,
    kSignatureImmTypeMask = 0x01u << kSignatureImmTypeShift,

    // Predicate used by either registers or immediate values (4 bits).
    // |........|XXXX....|........|........|
    kSignaturePredicateShift = 20,
    kSignaturePredicateMask = 0x0Fu << kSignaturePredicateShift,

    // Operand size (8 most significant bits).
    // |XXXXXXXX|........|........|........|
    kSignatureSizeShift = 24,
    kSignatureSizeMask = 0xFFu << kSignatureSizeShift
  };
  //! \endcond

  //! Constants useful for VirtId <-> Index translation.
  enum VirtIdConstants : uint32_t {
    //! Minimum valid packed-id.
    kVirtIdMin = 256,
    //! Maximum valid packed-id, excludes Globals::kInvalidId.
    kVirtIdMax = Globals::kInvalidId - 1,
    //! Count of valid packed-ids.
    kVirtIdCount = uint32_t(kVirtIdMax - kVirtIdMin + 1)
  };

  //! Tests whether the given `id` is a valid virtual register id. Since AsmJit
  //! supports both physical and virtual registers it must be able to distinguish
  //! between these two. The idea is that physical registers are always limited
  //! in size, so virtual identifiers start from `kVirtIdMin` and end at `kVirtIdMax`.
  static ASMJIT_INLINE bool isVirtId(uint32_t id) noexcept { return id - kVirtIdMin < uint32_t(kVirtIdCount); }
  //! Converts a real-id into a packed-id that can be stored in Operand.
  static ASMJIT_INLINE uint32_t indexToVirtId(uint32_t id) noexcept { return id + kVirtIdMin; }
  //! Converts a packed-id back to real-id.
  static ASMJIT_INLINE uint32_t virtIdToIndex(uint32_t id) noexcept { return id - kVirtIdMin; }

  //! \name Construction & Destruction
  //! \{

  //! \cond INTERNAL
  //! Initializes a `BaseReg` operand from `signature` and register `id`.
  inline void _initReg(uint32_t signature, uint32_t id) noexcept {
    _signature = signature;
    _baseId = id;
    _data[0] = 0;
    _data[1] = 0;
  }
  //! \endcond

  //! Initializes the operand from `other` operand (used by operator overloads).
  inline void copyFrom(const Operand_& other) noexcept { memcpy(this, &other, sizeof(Operand_)); }

  //! Resets the `Operand` to none.
  //!
  //! None operand is defined the following way:
  //!   - Its signature is zero (kOpNone, and the rest zero as well).
  //!   - Its id is `0`.
  //!   - The reserved8_4 field is set to `0`.
  //!   - The reserved12_4 field is set to zero.
  //!
  //! In other words, reset operands have all members set to zero. Reset operand
  //! must match the Operand state right after its construction. Alternatively,
  //! if you have an array of operands, you can simply use `memset()`.
  //!
  //! ```
  //! using namespace asmjit;
  //!
  //! Operand a;
  //! Operand b;
  //! assert(a == b);
  //!
  //! b = x86::eax;
  //! assert(a != b);
  //!
  //! b.reset();
  //! assert(a == b);
  //!
  //! memset(&b, 0, sizeof(Operand));
  //! assert(a == b);
  //! ```
  inline void reset() noexcept {
    _signature = 0;
    _baseId = 0;
    _data[0] = 0;
    _data[1] = 0;
  }

  //! \}

  //! \name Operator Overloads
  //! \{

  //! Tests whether this operand is the same as `other`.
  constexpr bool operator==(const Operand_& other) const noexcept { return  equals(other); }
  //! Tests whether this operand is not the same as `other`.
  constexpr bool operator!=(const Operand_& other) const noexcept { return !equals(other); }

  //! \}

  //! \name Cast
  //! \{

  //! Casts this operand to `T` type.
  template<typename T>
  inline T& as() noexcept { return static_cast<T&>(*this); }

  //! Casts this operand to `T` type (const).
  template<typename T>
  inline const T& as() const noexcept { return static_cast<const T&>(*this); }

  //! \}

  //! \name Accessors
  //! \{

  //! Tests whether the operand's signature matches the given signature `sign`.
  constexpr bool hasSignature(uint32_t signature) const noexcept { return _signature == signature; }
  //! Tests whether the operand's signature matches the signature of the `other` operand.
  constexpr bool hasSignature(const Operand_& other) const noexcept { return _signature == other.signature(); }

  //! Returns operand signature as unsigned 32-bit integer.
  //!
  //! Signature is first 4 bytes of the operand data. It's used mostly for
  //! operand checking as it's much faster to check 4 bytes at once than having
  //! to check these bytes individually.
  constexpr uint32_t signature() const noexcept { return _signature; }

  //! Sets the operand signature, see `signature()`.
  //!
  //! \note Improper use of `setSignature()` can lead to hard-to-debug errors.
  inline void setSignature(uint32_t signature) noexcept { _signature = signature; }

  //! \cond INTERNAL
  template<uint32_t mask>
  constexpr bool _hasSignaturePart() const noexcept {
    return (_signature & mask) != 0;
  }

  template<uint32_t mask>
  constexpr bool _hasSignaturePart(uint32_t signature) const noexcept {
    return (_signature & mask) == signature;
  }

  template<uint32_t mask>
  constexpr uint32_t _getSignaturePart() const noexcept {
    return (_signature >> Support::constCtz(mask)) & (mask >> Support::constCtz(mask));
  }

  template<uint32_t mask>
  inline void _setSignaturePart(uint32_t value) noexcept {
    ASMJIT_ASSERT((value & ~(mask >> Support::constCtz(mask))) == 0);
    _signature = (_signature & ~mask) | (value << Support::constCtz(mask));
  }
  //! \endcond

  //! Returns the type of the operand, see `OpType`.
  constexpr uint32_t opType() const noexcept { return _getSignaturePart<kSignatureOpTypeMask>(); }
  //! Tests whether the operand is none (`kOpNone`).
  constexpr bool isNone() const noexcept { return _signature == 0; }
  //! Tests whether the operand is a register (`kOpReg`).
  constexpr bool isReg() const noexcept { return opType() == kOpReg; }
  //! Tests whether the operand is a memory location (`kOpMem`).
  constexpr bool isMem() const noexcept { return opType() == kOpMem; }
  //! Tests whether the operand is an immediate (`kOpImm`).
  constexpr bool isImm() const noexcept { return opType() == kOpImm; }
  //! Tests whether the operand is a label (`kOpLabel`).
  constexpr bool isLabel() const noexcept { return opType() == kOpLabel; }

  //! Tests whether the operand is a physical register.
  constexpr bool isPhysReg() const noexcept { return isReg() && _baseId < 0xFFu; }
  //! Tests whether the operand is a virtual register.
  constexpr bool isVirtReg() const noexcept { return isReg() && _baseId > 0xFFu; }

  //! Tests whether the operand specifies a size (i.e. the size is not zero).
  constexpr bool hasSize() const noexcept { return _hasSignaturePart<kSignatureSizeMask>(); }
  //! Tests whether the size of the operand matches `size`.
  constexpr bool hasSize(uint32_t s) const noexcept { return size() == s; }

  //! Returns the size of the operand in bytes.
  //!
  //! The value returned depends on the operand type:
  //!   * None  - Should always return zero size.
  //!   * Reg   - Should always return the size of the register. If the register
  //!             size depends on architecture (like `x86::CReg` and `x86::DReg`)
  //!             the size returned should be the greatest possible (so it should
  //!             return 64-bit size in such case).
  //!   * Mem   - Size is optional and will be in most cases zero.
  //!   * Imm   - Should always return zero size.
  //!   * Label - Should always return zero size.
  constexpr uint32_t size() const noexcept { return _getSignaturePart<kSignatureSizeMask>(); }

  //! Returns the operand id.
  //!
  //! The value returned should be interpreted accordingly to the operand type:
  //!   * None  - Should be `0`.
  //!   * Reg   - Physical or virtual register id.
  //!   * Mem   - Multiple meanings - BASE address (register or label id), or
  //!             high value of a 64-bit absolute address.
  //!   * Imm   - Should be `0`.
  //!   * Label - Label id if it was created by using `newLabel()` or
  //!             `Globals::kInvalidId` if the label is invalid or not
  //!             initialized.
  constexpr uint32_t id() const noexcept { return _baseId; }

  //! Tests whether the operand is 100% equal to `other` operand.
  //!
  //! \note This basically performs a binary comparison, if aby bit is
  //! different the operands are not equal.
  constexpr bool equals(const Operand_& other) const noexcept {
    return (_signature == other._signature) &
           (_baseId    == other._baseId   ) &
           (_data[0]   == other._data[0]  ) &
           (_data[1]   == other._data[1]  ) ;
  }

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("Use equals() instead")
  constexpr bool isEqual(const Operand_& other) const noexcept { return equals(other); }
#endif //!ASMJIT_NO_DEPRECATED

  //! Tests whether the operand is a register matching `rType`.
  constexpr bool isReg(uint32_t rType) const noexcept {
    return (_signature & (kSignatureOpTypeMask | kSignatureRegTypeMask)) ==
           ((kOpReg << kSignatureOpTypeShift) | (rType << kSignatureRegTypeShift));
  }

  //! Tests whether the operand is register and of `rType` and `rId`.
  constexpr bool isReg(uint32_t rType, uint32_t rId) const noexcept {
    return isReg(rType) && id() == rId;
  }

  //! Tests whether the operand is a register or memory.
  constexpr bool isRegOrMem() const noexcept {
    return Support::isBetween<uint32_t>(opType(), kOpReg, kOpMem);
  }

  //! \}
};

// ============================================================================
// [asmjit::Operand]
// ============================================================================

//! Operand can contain register, memory location, immediate, or label.
class Operand : public Operand_ {
public:
  //! \name Construction & Destruction
  //! \{

  //! Creates `kOpNone` operand having all members initialized to zero.
  constexpr Operand() noexcept
    : Operand_{ kOpNone, 0u, { 0u, 0u }} {}

  //! Creates a cloned `other` operand.
  constexpr Operand(const Operand& other) noexcept = default;

  //! Creates a cloned `other` operand.
  constexpr explicit Operand(const Operand_& other)
    : Operand_(other) {}

  //! Creates an operand initialized to raw `[u0, u1, u2, u3]` values.
  constexpr Operand(Globals::Init_, uint32_t u0, uint32_t u1, uint32_t u2, uint32_t u3) noexcept
    : Operand_{ u0, u1, { u2, u3 }} {}

  //! Creates an uninitialized operand (dangerous).
  inline explicit Operand(Globals::NoInit_) noexcept {}

  //! \}

  //! \name Operator Overloads
  //! \{

  inline Operand& operator=(const Operand& other) noexcept = default;
  inline Operand& operator=(const Operand_& other) noexcept { return operator=(static_cast<const Operand&>(other)); }

  //! \}

  //! \name Utilities
  //! \{

  //! Clones this operand and returns its copy.
  constexpr Operand clone() const noexcept { return Operand(*this); }

  //! \}
};

static_assert(sizeof(Operand) == 16, "asmjit::Operand must be exactly 16 bytes long");

// ============================================================================
// [asmjit::Label]
// ============================================================================

//! Label (jump target or data location).
//!
//! Label represents a location in code typically used as a jump target, but
//! may be also a reference to some data or a static variable. Label has to be
//! explicitly created by BaseEmitter.
//!
//! Example of using labels:
//!
//! ```
//! // Create some emitter (for example x86::Assembler).
//! x86::Assembler a;
//!
//! // Create Label instance.
//! Label L1 = a.newLabel();
//!
//! // ... your code ...
//!
//! // Using label.
//! a.jump(L1);
//!
//! // ... your code ...
//!
//! // Bind label to the current position, see `BaseEmitter::bind()`.
//! a.bind(L1);
//! ```
class Label : public Operand {
public:
  //! Type of the Label.
  enum LabelType : uint32_t {
    //! Anonymous (unnamed) label.
    kTypeAnonymous = 0,
    //! Local label (always has parentId).
    kTypeLocal = 1,
    //! Global label (never has parentId).
    kTypeGlobal = 2,
    //! External label (references an external symbol).
    kTypeExternal = 3,
    //! Number of label types.
    kTypeCount = 4
  };

  //! \name Construction & Destruction
  //! \{

  //! Creates a label operand without ID (you must set the ID to make it valid).
  constexpr Label() noexcept
    : Operand(Globals::Init, kOpLabel, Globals::kInvalidId, 0, 0) {}

  //! Creates a cloned label operand of `other`.
  constexpr Label(const Label& other) noexcept
    : Operand(other) {}

  //! Creates a label operand of the given `id`.
  constexpr explicit Label(uint32_t id) noexcept
    : Operand(Globals::Init, kOpLabel, id, 0, 0) {}

  inline explicit Label(Globals::NoInit_) noexcept
    : Operand(Globals::NoInit) {}

  //! Resets the label, will reset all properties and set its ID to `Globals::kInvalidId`.
  inline void reset() noexcept {
    _signature = kOpLabel;
    _baseId = Globals::kInvalidId;
    _data[0] = 0;
    _data[1] = 0;
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline Label& operator=(const Label& other) noexcept = default;

  //! \}

  //! \name Accessors
  //! \{

  //! Tests whether the label was created by CodeHolder and/or an attached emitter.
  constexpr bool isValid() const noexcept { return _baseId != Globals::kInvalidId; }
  //! Sets the label `id`.
  inline void setId(uint32_t id) noexcept { _baseId = id; }

  //! \}
};

// ============================================================================
// [asmjit::BaseRegTraits]
// ============================================================================

//! \cond INTERNAL
//! Default register traits.
struct BaseRegTraits {
  //! RegType is not valid by default.
  static constexpr uint32_t kValid = 0;
  //! Count of registers (0 if none).
  static constexpr uint32_t kCount = 0;
  //! Everything is void by default.
  static constexpr uint32_t kTypeId = 0;

  //! Zero type by default.
  static constexpr uint32_t kType = 0;
  //! Zero group by default.
  static constexpr uint32_t kGroup = 0;
  //! No size by default.
  static constexpr uint32_t kSize = 0;

  //! Empty signature by default (not even having operand type set to register).
  static constexpr uint32_t kSignature = 0;
};
//! \endcond

// ============================================================================
// [asmjit::BaseReg]
// ============================================================================

//! Structure that allows to extract a register information based on the signature.
//!
//! This information is compatible with operand's signature (32-bit integer)
//! and `RegInfo` just provides easy way to access it.
struct RegInfo {
  inline void reset(uint32_t signature = 0) noexcept { _signature = signature; }
  inline void setSignature(uint32_t signature) noexcept { _signature = signature; }

  template<uint32_t mask>
  constexpr uint32_t _getSignaturePart() const noexcept {
    return (_signature >> Support::constCtz(mask)) & (mask >> Support::constCtz(mask));
  }

  constexpr bool isValid() const noexcept { return _signature != 0; }
  constexpr uint32_t signature() const noexcept { return _signature; }
  constexpr uint32_t opType() const noexcept { return _getSignaturePart<Operand::kSignatureOpTypeMask>(); }
  constexpr uint32_t group() const noexcept { return _getSignaturePart<Operand::kSignatureRegGroupMask>(); }
  constexpr uint32_t type() const noexcept { return _getSignaturePart<Operand::kSignatureRegTypeMask>(); }
  constexpr uint32_t size() const noexcept { return _getSignaturePart<Operand::kSignatureSizeMask>(); }

  uint32_t _signature;
};

//! Physical or virtual register operand.
class BaseReg : public Operand {
public:
  static constexpr uint32_t kBaseSignature =
    kSignatureOpTypeMask   |
    kSignatureRegTypeMask  |
    kSignatureRegGroupMask |
    kSignatureSizeMask     ;

  //! Architecture neutral register types.
  //!
  //! These must be reused by any platform that contains that types. All GP
  //! and VEC registers are also allowed by design to be part of a BASE|INDEX
  //! of a memory operand.
  enum RegType : uint32_t {
    //! No register - unused, invalid, multiple meanings.
    kTypeNone = 0,

    // (1 is used as a LabelTag)

    //! 8-bit low general purpose register (X86).
    kTypeGp8Lo = 2,
    //! 8-bit high general purpose register (X86).
    kTypeGp8Hi = 3,
    //! 16-bit general purpose register (X86).
    kTypeGp16 = 4,
    //! 32-bit general purpose register (X86|ARM).
    kTypeGp32 = 5,
    //! 64-bit general purpose register (X86|ARM).
    kTypeGp64 = 6,
    //! 8-bit view of a vector register (ARM).
    kTypeVec8 = 7,
    //! 16-bit view of a vector register (ARM).
    kTypeVec16 = 8,
    //! 32-bit view of a vector register (ARM).
    kTypeVec32 = 9,
    //! 64-bit view of a vector register (ARM).
    kTypeVec64 = 10,
    //! 128-bit view of a vector register (X86|ARM).
    kTypeVec128 = 11,
    //! 256-bit view of a vector register (X86).
    kTypeVec256 = 12,
    //! 512-bit view of a vector register (X86).
    kTypeVec512 = 13,
    //! 1024-bit view of a vector register (future).
    kTypeVec1024 = 14,
    //! Other0 register, should match `kOther0` group.
    kTypeOther0 = 15,
    //! Other1 register, should match `kOther1` group.
    kTypeOther1 = 16,
    //! Universal id of IP/PC register (if separate).
    kTypeIP = 17,
    //! Start of platform dependent register types.
    kTypeCustom = 18,
    //! Maximum possible register type value.
    kTypeMax = 31
  };

  //! Register group (architecture neutral), and some limits.
  enum RegGroup : uint32_t {
    //! General purpose register group compatible with all backends.
    kGroupGp = 0,
    //! Vector register group compatible with all backends.
    kGroupVec = 1,
    //! Group that is architecture dependent.
    kGroupOther0 = 2,
    //! Group that is architecture dependent.
    kGroupOther1 = 3,
    //! Count of register groups used by physical and virtual registers.
    kGroupVirt = 4,
    //! Count of register groups used by physical registers only.
    kGroupCount = 16
  };

  enum Id : uint32_t {
    //! None or any register (mostly internal).
    kIdBad = 0xFFu
  };

  //! A helper used by constructors.
  struct SignatureAndId {
    uint32_t _signature;
    uint32_t _id;

    inline SignatureAndId() noexcept = default;
    constexpr SignatureAndId(const SignatureAndId& other) noexcept = default;

    constexpr explicit SignatureAndId(uint32_t signature, uint32_t id) noexcept
      : _signature(signature),
        _id(id) {}

    constexpr uint32_t signature() const noexcept { return _signature; }
    constexpr uint32_t id() const noexcept { return _id; }
  };

  static constexpr uint32_t kSignature = kOpReg;

  //! \name Construction & Destruction
  //! \{

  //! Creates a dummy register operand.
  constexpr BaseReg() noexcept
    : Operand(Globals::Init, kSignature, kIdBad, 0, 0) {}

  //! Creates a new register operand which is the same as `other` .
  constexpr BaseReg(const BaseReg& other) noexcept
    : Operand(other) {}

  //! Creates a new register operand compatible with `other`, but with a different `rId`.
  constexpr BaseReg(const BaseReg& other, uint32_t rId) noexcept
    : Operand(Globals::Init, other._signature, rId, 0, 0) {}

  //! Creates a register initialized to `signature` and `rId`.
  constexpr explicit BaseReg(const SignatureAndId& sid) noexcept
    : Operand(Globals::Init, sid._signature, sid._id, 0, 0) {}

  inline explicit BaseReg(Globals::NoInit_) noexcept
    : Operand(Globals::NoInit) {}

  /*! Creates a new register from register signature `rSgn` and id. */
  static inline BaseReg fromSignatureAndId(uint32_t rSgn, uint32_t rId) noexcept {
    return BaseReg(SignatureAndId(rSgn, rId));
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline BaseReg& operator=(const BaseReg& other) noexcept = default;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns base signature of the register associated with each register type.
  //!
  //! Base signature only contains the operand type, register type, register
  //! group, and register size. It doesn't contain element type, predicate, or
  //! other architecture-specific data. Base signature is a signature that is
  //! provided by architecture-specific `RegTraits`, like \ref x86::RegTraits.
  constexpr uint32_t baseSignature() const noexcept {
    return _signature & (kBaseSignature);
  }

  //! Tests whether the operand's base signature matches the given signature `sign`.
  constexpr bool hasBaseSignature(uint32_t signature) const noexcept { return baseSignature() == signature; }
  //! Tests whether the operand's base signature matches the base signature of the `other` operand.
  constexpr bool hasBaseSignature(const BaseReg& other) const noexcept { return baseSignature() == other.baseSignature(); }

  //! Tests whether this register is the same as `other`.
  //!
  //! This is just an optimization. Registers by default only use the first
  //! 8 bytes of Operand data, so this method takes advantage of this knowledge
  //! and only compares these 8 bytes. If both operands were created correctly
  //! both \ref equals() and \ref isSame() should give the same answer, however,
  //! if any of these two contains garbage or other metadata in the upper 8
  //! bytes then \ref isSame() may return `true` in cases in which \ref equals()
  //! returns false.
  constexpr bool isSame(const BaseReg& other) const noexcept {
    return (_signature == other._signature) & (_baseId == other._baseId);
  }

  //! Tests whether the register is valid (either virtual or physical).
  constexpr bool isValid() const noexcept { return (_signature != 0) & (_baseId != kIdBad); }

  //! Tests whether this is a physical register.
  constexpr bool isPhysReg() const noexcept { return _baseId < kIdBad; }
  //! Tests whether this is a virtual register.
  constexpr bool isVirtReg() const noexcept { return _baseId > kIdBad; }

  //! Tests whether the register type matches `type` - same as `isReg(type)`, provided for convenience.
  constexpr bool isType(uint32_t type) const noexcept { return (_signature & kSignatureRegTypeMask) == (type << kSignatureRegTypeShift); }
  //! Tests whether the register group matches `group`.
  constexpr bool isGroup(uint32_t group) const noexcept { return (_signature & kSignatureRegGroupMask) == (group << kSignatureRegGroupShift); }

  //! Tests whether the register is a general purpose register (any size).
  constexpr bool isGp() const noexcept { return isGroup(kGroupGp); }
  //! Tests whether the register is a vector register.
  constexpr bool isVec() const noexcept { return isGroup(kGroupVec); }

  using Operand_::isReg;

  //! Same as `isType()`, provided for convenience.
  constexpr bool isReg(uint32_t rType) const noexcept { return isType(rType); }
  //! Tests whether the register type matches `type` and register id matches `rId`.
  constexpr bool isReg(uint32_t rType, uint32_t rId) const noexcept { return isType(rType) && id() == rId; }

  //! Returns the type of the register.
  constexpr uint32_t type() const noexcept { return _getSignaturePart<kSignatureRegTypeMask>(); }
  //! Returns the register group.
  constexpr uint32_t group() const noexcept { return _getSignaturePart<kSignatureRegGroupMask>(); }

  //! Returns operation predicate of the register (ARM/AArch64).
  //!
  //! The meaning depends on architecture, for example on ARM hardware this
  //! describes \ref arm::Predicate::ShiftOp of the register.
  constexpr uint32_t predicate() const noexcept { return _getSignaturePart<kSignaturePredicateMask>(); }

  //! Sets operation predicate of the register to `predicate` (ARM/AArch64).
  //!
  //! The meaning depends on architecture, for example on ARM hardware this
  //! describes \ref arm::Predicate::ShiftOp of the register.
  inline void setPredicate(uint32_t predicate) noexcept { _setSignaturePart<kSignaturePredicateMask>(predicate); }

  //! Resets shift operation type of the register to the default value (ARM/AArch64).
  inline void resetPredicate() noexcept { _setSignaturePart<kSignaturePredicateMask>(0); }

  //! Clones the register operand.
  constexpr BaseReg clone() const noexcept { return BaseReg(*this); }

  //! Casts this register to `RegT` by also changing its signature.
  //!
  //! \note Improper use of `cloneAs()` can lead to hard-to-debug errors.
  template<typename RegT>
  constexpr RegT cloneAs() const noexcept { return RegT(RegT::kSignature, id()); }

  //! Casts this register to `other` by also changing its signature.
  //!
  //! \note Improper use of `cloneAs()` can lead to hard-to-debug errors.
  template<typename RegT>
  constexpr RegT cloneAs(const RegT& other) const noexcept { return RegT(SignatureAndId(other.signature(), id())); }

  //! Sets the register id to `rId`.
  inline void setId(uint32_t rId) noexcept { _baseId = rId; }

  //! Sets a 32-bit operand signature based on traits of `RegT`.
  template<typename RegT>
  inline void setSignatureT() noexcept { _signature = RegT::kSignature; }

  //! Sets the register `signature` and `rId`.
  inline void setSignatureAndId(uint32_t signature, uint32_t rId) noexcept {
    _signature = signature;
    _baseId = rId;
  }

  //! \}

  //! \name Static Functions
  //! \{

  //! Tests whether the `op` operand is a general purpose register.
  static inline bool isGp(const Operand_& op) noexcept {
    // Check operand type and register group. Not interested in register type and size.
    const uint32_t kSgn = (kOpReg   << kSignatureOpTypeShift  ) |
                          (kGroupGp << kSignatureRegGroupShift) ;
    return (op.signature() & (kSignatureOpTypeMask | kSignatureRegGroupMask)) == kSgn;
  }

  //! Tests whether the `op` operand is a vector register.
  static inline bool isVec(const Operand_& op) noexcept {
    // Check operand type and register group. Not interested in register type and size.
    const uint32_t kSgn = (kOpReg    << kSignatureOpTypeShift  ) |
                          (kGroupVec << kSignatureRegGroupShift) ;
    return (op.signature() & (kSignatureOpTypeMask | kSignatureRegGroupMask)) == kSgn;
  }

  //! Tests whether the `op` is a general purpose register of the given `rId`.
  static inline bool isGp(const Operand_& op, uint32_t rId) noexcept { return isGp(op) & (op.id() == rId); }
  //! Tests whether the `op` is a vector register of the given `rId`.
  static inline bool isVec(const Operand_& op, uint32_t rId) noexcept { return isVec(op) & (op.id() == rId); }

  //! \}
};

// ============================================================================
// [asmjit::RegOnly]
// ============================================================================

//! RegOnly is 8-byte version of `BaseReg` that allows to store either register
//! or nothing.
//!
//! This class was designed to decrease the space consumed by each extra "operand"
//! in `BaseEmitter` and `InstNode` classes.
struct RegOnly {
  //! Type of the operand, either `kOpNone` or `kOpReg`.
  uint32_t _signature;
  //! Physical or virtual register id.
  uint32_t _id;

  //! \name Construction & Destruction
  //! \{

  //! Initializes the `RegOnly` instance to hold register `signature` and `id`.
  inline void init(uint32_t signature, uint32_t id) noexcept {
    _signature = signature;
    _id = id;
  }

  inline void init(const BaseReg& reg) noexcept { init(reg.signature(), reg.id()); }
  inline void init(const RegOnly& reg) noexcept { init(reg.signature(), reg.id()); }

  //! Resets the `RegOnly` members to zeros (none).
  inline void reset() noexcept { init(0, 0); }

  //! \}

  //! \name Accessors
  //! \{

  //! Tests whether this ExtraReg is none (same as calling `Operand_::isNone()`).
  constexpr bool isNone() const noexcept { return _signature == 0; }
  //! Tests whether the register is valid (either virtual or physical).
  constexpr bool isReg() const noexcept { return _signature != 0; }

  //! Tests whether this is a physical register.
  constexpr bool isPhysReg() const noexcept { return _id < BaseReg::kIdBad; }
  //! Tests whether this is a virtual register (used by `BaseCompiler`).
  constexpr bool isVirtReg() const noexcept { return _id > BaseReg::kIdBad; }

  //! Returns the register signature or 0 if no register is assigned.
  constexpr uint32_t signature() const noexcept { return _signature; }
  //! Returns the register id.
  //!
  //! \note Always check whether the register is assigned before using the
  //! returned identifier as non-assigned `RegOnly` instance would return
  //! zero id, which is still a valid register id.
  constexpr uint32_t id() const noexcept { return _id; }

  //! Sets the register id.
  inline void setId(uint32_t id) noexcept { _id = id; }

  //! \cond INTERNAL
  //!
  //! Extracts information from operand's signature.
  template<uint32_t mask>
  constexpr uint32_t _getSignaturePart() const noexcept {
    return (_signature >> Support::constCtz(mask)) & (mask >> Support::constCtz(mask));
  }
  //! \endcond

  //! Returns the type of the register.
  constexpr uint32_t type() const noexcept { return _getSignaturePart<Operand::kSignatureRegTypeMask>(); }
  //! Returns the register group.
  constexpr uint32_t group() const noexcept { return _getSignaturePart<Operand::kSignatureRegGroupMask>(); }

  //! \}

  //! \name Utilities
  //! \{

  //! Converts this ExtraReg to a real `RegT` operand.
  template<typename RegT>
  constexpr RegT toReg() const noexcept { return RegT(BaseReg::SignatureAndId(_signature, _id)); }

  //! \}
};

// ============================================================================
// [asmjit::BaseMem]
// ============================================================================

//! Base class for all memory operands.
//!
//! \note It's tricky to pack all possible cases that define a memory operand
//! into just 16 bytes. The `BaseMem` splits data into the following parts:
//!
//!   - BASE - Base register or label - requires 36 bits total. 4 bits are used
//!     to encode the type of the BASE operand (label vs. register type) and the
//!     remaining 32 bits define the BASE id, which can be a physical or virtual
//!     register index. If BASE type is zero, which is never used as a register
//!     type and label doesn't use it as well then BASE field contains a high
//!     DWORD of a possible 64-bit absolute address, which is possible on X64.
//!
//!   - INDEX - Index register (or theoretically Label, which doesn't make sense).
//!     Encoding is similar to BASE - it also requires 36 bits and splits the
//!     encoding to INDEX type (4 bits defining the register type) and id (32-bits).
//!
//!   - OFFSET - A relative offset of the address. Basically if BASE is specified
//!     the relative displacement adjusts BASE and an optional INDEX. if BASE is
//!     not specified then the OFFSET should be considered as ABSOLUTE address (at
//!     least on X86). In that case its low 32 bits are stored in DISPLACEMENT
//!     field and the remaining high 32 bits are stored in BASE.
//!
//!   - OTHER - There is rest 8 bits that can be used for whatever purpose. For
//!     example \ref x86::Mem operand uses these bits to store segment override
//!     prefix and index shift (or scale).
class BaseMem : public Operand {
public:
  //! \cond INTERNAL
  //! Used internally to construct `BaseMem` operand from decomposed data.
  struct Decomposed {
    uint32_t baseType;
    uint32_t baseId;
    uint32_t indexType;
    uint32_t indexId;
    int32_t offset;
    uint32_t size;
    uint32_t flags;
  };
  //! \endcond

  //! \name Construction & Destruction
  //! \{

  //! Creates a default `BaseMem` operand, that points to [0].
  constexpr BaseMem() noexcept
    : Operand(Globals::Init, kOpMem, 0, 0, 0) {}

  //! Creates a `BaseMem` operand that is a clone of `other`.
  constexpr BaseMem(const BaseMem& other) noexcept
    : Operand(other) {}

  //! Creates a `BaseMem` operand from `baseReg` and `offset`.
  //!
  //! \note This is an architecture independent constructor that can be used to
  //! create an architecture independent memory operand to be used in portable
  //! code that can handle multiple architectures.
  constexpr explicit BaseMem(const BaseReg& baseReg, int32_t offset = 0) noexcept
    : Operand(Globals::Init,
              kOpMem | (baseReg.type() << kSignatureMemBaseTypeShift),
              baseReg.id(),
              0,
              uint32_t(offset)) {}

  //! \cond INTERNAL

  //! Creates a `BaseMem` operand from 4 integers as used by `Operand_` struct.
  constexpr BaseMem(Globals::Init_, uint32_t u0, uint32_t u1, uint32_t u2, uint32_t u3) noexcept
    : Operand(Globals::Init, u0, u1, u2, u3) {}

  constexpr BaseMem(const Decomposed& d) noexcept
    : Operand(Globals::Init,
              kOpMem | (d.baseType  << kSignatureMemBaseTypeShift )
                     | (d.indexType << kSignatureMemIndexTypeShift)
                     | (d.size      << kSignatureSizeShift        )
                     | d.flags,
              d.baseId,
              d.indexId,
              uint32_t(d.offset)) {}

  //! \endcond

  //! Creates a completely uninitialized `BaseMem` operand.
  inline explicit BaseMem(Globals::NoInit_) noexcept
    : Operand(Globals::NoInit) {}

  //! Resets the memory operand - after the reset the memory points to [0].
  inline void reset() noexcept {
    _signature = kOpMem;
    _baseId = 0;
    _data[0] = 0;
    _data[1] = 0;
  }

  //! \}

  //! \name Overloaded Operators
  //! \{

  inline BaseMem& operator=(const BaseMem& other) noexcept { copyFrom(other); return *this; }

  //! \}

  //! \name Accessors
  //! \{

  //! Clones the memory operand.
  constexpr BaseMem clone() const noexcept { return BaseMem(*this); }

  //! Creates a new copy of this memory operand adjusted by `off`.
  inline BaseMem cloneAdjusted(int64_t off) const noexcept {
    BaseMem result(*this);
    result.addOffset(off);
    return result;
  }

  //! Tests whether this memory operand is a register home (only used by \ref asmjit_compiler)
  constexpr bool isRegHome() const noexcept { return _hasSignaturePart<kSignatureMemRegHomeFlag>(); }
  //! Mark this memory operand as register home (only used by \ref asmjit_compiler).
  inline void setRegHome() noexcept { _signature |= kSignatureMemRegHomeFlag; }
  //! Marks this operand to not be a register home (only used by \ref asmjit_compiler).
  inline void clearRegHome() noexcept { _signature &= ~kSignatureMemRegHomeFlag; }

  //! Tests whether the memory operand has a BASE register or label specified.
  constexpr bool hasBase() const noexcept { return (_signature & kSignatureMemBaseTypeMask) != 0; }
  //! Tests whether the memory operand has an INDEX register specified.
  constexpr bool hasIndex() const noexcept { return (_signature & kSignatureMemIndexTypeMask) != 0; }
  //! Tests whether the memory operand has BASE or INDEX register.
  constexpr bool hasBaseOrIndex() const noexcept { return (_signature & kSignatureMemBaseIndexMask) != 0; }
  //! Tests whether the memory operand has BASE and INDEX register.
  constexpr bool hasBaseAndIndex() const noexcept { return (_signature & kSignatureMemBaseTypeMask) != 0 && (_signature & kSignatureMemIndexTypeMask) != 0; }

  //! Tests whether the BASE operand is a register (registers start after `kLabelTag`).
  constexpr bool hasBaseReg() const noexcept { return (_signature & kSignatureMemBaseTypeMask) > (Label::kLabelTag << kSignatureMemBaseTypeShift); }
  //! Tests whether the BASE operand is a label.
  constexpr bool hasBaseLabel() const noexcept { return (_signature & kSignatureMemBaseTypeMask) == (Label::kLabelTag << kSignatureMemBaseTypeShift); }
  //! Tests whether the INDEX operand is a register (registers start after `kLabelTag`).
  constexpr bool hasIndexReg() const noexcept { return (_signature & kSignatureMemIndexTypeMask) > (Label::kLabelTag << kSignatureMemIndexTypeShift); }

  //! Returns the type of the BASE register (0 if this memory operand doesn't
  //! use the BASE register).
  //!
  //! \note If the returned type is one (a value never associated to a register
  //! type) the BASE is not register, but it's a label. One equals to `kLabelTag`.
  //! You should always check `hasBaseLabel()` before using `baseId()` result.
  constexpr uint32_t baseType() const noexcept { return _getSignaturePart<kSignatureMemBaseTypeMask>(); }

  //! Returns the type of an INDEX register (0 if this memory operand doesn't
  //! use the INDEX register).
  constexpr uint32_t indexType() const noexcept { return _getSignaturePart<kSignatureMemIndexTypeMask>(); }

  //! This is used internally for BASE+INDEX validation.
  constexpr uint32_t baseAndIndexTypes() const noexcept { return _getSignaturePart<kSignatureMemBaseIndexMask>(); }

  //! Returns both BASE (4:0 bits) and INDEX (9:5 bits) types combined into a
  //! single value.
  //!
  //! \remarks Returns id of the BASE register or label (if the BASE was
  //! specified as label).
  constexpr uint32_t baseId() const noexcept { return _baseId; }

  //! Returns the id of the INDEX register.
  constexpr uint32_t indexId() const noexcept { return _data[kDataMemIndexId]; }

  //! Sets the id of the BASE register (without modifying its type).
  inline void setBaseId(uint32_t rId) noexcept { _baseId = rId; }
  //! Sets the id of the INDEX register (without modifying its type).
  inline void setIndexId(uint32_t rId) noexcept { _data[kDataMemIndexId] = rId; }

  //! Sets the base register to type and id of the given `base` operand.
  inline void setBase(const BaseReg& base) noexcept { return _setBase(base.type(), base.id()); }
  //! Sets the index register to type and id of the given `index` operand.
  inline void setIndex(const BaseReg& index) noexcept { return _setIndex(index.type(), index.id()); }

  //! \cond INTERNAL
  inline void _setBase(uint32_t rType, uint32_t rId) noexcept {
    _setSignaturePart<kSignatureMemBaseTypeMask>(rType);
    _baseId = rId;
  }

  inline void _setIndex(uint32_t rType, uint32_t rId) noexcept {
    _setSignaturePart<kSignatureMemIndexTypeMask>(rType);
    _data[kDataMemIndexId] = rId;
  }
  //! \endcond

  //! Resets the memory operand's BASE register or label.
  inline void resetBase() noexcept { _setBase(0, 0); }
  //! Resets the memory operand's INDEX register.
  inline void resetIndex() noexcept { _setIndex(0, 0); }

  //! Sets the memory operand size (in bytes).
  inline void setSize(uint32_t size) noexcept { _setSignaturePart<kSignatureSizeMask>(size); }

  //! Tests whether the memory operand has a 64-bit offset or absolute address.
  //!
  //! If this is true then `hasBase()` must always report false.
  constexpr bool isOffset64Bit() const noexcept { return baseType() == 0; }

  //! Tests whether the memory operand has a non-zero offset or absolute address.
  constexpr bool hasOffset() const noexcept {
    return (_data[kDataMemOffsetLo] | uint32_t(_baseId & Support::bitMaskFromBool<uint32_t>(isOffset64Bit()))) != 0;
  }

  //! Returns either relative offset or absolute address as 64-bit integer.
  constexpr int64_t offset() const noexcept {
    return isOffset64Bit() ? int64_t(uint64_t(_data[kDataMemOffsetLo]) | (uint64_t(_baseId) << 32))
                           : int64_t(int32_t(_data[kDataMemOffsetLo])); // Sign extend 32-bit offset.
  }

  //! Returns a 32-bit low part of a 64-bit offset or absolute address.
  constexpr int32_t offsetLo32() const noexcept { return int32_t(_data[kDataMemOffsetLo]); }
  //! Returns a 32-but high part of a 64-bit offset or absolute address.
  //!
  //! \note This function is UNSAFE and returns garbage if `isOffset64Bit()`
  //! returns false. Never use it blindly without checking it first.
  constexpr int32_t offsetHi32() const noexcept { return int32_t(_baseId); }

  //! Sets a 64-bit offset or an absolute address to `offset`.
  //!
  //! \note This functions attempts to set both high and low parts of a 64-bit
  //! offset, however, if the operand has a BASE register it will store only the
  //! low 32 bits of the offset / address as there is no way to store both BASE
  //! and 64-bit offset, and there is currently no architecture that has such
  //! capability targeted by AsmJit.
  inline void setOffset(int64_t offset) noexcept {
    uint32_t lo = uint32_t(uint64_t(offset) & 0xFFFFFFFFu);
    uint32_t hi = uint32_t(uint64_t(offset) >> 32);
    uint32_t hiMsk = Support::bitMaskFromBool<uint32_t>(isOffset64Bit());

    _data[kDataMemOffsetLo] = lo;
    _baseId = (hi & hiMsk) | (_baseId & ~hiMsk);
  }
  //! Sets a low 32-bit offset to `offset` (don't use without knowing how BaseMem works).
  inline void setOffsetLo32(int32_t offset) noexcept { _data[kDataMemOffsetLo] = uint32_t(offset); }

  //! Adjusts the offset by `offset`.
  //!
  //! \note This is a fast function that doesn't use the HI 32-bits of a
  //! 64-bit offset. Use it only if you know that there is a BASE register
  //! and the offset is only 32 bits anyway.

  //! Adjusts the memory operand offset by a `offset`.
  inline void addOffset(int64_t offset) noexcept {
    if (isOffset64Bit()) {
      int64_t result = offset + int64_t(uint64_t(_data[kDataMemOffsetLo]) | (uint64_t(_baseId) << 32));
      _data[kDataMemOffsetLo] = uint32_t(uint64_t(result) & 0xFFFFFFFFu);
      _baseId                 = uint32_t(uint64_t(result) >> 32);
    }
    else {
      _data[kDataMemOffsetLo] += uint32_t(uint64_t(offset) & 0xFFFFFFFFu);
    }
  }

  //! Adds `offset` to a low 32-bit offset part (don't use without knowing how
  //! BaseMem works).
  inline void addOffsetLo32(int32_t offset) noexcept { _data[kDataMemOffsetLo] += uint32_t(offset); }

  //! Resets the memory offset to zero.
  inline void resetOffset() noexcept { setOffset(0); }

  //! Resets the lo part of the memory offset to zero (don't use without knowing
  //! how BaseMem works).
  inline void resetOffsetLo32() noexcept { setOffsetLo32(0); }

  //! \}
};

// ============================================================================
// [asmjit::Imm]
// ============================================================================

//! Immediate operand.
//!
//! Immediate operand is usually part of instruction itself. It's inlined after
//! or before the instruction opcode. Immediates can be only signed or unsigned
//! integers.
//!
//! To create an immediate operand use `asmjit::imm()` helper, which can be used
//! with any type, not just the default 64-bit int.
class Imm : public Operand {
public:
  //! Type of the immediate.
  enum Type : uint32_t {
    //! Immediate is integer.
    kTypeInteger = 0,
    //! Immediate is a floating point stored as double-precision.
    kTypeDouble = 1
  };

  //! \name Construction & Destruction
  //! \{

  //! Creates a new immediate value (initial value is 0).
  inline constexpr Imm() noexcept
    : Operand(Globals::Init, kOpImm, 0, 0, 0) {}

  //! Creates a new immediate value from `other`.
  inline constexpr Imm(const Imm& other) noexcept
    : Operand(other) {}

  //! Creates a new immediate value from ARM/AArch64 specific `shift`.
  inline constexpr Imm(const arm::Shift& shift) noexcept
    : Operand(Globals::Init, kOpImm | (shift.op() << kSignaturePredicateShift),
              0,
              Support::unpackU32At0(shift.value()),
              Support::unpackU32At1(shift.value())) {}

  //! Creates a new signed immediate value, assigning the value to `val` and
  //! an architecture-specific predicate to `predicate`.
  //!
  //! \note Predicate is currently only used by ARM architectures.
  template<typename T>
  inline constexpr Imm(const T& val, const uint32_t predicate = 0) noexcept
    : Operand(Globals::Init, kOpImm | (predicate << kSignaturePredicateShift),
              0,
              Support::unpackU32At0(int64_t(val)),
              Support::unpackU32At1(int64_t(val))) {}

  inline Imm(const float& val, const uint32_t predicate = 0) noexcept
    : Operand(Globals::Init, kOpImm | (predicate << kSignaturePredicateShift), 0, 0, 0) { setValue(val); }

  inline Imm(const double& val, const uint32_t predicate = 0) noexcept
    : Operand(Globals::Init, kOpImm | (predicate << kSignaturePredicateShift), 0, 0, 0) { setValue(val); }

  inline explicit Imm(Globals::NoInit_) noexcept
    : Operand(Globals::NoInit) {}

  //! \}

  //! \name Overloaded Operators
  //! \{

  //! Assigns the value of the `other` operand to this immediate.
  inline Imm& operator=(const Imm& other) noexcept { copyFrom(other); return *this; }

  //! \}

  //! \name Accessors
  //! \{

  //! Returns immediate type, see \ref Type.
  constexpr uint32_t type() const noexcept { return _getSignaturePart<kSignatureImmTypeMask>(); }
  //! Sets the immediate type to `type`, see \ref Type.
  inline void setType(uint32_t type) noexcept { _setSignaturePart<kSignatureImmTypeMask>(type); }
  //! Resets immediate type to `kTypeInteger`.
  inline void resetType() noexcept { setType(kTypeInteger); }

  //! Returns operation predicate of the immediate.
  //!
  //! The meaning depends on architecture, for example on ARM hardware this
  //! describes \ref arm::Predicate::ShiftOp of the immediate.
  constexpr uint32_t predicate() const noexcept { return _getSignaturePart<kSignaturePredicateMask>(); }

  //! Sets operation predicate of the immediate to `predicate`.
  //!
  //! The meaning depends on architecture, for example on ARM hardware this
  //! describes \ref arm::Predicate::ShiftOp of the immediate.
  inline void setPredicate(uint32_t predicate) noexcept { _setSignaturePart<kSignaturePredicateMask>(predicate); }

  //! Resets the shift operation type of the immediate to the default value (no operation).
  inline void resetPredicate() noexcept { _setSignaturePart<kSignaturePredicateMask>(0); }

  //! Returns the immediate value as `int64_t`, which is the internal format Imm uses.
  constexpr int64_t value() const noexcept {
    return int64_t((uint64_t(_data[kDataImmValueHi]) << 32) | _data[kDataImmValueLo]);
  }

  //! Tests whether this immediate value is integer of any size.
  constexpr uint32_t isInteger() const noexcept { return type() == kTypeInteger; }
  //! Tests whether this immediate value is a double precision floating point value.
  constexpr uint32_t isDouble() const noexcept { return type() == kTypeDouble; }

  //! Tests whether the immediate can be casted to 8-bit signed integer.
  constexpr bool isInt8() const noexcept { return type() == kTypeInteger && Support::isInt8(value()); }
  //! Tests whether the immediate can be casted to 8-bit unsigned integer.
  constexpr bool isUInt8() const noexcept { return type() == kTypeInteger && Support::isUInt8(value()); }
  //! Tests whether the immediate can be casted to 16-bit signed integer.
  constexpr bool isInt16() const noexcept { return type() == kTypeInteger && Support::isInt16(value()); }
  //! Tests whether the immediate can be casted to 16-bit unsigned integer.
  constexpr bool isUInt16() const noexcept { return type() == kTypeInteger && Support::isUInt16(value()); }
  //! Tests whether the immediate can be casted to 32-bit signed integer.
  constexpr bool isInt32() const noexcept { return type() == kTypeInteger && Support::isInt32(value()); }
  //! Tests whether the immediate can be casted to 32-bit unsigned integer.
  constexpr bool isUInt32() const noexcept { return type() == kTypeInteger && _data[kDataImmValueHi] == 0; }

  //! Returns the immediate value casted to `T`.
  //!
  //! The value is masked before it's casted to `T` so the returned value is
  //! simply the representation of `T` considering the original value's lowest
  //! bits.
  template<typename T>
  inline T valueAs() const noexcept { return Support::immediateToT<T>(value()); }

  //! Returns low 32-bit signed integer.
  constexpr int32_t int32Lo() const noexcept { return int32_t(_data[kDataImmValueLo]); }
  //! Returns high 32-bit signed integer.
  constexpr int32_t int32Hi() const noexcept { return int32_t(_data[kDataImmValueHi]); }
  //! Returns low 32-bit signed integer.
  constexpr uint32_t uint32Lo() const noexcept { return _data[kDataImmValueLo]; }
  //! Returns high 32-bit signed integer.
  constexpr uint32_t uint32Hi() const noexcept { return _data[kDataImmValueHi]; }

  //! Sets immediate value to `val`, the value is casted to a signed 64-bit integer.
  template<typename T>
  inline void setValue(const T& val) noexcept {
    _setValueInternal(Support::immediateFromT(val), std::is_floating_point<T>::value ? kTypeDouble : kTypeInteger);
  }

  inline void _setValueInternal(int64_t val, uint32_t type) noexcept {
    setType(type);
    _data[kDataImmValueHi] = uint32_t(uint64_t(val) >> 32);
    _data[kDataImmValueLo] = uint32_t(uint64_t(val) & 0xFFFFFFFFu);
  }

  //! \}

  //! \name Utilities
  //! \{

  //! Clones the immediate operand.
  constexpr Imm clone() const noexcept { return Imm(*this); }

  inline void signExtend8Bits() noexcept { setValue(int64_t(valueAs<int8_t>())); }
  inline void signExtend16Bits() noexcept { setValue(int64_t(valueAs<int16_t>())); }
  inline void signExtend32Bits() noexcept { setValue(int64_t(valueAs<int32_t>())); }

  inline void zeroExtend8Bits() noexcept { setValue(valueAs<uint8_t>()); }
  inline void zeroExtend16Bits() noexcept { setValue(valueAs<uint16_t>()); }
  inline void zeroExtend32Bits() noexcept { _data[kDataImmValueHi] = 0u; }

  //! \}

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("Use valueAs<int8_t>() instead")
  inline int8_t i8() const noexcept { return valueAs<int8_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<uint8_t>() instead")
  inline uint8_t u8() const noexcept { return valueAs<uint8_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<int16_t>() instead")
  inline int16_t i16() const noexcept { return valueAs<int16_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<uint16_t>() instead")
  inline uint16_t u16() const noexcept { return valueAs<uint16_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<int32_t>() instead")
  inline int32_t i32() const noexcept { return valueAs<int32_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<uint32_t>() instead")
  inline uint32_t u32() const noexcept { return valueAs<uint32_t>(); }

  ASMJIT_DEPRECATED("Use value() instead")
  inline int64_t i64() const noexcept { return value(); }

  ASMJIT_DEPRECATED("Use valueAs<uint64_t>() instead")
  inline uint64_t u64() const noexcept { return valueAs<uint64_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<intptr_t>() instead")
  inline intptr_t iptr() const noexcept { return valueAs<intptr_t>(); }

  ASMJIT_DEPRECATED("Use valueAs<uintptr_t>() instead")
  inline uintptr_t uptr() const noexcept { return valueAs<uintptr_t>(); }

  ASMJIT_DEPRECATED("Use int32Lo() instead")
  inline int32_t i32Lo() const noexcept { return int32Lo(); }

  ASMJIT_DEPRECATED("Use uint32Lo() instead")
  inline uint32_t u32Lo() const noexcept { return uint32Lo(); }

  ASMJIT_DEPRECATED("Use int32Hi() instead")
  inline int32_t i32Hi() const noexcept { return int32Hi(); }

  ASMJIT_DEPRECATED("Use uint32Hi() instead")
  inline uint32_t u32Hi() const noexcept { return uint32Hi(); }
#endif // !ASMJIT_NO_DEPRECATED
};

//! Creates a new immediate operand.
//!
//! Using `imm(x)` is much nicer than using `Imm(x)` as this is a template
//! which can accept any integer including pointers and function pointers.
template<typename T>
static constexpr Imm imm(const T& val) noexcept { return Imm(val); }

//! \}

// ============================================================================
// [asmjit::Globals::none]
// ============================================================================

namespace Globals {
  //! \ingroup asmjit_assembler
  //!
  //! A default-constructed operand of `Operand_::kOpNone` type.
  static constexpr const Operand none;
}

// ============================================================================
// [asmjit::Support::ForwardOp]
// ============================================================================

//! \cond INTERNAL
namespace Support {

template<typename T, bool IsIntegral>
struct ForwardOpImpl {
  static ASMJIT_INLINE const T& forward(const T& value) noexcept { return value; }
};

template<typename T>
struct ForwardOpImpl<T, true> {
  static ASMJIT_INLINE Imm forward(const T& value) noexcept { return Imm(value); }
};

//! Either forwards operand T or returns a new operand for T if T is a type
//! convertible to operand. At the moment this is only used to convert integers
//! to \ref Imm operands.
template<typename T>
struct ForwardOp : public ForwardOpImpl<T, std::is_integral<typename std::decay<T>::type>::value> {};

}

//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_OPERAND_H_INCLUDED
