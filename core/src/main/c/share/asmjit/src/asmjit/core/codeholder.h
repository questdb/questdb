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

#ifndef ASMJIT_CORE_CODEHOLDER_H_INCLUDED
#define ASMJIT_CORE_CODEHOLDER_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/codebuffer.h"
#include "../core/datatypes.h"
#include "../core/errorhandler.h"
#include "../core/operand.h"
#include "../core/string.h"
#include "../core/support.h"
#include "../core/target.h"
#include "../core/zone.h"
#include "../core/zonehash.h"
#include "../core/zonestring.h"
#include "../core/zonetree.h"
#include "../core/zonevector.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [Forward Declarations]
// ============================================================================

class BaseEmitter;
class CodeHolder;
class LabelEntry;
class Logger;

// ============================================================================
// [asmjit::AlignMode]
// ============================================================================

//! Align mode.
enum AlignMode : uint32_t {
  //! Align executable code.
  kAlignCode = 0,
  //! Align non-executable code.
  kAlignData = 1,
  //! Align by a sequence of zeros.
  kAlignZero = 2,
  //! Count of alignment modes.
  kAlignCount = 3
};

// ============================================================================
// [asmjit::Expression]
// ============================================================================

//! Expression node that can reference constants, labels, and another expressions.
struct Expression {
  //! Operation type.
  enum OpType : uint8_t {
    //! Addition.
    kOpAdd = 0,
    //! Subtraction.
    kOpSub = 1,
    //! Multiplication
    kOpMul = 2,
    //! Logical left shift.
    kOpSll = 3,
    //! Logical right shift.
    kOpSrl = 4,
    //! Arithmetic right shift.
    kOpSra = 5
  };

  //! Type of \ref Value.
  enum ValueType : uint8_t {
    //! No value or invalid.
    kValueNone = 0,
    //! Value is 64-bit unsigned integer (constant).
    kValueConstant = 1,
    //! Value is \ref LabelEntry, which references a \ref Label.
    kValueLabel = 2,
    //! Value is \ref Expression
    kValueExpression = 3
  };

  //! Expression value.
  union Value {
    //! Constant.
    uint64_t constant;
    //! Pointer to another expression.
    Expression* expression;
    //! Poitner to \ref LabelEntry.
    LabelEntry* label;
  };

  //! Operation type.
  uint8_t opType;
  //! Value types of \ref value.
  uint8_t valueType[2];
  //! Reserved for future use, should be initialized to zero.
  uint8_t reserved[5];
  //! Expression left and right values.
  Value value[2];

  //! Resets the whole expression.
  //!
  //! Changes both values to \ref kValueNone.
  inline void reset() noexcept { memset(this, 0, sizeof(*this)); }

  //! Sets the value type at `index` to \ref kValueConstant and its content to `constant`.
  inline void setValueAsConstant(size_t index, uint64_t constant) noexcept {
    valueType[index] = kValueConstant;
    value[index].constant = constant;
  }

  //! Sets the value type at `index` to \ref kValueLabel and its content to `labelEntry`.
  inline void setValueAsLabel(size_t index, LabelEntry* labelEntry) noexcept {
    valueType[index] = kValueLabel;
    value[index].label = labelEntry;
  }

  //! Sets the value type at `index` to \ref kValueExpression and its content to `expression`.
  inline void setValueAsExpression(size_t index, Expression* expression) noexcept {
    valueType[index] = kValueLabel;
    value[index].expression = expression;
  }
};

// ============================================================================
// [asmjit::Section]
// ============================================================================

//! Section entry.
class Section {
public:
  //! Section id.
  uint32_t _id;
  //! Section flags.
  uint32_t _flags;
  //! Section alignment requirements (0 if no requirements).
  uint32_t _alignment;
  //! Order (lower value means higher priority).
  int32_t _order;
  //! Offset of this section from base-address.
  uint64_t _offset;
  //! Virtual size of the section (zero initialized sections).
  uint64_t _virtualSize;
  //! Section name (max 35 characters, PE allows max 8).
  FixedString<Globals::kMaxSectionNameSize + 1> _name;
  //! Code or data buffer.
  CodeBuffer _buffer;

  //! Section flags.
  enum Flags : uint32_t {
    //! Executable (.text sections).
    kFlagExec = 0x00000001u,
    //! Read-only (.text and .data sections).
    kFlagConst = 0x00000002u,
    //! Zero initialized by the loader (BSS).
    kFlagZero = 0x00000004u,
    //! Info / comment flag.
    kFlagInfo = 0x00000008u,
    //! Section created implicitly and can be deleted by \ref Target.
    kFlagImplicit = 0x80000000u
  };

  //! \name Accessors
  //! \{

  //! Returns the section id.
  inline uint32_t id() const noexcept { return _id; }
  //! Returns the section name, as a null terminated string.
  inline const char* name() const noexcept { return _name.str; }

  //! Returns the section data.
  inline uint8_t* data() noexcept { return _buffer.data(); }
  //! \overload
  inline const uint8_t* data() const noexcept { return _buffer.data(); }

  //! Returns the section flags, see \ref Flags.
  inline uint32_t flags() const noexcept { return _flags; }
  //! Tests whether the section has the given `flag`.
  inline bool hasFlag(uint32_t flag) const noexcept { return (_flags & flag) != 0; }
  //! Adds `flags` to the section flags.
  inline void addFlags(uint32_t flags) noexcept { _flags |= flags; }
  //! Removes `flags` from the section flags.
  inline void clearFlags(uint32_t flags) noexcept { _flags &= ~flags; }

  //! Returns the minimum section alignment
  inline uint32_t alignment() const noexcept { return _alignment; }
  //! Sets the minimum section alignment
  inline void setAlignment(uint32_t alignment) noexcept { _alignment = alignment; }

  //! Returns the section order, which has a higher priority than section id.
  inline int32_t order() const noexcept { return _order; }

  //! Returns the section offset, relative to base.
  inline uint64_t offset() const noexcept { return _offset; }
  //! Set the section offset.
  inline void setOffset(uint64_t offset) noexcept { _offset = offset; }

  //! Returns the virtual size of the section.
  //!
  //! Virtual size is initially zero and is never changed by AsmJit. It's normal
  //! if virtual size is smaller than size returned by `bufferSize()` as the buffer
  //! stores real data emitted by assemblers or appended by users.
  //!
  //! Use `realSize()` to get the real and final size of this section.
  inline uint64_t virtualSize() const noexcept { return _virtualSize; }
  //! Sets the virtual size of the section.
  inline void setVirtualSize(uint64_t virtualSize) noexcept { _virtualSize = virtualSize; }

  //! Returns the buffer size of the section.
  inline size_t bufferSize() const noexcept { return _buffer.size(); }
  //! Returns the real size of the section calculated from virtual and buffer sizes.
  inline uint64_t realSize() const noexcept { return Support::max<uint64_t>(virtualSize(), bufferSize()); }

  //! Returns the `CodeBuffer` used by this section.
  inline CodeBuffer& buffer() noexcept { return _buffer; }
  //! Returns the `CodeBuffer` used by this section (const).
  inline const CodeBuffer& buffer() const noexcept { return _buffer; }

  //! \}
};

// ============================================================================
// [asmjit::OffsetFormat]
// ============================================================================

//! Provides information about formatting offsets, absolute addresses, or their
//! parts. Offset format is used by both \ref RelocEntry and \ref LabelLink.
//!
//! The illustration above describes the relation of region size and offset size.
//! Region size is the size of the whole unit whereas offset size is the size of
//! the unit that will be patched.
//!
//! ```
//! +-> Code buffer |   The subject of the relocation (region)  |
//! |               | (Word-Offset)  (Word-Size)                |
//! |xxxxxxxxxxxxxxx|................|*PATCHED*|................|xxxxxxxxxxxx->
//!                                  |         |
//!     [Word Offset points here]----+         +--- [WordOffset + WordSize]
//! ```
//!
//! Once the offset word has been located it can be patched like this:
//!
//! ```
//!                               |ImmDiscardLSB (discard LSB bits).
//!                               |..
//! [0000000000000iiiiiiiiiiiiiiiiiDD] - Offset value (32-bit)
//! [000000000000000iiiiiiiiiiiiiiiii] - Offset value after discard LSB.
//! [00000000000iiiiiiiiiiiiiiiii0000] - Offset value shifted by ImmBitShift.
//! [xxxxxxxxxxxiiiiiiiiiiiiiiiiixxxx] - Patched word (32-bit)
//!             |...............|
//!               (ImmBitCount) +- ImmBitShift
//! ```
struct OffsetFormat {
  //! Type of the displacement.
  uint8_t _type;
  //! Encoding flags.
  uint8_t _flags;
  //! Size of the region (in bytes) containing the offset value, if the offset
  //! value is part of an instruction, otherwise it would be the same as
  //! `_valueSize`.
  uint8_t _regionSize;
  //! Size of the offset value, in bytes (1, 2, 4, or 8).
  uint8_t _valueSize;
  //! Offset of the offset value, in bytes, relative to the start of the region
  //! or data. Value offset would be zero if both region size and value size are
  //! equal.
  uint8_t _valueOffset;
  //! Size of the displacement immediate value in bits.
  uint8_t _immBitCount;
  //! Shift of the displacement immediate value in bits in the target word.
  uint8_t _immBitShift;
  //! Number of least significant bits to discard before writing the immediate
  //! to the destination. All discarded bits must be zero otherwise the value
  //! is invalid.
  uint8_t _immDiscardLsb;

  //! Type of the displacement.
  enum Type : uint8_t {
    //! A value having `_immBitCount` bits and shifted by `_immBitShift`.
    //!
    //! This displacement type is sufficient for both X86/X64 and many other
    //! architectures that store displacement as continuous bits within a machine
    //! word.
    kTypeCommon = 0,
    //! AARCH64 ADR format of `[.|immlo:2|.....|immhi:19|.....]`.
    kTypeAArch64_ADR,
    //! AARCH64 ADRP format of `[.|immlo:2|.....|immhi:19|.....]` (4kB pages).
    kTypeAArch64_ADRP,

    //! Count of displacement types.
    kTypeCount
  };

  //! Returns the type of the displacement.
  inline uint32_t type() const noexcept { return _type; }

  //! Returns flags.
  inline uint32_t flags() const noexcept { return _flags; }

  //! Returns the size of the region/instruction where the displacement is encoded.
  inline uint32_t regionSize() const noexcept { return _regionSize; }

  //! Returns the the offset of the word relative to the start of the region
  //! where the displacement is.
  inline uint32_t valueOffset() const noexcept { return _valueOffset; }

  //! Returns the size of the data-type (word) that contains the displacement, in bytes.
  inline uint32_t valueSize() const noexcept { return _valueSize; }
  //! Returns the count of bits of the displacement value in the data it's stored in.
  inline uint32_t immBitCount() const noexcept { return _immBitCount; }
  //! Returns the bit-shift of the displacement value in the data it's stored in.
  inline uint32_t immBitShift() const noexcept { return _immBitShift; }
  //! Returns the number of least significant bits of the displacement value,
  //! that must be zero and that are not part of the encoded data.
  inline uint32_t immDiscardLsb() const noexcept { return _immDiscardLsb; }

  //! Resets this offset format to a simple data value of `dataSize` bytes.
  //!
  //! The region will be the same size as data and immediate bits would correspond
  //! to `dataSize * 8`. There will be no immediate bit shift or discarded bits.
  inline void resetToDataValue(size_t dataSize) noexcept {
    ASMJIT_ASSERT(dataSize <= 8u);

    _type = uint8_t(kTypeCommon);
    _flags = uint8_t(0);
    _regionSize = uint8_t(dataSize);
    _valueSize = uint8_t(dataSize);
    _valueOffset = uint8_t(0);
    _immBitCount = uint8_t(dataSize * 8u);
    _immBitShift = uint8_t(0);
    _immDiscardLsb = uint8_t(0);
  }

  inline void resetToImmValue(uint32_t type, size_t valueSize, uint32_t immBitShift, uint32_t immBitCount, uint32_t immDiscardLsb) noexcept {
    ASMJIT_ASSERT(valueSize <= 8u);
    ASMJIT_ASSERT(immBitShift < valueSize * 8u);
    ASMJIT_ASSERT(immBitCount <= 64u);
    ASMJIT_ASSERT(immDiscardLsb <= 64u);

    _type = uint8_t(type);
    _flags = uint8_t(0);
    _regionSize = uint8_t(valueSize);
    _valueSize = uint8_t(valueSize);
    _valueOffset = uint8_t(0);
    _immBitCount = uint8_t(immBitCount);
    _immBitShift = uint8_t(immBitShift);
    _immDiscardLsb = uint8_t(immDiscardLsb);
  }

  inline void setRegion(size_t regionSize, size_t valueOffset) noexcept {
    _regionSize = uint8_t(regionSize);
    _valueOffset = uint8_t(valueOffset);
  }

  inline void setLeadingAndTrailingSize(size_t leadingSize, size_t trailingSize) noexcept {
    _regionSize = uint8_t(leadingSize + trailingSize + _valueSize);
    _valueOffset = uint8_t(leadingSize);
  }
};

// ============================================================================
// [asmjit::RelocEntry]
// ============================================================================

//! Relocation entry.
struct RelocEntry {
  //! Relocation id.
  uint32_t _id;
  //! Type of the relocation.
  uint32_t _relocType;
  //! Format of the relocated value.
  OffsetFormat _format;
  //! Source section id.
  uint32_t _sourceSectionId;
  //! Target section id.
  uint32_t _targetSectionId;
  //! Source offset (relative to start of the section).
  uint64_t _sourceOffset;
  //! Payload (target offset, target address, expression, etc).
  uint64_t _payload;

  //! Relocation type.
  enum RelocType : uint32_t {
    //! None/deleted (no relocation).
    kTypeNone = 0,
    //! Expression evaluation, `_payload` is pointer to `Expression`.
    kTypeExpression = 1,
    //! Relocate absolute to absolute.
    kTypeAbsToAbs = 2,
    //! Relocate relative to absolute.
    kTypeRelToAbs = 3,
    //! Relocate absolute to relative.
    kTypeAbsToRel = 4,
    //! Relocate absolute to relative or use trampoline.
    kTypeX64AddressEntry = 5
  };

  //! \name Accessors
  //! \{

  inline uint32_t id() const noexcept { return _id; }

  inline uint32_t relocType() const noexcept { return _relocType; }
  inline const OffsetFormat& format() const noexcept { return _format; }

  inline uint32_t sourceSectionId() const noexcept { return _sourceSectionId; }
  inline uint32_t targetSectionId() const noexcept { return _targetSectionId; }

  inline uint64_t sourceOffset() const noexcept { return _sourceOffset; }
  inline uint64_t payload() const noexcept { return _payload; }

  Expression* payloadAsExpression() const noexcept {
    return reinterpret_cast<Expression*>(uintptr_t(_payload));
  }

  //! \}
};

// ============================================================================
// [asmjit::LabelLink]
// ============================================================================

//! Data structure used to link either unbound labels or cross-section links.
struct LabelLink {
  //! Next link (single-linked list).
  LabelLink* next;
  //! Section id where the label is bound.
  uint32_t sectionId;
  //! Relocation id or Globals::kInvalidId.
  uint32_t relocId;
  //! Label offset relative to the start of the section.
  size_t offset;
  //! Inlined rel8/rel32.
  intptr_t rel;
  //! Offset format information.
  OffsetFormat format;
};

// ============================================================================
// [asmjit::LabelEntry]
// ============================================================================

//! Label entry.
//!
//! Contains the following properties:
//!   * Label id - This is the only thing that is set to the `Label` operand.
//!   * Label name - Optional, used mostly to create executables and libraries.
//!   * Label type - Type of the label, default `Label::kTypeAnonymous`.
//!   * Label parent id - Derived from many assemblers that allow to define a
//!       local label that falls under a global label. This allows to define
//!       many labels of the same name that have different parent (global) label.
//!   * Offset - offset of the label bound by `Assembler`.
//!   * Links - single-linked list that contains locations of code that has
//!       to be patched when the label gets bound. Every use of unbound label
//!       adds one link to `_links` list.
//!   * HVal - Hash value of label's name and optionally parentId.
//!   * HashNext - Hash-table implementation detail.
class LabelEntry : public ZoneHashNode {
public:
  // Let's round the size of `LabelEntry` to 64 bytes (as `ZoneAllocator` has
  // granularity of 32 bytes anyway). This gives `_name` the remaining space,
  // which is should be 16 bytes on 64-bit and 28 bytes on 32-bit architectures.
  enum : uint32_t {
    kStaticNameSize =
      64 - (sizeof(ZoneHashNode) + 8 + sizeof(Section*) + sizeof(size_t) + sizeof(LabelLink*))
  };

  //! Label type, see `Label::LabelType`.
  uint8_t _type;
  //! Must be zero.
  uint8_t _flags;
  //! Reserved.
  uint16_t _reserved16;
  //! Label parent id or zero.
  uint32_t _parentId;
  //! Label offset relative to the start of the `_section`.
  uint64_t _offset;
  //! Section where the label was bound.
  Section* _section;
  //! Label links.
  LabelLink* _links;
  //! Label name.
  ZoneString<kStaticNameSize> _name;

  //! \name Accessors
  //! \{

  // NOTE: Label id is stored in `_customData`, which is provided by ZoneHashNode
  // to fill a padding that a C++ compiler targeting 64-bit CPU will add to align
  // the structure to 64-bits.

  //! Returns label id.
  inline uint32_t id() const noexcept { return _customData; }
  //! Sets label id (internal, used only by `CodeHolder`).
  inline void _setId(uint32_t id) noexcept { _customData = id; }

  //! Returns label type, see `Label::LabelType`.
  inline uint32_t type() const noexcept { return _type; }
  //! Returns label flags, returns 0 at the moment.
  inline uint32_t flags() const noexcept { return _flags; }

  //! Tests whether the label has a parent label.
  inline bool hasParent() const noexcept { return _parentId != Globals::kInvalidId; }
  //! Returns label's parent id.
  inline uint32_t parentId() const noexcept { return _parentId; }

  //! Returns the section where the label was bound.
  //!
  //! If the label was not yet bound the return value is `nullptr`.
  inline Section* section() const noexcept { return _section; }

  //! Tests whether the label has name.
  inline bool hasName() const noexcept { return !_name.empty(); }

  //! Returns the label's name.
  //!
  //! \note Local labels will return their local name without their parent
  //! part, for example ".L1".
  inline const char* name() const noexcept { return _name.data(); }

  //! Returns size of label's name.
  //!
  //! \note Label name is always null terminated, so you can use `strlen()` to
  //! get it, however, it's also cached in `LabelEntry` itself, so if you want
  //! to know the size the fastest way is to call `LabelEntry::nameSize()`.
  inline uint32_t nameSize() const noexcept { return _name.size(); }

  //! Returns links associated with this label.
  inline LabelLink* links() const noexcept { return _links; }

  //! Tests whether the label is bound.
  inline bool isBound() const noexcept { return _section != nullptr; }
  //! Tests whether the label is bound to a the given `sectionId`.
  inline bool isBoundTo(Section* section) const noexcept { return _section == section; }

  //! Returns the label offset (only useful if the label is bound).
  inline uint64_t offset() const noexcept { return _offset; }

  //! Returns the hash-value of label's name and its parent label (if any).
  //!
  //! Label hash is calculated as `HASH(Name) ^ ParentId`. The hash function
  //! is implemented in `Support::hashString()` and `Support::hashRound()`.
  inline uint32_t hashCode() const noexcept { return _hashCode; }

  //! \}
};

// ============================================================================
// [asmjit::AddressTableEntry]
// ============================================================================

//! Entry in an address table.
class AddressTableEntry : public ZoneTreeNodeT<AddressTableEntry> {
public:
  ASMJIT_NONCOPYABLE(AddressTableEntry)

  //! Address.
  uint64_t _address;
  //! Slot.
  uint32_t _slot;

  //! \name Construction & Destruction
  //! \{

  inline explicit AddressTableEntry(uint64_t address) noexcept
    : _address(address),
      _slot(0xFFFFFFFFu) {}

  //! \}

  //! \name Accessors
  //! \{

  inline uint64_t address() const noexcept { return _address; }
  inline uint32_t slot() const noexcept { return _slot; }

  inline bool hasAssignedSlot() const noexcept { return _slot != 0xFFFFFFFFu; }

  inline bool operator<(const AddressTableEntry& other) const noexcept { return _address < other._address; }
  inline bool operator>(const AddressTableEntry& other) const noexcept { return _address > other._address; }

  inline bool operator<(uint64_t queryAddress) const noexcept { return _address < queryAddress; }
  inline bool operator>(uint64_t queryAddress) const noexcept { return _address > queryAddress; }

  //! \}
};

// ============================================================================
// [asmjit::CodeHolder]
// ============================================================================

//! Contains basic information about the target architecture and its options.
//!
//! In addition, it holds assembled code & data (including sections, labels, and
//! relocation information). `CodeHolder` can store both binary and intermediate
//! representation of assembly, which can be generated by \ref BaseAssembler,
//! \ref BaseBuilder, and \ref BaseCompiler
//!
//! \note `CodeHolder` has an ability to attach an \ref ErrorHandler, however,
//! the error handler is not triggered by `CodeHolder` itself, it's instead
//! propagated to all emitters that attach to it.
class CodeHolder {
public:
  ASMJIT_NONCOPYABLE(CodeHolder)

  //! Environment information.
  Environment _environment;
  //! Base address or \ref Globals::kNoBaseAddress.
  uint64_t _baseAddress;

  //! Attached `Logger`, used by all consumers.
  Logger* _logger;
  //! Attached `ErrorHandler`.
  ErrorHandler* _errorHandler;

  //! Code zone (used to allocate core structures).
  Zone _zone;
  //! Zone allocator, used to manage internal containers.
  ZoneAllocator _allocator;

  //! Attached emitters.
  ZoneVector<BaseEmitter*> _emitters;
  //! Section entries.
  ZoneVector<Section*> _sections;
  //! Section entries sorted by section order and then section id.
  ZoneVector<Section*> _sectionsByOrder;
  //! Label entries.
  ZoneVector<LabelEntry*> _labelEntries;
  //! Relocation entries.
  ZoneVector<RelocEntry*> _relocations;
  //! Label name -> LabelEntry (only named labels).
  ZoneHash<LabelEntry> _namedLabels;

  //! Count of label links, which are not resolved.
  size_t _unresolvedLinkCount;
  //! Pointer to an address table section (or null if this section doesn't exist).
  Section* _addressTableSection;
  //! Address table entries.
  ZoneTree<AddressTableEntry> _addressTableEntries;

  //! Options that can be used with \ref copySectionData() and \ref copyFlattenedData().
  enum CopyOptions : uint32_t {
    //! If virtual size of a section is greater than the size of its \ref CodeBuffer
    //! then all bytes between the buffer size and virtual size will be zeroed.
    //! If this option is not set then those bytes would be left as is, which
    //! means that if the user didn't initialize them they would have a previous
    //! content, which may be unwanted.
    kCopyPadSectionBuffer = 0x00000001u,

#ifndef ASMJIT_NO_DEPRECATED
    kCopyWithPadding = kCopyPadSectionBuffer,
#endif // !ASMJIT_NO_DEPRECATED

    //! Zeroes the target buffer if the flattened data is less than the destination
    //! size. This option works only with \ref copyFlattenedData() as it processes
    //! multiple sections. It is ignored by \ref copySectionData().
    kCopyPadTargetBuffer = 0x00000002u
  };

  //! \name Construction & Destruction
  //! \{

  //! Creates an uninitialized CodeHolder (you must init() it before it can be used).
  ASMJIT_API CodeHolder() noexcept;
  //! Destroys the CodeHolder.
  ASMJIT_API ~CodeHolder() noexcept;

  //! Tests whether the `CodeHolder` has been initialized.
  //!
  //! Emitters can be only attached to initialized `CodeHolder` instances.
  inline bool isInitialized() const noexcept { return _environment.isInitialized(); }

  //! Initializes CodeHolder to hold code described by code `info`.
  ASMJIT_API Error init(const Environment& environment, uint64_t baseAddress = Globals::kNoBaseAddress) noexcept;
  //! Detaches all code-generators attached and resets the `CodeHolder`.
  ASMJIT_API void reset(uint32_t resetPolicy = Globals::kResetSoft) noexcept;

  //! \}

  //! \name Attach & Detach
  //! \{

  //! Attaches an emitter to this `CodeHolder`.
  ASMJIT_API Error attach(BaseEmitter* emitter) noexcept;
  //! Detaches an emitter from this `CodeHolder`.
  ASMJIT_API Error detach(BaseEmitter* emitter) noexcept;

  //! \}

  //! \name Allocators
  //! \{

  //! Returns the allocator that the `CodeHolder` uses.
  //!
  //! \note This should be only used for AsmJit's purposes. Code holder uses
  //! arena allocator to allocate everything, so anything allocated through
  //! this allocator will be invalidated by \ref CodeHolder::reset() or by
  //! CodeHolder's destructor.
  inline ZoneAllocator* allocator() const noexcept { return const_cast<ZoneAllocator*>(&_allocator); }

  //! \}

  //! \name Code & Architecture
  //! \{

  //! Returns the target environment information, see \ref Environment.
  inline const Environment& environment() const noexcept { return _environment; }

  //! Returns the target architecture.
  inline uint32_t arch() const noexcept { return environment().arch(); }
  //! Returns the target sub-architecture.
  inline uint32_t subArch() const noexcept { return environment().subArch(); }

  //! Tests whether a static base-address is set.
  inline bool hasBaseAddress() const noexcept { return _baseAddress != Globals::kNoBaseAddress; }
  //! Returns a static base-address or \ref Globals::kNoBaseAddress, if not set.
  inline uint64_t baseAddress() const noexcept { return _baseAddress; }

  //! \}

  //! \name Emitters
  //! \{

  //! Returns a vector of attached emitters.
  inline const ZoneVector<BaseEmitter*>& emitters() const noexcept { return _emitters; }

  //! \}

  //! \name Logging
  //! \{

  //! Returns the attached logger, see \ref Logger.
  inline Logger* logger() const noexcept { return _logger; }
  //! Attaches a `logger` to CodeHolder and propagates it to all attached emitters.
  ASMJIT_API void setLogger(Logger* logger) noexcept;
  //! Resets the logger to none.
  inline void resetLogger() noexcept { setLogger(nullptr); }

  //! \name Error Handling
  //! \{

  //! Tests whether the CodeHolder has an attached error handler, see \ref ErrorHandler.
  inline bool hasErrorHandler() const noexcept { return _errorHandler != nullptr; }
  //! Returns the attached error handler.
  inline ErrorHandler* errorHandler() const noexcept { return _errorHandler; }
  //! Attach an error handler to this `CodeHolder`.
  ASMJIT_API void setErrorHandler(ErrorHandler* errorHandler) noexcept;
  //! Resets the error handler to none.
  inline void resetErrorHandler() noexcept { setErrorHandler(nullptr); }

  //! \}

  //! \name Code Buffer
  //! \{

  //! Makes sure that at least `n` bytes can be added to CodeHolder's buffer `cb`.
  //!
  //! \note The buffer `cb` must be managed by `CodeHolder` - otherwise the
  //! behavior of the function is undefined.
  ASMJIT_API Error growBuffer(CodeBuffer* cb, size_t n) noexcept;

  //! Reserves the size of `cb` to at least `n` bytes.
  //!
  //! \note The buffer `cb` must be managed by `CodeHolder` - otherwise the
  //! behavior of the function is undefined.
  ASMJIT_API Error reserveBuffer(CodeBuffer* cb, size_t n) noexcept;

  //! \}

  //! \name Sections
  //! \{

  //! Returns an array of `Section*` records.
  inline const ZoneVector<Section*>& sections() const noexcept { return _sections; }
  //! Returns an array of `Section*` records sorted according to section order first, then section id.
  inline const ZoneVector<Section*>& sectionsByOrder() const noexcept { return _sectionsByOrder; }
  //! Returns the number of sections.
  inline uint32_t sectionCount() const noexcept { return _sections.size(); }

  //! Tests whether the given `sectionId` is valid.
  inline bool isSectionValid(uint32_t sectionId) const noexcept { return sectionId < _sections.size(); }

  //! Creates a new section and return its pointer in `sectionOut`.
  //!
  //! Returns `Error`, does not report a possible error to `ErrorHandler`.
  ASMJIT_API Error newSection(Section** sectionOut, const char* name, size_t nameSize = SIZE_MAX, uint32_t flags = 0, uint32_t alignment = 1, int32_t order = 0) noexcept;

  //! Returns a section entry of the given index.
  inline Section* sectionById(uint32_t sectionId) const noexcept { return _sections[sectionId]; }

  //! Returns section-id that matches the given `name`.
  //!
  //! If there is no such section `Section::kInvalidId` is returned.
  ASMJIT_API Section* sectionByName(const char* name, size_t nameSize = SIZE_MAX) const noexcept;

  //! Returns '.text' section (section that commonly represents code).
  //!
  //! \note Text section is always the first section in \ref CodeHolder::sections() array.
  inline Section* textSection() const noexcept { return _sections[0]; }

  //! Tests whether '.addrtab' section exists.
  inline bool hasAddressTable() const noexcept { return _addressTableSection != nullptr; }

  //! Returns '.addrtab' section.
  //!
  //! This section is used exclusively by AsmJit to store absolute 64-bit
  //! addresses that cannot be encoded in instructions like 'jmp' or 'call'.
  //!
  //! \note This section is created on demand, the returned pointer can be null.
  inline Section* addressTableSection() const noexcept { return _addressTableSection; }

  //! Ensures that '.addrtab' section exists (creates it if it doesn't) and
  //! returns it. Can return `nullptr` on out of memory condition.
  ASMJIT_API Section* ensureAddressTableSection() noexcept;

  //! Used to add an address to an address table.
  //!
  //! This implicitly calls `ensureAddressTableSection()` and then creates
  //! `AddressTableEntry` that is inserted to `_addressTableEntries`. If the
  //! address already exists this operation does nothing as the same addresses
  //! use the same slot.
  //!
  //! This function should be considered internal as it's used by assemblers to
  //! insert an absolute address into the address table. Inserting address into
  //! address table without creating a particula relocation entry makes no sense.
  ASMJIT_API Error addAddressToAddressTable(uint64_t address) noexcept;

  //! \}

  //! \name Labels & Symbols
  //! \{

  //! Returns array of `LabelEntry*` records.
  inline const ZoneVector<LabelEntry*>& labelEntries() const noexcept { return _labelEntries; }

  //! Returns number of labels created.
  inline uint32_t labelCount() const noexcept { return _labelEntries.size(); }

  //! Tests whether the label having `id` is valid (i.e. created by `newLabelEntry()`).
  inline bool isLabelValid(uint32_t labelId) const noexcept {
    return labelId < _labelEntries.size();
  }

  //! Tests whether the `label` is valid (i.e. created by `newLabelEntry()`).
  inline bool isLabelValid(const Label& label) const noexcept {
    return label.id() < _labelEntries.size();
  }

  //! \overload
  inline bool isLabelBound(uint32_t labelId) const noexcept {
    return isLabelValid(labelId) && _labelEntries[labelId]->isBound();
  }

  //! Tests whether the `label` is already bound.
  //!
  //! Returns `false` if the `label` is not valid.
  inline bool isLabelBound(const Label& label) const noexcept {
    return isLabelBound(label.id());
  }

  //! Returns LabelEntry of the given label `id`.
  inline LabelEntry* labelEntry(uint32_t labelId) const noexcept {
    return isLabelValid(labelId) ? _labelEntries[labelId] : static_cast<LabelEntry*>(nullptr);
  }

  //! Returns LabelEntry of the given `label`.
  inline LabelEntry* labelEntry(const Label& label) const noexcept {
    return labelEntry(label.id());
  }

  //! Returns offset of a `Label` by its `labelId`.
  //!
  //! The offset returned is relative to the start of the section. Zero offset
  //! is returned for unbound labels, which is their initial offset value.
  inline uint64_t labelOffset(uint32_t labelId) const noexcept {
    ASMJIT_ASSERT(isLabelValid(labelId));
    return _labelEntries[labelId]->offset();
  }

  //! \overload
  inline uint64_t labelOffset(const Label& label) const noexcept {
    return labelOffset(label.id());
  }

  //! Returns offset of a label by it's `labelId` relative to the base offset.
  //!
  //! \remarks The offset of the section where the label is bound must be valid
  //! in order to use this function, otherwise the value returned will not be
  //! reliable.
  inline uint64_t labelOffsetFromBase(uint32_t labelId) const noexcept {
    ASMJIT_ASSERT(isLabelValid(labelId));
    const LabelEntry* le = _labelEntries[labelId];
    return (le->isBound() ? le->section()->offset() : uint64_t(0)) + le->offset();
  }

  //! \overload
  inline uint64_t labelOffsetFromBase(const Label& label) const noexcept {
    return labelOffsetFromBase(label.id());
  }

  //! Creates a new anonymous label and return its id in `idOut`.
  //!
  //! Returns `Error`, does not report error to `ErrorHandler`.
  ASMJIT_API Error newLabelEntry(LabelEntry** entryOut) noexcept;

  //! Creates a new named \ref LabelEntry of the given label `type`.
  //!
  //! \param entryOut Where to store the created \ref LabelEntry.
  //! \param name The name of the label.
  //! \param nameSize The length of `name` argument, or `SIZE_MAX` if `name` is
  //!        a null terminated string, which means that the `CodeHolder` will
  //!        use `strlen()` to determine the length.
  //! \param type The type of the label to create, see \ref Label::LabelType.
  //! \param parentId Parent id of a local label, otherwise it must be
  //!        \ref Globals::kInvalidId.
  //!
  //! \retval Always returns \ref Error, does not report a possible error to
  //!         the attached \ref ErrorHandler.
  //!
  //! AsmJit has a support for local labels (\ref Label::kTypeLocal) which
  //! require a parent label id (parentId). The names of local labels can
  //! conflict with names of other local labels that have a different parent.
  ASMJIT_API Error newNamedLabelEntry(LabelEntry** entryOut, const char* name, size_t nameSize, uint32_t type, uint32_t parentId = Globals::kInvalidId) noexcept;

  //! Returns a label by name.
  //!
  //! If the named label doesn't a default constructed \ref Label is returned,
  //! which has its id set to \ref Globals::kInvalidId.
  inline Label labelByName(const char* name, size_t nameSize = SIZE_MAX, uint32_t parentId = Globals::kInvalidId) noexcept {
    return Label(labelIdByName(name, nameSize, parentId));
  }

  //! Returns a label id by name.
  //!
  //! If the named label doesn't exist \ref Globals::kInvalidId is returned.
  ASMJIT_API uint32_t labelIdByName(const char* name, size_t nameSize = SIZE_MAX, uint32_t parentId = Globals::kInvalidId) noexcept;

  //! Tests whether there are any unresolved label links.
  inline bool hasUnresolvedLinks() const noexcept { return _unresolvedLinkCount != 0; }
  //! Returns the number of label links, which are unresolved.
  inline size_t unresolvedLinkCount() const noexcept { return _unresolvedLinkCount; }

  //! Creates a new label-link used to store information about yet unbound labels.
  //!
  //! Returns `null` if the allocation failed.
  ASMJIT_API LabelLink* newLabelLink(LabelEntry* le, uint32_t sectionId, size_t offset, intptr_t rel, const OffsetFormat& format) noexcept;

  //! Resolves cross-section links (`LabelLink`) associated with each label that
  //! was used as a destination in code of a different section. It's only useful
  //! to people that use multiple sections as it will do nothing if the code only
  //! contains a single section in which cross-section links are not possible.
  ASMJIT_API Error resolveUnresolvedLinks() noexcept;

  //! Binds a label to a given `sectionId` and `offset` (relative to start of the section).
  //!
  //! This function is generally used by `BaseAssembler::bind()` to do the heavy lifting.
  ASMJIT_API Error bindLabel(const Label& label, uint32_t sectionId, uint64_t offset) noexcept;

  //! \}

  //! \name Relocations
  //! \{

  //! Tests whether the code contains relocation entries.
  inline bool hasRelocEntries() const noexcept { return !_relocations.empty(); }
  //! Returns array of `RelocEntry*` records.
  inline const ZoneVector<RelocEntry*>& relocEntries() const noexcept { return _relocations; }

  //! Returns a RelocEntry of the given `id`.
  inline RelocEntry* relocEntry(uint32_t id) const noexcept { return _relocations[id]; }

  //! Creates a new relocation entry of type `relocType`.
  //!
  //! Additional fields can be set after the relocation entry was created.
  ASMJIT_API Error newRelocEntry(RelocEntry** dst, uint32_t relocType) noexcept;

  //! \}

  //! \name Utilities
  //! \{

  //! Flattens all sections by recalculating their offsets, starting at 0.
  //!
  //! \note This should never be called more than once.
  ASMJIT_API Error flatten() noexcept;

  //! Returns computed the size of code & data of all sections.
  //!
  //! \note All sections will be iterated over and the code size returned
  //! would represent the minimum code size of all combined sections after
  //! applying minimum alignment. Code size may decrease after calling
  //! `flatten()` and `relocateToBase()`.
  ASMJIT_API size_t codeSize() const noexcept;

  //! Relocates the code to the given `baseAddress`.
  //!
  //! \param baseAddress Absolute base address where the code will be relocated
  //! to. Please note that nothing is copied to such base address, it's just an
  //! absolute value used by the relocator to resolve all stored relocations.
  //!
  //! \note This should never be called more than once.
  ASMJIT_API Error relocateToBase(uint64_t baseAddress) noexcept;

  //! Copies a single section into `dst`.
  ASMJIT_API Error copySectionData(void* dst, size_t dstSize, uint32_t sectionId, uint32_t copyOptions = 0) noexcept;

  //! Copies all sections into `dst`.
  //!
  //! This should only be used if the data was flattened and there are no gaps
  //! between the sections. The `dstSize` is always checked and the copy will
  //! never write anything outside the provided buffer.
  ASMJIT_API Error copyFlattenedData(void* dst, size_t dstSize, uint32_t copyOptions = 0) noexcept;

  //! \}

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("Use 'CodeHolder::init(const Environment& environment, uint64_t baseAddress)' instead")
  inline Error init(const CodeInfo& codeInfo) noexcept { return init(codeInfo._environment, codeInfo._baseAddress); }

  ASMJIT_DEPRECATED("Use nevironment() instead")
  inline CodeInfo codeInfo() const noexcept { return CodeInfo(_environment, _baseAddress); }

  ASMJIT_DEPRECATED("Use BaseEmitter::encodingOptions() - this function always returns zero")
  inline uint32_t emitterOptions() const noexcept { return 0; }

  ASMJIT_DEPRECATED("Use BaseEmitter::addEncodingOptions() - this function does nothing")
  inline void addEmitterOptions(uint32_t options) noexcept { DebugUtils::unused(options); }

  ASMJIT_DEPRECATED("Use BaseEmitter::clearEncodingOptions() - this function does nothing")
  inline void clearEmitterOptions(uint32_t options) noexcept { DebugUtils::unused(options); }
#endif // !ASMJIT_NO_DEPRECATED
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_CODEHOLDER_H_INCLUDED
