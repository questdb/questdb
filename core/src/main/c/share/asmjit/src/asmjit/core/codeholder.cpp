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
#include "../core/assembler.h"
#include "../core/codewriter_p.h"
#include "../core/logger.h"
#include "../core/support.h"

#include <algorithm>
#include <tuple>

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [Globals]
// ============================================================================

static const char CodeHolder_addrTabName[] = ".addrtab";

//! Encode MOD byte.
static inline uint32_t x86EncodeMod(uint32_t m, uint32_t o, uint32_t rm) noexcept {
  return (m << 6) | (o << 3) | rm;
}

// ============================================================================
// [asmjit::LabelLinkIterator]
// ============================================================================

class LabelLinkIterator {
public:
  ASMJIT_INLINE LabelLinkIterator(LabelEntry* le) noexcept { reset(le); }

  ASMJIT_INLINE explicit operator bool() const noexcept { return isValid(); }
  ASMJIT_INLINE bool isValid() const noexcept { return _link != nullptr; }

  ASMJIT_INLINE LabelLink* link() const noexcept { return _link; }
  ASMJIT_INLINE LabelLink* operator->() const noexcept { return _link; }

  ASMJIT_INLINE void reset(LabelEntry* le) noexcept {
    _pPrev = &le->_links;
    _link = *_pPrev;
  }

  ASMJIT_INLINE void next() noexcept {
    _pPrev = &_link->next;
    _link = *_pPrev;
  }

  ASMJIT_INLINE void resolveAndNext(CodeHolder* code) noexcept {
    LabelLink* linkToDelete = _link;

    _link = _link->next;
    *_pPrev = _link;

    code->_unresolvedLinkCount--;
    code->_allocator.release(linkToDelete, sizeof(LabelLink));
  }

  LabelLink** _pPrev;
  LabelLink* _link;
};

// ============================================================================
// [asmjit::CodeHolder - Utilities]
// ============================================================================

static void CodeHolder_resetInternal(CodeHolder* self, uint32_t resetPolicy) noexcept {
  uint32_t i;
  const ZoneVector<BaseEmitter*>& emitters = self->emitters();

  i = emitters.size();
  while (i)
    self->detach(emitters[--i]);

  // Reset everything into its construction state.
  self->_environment.reset();
  self->_baseAddress = Globals::kNoBaseAddress;
  self->_logger = nullptr;
  self->_errorHandler = nullptr;

  // Reset all sections.
  uint32_t numSections = self->_sections.size();
  for (i = 0; i < numSections; i++) {
    Section* section = self->_sections[i];
    if (section->_buffer.data() && !section->_buffer.isExternal())
      ::free(section->_buffer._data);
    section->_buffer._data = nullptr;
    section->_buffer._capacity = 0;
  }

  // Reset zone allocator and all containers using it.
  ZoneAllocator* allocator = self->allocator();

  self->_emitters.reset();
  self->_namedLabels.reset();
  self->_relocations.reset();
  self->_labelEntries.reset();
  self->_sections.reset();
  self->_sectionsByOrder.reset();

  self->_unresolvedLinkCount = 0;
  self->_addressTableSection = nullptr;
  self->_addressTableEntries.reset();

  allocator->reset(&self->_zone);
  self->_zone.reset(resetPolicy);
}

static void CodeHolder_onSettingsUpdated(CodeHolder* self) noexcept {
  // Notify all attached emitters about a settings update.
  for (BaseEmitter* emitter : self->emitters()) {
    emitter->onSettingsUpdated();
  }
}

// ============================================================================
// [asmjit::CodeHolder - Construction / Destruction]
// ============================================================================

CodeHolder::CodeHolder() noexcept
  : _environment(),
    _baseAddress(Globals::kNoBaseAddress),
    _logger(nullptr),
    _errorHandler(nullptr),
    _zone(16384 - Zone::kBlockOverhead),
    _allocator(&_zone),
    _unresolvedLinkCount(0),
    _addressTableSection(nullptr) {}

CodeHolder::~CodeHolder() noexcept {
  CodeHolder_resetInternal(this, Globals::kResetHard);
}

// ============================================================================
// [asmjit::CodeHolder - Init / Reset]
// ============================================================================

inline void CodeHolder_setSectionDefaultName(
  Section* section,
  char c0 = 0, char c1 = 0, char c2 = 0, char c3 = 0,
  char c4 = 0, char c5 = 0, char c6 = 0, char c7 = 0) noexcept {

  section->_name.u32[0] = Support::bytepack32_4x8(uint8_t(c0), uint8_t(c1), uint8_t(c2), uint8_t(c3));
  section->_name.u32[1] = Support::bytepack32_4x8(uint8_t(c4), uint8_t(c5), uint8_t(c6), uint8_t(c7));
}

Error CodeHolder::init(const Environment& environment, uint64_t baseAddress) noexcept {
  // Cannot reinitialize if it's locked or there is one or more emitter attached.
  if (isInitialized())
    return DebugUtils::errored(kErrorAlreadyInitialized);

  // If we are just initializing there should be no emitters attached.
  ASMJIT_ASSERT(_emitters.empty());

  // Create a default section and insert it to the `_sections` array.
  Error err = _sections.willGrow(&_allocator) |
              _sectionsByOrder.willGrow(&_allocator);
  if (err == kErrorOk) {
    Section* section = _allocator.allocZeroedT<Section>();
    if (ASMJIT_LIKELY(section)) {
      section->_flags = Section::kFlagExec | Section::kFlagConst;
      CodeHolder_setSectionDefaultName(section, '.', 't', 'e', 'x', 't');
      _sections.appendUnsafe(section);
      _sectionsByOrder.appendUnsafe(section);
    }
    else {
      err = DebugUtils::errored(kErrorOutOfMemory);
    }
  }

  if (ASMJIT_UNLIKELY(err)) {
    _zone.reset();
    return err;
  }
  else {
    _environment = environment;
    _baseAddress = baseAddress;
    return kErrorOk;
  }
}

void CodeHolder::reset(uint32_t resetPolicy) noexcept {
  CodeHolder_resetInternal(this, resetPolicy);
}

// ============================================================================
// [asmjit::CodeHolder - Attach / Detach]
// ============================================================================

Error CodeHolder::attach(BaseEmitter* emitter) noexcept {
  // Catch a possible misuse of the API.
  if (ASMJIT_UNLIKELY(!emitter))
    return DebugUtils::errored(kErrorInvalidArgument);

  // Invalid emitter, this should not be possible.
  uint32_t type = emitter->emitterType();
  if (ASMJIT_UNLIKELY(type == BaseEmitter::kTypeNone || type >= BaseEmitter::kTypeCount))
    return DebugUtils::errored(kErrorInvalidState);

  // This is suspicious, but don't fail if `emitter` is already attached
  // to this code holder. This is not error, but it's not recommended.
  if (emitter->_code != nullptr) {
    if (emitter->_code == this)
      return kErrorOk;
    return DebugUtils::errored(kErrorInvalidState);
  }

  // Reserve the space now as we cannot fail after `onAttach()` succeeded.
  ASMJIT_PROPAGATE(_emitters.willGrow(&_allocator, 1));
  ASMJIT_PROPAGATE(emitter->onAttach(this));

  // Connect CodeHolder <-> BaseEmitter.
  ASMJIT_ASSERT(emitter->_code == this);
  _emitters.appendUnsafe(emitter);

  return kErrorOk;
}

Error CodeHolder::detach(BaseEmitter* emitter) noexcept {
  if (ASMJIT_UNLIKELY(!emitter))
    return DebugUtils::errored(kErrorInvalidArgument);

  if (ASMJIT_UNLIKELY(emitter->_code != this))
    return DebugUtils::errored(kErrorInvalidState);

  // NOTE: We always detach if we were asked to, if error happens during
  // `emitter->onDetach()` we just propagate it, but the BaseEmitter will
  // be detached.
  Error err = kErrorOk;
  if (!emitter->isDestroyed())
    err = emitter->onDetach(this);

  // Disconnect CodeHolder <-> BaseEmitter.
  uint32_t index = _emitters.indexOf(emitter);
  ASMJIT_ASSERT(index != Globals::kNotFound);

  _emitters.removeAt(index);
  emitter->_code = nullptr;

  return err;
}

// ============================================================================
// [asmjit::CodeHolder - Logging]
// ============================================================================

void CodeHolder::setLogger(Logger* logger) noexcept {
#ifndef ASMJIT_NO_LOGGING
  _logger = logger;
  CodeHolder_onSettingsUpdated(this);
#else
  DebugUtils::unused(logger);
#endif
}

// ============================================================================
// [asmjit::CodeHolder - Error Handling]
// ============================================================================

void CodeHolder::setErrorHandler(ErrorHandler* errorHandler) noexcept {
  _errorHandler = errorHandler;
  CodeHolder_onSettingsUpdated(this);
}

// ============================================================================
// [asmjit::CodeHolder - Code Buffer]
// ============================================================================

static Error CodeHolder_reserveInternal(CodeHolder* self, CodeBuffer* cb, size_t n) noexcept {
  uint8_t* oldData = cb->_data;
  uint8_t* newData;

  if (oldData && !cb->isExternal())
    newData = static_cast<uint8_t*>(::realloc(oldData, n));
  else
    newData = static_cast<uint8_t*>(::malloc(n));

  if (ASMJIT_UNLIKELY(!newData))
    return DebugUtils::errored(kErrorOutOfMemory);

  cb->_data = newData;
  cb->_capacity = n;

  // Update pointers used by assemblers, if attached.
  for (BaseEmitter* emitter : self->emitters()) {
    if (emitter->isAssembler()) {
      BaseAssembler* a = static_cast<BaseAssembler*>(emitter);
      if (&a->_section->_buffer == cb) {
        size_t offset = a->offset();

        a->_bufferData = newData;
        a->_bufferEnd  = newData + n;
        a->_bufferPtr  = newData + offset;
      }
    }
  }

  return kErrorOk;
}

Error CodeHolder::growBuffer(CodeBuffer* cb, size_t n) noexcept {
  // The size of the section must be valid.
  size_t size = cb->size();
  if (ASMJIT_UNLIKELY(n > std::numeric_limits<uintptr_t>::max() - size))
    return DebugUtils::errored(kErrorOutOfMemory);

  // We can now check if growing the buffer is really necessary. It's unlikely
  // that this function is called while there is still room for `n` bytes.
  size_t capacity = cb->capacity();
  size_t required = cb->size() + n;
  if (ASMJIT_UNLIKELY(required <= capacity))
    return kErrorOk;

  if (cb->isFixed())
    return DebugUtils::errored(kErrorTooLarge);

  size_t kInitialCapacity = 8096;
  if (capacity < kInitialCapacity)
    capacity = kInitialCapacity;
  else
    capacity += Globals::kAllocOverhead;

  do {
    size_t old = capacity;
    if (capacity < Globals::kGrowThreshold)
      capacity *= 2;
    else
      capacity += Globals::kGrowThreshold;

    // Overflow.
    if (ASMJIT_UNLIKELY(old > capacity))
      return DebugUtils::errored(kErrorOutOfMemory);
  } while (capacity - Globals::kAllocOverhead < required);

  return CodeHolder_reserveInternal(this, cb, capacity - Globals::kAllocOverhead);
}

Error CodeHolder::reserveBuffer(CodeBuffer* cb, size_t n) noexcept {
  size_t capacity = cb->capacity();

  if (n <= capacity)
    return kErrorOk;

  if (cb->isFixed())
    return DebugUtils::errored(kErrorTooLarge);

  return CodeHolder_reserveInternal(this, cb, n);
}

// ============================================================================
// [asmjit::CodeHolder - Sections]
// ============================================================================

Error CodeHolder::newSection(Section** sectionOut, const char* name, size_t nameSize, uint32_t flags, uint32_t alignment, int32_t order) noexcept {
  *sectionOut = nullptr;

  if (nameSize == SIZE_MAX)
    nameSize = strlen(name);

  if (alignment == 0)
    alignment = 1;

  if (ASMJIT_UNLIKELY(!Support::isPowerOf2(alignment)))
    return DebugUtils::errored(kErrorInvalidArgument);

  if (ASMJIT_UNLIKELY(nameSize > Globals::kMaxSectionNameSize))
    return DebugUtils::errored(kErrorInvalidSectionName);

  uint32_t sectionId = _sections.size();
  if (ASMJIT_UNLIKELY(sectionId == Globals::kInvalidId))
    return DebugUtils::errored(kErrorTooManySections);

  ASMJIT_PROPAGATE(_sections.willGrow(&_allocator));
  ASMJIT_PROPAGATE(_sectionsByOrder.willGrow(&_allocator));

  Section* section = _allocator.allocZeroedT<Section>();
  if (ASMJIT_UNLIKELY(!section))
    return DebugUtils::errored(kErrorOutOfMemory);

  section->_id = sectionId;
  section->_flags = flags;
  section->_alignment = alignment;
  section->_order = order;
  memcpy(section->_name.str, name, nameSize);

  Section** insertPosition = std::lower_bound(_sectionsByOrder.begin(), _sectionsByOrder.end(), section, [](const Section* a, const Section* b) {
    return std::make_tuple(a->order(), a->id()) < std::make_tuple(b->order(), b->id());
  });

  _sections.appendUnsafe(section);
  _sectionsByOrder.insertUnsafe((size_t)(insertPosition - _sectionsByOrder.data()), section);

  *sectionOut = section;
  return kErrorOk;
}

Section* CodeHolder::sectionByName(const char* name, size_t nameSize) const noexcept {
  if (nameSize == SIZE_MAX)
    nameSize = strlen(name);

  // This could be also put in a hash-table similarly like we do with labels,
  // however it's questionable as the number of sections should be pretty low
  // in general. Create an issue if this becomes a problem.
  if (nameSize <= Globals::kMaxSectionNameSize) {
    for (Section* section : _sections)
      if (memcmp(section->_name.str, name, nameSize) == 0 && section->_name.str[nameSize] == '\0')
        return section;
  }

  return nullptr;
}

Section* CodeHolder::ensureAddressTableSection() noexcept {
  if (_addressTableSection)
    return _addressTableSection;

  newSection(&_addressTableSection, CodeHolder_addrTabName, sizeof(CodeHolder_addrTabName) - 1, 0, _environment.registerSize(), std::numeric_limits<int32_t>::max());
  return _addressTableSection;
}

Error CodeHolder::addAddressToAddressTable(uint64_t address) noexcept {
  AddressTableEntry* entry = _addressTableEntries.get(address);
  if (entry)
    return kErrorOk;

  Section* section = ensureAddressTableSection();
  if (ASMJIT_UNLIKELY(!section))
    return DebugUtils::errored(kErrorOutOfMemory);

  entry = _zone.newT<AddressTableEntry>(address);
  if (ASMJIT_UNLIKELY(!entry))
    return DebugUtils::errored(kErrorOutOfMemory);

  _addressTableEntries.insert(entry);
  section->_virtualSize += _environment.registerSize();

  return kErrorOk;
}

// ============================================================================
// [asmjit::CodeHolder - Labels / Symbols]
// ============================================================================

//! Only used to lookup a label from `_namedLabels`.
class LabelByName {
public:
  inline LabelByName(const char* key, size_t keySize, uint32_t hashCode, uint32_t parentId) noexcept
    : _key(key),
      _keySize(uint32_t(keySize)),
      _hashCode(hashCode),
      _parentId(parentId) {}

  inline uint32_t hashCode() const noexcept { return _hashCode; }

  inline bool matches(const LabelEntry* entry) const noexcept {
    return entry->nameSize() == _keySize &&
           entry->parentId() == _parentId &&
           ::memcmp(entry->name(), _key, _keySize) == 0;
  }

  const char* _key;
  uint32_t _keySize;
  uint32_t _hashCode;
  uint32_t _parentId;
};

// Returns a hash of `name` and fixes `nameSize` if it's `SIZE_MAX`.
static uint32_t CodeHolder_hashNameAndGetSize(const char* name, size_t& nameSize) noexcept {
  uint32_t hashCode = 0;
  if (nameSize == SIZE_MAX) {
    size_t i = 0;
    for (;;) {
      uint8_t c = uint8_t(name[i]);
      if (!c) break;
      hashCode = Support::hashRound(hashCode, c);
      i++;
    }
    nameSize = i;
  }
  else {
    for (size_t i = 0; i < nameSize; i++) {
      uint8_t c = uint8_t(name[i]);
      if (ASMJIT_UNLIKELY(!c)) return DebugUtils::errored(kErrorInvalidLabelName);
      hashCode = Support::hashRound(hashCode, c);
    }
  }
  return hashCode;
}

LabelLink* CodeHolder::newLabelLink(LabelEntry* le, uint32_t sectionId, size_t offset, intptr_t rel, const OffsetFormat& format) noexcept {
  LabelLink* link = _allocator.allocT<LabelLink>();
  if (ASMJIT_UNLIKELY(!link)) return nullptr;

  link->next = le->_links;
  le->_links = link;

  link->sectionId = sectionId;
  link->relocId = Globals::kInvalidId;
  link->offset = offset;
  link->rel = rel;
  link->format = format;

  _unresolvedLinkCount++;
  return link;
}

Error CodeHolder::newLabelEntry(LabelEntry** entryOut) noexcept {
  *entryOut = nullptr;

  uint32_t labelId = _labelEntries.size();
  if (ASMJIT_UNLIKELY(labelId == Globals::kInvalidId))
    return DebugUtils::errored(kErrorTooManyLabels);

  ASMJIT_PROPAGATE(_labelEntries.willGrow(&_allocator));
  LabelEntry* le = _allocator.allocZeroedT<LabelEntry>();

  if (ASMJIT_UNLIKELY(!le))
    return DebugUtils::errored(kErrorOutOfMemory);

  le->_setId(labelId);
  le->_parentId = Globals::kInvalidId;
  le->_offset = 0;
  _labelEntries.appendUnsafe(le);

  *entryOut = le;
  return kErrorOk;
}

Error CodeHolder::newNamedLabelEntry(LabelEntry** entryOut, const char* name, size_t nameSize, uint32_t type, uint32_t parentId) noexcept {
  *entryOut = nullptr;
  uint32_t hashCode = CodeHolder_hashNameAndGetSize(name, nameSize);

  if (ASMJIT_UNLIKELY(nameSize == 0))
    return DebugUtils::errored(kErrorInvalidLabelName);

  if (ASMJIT_UNLIKELY(nameSize > Globals::kMaxLabelNameSize))
    return DebugUtils::errored(kErrorLabelNameTooLong);

  switch (type) {
    case Label::kTypeLocal:
      if (ASMJIT_UNLIKELY(parentId >= _labelEntries.size()))
        return DebugUtils::errored(kErrorInvalidParentLabel);

      hashCode ^= parentId;
      break;

    case Label::kTypeGlobal:
    case Label::kTypeExternal:
      if (ASMJIT_UNLIKELY(parentId != Globals::kInvalidId))
        return DebugUtils::errored(kErrorNonLocalLabelCannotHaveParent);
      break;

    default:
      return DebugUtils::errored(kErrorInvalidArgument);
  }

  // Don't allow to insert duplicates. Local labels allow duplicates that have
  // different id, this is already accomplished by having a different hashes
  // between the same label names having different parent labels.
  LabelEntry* le = _namedLabels.get(LabelByName(name, nameSize, hashCode, parentId));
  if (ASMJIT_UNLIKELY(le))
    return DebugUtils::errored(kErrorLabelAlreadyDefined);

  Error err = kErrorOk;
  uint32_t labelId = _labelEntries.size();

  if (ASMJIT_UNLIKELY(labelId == Globals::kInvalidId))
    return DebugUtils::errored(kErrorTooManyLabels);

  ASMJIT_PROPAGATE(_labelEntries.willGrow(&_allocator));
  le = _allocator.allocZeroedT<LabelEntry>();

  if (ASMJIT_UNLIKELY(!le))
    return DebugUtils::errored(kErrorOutOfMemory);

  le->_hashCode = hashCode;
  le->_setId(labelId);
  le->_type = uint8_t(type);
  le->_parentId = parentId;
  le->_offset = 0;
  ASMJIT_PROPAGATE(le->_name.setData(&_zone, name, nameSize));

  _labelEntries.appendUnsafe(le);
  _namedLabels.insert(allocator(), le);

  *entryOut = le;
  return err;
}

uint32_t CodeHolder::labelIdByName(const char* name, size_t nameSize, uint32_t parentId) noexcept {
  uint32_t hashCode = CodeHolder_hashNameAndGetSize(name, nameSize);
  if (ASMJIT_UNLIKELY(!nameSize))
    return 0;

  if (parentId != Globals::kInvalidId)
    hashCode ^= parentId;

  LabelEntry* le = _namedLabels.get(LabelByName(name, nameSize, hashCode, parentId));
  return le ? le->id() : uint32_t(Globals::kInvalidId);
}

ASMJIT_API Error CodeHolder::resolveUnresolvedLinks() noexcept {
  if (!hasUnresolvedLinks())
    return kErrorOk;

  Error err = kErrorOk;
  for (LabelEntry* le : labelEntries()) {
    if (!le->isBound())
      continue;

    LabelLinkIterator link(le);
    if (link) {
      Support::FastUInt8 of = 0;
      Section* toSection = le->section();
      uint64_t toOffset = Support::addOverflow(toSection->offset(), le->offset(), &of);

      do {
        uint32_t linkSectionId = link->sectionId;
        if (link->relocId == Globals::kInvalidId) {
          Section* fromSection = sectionById(linkSectionId);
          size_t linkOffset = link->offset;

          CodeBuffer& buf = _sections[linkSectionId]->buffer();
          ASMJIT_ASSERT(linkOffset < buf.size());

          // Calculate the offset relative to the start of the virtual base.
          Support::FastUInt8 localOF = of;
          uint64_t fromOffset = Support::addOverflow<uint64_t>(fromSection->offset(), linkOffset, &localOF);
          int64_t displacement = int64_t(toOffset - fromOffset + uint64_t(int64_t(link->rel)));

          if (!localOF) {
            ASMJIT_ASSERT(size_t(linkOffset) < buf.size());
            ASMJIT_ASSERT(buf.size() - size_t(linkOffset) >= link->format.valueSize());

            // Overwrite a real displacement in the CodeBuffer.
            if (CodeWriterUtils::writeOffset(buf._data + linkOffset, displacement, link->format)) {
              link.resolveAndNext(this);
              continue;
            }
          }

          err = DebugUtils::errored(kErrorInvalidDisplacement);
          // Falls through to `link.next()`.
        }

        link.next();
      } while (link);
    }
  }

  return err;
}

ASMJIT_API Error CodeHolder::bindLabel(const Label& label, uint32_t toSectionId, uint64_t toOffset) noexcept {
  LabelEntry* le = labelEntry(label);
  if (ASMJIT_UNLIKELY(!le))
    return DebugUtils::errored(kErrorInvalidLabel);

  if (ASMJIT_UNLIKELY(toSectionId > _sections.size()))
    return DebugUtils::errored(kErrorInvalidSection);

  // Label can be bound only once.
  if (ASMJIT_UNLIKELY(le->isBound()))
    return DebugUtils::errored(kErrorLabelAlreadyBound);

  // Bind the label.
  Section* section = _sections[toSectionId];
  le->_section = section;
  le->_offset = toOffset;

  Error err = kErrorOk;
  CodeBuffer& buf = section->buffer();

  // Fix all links to this label we have collected so far if they are within
  // the same section. We ignore any inter-section links as these have to be
  // fixed later.
  LabelLinkIterator link(le);
  while (link) {
    uint32_t linkSectionId = link->sectionId;
    size_t linkOffset = link->offset;

    uint32_t relocId = link->relocId;
    if (relocId != Globals::kInvalidId) {
      // Adjust relocation data only.
      RelocEntry* re = _relocations[relocId];
      re->_payload += toOffset;
      re->_targetSectionId = toSectionId;
    }
    else {
      if (linkSectionId != toSectionId) {
        link.next();
        continue;
      }

      ASMJIT_ASSERT(linkOffset < buf.size());
      int64_t displacement = int64_t(toOffset - uint64_t(linkOffset) + uint64_t(int64_t(link->rel)));

      // Size of the value we are going to patch. Only BYTE/DWORD is allowed.
      ASMJIT_ASSERT(buf.size() - size_t(linkOffset) >= link->format.regionSize());

      // Overwrite a real displacement in the CodeBuffer.
      if (!CodeWriterUtils::writeOffset(buf._data + linkOffset, displacement, link->format)) {
        err = DebugUtils::errored(kErrorInvalidDisplacement);
        link.next();
        continue;
      }
    }

    link.resolveAndNext(this);
  }

  return err;
}

// ============================================================================
// [asmjit::BaseEmitter - Relocations]
// ============================================================================

Error CodeHolder::newRelocEntry(RelocEntry** dst, uint32_t relocType) noexcept {
  ASMJIT_PROPAGATE(_relocations.willGrow(&_allocator));

  uint32_t relocId = _relocations.size();
  if (ASMJIT_UNLIKELY(relocId == Globals::kInvalidId))
    return DebugUtils::errored(kErrorTooManyRelocations);

  RelocEntry* re = _allocator.allocZeroedT<RelocEntry>();
  if (ASMJIT_UNLIKELY(!re))
    return DebugUtils::errored(kErrorOutOfMemory);

  re->_id = relocId;
  re->_relocType = uint8_t(relocType);
  re->_sourceSectionId = Globals::kInvalidId;
  re->_targetSectionId = Globals::kInvalidId;
  _relocations.appendUnsafe(re);

  *dst = re;
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseEmitter - Expression Evaluation]
// ============================================================================

static Error CodeHolder_evaluateExpression(CodeHolder* self, Expression* exp, uint64_t* out) noexcept {
  uint64_t value[2];
  for (size_t i = 0; i < 2; i++) {
    uint64_t v;
    switch (exp->valueType[i]) {
      case Expression::kValueNone: {
        v = 0;
        break;
      }

      case Expression::kValueConstant: {
        v = exp->value[i].constant;
        break;
      }

      case Expression::kValueLabel: {
        LabelEntry* le = exp->value[i].label;
        if (!le->isBound())
          return DebugUtils::errored(kErrorExpressionLabelNotBound);
        v = le->section()->offset() + le->offset();
        break;
      }

      case Expression::kValueExpression: {
        Expression* nested = exp->value[i].expression;
        ASMJIT_PROPAGATE(CodeHolder_evaluateExpression(self, nested, &v));
        break;
      }

      default:
        return DebugUtils::errored(kErrorInvalidState);
    }

    value[i] = v;
  }

  uint64_t result;
  uint64_t& a = value[0];
  uint64_t& b = value[1];

  switch (exp->opType) {
    case Expression::kOpAdd:
      result = a + b;
      break;

    case Expression::kOpSub:
      result = a - b;
      break;

    case Expression::kOpMul:
      result = a * b;
      break;

    case Expression::kOpSll:
      result = (b > 63) ? uint64_t(0) : uint64_t(a << b);
      break;

    case Expression::kOpSrl:
      result = (b > 63) ? uint64_t(0) : uint64_t(a >> b);
      break;

    case Expression::kOpSra:
      result = Support::sar(a, Support::min<uint64_t>(b, 63));
      break;

    default:
      return DebugUtils::errored(kErrorInvalidState);
  }

  *out = result;
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseEmitter - Utilities]
// ============================================================================

Error CodeHolder::flatten() noexcept {
  uint64_t offset = 0;
  for (Section* section : _sectionsByOrder) {
    uint64_t realSize = section->realSize();
    if (realSize) {
      uint64_t alignedOffset = Support::alignUp(offset, section->alignment());
      if (ASMJIT_UNLIKELY(alignedOffset < offset))
        return DebugUtils::errored(kErrorTooLarge);

      Support::FastUInt8 of = 0;
      offset = Support::addOverflow(alignedOffset, realSize, &of);

      if (ASMJIT_UNLIKELY(of))
        return DebugUtils::errored(kErrorTooLarge);
    }
  }

  // Now we know that we can assign offsets of all sections properly.
  Section* prev = nullptr;
  offset = 0;
  for (Section* section : _sectionsByOrder) {
    uint64_t realSize = section->realSize();
    if (realSize)
      offset = Support::alignUp(offset, section->alignment());
    section->_offset = offset;

    // Make sure the previous section extends a bit to cover the alignment.
    if (prev)
      prev->_virtualSize = offset - prev->_offset;

    prev = section;
    offset += realSize;
  }

  return kErrorOk;
}

size_t CodeHolder::codeSize() const noexcept {
  Support::FastUInt8 of = 0;
  uint64_t offset = 0;

  for (Section* section : _sectionsByOrder) {
    uint64_t realSize = section->realSize();

    if (realSize) {
      uint64_t alignedOffset = Support::alignUp(offset, section->alignment());
      ASMJIT_ASSERT(alignedOffset >= offset);
      offset = Support::addOverflow(alignedOffset, realSize, &of);
    }
  }

  if ((sizeof(uint64_t) > sizeof(size_t) && offset > SIZE_MAX) || of)
    return SIZE_MAX;

  return size_t(offset);
}

Error CodeHolder::relocateToBase(uint64_t baseAddress) noexcept {
  // Base address must be provided.
  if (ASMJIT_UNLIKELY(baseAddress == Globals::kNoBaseAddress))
    return DebugUtils::errored(kErrorInvalidArgument);

  _baseAddress = baseAddress;
  uint32_t addressSize = _environment.registerSize();

  Section* addressTableSection = _addressTableSection;
  uint32_t addressTableEntryCount = 0;
  uint8_t* addressTableEntryData = nullptr;

  if (addressTableSection) {
    ASMJIT_PROPAGATE(
      reserveBuffer(&addressTableSection->_buffer, size_t(addressTableSection->virtualSize())));
    addressTableEntryData = addressTableSection->_buffer.data();
  }

  // Relocate all recorded locations.
  for (const RelocEntry* re : _relocations) {
    // Possibly deleted or optimized-out entry.
    if (re->relocType() == RelocEntry::kTypeNone)
      continue;

    Section* sourceSection = sectionById(re->sourceSectionId());
    Section* targetSection = nullptr;

    if (re->targetSectionId() != Globals::kInvalidId)
      targetSection = sectionById(re->targetSectionId());

    uint64_t value = re->payload();
    uint64_t sectionOffset = sourceSection->offset();
    uint64_t sourceOffset = re->sourceOffset();

    // Make sure that the `RelocEntry` doesn't go out of bounds.
    size_t regionSize = re->format().regionSize();
    if (ASMJIT_UNLIKELY(re->sourceOffset() >= sourceSection->bufferSize() ||
                        sourceSection->bufferSize() - size_t(re->sourceOffset()) < regionSize))
      return DebugUtils::errored(kErrorInvalidRelocEntry);

    uint8_t* buffer = sourceSection->data();
    size_t valueOffset = size_t(re->sourceOffset()) + re->format().valueOffset();

    switch (re->relocType()) {
      case RelocEntry::kTypeExpression: {
        Expression* expression = (Expression*)(uintptr_t(value));
        ASMJIT_PROPAGATE(CodeHolder_evaluateExpression(this, expression, &value));
        break;
      }

      case RelocEntry::kTypeAbsToAbs: {
        break;
      }

      case RelocEntry::kTypeRelToAbs: {
        // Value is currently a relative offset from the start of its section.
        // We have to convert it to an absolute offset (including base address).
        if (ASMJIT_UNLIKELY(!targetSection))
          return DebugUtils::errored(kErrorInvalidRelocEntry);

        //value += baseAddress + sectionOffset + sourceOffset + regionSize;
        value += baseAddress + targetSection->offset();
        break;
      }

      case RelocEntry::kTypeAbsToRel: {
        value -= baseAddress + sectionOffset + sourceOffset + regionSize;
        if (addressSize > 4 && !Support::isInt32(int64_t(value)))
          return DebugUtils::errored(kErrorRelocOffsetOutOfRange);
        break;
      }

      case RelocEntry::kTypeX64AddressEntry: {
        if (re->format().valueSize() != 4 || valueOffset < 2)
          return DebugUtils::errored(kErrorInvalidRelocEntry);

        // First try whether a relative 32-bit displacement would work.
        value -= baseAddress + sectionOffset + sourceOffset + regionSize;
        if (!Support::isInt32(int64_t(value))) {
          // Relative 32-bit displacement is not possible, use '.addrtab' section.
          AddressTableEntry* atEntry = _addressTableEntries.get(re->payload());
          if (ASMJIT_UNLIKELY(!atEntry))
            return DebugUtils::errored(kErrorInvalidRelocEntry);

          // Cannot be null as we have just matched the `AddressTableEntry`.
          ASMJIT_ASSERT(addressTableSection != nullptr);

          if (!atEntry->hasAssignedSlot())
            atEntry->_slot = addressTableEntryCount++;

          size_t atEntryIndex = size_t(atEntry->slot()) * addressSize;
          uint64_t addrSrc = sectionOffset + sourceOffset + regionSize;
          uint64_t addrDst = addressTableSection->offset() + uint64_t(atEntryIndex);

          value = addrDst - addrSrc;
          if (!Support::isInt32(int64_t(value)))
            return DebugUtils::errored(kErrorRelocOffsetOutOfRange);

          // Bytes that replace [REX, OPCODE] bytes.
          uint32_t byte0 = 0xFF;
          uint32_t byte1 = buffer[valueOffset - 1];

          if (byte1 == 0xE8) {
            // Patch CALL/MOD byte to FF /2 (-> 0x15).
            byte1 = x86EncodeMod(0, 2, 5);
          }
          else if (byte1 == 0xE9) {
            // Patch JMP/MOD byte to FF /4 (-> 0x25).
            byte1 = x86EncodeMod(0, 4, 5);
          }
          else {
            return DebugUtils::errored(kErrorInvalidRelocEntry);
          }

          // Patch `jmp/call` instruction.
          buffer[valueOffset - 2] = uint8_t(byte0);
          buffer[valueOffset - 1] = uint8_t(byte1);

          Support::writeU64uLE(addressTableEntryData + atEntryIndex, re->payload());
        }
        break;
      }

      default:
        return DebugUtils::errored(kErrorInvalidRelocEntry);
    }

    switch (re->format().valueSize()) {
      case 1:
        Support::writeU8(buffer + valueOffset, uint32_t(value & 0xFFu));
        break;

      case 2:
        Support::writeU16uLE(buffer + valueOffset, uint32_t(value & 0xFFFFu));
        break;

      case 4:
        Support::writeU32uLE(buffer + valueOffset, uint32_t(value & 0xFFFFFFFFu));
        break;

      case 8:
        Support::writeU64uLE(buffer + valueOffset, value);
        break;

      default:
        return DebugUtils::errored(kErrorInvalidRelocEntry);
    }
  }

  // Fixup the virtual size of the address table if it's the last section.
  if (_sectionsByOrder.last() == addressTableSection) {
    size_t addressTableSize = addressTableEntryCount * addressSize;
    addressTableSection->_buffer._size = addressTableSize;
    addressTableSection->_virtualSize = addressTableSize;
  }

  return kErrorOk;
}

Error CodeHolder::copySectionData(void* dst, size_t dstSize, uint32_t sectionId, uint32_t copyOptions) noexcept {
  if (ASMJIT_UNLIKELY(!isSectionValid(sectionId)))
    return DebugUtils::errored(kErrorInvalidSection);

  Section* section = sectionById(sectionId);
  size_t bufferSize = section->bufferSize();

  if (ASMJIT_UNLIKELY(dstSize < bufferSize))
    return DebugUtils::errored(kErrorInvalidArgument);

  memcpy(dst, section->data(), bufferSize);

  if (bufferSize < dstSize && (copyOptions & kCopyPadSectionBuffer)) {
    size_t paddingSize = dstSize - bufferSize;
    memset(static_cast<uint8_t*>(dst) + bufferSize, 0, paddingSize);
  }

  return kErrorOk;
}

Error CodeHolder::copyFlattenedData(void* dst, size_t dstSize, uint32_t copyOptions) noexcept {
  size_t end = 0;
  for (Section* section : _sectionsByOrder) {
    if (section->offset() > dstSize)
      return DebugUtils::errored(kErrorInvalidArgument);

    size_t bufferSize = section->bufferSize();
    size_t offset = size_t(section->offset());

    if (ASMJIT_UNLIKELY(dstSize - offset < bufferSize))
      return DebugUtils::errored(kErrorInvalidArgument);

    uint8_t* dstTarget = static_cast<uint8_t*>(dst) + offset;
    size_t paddingSize = 0;
    memcpy(dstTarget, section->data(), bufferSize);

    if ((copyOptions & kCopyPadSectionBuffer) && bufferSize < section->virtualSize()) {
      paddingSize = Support::min<size_t>(dstSize - offset, size_t(section->virtualSize())) - bufferSize;
      memset(dstTarget + bufferSize, 0, paddingSize);
    }

    end = Support::max(end, offset + bufferSize + paddingSize);
  }

  if (end < dstSize && (copyOptions & kCopyPadTargetBuffer)) {
    memset(static_cast<uint8_t*>(dst) + end, 0, dstSize - end);
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::CodeHolder - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
UNIT(code_holder) {
  CodeHolder code;

  INFO("Verifying CodeHolder::init()");
  Environment env;
  env.init(Environment::kArchX86);

  code.init(env);
  EXPECT(code.arch() == Environment::kArchX86);

  INFO("Verifying named labels");
  LabelEntry* le;
  EXPECT(code.newNamedLabelEntry(&le, "NamedLabel", SIZE_MAX, Label::kTypeGlobal) == kErrorOk);
  EXPECT(strcmp(le->name(), "NamedLabel") == 0);
  EXPECT(code.labelIdByName("NamedLabel") == le->id());

  INFO("Verifying section ordering");
  Section* section1;
  EXPECT(code.newSection(&section1, "high-priority", SIZE_MAX, 0, 1, -1) == kErrorOk);
  EXPECT(code.sections()[1] == section1);
  EXPECT(code.sectionsByOrder()[0] == section1);

  Section* section0;
  EXPECT(code.newSection(&section0, "higher-priority", SIZE_MAX, 0, 1, -2) == kErrorOk);
  EXPECT(code.sections()[2] == section0);
  EXPECT(code.sectionsByOrder()[0] == section0);
  EXPECT(code.sectionsByOrder()[1] == section1);

  Section* section3;
  EXPECT(code.newSection(&section3, "low-priority", SIZE_MAX, 0, 1, 2) == kErrorOk);
  EXPECT(code.sections()[3] == section3);
  EXPECT(code.sectionsByOrder()[3] == section3);

}
#endif

ASMJIT_END_NAMESPACE
