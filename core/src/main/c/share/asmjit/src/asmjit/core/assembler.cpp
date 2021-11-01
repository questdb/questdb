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
#include "../core/constpool.h"
#include "../core/emitterutils_p.h"
#include "../core/formatter.h"
#include "../core/logger.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::BaseAssembler - Construction / Destruction]
// ============================================================================

BaseAssembler::BaseAssembler() noexcept
  : BaseEmitter(kTypeAssembler) {}

BaseAssembler::~BaseAssembler() noexcept {}

// ============================================================================
// [asmjit::BaseAssembler - Buffer Management]
// ============================================================================

Error BaseAssembler::setOffset(size_t offset) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  size_t size = Support::max<size_t>(_section->bufferSize(), this->offset());
  if (ASMJIT_UNLIKELY(offset > size))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  _bufferPtr = _bufferData + offset;
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseAssembler - Section Management]
// ============================================================================

static void BaseAssembler_initSection(BaseAssembler* self, Section* section) noexcept {
  uint8_t* p = section->_buffer._data;

  self->_section = section;
  self->_bufferData = p;
  self->_bufferPtr  = p + section->_buffer._size;
  self->_bufferEnd  = p + section->_buffer._capacity;
}

Error BaseAssembler::section(Section* section) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  if (!_code->isSectionValid(section->id()) || _code->_sections[section->id()] != section)
    return reportError(DebugUtils::errored(kErrorInvalidSection));

#ifndef ASMJIT_NO_LOGGING
  if (_logger)
    _logger->logf(".section %s {#%u}\n", section->name(), section->id());
#endif

  BaseAssembler_initSection(this, section);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseAssembler - Label Management]
// ============================================================================

Label BaseAssembler::newLabel() {
  uint32_t labelId = Globals::kInvalidId;
  if (ASMJIT_LIKELY(_code)) {
    LabelEntry* le;
    Error err = _code->newLabelEntry(&le);
    if (ASMJIT_UNLIKELY(err))
      reportError(err);
    else
      labelId = le->id();
  }
  return Label(labelId);
}

Label BaseAssembler::newNamedLabel(const char* name, size_t nameSize, uint32_t type, uint32_t parentId) {
  uint32_t labelId = Globals::kInvalidId;
  if (ASMJIT_LIKELY(_code)) {
    LabelEntry* le;
    Error err = _code->newNamedLabelEntry(&le, name, nameSize, type, parentId);
    if (ASMJIT_UNLIKELY(err))
      reportError(err);
    else
      labelId = le->id();
  }
  return Label(labelId);
}

Error BaseAssembler::bind(const Label& label) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  Error err = _code->bindLabel(label, _section->id(), offset());

#ifndef ASMJIT_NO_LOGGING
  if (_logger)
    EmitterUtils::logLabelBound(this, label);
#endif

  resetInlineComment();
  if (err)
    return reportError(err);

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseAssembler - Embed]
// ============================================================================

Error BaseAssembler::embed(const void* data, size_t dataSize) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  if (dataSize == 0)
    return kErrorOk;

  CodeWriter writer(this);
  ASMJIT_PROPAGATE(writer.ensureSpace(this, dataSize));

  writer.emitData(data, dataSize);
  writer.done(this);

#ifndef ASMJIT_NO_LOGGING
  if (_logger) {
    StringTmp<512> sb;
    Formatter::formatData(sb, _logger->flags(), arch(), Type::kIdU8, data, dataSize, 1);
    sb.append('\n');
    _logger->log(sb);
  }
#endif

  return kErrorOk;
}

Error BaseAssembler::embedDataArray(uint32_t typeId, const void* data, size_t itemCount, size_t repeatCount) {
  uint32_t deabstractDelta = Type::deabstractDeltaOfSize(registerSize());
  uint32_t finalTypeId = Type::deabstract(typeId, deabstractDelta);

  if (ASMJIT_UNLIKELY(!Type::isValid(finalTypeId)))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  if (itemCount == 0 || repeatCount == 0)
    return kErrorOk;

  uint32_t typeSize = Type::sizeOf(finalTypeId);
  Support::FastUInt8 of = 0;

  size_t dataSize = Support::mulOverflow(itemCount, size_t(typeSize), &of);
  size_t totalSize = Support::mulOverflow(dataSize, repeatCount, &of);

  if (ASMJIT_UNLIKELY(of))
    return reportError(DebugUtils::errored(kErrorOutOfMemory));

  CodeWriter writer(this);
  ASMJIT_PROPAGATE(writer.ensureSpace(this, totalSize));

  for (size_t i = 0; i < repeatCount; i++)
    writer.emitData(data, dataSize);

  writer.done(this);

#ifndef ASMJIT_NO_LOGGING
  if (_logger) {
    StringTmp<512> sb;
    Formatter::formatData(sb, _logger->flags(), arch(), typeId, data, itemCount, repeatCount);
    sb.append('\n');
    _logger->log(sb);
  }
#endif

  return kErrorOk;
}

#ifndef ASMJIT_NO_LOGGING
static const uint8_t dataTypeIdBySize[9] = {
  Type::kIdVoid, // [0] (invalid)
  Type::kIdU8,   // [1] (uint8_t)
  Type::kIdU16,  // [2] (uint16_t)
  Type::kIdVoid, // [3] (invalid)
  Type::kIdU32,  // [4] (uint32_t)
  Type::kIdVoid, // [5] (invalid)
  Type::kIdVoid, // [6] (invalid)
  Type::kIdVoid, // [7] (invalid)
  Type::kIdU64   // [8] (uint64_t)
};
#endif

Error BaseAssembler::embedConstPool(const Label& label, const ConstPool& pool) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  if (ASMJIT_UNLIKELY(!isLabelValid(label)))
    return reportError(DebugUtils::errored(kErrorInvalidLabel));

  ASMJIT_PROPAGATE(align(kAlignData, uint32_t(pool.alignment())));
  ASMJIT_PROPAGATE(bind(label));

  size_t size = pool.size();
  if (!size)
    return kErrorOk;

  CodeWriter writer(this);
  ASMJIT_PROPAGATE(writer.ensureSpace(this, size));

#ifndef ASMJIT_NO_LOGGING
  uint8_t* data = writer.cursor();
#endif

  pool.fill(writer.cursor());
  writer.advance(size);
  writer.done(this);

#ifndef ASMJIT_NO_LOGGING
  if (_logger) {
    uint32_t dataSizeLog2 = Support::min<uint32_t>(Support::ctz(pool.minItemSize()), 3);
    uint32_t dataSize = 1 << dataSizeLog2;

    StringTmp<512> sb;
    Formatter::formatData(sb, _logger->flags(), arch(), dataTypeIdBySize[dataSize], data, size >> dataSizeLog2);
    sb.append('\n');
    _logger->log(sb);
  }
#endif

  return kErrorOk;
}

Error BaseAssembler::embedLabel(const Label& label, size_t dataSize) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  ASMJIT_ASSERT(_code != nullptr);
  RelocEntry* re;
  LabelEntry* le = _code->labelEntry(label);

  if (ASMJIT_UNLIKELY(!le))
    return reportError(DebugUtils::errored(kErrorInvalidLabel));

  if (dataSize == 0)
    dataSize = registerSize();

  if (ASMJIT_UNLIKELY(!Support::isPowerOf2(dataSize) || dataSize > 8))
    return reportError(DebugUtils::errored(kErrorInvalidOperandSize));

  CodeWriter writer(this);
  ASMJIT_PROPAGATE(writer.ensureSpace(this, dataSize));

#ifndef ASMJIT_NO_LOGGING
  if (_logger) {
    StringTmp<256> sb;
    sb.append('.');
    Formatter::formatDataType(sb, _logger->flags(), arch(), dataTypeIdBySize[dataSize]);
    sb.append(' ');
    Formatter::formatLabel(sb, 0, this, label.id());
    sb.append('\n');
    _logger->log(sb);
  }
#endif

  Error err = _code->newRelocEntry(&re, RelocEntry::kTypeRelToAbs);
  if (ASMJIT_UNLIKELY(err))
    return reportError(err);

  re->_sourceSectionId = _section->id();
  re->_sourceOffset = offset();
  re->_format.resetToDataValue(uint32_t(dataSize));

  if (le->isBound()) {
    re->_targetSectionId = le->section()->id();
    re->_payload = le->offset();
  }
  else {
    OffsetFormat of;
    of.resetToDataValue(uint32_t(dataSize));

    LabelLink* link = _code->newLabelLink(le, _section->id(), offset(), 0, of);
    if (ASMJIT_UNLIKELY(!link))
      return reportError(DebugUtils::errored(kErrorOutOfMemory));

    link->relocId = re->id();
  }

  // Emit dummy DWORD/QWORD depending on the data size.
  writer.emitZeros(dataSize);
  writer.done(this);

  return kErrorOk;
}

Error BaseAssembler::embedLabelDelta(const Label& label, const Label& base, size_t dataSize) {
  if (ASMJIT_UNLIKELY(!_code))
    return reportError(DebugUtils::errored(kErrorNotInitialized));

  LabelEntry* labelEntry = _code->labelEntry(label);
  LabelEntry* baseEntry = _code->labelEntry(base);

  if (ASMJIT_UNLIKELY(!labelEntry || !baseEntry))
    return reportError(DebugUtils::errored(kErrorInvalidLabel));

  if (dataSize == 0)
    dataSize = registerSize();

  if (ASMJIT_UNLIKELY(!Support::isPowerOf2(dataSize) || dataSize > 8))
    return reportError(DebugUtils::errored(kErrorInvalidOperandSize));

  CodeWriter writer(this);
  ASMJIT_PROPAGATE(writer.ensureSpace(this, dataSize));

#ifndef ASMJIT_NO_LOGGING
  if (_logger) {
    StringTmp<256> sb;
    sb.append('.');
    Formatter::formatDataType(sb, _logger->flags(), arch(), dataTypeIdBySize[dataSize]);
    sb.append(" (");
    Formatter::formatLabel(sb, 0, this, label.id());
    sb.append(" - ");
    Formatter::formatLabel(sb, 0, this, base.id());
    sb.append(")\n");
    _logger->log(sb);
  }
#endif

  // If both labels are bound within the same section it means the delta can be calculated now.
  if (labelEntry->isBound() && baseEntry->isBound() && labelEntry->section() == baseEntry->section()) {
    uint64_t delta = labelEntry->offset() - baseEntry->offset();
    writer.emitValueLE(delta, dataSize);
  }
  else {
    RelocEntry* re;
    Error err = _code->newRelocEntry(&re, RelocEntry::kTypeExpression);
    if (ASMJIT_UNLIKELY(err))
      return reportError(err);

    Expression* exp = _code->_zone.newT<Expression>();
    if (ASMJIT_UNLIKELY(!exp))
      return reportError(DebugUtils::errored(kErrorOutOfMemory));

    exp->reset();
    exp->opType = Expression::kOpSub;
    exp->setValueAsLabel(0, labelEntry);
    exp->setValueAsLabel(1, baseEntry);

    re->_format.resetToDataValue(dataSize);
    re->_sourceSectionId = _section->id();
    re->_sourceOffset = offset();
    re->_payload = (uint64_t)(uintptr_t)exp;

    writer.emitZeros(dataSize);
  }

  writer.done(this);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseAssembler - Comment]
// ============================================================================

Error BaseAssembler::comment(const char* data, size_t size) {
  if (!hasEmitterFlag(kFlagLogComments)) {
    if (!hasEmitterFlag(kFlagAttached))
      return reportError(DebugUtils::errored(kErrorNotInitialized));
    return kErrorOk;
  }

#ifndef ASMJIT_NO_LOGGING
  // Logger cannot be NULL if `kFlagLogComments` is set.
  ASMJIT_ASSERT(_logger != nullptr);

  _logger->log(data, size);
  _logger->log("\n", 1);
  return kErrorOk;
#else
  DebugUtils::unused(data, size);
  return kErrorOk;
#endif
}

// ============================================================================
// [asmjit::BaseAssembler - Events]
// ============================================================================

Error BaseAssembler::onAttach(CodeHolder* code) noexcept {
  ASMJIT_PROPAGATE(Base::onAttach(code));

  // Attach to the end of the .text section.
  BaseAssembler_initSection(this, code->_sections[0]);

  return kErrorOk;
}

Error BaseAssembler::onDetach(CodeHolder* code) noexcept {
  _section    = nullptr;
  _bufferData = nullptr;
  _bufferEnd  = nullptr;
  _bufferPtr  = nullptr;
  return Base::onDetach(code);
}

ASMJIT_END_NAMESPACE
