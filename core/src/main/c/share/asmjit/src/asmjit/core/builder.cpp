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
#ifndef ASMJIT_NO_BUILDER

#include "../core/builder.h"
#include "../core/emitterutils_p.h"
#include "../core/errorhandler.h"
#include "../core/formatter.h"
#include "../core/logger.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::PostponedErrorHandler (Internal)]
// ============================================================================

//! Postponed error handler that never throws. Used as a temporal error handler
//! to run passes. If error occurs, the caller is notified and will call the
//! real error handler, that can throw.
class PostponedErrorHandler : public ErrorHandler {
public:
  void handleError(Error err, const char* message, BaseEmitter* origin) override {
    DebugUtils::unused(err, origin);
    _message.assign(message);
  }

  StringTmp<128> _message;
};

// ============================================================================
// [asmjit::BaseBuilder - Utilities]
// ============================================================================

static void BaseBuilder_deletePasses(BaseBuilder* self) noexcept {
  for (Pass* pass : self->_passes)
    pass->~Pass();
  self->_passes.reset();
}

// ============================================================================
// [asmjit::BaseBuilder - Construction / Destruction]
// ============================================================================

BaseBuilder::BaseBuilder() noexcept
  : BaseEmitter(kTypeBuilder),
    _codeZone(32768 - Zone::kBlockOverhead),
    _dataZone(16384 - Zone::kBlockOverhead),
    _passZone(65536 - Zone::kBlockOverhead),
    _allocator(&_codeZone) {}

BaseBuilder::~BaseBuilder() noexcept {
  BaseBuilder_deletePasses(this);
}

// ============================================================================
// [asmjit::BaseBuilder - Node Management]
// ============================================================================

Error BaseBuilder::_newInstNode(InstNode** out, uint32_t instId, uint32_t instOptions, uint32_t opCount) {
  uint32_t opCapacity = InstNode::capacityOfOpCount(opCount);
  ASMJIT_ASSERT(opCapacity >= InstNode::kBaseOpCapacity);

  InstNode* node = _allocator.allocT<InstNode>(InstNode::nodeSizeOfOpCapacity(opCapacity));
  if (ASMJIT_UNLIKELY(!node))
    return reportError(DebugUtils::errored(kErrorOutOfMemory));

  *out = new(node) InstNode(this, instId, instOptions, opCount, opCapacity);
  return kErrorOk;
}


Error BaseBuilder::_newLabelNode(LabelNode** out) {
  *out = nullptr;

  ASMJIT_PROPAGATE(_newNodeT<LabelNode>(out));
  return registerLabelNode(*out);
}

Error BaseBuilder::_newAlignNode(AlignNode** out, uint32_t alignMode, uint32_t alignment) {
  *out = nullptr;
  return _newNodeT<AlignNode>(out, alignMode, alignment);
}

Error BaseBuilder::_newEmbedDataNode(EmbedDataNode** out, uint32_t typeId, const void* data, size_t itemCount, size_t repeatCount) {
  *out = nullptr;

  uint32_t deabstractDelta = Type::deabstractDeltaOfSize(registerSize());
  uint32_t finalTypeId = Type::deabstract(typeId, deabstractDelta);

  if (ASMJIT_UNLIKELY(!Type::isValid(finalTypeId)))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  uint32_t typeSize = Type::sizeOf(finalTypeId);
  Support::FastUInt8 of = 0;

  size_t dataSize = Support::mulOverflow(itemCount, size_t(typeSize), &of);
  if (ASMJIT_UNLIKELY(of))
    return reportError(DebugUtils::errored(kErrorOutOfMemory));

  EmbedDataNode* node;
  ASMJIT_PROPAGATE(_newNodeT<EmbedDataNode>(&node));

  node->_embed._typeId = uint8_t(typeId);
  node->_embed._typeSize = uint8_t(typeSize);
  node->_itemCount = itemCount;
  node->_repeatCount = repeatCount;

  uint8_t* dstData = node->_inlineData;
  if (dataSize > EmbedDataNode::kInlineBufferSize) {
    dstData = static_cast<uint8_t*>(_dataZone.alloc(dataSize, 8));
    if (ASMJIT_UNLIKELY(!dstData))
      return reportError(DebugUtils::errored(kErrorOutOfMemory));
    node->_externalData = dstData;
  }

  if (data)
    memcpy(dstData, data, dataSize);

  *out = node;
  return kErrorOk;
}

Error BaseBuilder::_newConstPoolNode(ConstPoolNode** out) {
  *out = nullptr;

  ASMJIT_PROPAGATE(_newNodeT<ConstPoolNode>(out));
  return registerLabelNode(*out);
}

Error BaseBuilder::_newCommentNode(CommentNode** out, const char* data, size_t size) {
  *out = nullptr;

  if (data) {
    if (size == SIZE_MAX)
      size = strlen(data);

    if (size > 0) {
      data = static_cast<char*>(_dataZone.dup(data, size, true));
      if (ASMJIT_UNLIKELY(!data))
        return reportError(DebugUtils::errored(kErrorOutOfMemory));
    }
  }

  return _newNodeT<CommentNode>(out, data);
}

BaseNode* BaseBuilder::addNode(BaseNode* node) noexcept {
  ASMJIT_ASSERT(node);
  ASMJIT_ASSERT(!node->_prev);
  ASMJIT_ASSERT(!node->_next);
  ASMJIT_ASSERT(!node->isActive());

  if (!_cursor) {
    if (!_firstNode) {
      _firstNode = node;
      _lastNode = node;
    }
    else {
      node->_next = _firstNode;
      _firstNode->_prev = node;
      _firstNode = node;
    }
  }
  else {
    BaseNode* prev = _cursor;
    BaseNode* next = _cursor->next();

    node->_prev = prev;
    node->_next = next;

    prev->_next = node;
    if (next)
      next->_prev = node;
    else
      _lastNode = node;
  }

  node->addFlags(BaseNode::kFlagIsActive);
  if (node->isSection())
    _dirtySectionLinks = true;

  _cursor = node;
  return node;
}

BaseNode* BaseBuilder::addAfter(BaseNode* node, BaseNode* ref) noexcept {
  ASMJIT_ASSERT(node);
  ASMJIT_ASSERT(ref);

  ASMJIT_ASSERT(!node->_prev);
  ASMJIT_ASSERT(!node->_next);

  BaseNode* prev = ref;
  BaseNode* next = ref->next();

  node->_prev = prev;
  node->_next = next;

  node->addFlags(BaseNode::kFlagIsActive);
  if (node->isSection())
    _dirtySectionLinks = true;

  prev->_next = node;
  if (next)
    next->_prev = node;
  else
    _lastNode = node;

  return node;
}

BaseNode* BaseBuilder::addBefore(BaseNode* node, BaseNode* ref) noexcept {
  ASMJIT_ASSERT(node != nullptr);
  ASMJIT_ASSERT(!node->_prev);
  ASMJIT_ASSERT(!node->_next);
  ASMJIT_ASSERT(!node->isActive());
  ASMJIT_ASSERT(ref != nullptr);
  ASMJIT_ASSERT(ref->isActive());

  BaseNode* prev = ref->prev();
  BaseNode* next = ref;

  node->_prev = prev;
  node->_next = next;

  node->addFlags(BaseNode::kFlagIsActive);
  if (node->isSection())
    _dirtySectionLinks = true;

  next->_prev = node;
  if (prev)
    prev->_next = node;
  else
    _firstNode = node;

  return node;
}

BaseNode* BaseBuilder::removeNode(BaseNode* node) noexcept {
  if (!node->isActive())
    return node;

  BaseNode* prev = node->prev();
  BaseNode* next = node->next();

  if (_firstNode == node)
    _firstNode = next;
  else
    prev->_next = next;

  if (_lastNode == node)
    _lastNode  = prev;
  else
    next->_prev = prev;

  node->_prev = nullptr;
  node->_next = nullptr;
  node->clearFlags(BaseNode::kFlagIsActive);
  if (node->isSection())
    _dirtySectionLinks = true;

  if (_cursor == node)
    _cursor = prev;

  return node;
}

void BaseBuilder::removeNodes(BaseNode* first, BaseNode* last) noexcept {
  if (first == last) {
    removeNode(first);
    return;
  }

  if (!first->isActive())
    return;

  BaseNode* prev = first->prev();
  BaseNode* next = last->next();

  if (_firstNode == first)
    _firstNode = next;
  else
    prev->_next = next;

  if (_lastNode == last)
    _lastNode  = prev;
  else
    next->_prev = prev;

  BaseNode* node = first;
  uint32_t didRemoveSection = false;

  for (;;) {
    next = node->next();
    ASMJIT_ASSERT(next != nullptr);

    node->_prev = nullptr;
    node->_next = nullptr;
    node->clearFlags(BaseNode::kFlagIsActive);
    didRemoveSection |= uint32_t(node->isSection());

    if (_cursor == node)
      _cursor = prev;

    if (node == last)
      break;
    node = next;
  }

  if (didRemoveSection)
    _dirtySectionLinks = true;
}

BaseNode* BaseBuilder::setCursor(BaseNode* node) noexcept {
  BaseNode* old = _cursor;
  _cursor = node;
  return old;
}

// ============================================================================
// [asmjit::BaseBuilder - Section]
// ============================================================================

Error BaseBuilder::sectionNodeOf(SectionNode** out, uint32_t sectionId) {
  *out = nullptr;

  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (ASMJIT_UNLIKELY(!_code->isSectionValid(sectionId)))
    return reportError(DebugUtils::errored(kErrorInvalidSection));

  if (sectionId >= _sectionNodes.size()) {
    Error err = _sectionNodes.reserve(&_allocator, sectionId + 1);
    if (ASMJIT_UNLIKELY(err != kErrorOk))
      return reportError(err);
  }

  SectionNode* node = nullptr;
  if (sectionId < _sectionNodes.size())
    node = _sectionNodes[sectionId];

  if (!node) {
    ASMJIT_PROPAGATE(_newNodeT<SectionNode>(&node, sectionId));

    // We have already reserved enough space, this cannot fail now.
    if (sectionId >= _sectionNodes.size())
      _sectionNodes.resize(&_allocator, sectionId + 1);

    _sectionNodes[sectionId] = node;
  }

  *out = node;
  return kErrorOk;
}

Error BaseBuilder::section(Section* section) {
  SectionNode* node;
  ASMJIT_PROPAGATE(sectionNodeOf(&node, section->id()));

  if (!node->isActive()) {
    // Insert the section at the end if it was not part of the code.
    addAfter(node, lastNode());
    _cursor = node;
  }
  else {
    // This is a bit tricky. We cache section links to make sure that
    // switching sections doesn't involve traversal in linked-list unless
    // the position of the section has changed.
    if (hasDirtySectionLinks())
      updateSectionLinks();

    if (node->_nextSection)
      _cursor = node->_nextSection->_prev;
    else
      _cursor = _lastNode;
  }

  return kErrorOk;
}

void BaseBuilder::updateSectionLinks() noexcept {
  if (!_dirtySectionLinks)
    return;

  BaseNode* node_ = _firstNode;
  SectionNode* currentSection = nullptr;

  while (node_) {
    if (node_->isSection()) {
      if (currentSection)
        currentSection->_nextSection = node_->as<SectionNode>();
      currentSection = node_->as<SectionNode>();
    }
    node_ = node_->next();
  }

  if (currentSection)
    currentSection->_nextSection = nullptr;

  _dirtySectionLinks = false;
}

// ============================================================================
// [asmjit::BaseBuilder - Labels]
// ============================================================================

Error BaseBuilder::labelNodeOf(LabelNode** out, uint32_t labelId) {
  *out = nullptr;

  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  uint32_t index = labelId;
  if (ASMJIT_UNLIKELY(index >= _code->labelCount()))
    return DebugUtils::errored(kErrorInvalidLabel);

  if (index >= _labelNodes.size())
    ASMJIT_PROPAGATE(_labelNodes.resize(&_allocator, index + 1));

  LabelNode* node = _labelNodes[index];
  if (!node) {
    ASMJIT_PROPAGATE(_newNodeT<LabelNode>(&node, labelId));
    _labelNodes[index] = node;
  }

  *out = node;
  return kErrorOk;
}

Error BaseBuilder::registerLabelNode(LabelNode* node) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  LabelEntry* le;
  ASMJIT_PROPAGATE(_code->newLabelEntry(&le));
  uint32_t labelId = le->id();

  // We just added one label so it must be true.
  ASMJIT_ASSERT(_labelNodes.size() < labelId + 1);
  ASMJIT_PROPAGATE(_labelNodes.resize(&_allocator, labelId + 1));

  _labelNodes[labelId] = node;
  node->_labelId = labelId;

  return kErrorOk;
}

static Error BaseBuilder_newLabelInternal(BaseBuilder* self, uint32_t labelId) {
  ASMJIT_ASSERT(self->_labelNodes.size() < labelId + 1);

  uint32_t growBy = labelId - self->_labelNodes.size();
  Error err = self->_labelNodes.willGrow(&self->_allocator, growBy);

  if (ASMJIT_UNLIKELY(err))
    return self->reportError(err);

  LabelNode* node;
  ASMJIT_PROPAGATE(self->_newNodeT<LabelNode>(&node, labelId));

  self->_labelNodes.resize(&self->_allocator, labelId + 1);
  self->_labelNodes[labelId] = node;
  node->_labelId = labelId;
  return kErrorOk;
}

Label BaseBuilder::newLabel() {
  uint32_t labelId = Globals::kInvalidId;
  LabelEntry* le;

  if (_code &&
      _code->newLabelEntry(&le) == kErrorOk &&
      BaseBuilder_newLabelInternal(this, le->id()) == kErrorOk) {
    labelId = le->id();
  }

  return Label(labelId);
}

Label BaseBuilder::newNamedLabel(const char* name, size_t nameSize, uint32_t type, uint32_t parentId) {
  uint32_t labelId = Globals::kInvalidId;
  LabelEntry* le;

  if (_code &&
      _code->newNamedLabelEntry(&le, name, nameSize, type, parentId) == kErrorOk &&
      BaseBuilder_newLabelInternal(this, le->id()) == kErrorOk) {
    labelId = le->id();
  }

  return Label(labelId);
}

Error BaseBuilder::bind(const Label& label) {
  LabelNode* node;
  ASMJIT_PROPAGATE(labelNodeOf(&node, label));

  addNode(node);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseBuilder - Passes]
// ============================================================================

ASMJIT_FAVOR_SIZE Pass* BaseBuilder::passByName(const char* name) const noexcept {
  for (Pass* pass : _passes)
    if (strcmp(pass->name(), name) == 0)
      return pass;
  return nullptr;
}

ASMJIT_FAVOR_SIZE Error BaseBuilder::addPass(Pass* pass) noexcept {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (ASMJIT_UNLIKELY(pass == nullptr)) {
    // Since this is directly called by `addPassT()` we treat `null` argument
    // as out-of-memory condition. Otherwise it would be API misuse.
    return DebugUtils::errored(kErrorOutOfMemory);
  }
  else if (ASMJIT_UNLIKELY(pass->_cb)) {
    // Kinda weird, but okay...
    if (pass->_cb == this)
      return kErrorOk;
    return DebugUtils::errored(kErrorInvalidState);
  }

  ASMJIT_PROPAGATE(_passes.append(&_allocator, pass));
  pass->_cb = this;
  return kErrorOk;
}

ASMJIT_FAVOR_SIZE Error BaseBuilder::deletePass(Pass* pass) noexcept {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (ASMJIT_UNLIKELY(pass == nullptr))
    return DebugUtils::errored(kErrorInvalidArgument);

  if (pass->_cb != nullptr) {
    if (pass->_cb != this)
      return DebugUtils::errored(kErrorInvalidState);

    uint32_t index = _passes.indexOf(pass);
    ASMJIT_ASSERT(index != Globals::kNotFound);

    pass->_cb = nullptr;
    _passes.removeAt(index);
  }

  pass->~Pass();
  return kErrorOk;
}

Error BaseBuilder::runPasses() {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (_passes.empty())
    return kErrorOk;

  ErrorHandler* prev = errorHandler();
  PostponedErrorHandler postponed;

  Error err = kErrorOk;
  setErrorHandler(&postponed);

  for (Pass* pass : _passes) {
    _passZone.reset();
    err = pass->run(&_passZone, _logger);
    if (err)
      break;
  }
  _passZone.reset();
  setErrorHandler(prev);

  if (ASMJIT_UNLIKELY(err))
    return reportError(err, !postponed._message.empty() ? postponed._message.data() : nullptr);

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseBuilder - Emit]
// ============================================================================

Error BaseBuilder::_emit(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt) {
  uint32_t opCount = EmitterUtils::opCountFromEmitArgs(o0, o1, o2, opExt);
  uint32_t options = instOptions() | forcedInstOptions();

  if (options & BaseInst::kOptionReserved) {
    if (ASMJIT_UNLIKELY(!_code))
      return DebugUtils::errored(kErrorNotInitialized);

#ifndef ASMJIT_NO_VALIDATION
    // Strict validation.
    if (hasValidationOption(kValidationOptionIntermediate)) {
      Operand_ opArray[Globals::kMaxOpCount];
      EmitterUtils::opArrayFromEmitArgs(opArray, o0, o1, o2, opExt);

      Error err = InstAPI::validate(arch(), BaseInst(instId, options, _extraReg), opArray, opCount);
      if (ASMJIT_UNLIKELY(err)) {
        resetInstOptions();
        resetExtraReg();
        resetInlineComment();
        return reportError(err);
      }
    }
#endif

    // Clear options that should never be part of `InstNode`.
    options &= ~BaseInst::kOptionReserved;
  }

  uint32_t opCapacity = InstNode::capacityOfOpCount(opCount);
  ASMJIT_ASSERT(opCapacity >= InstNode::kBaseOpCapacity);

  InstNode* node = _allocator.allocT<InstNode>(InstNode::nodeSizeOfOpCapacity(opCapacity));
  const char* comment = inlineComment();

  resetInstOptions();
  resetInlineComment();

  if (ASMJIT_UNLIKELY(!node)) {
    resetExtraReg();
    return reportError(DebugUtils::errored(kErrorOutOfMemory));
  }

  node = new(node) InstNode(this, instId, options, opCount, opCapacity);
  node->setExtraReg(extraReg());
  node->setOp(0, o0);
  node->setOp(1, o1);
  node->setOp(2, o2);
  for (uint32_t i = 3; i < opCount; i++)
    node->setOp(i, opExt[i - 3]);
  node->resetOpRange(opCount, opCapacity);

  if (comment)
    node->setInlineComment(static_cast<char*>(_dataZone.dup(comment, strlen(comment), true)));

  addNode(node);
  resetExtraReg();
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseBuilder - Align]
// ============================================================================

Error BaseBuilder::align(uint32_t alignMode, uint32_t alignment) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  AlignNode* node;
  ASMJIT_PROPAGATE(_newAlignNode(&node, alignMode, alignment));

  addNode(node);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseBuilder - Embed]
// ============================================================================

Error BaseBuilder::embed(const void* data, size_t dataSize) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  EmbedDataNode* node;
  ASMJIT_PROPAGATE(_newEmbedDataNode(&node, Type::kIdU8, data, dataSize));

  addNode(node);
  return kErrorOk;
}

Error BaseBuilder::embedDataArray(uint32_t typeId, const void* data, size_t itemCount, size_t itemRepeat) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  EmbedDataNode* node;
  ASMJIT_PROPAGATE(_newEmbedDataNode(&node, typeId, data, itemCount, itemRepeat));

  addNode(node);
  return kErrorOk;
}

Error BaseBuilder::embedConstPool(const Label& label, const ConstPool& pool) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (!isLabelValid(label))
    return reportError(DebugUtils::errored(kErrorInvalidLabel));

  ASMJIT_PROPAGATE(align(kAlignData, uint32_t(pool.alignment())));
  ASMJIT_PROPAGATE(bind(label));

  EmbedDataNode* node;
  ASMJIT_PROPAGATE(_newEmbedDataNode(&node, Type::kIdU8, nullptr, pool.size()));

  pool.fill(node->data());
  addNode(node);
  return kErrorOk;
}

// EmbedLabel / EmbedLabelDelta
// ----------------------------
//
// If dataSize is zero it means that the size is the same as target register
// width, however, if it's provided we really want to validate whether it's
// within the possible range.

static inline bool BaseBuilder_checkDataSize(size_t dataSize) noexcept {
  return !dataSize || (Support::isPowerOf2(dataSize) && dataSize <= 8);
}

Error BaseBuilder::embedLabel(const Label& label, size_t dataSize) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (!BaseBuilder_checkDataSize(dataSize))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  EmbedLabelNode* node;
  ASMJIT_PROPAGATE(_newNodeT<EmbedLabelNode>(&node, label.id(), uint32_t(dataSize)));

  addNode(node);
  return kErrorOk;
}

Error BaseBuilder::embedLabelDelta(const Label& label, const Label& base, size_t dataSize) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  if (!BaseBuilder_checkDataSize(dataSize))
    return reportError(DebugUtils::errored(kErrorInvalidArgument));

  EmbedLabelDeltaNode* node;
  ASMJIT_PROPAGATE(_newNodeT<EmbedLabelDeltaNode>(&node, label.id(), base.id(), uint32_t(dataSize)));

  addNode(node);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseBuilder - Comment]
// ============================================================================

Error BaseBuilder::comment(const char* data, size_t size) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

  CommentNode* node;
  ASMJIT_PROPAGATE(_newCommentNode(&node, data, size));

  addNode(node);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseBuilder - Serialize]
// ============================================================================

Error BaseBuilder::serializeTo(BaseEmitter* dst) {
  Error err = kErrorOk;
  BaseNode* node_ = _firstNode;

  Operand_ opArray[Globals::kMaxOpCount];

  do {
    dst->setInlineComment(node_->inlineComment());

    if (node_->isInst()) {
      InstNode* node = node_->as<InstNode>();

      // NOTE: Inlined to remove one additional call per instruction.
      dst->setInstOptions(node->instOptions());
      dst->setExtraReg(node->extraReg());

      const Operand_* op = node->operands();
      const Operand_* opExt = EmitterUtils::noExt;

      uint32_t opCount = node->opCount();
      if (opCount > 3) {
        uint32_t i = 4;
        opArray[3] = op[3];

        while (i < opCount) {
          opArray[i].copyFrom(op[i]);
          i++;
        }
        while (i < Globals::kMaxOpCount) {
          opArray[i].reset();
          i++;
        }
        opExt = opArray + 3;
      }

      err = dst->_emit(node->id(), op[0], op[1], op[2], opExt);
    }
    else if (node_->isLabel()) {
      if (node_->isConstPool()) {
        ConstPoolNode* node = node_->as<ConstPoolNode>();
        err = dst->embedConstPool(node->label(), node->constPool());
      }
      else {
        LabelNode* node = node_->as<LabelNode>();
        err = dst->bind(node->label());
      }
    }
    else if (node_->isAlign()) {
      AlignNode* node = node_->as<AlignNode>();
      err = dst->align(node->alignMode(), node->alignment());
    }
    else if (node_->isEmbedData()) {
      EmbedDataNode* node = node_->as<EmbedDataNode>();
      err = dst->embedDataArray(node->typeId(), node->data(), node->itemCount(), node->repeatCount());
    }
    else if (node_->isEmbedLabel()) {
      EmbedLabelNode* node = node_->as<EmbedLabelNode>();
      err = dst->embedLabel(node->label(), node->dataSize());
    }
    else if (node_->isEmbedLabelDelta()) {
      EmbedLabelDeltaNode* node = node_->as<EmbedLabelDeltaNode>();
      err = dst->embedLabelDelta(node->label(), node->baseLabel(), node->dataSize());
    }
    else if (node_->isSection()) {
      SectionNode* node = node_->as<SectionNode>();
      err = dst->section(_code->sectionById(node->id()));
    }
    else if (node_->isComment()) {
      CommentNode* node = node_->as<CommentNode>();
      err = dst->comment(node->inlineComment());
    }

    if (err) break;
    node_ = node_->next();
  } while (node_);

  return err;
}

// ============================================================================
// [asmjit::BaseBuilder - Events]
// ============================================================================

Error BaseBuilder::onAttach(CodeHolder* code) noexcept {
  ASMJIT_PROPAGATE(Base::onAttach(code));

  SectionNode* initialSection;
  Error err = sectionNodeOf(&initialSection, 0);

  if (!err)
    err = _passes.willGrow(&_allocator, 8);

  if (ASMJIT_UNLIKELY(err)) {
    onDetach(code);
    return err;
  }

  _cursor = initialSection;
  _firstNode = initialSection;
  _lastNode = initialSection;
  initialSection->setFlags(BaseNode::kFlagIsActive);

  return kErrorOk;
}

Error BaseBuilder::onDetach(CodeHolder* code) noexcept {
  BaseBuilder_deletePasses(this);
  _sectionNodes.reset();
  _labelNodes.reset();

  _allocator.reset(&_codeZone);
  _codeZone.reset();
  _dataZone.reset();
  _passZone.reset();

  _nodeFlags = 0;

  _cursor = nullptr;
  _firstNode = nullptr;
  _lastNode = nullptr;

  return Base::onDetach(code);
}

// ============================================================================
// [asmjit::Pass - Construction / Destruction]
// ============================================================================

Pass::Pass(const char* name) noexcept
  : _name(name) {}
Pass::~Pass() noexcept {}

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_BUILDER
