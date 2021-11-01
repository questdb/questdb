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
#ifndef ASMJIT_NO_COMPILER

#include "../core/formatter.h"
#include "../core/ralocal_p.h"
#include "../core/rapass_p.h"
#include "../core/support.h"
#include "../core/type.h"
#include "../core/zonestack.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::RABlock - Control Flow]
// ============================================================================

Error RABlock::appendSuccessor(RABlock* successor) noexcept {
  RABlock* predecessor = this;

  if (predecessor->_successors.contains(successor))
    return kErrorOk;
  ASMJIT_ASSERT(!successor->_predecessors.contains(predecessor));

  ASMJIT_PROPAGATE(successor->_predecessors.willGrow(allocator()));
  ASMJIT_PROPAGATE(predecessor->_successors.willGrow(allocator()));

  predecessor->_successors.appendUnsafe(successor);
  successor->_predecessors.appendUnsafe(predecessor);

  return kErrorOk;
}

Error RABlock::prependSuccessor(RABlock* successor) noexcept {
  RABlock* predecessor = this;

  if (predecessor->_successors.contains(successor))
    return kErrorOk;
  ASMJIT_ASSERT(!successor->_predecessors.contains(predecessor));

  ASMJIT_PROPAGATE(successor->_predecessors.willGrow(allocator()));
  ASMJIT_PROPAGATE(predecessor->_successors.willGrow(allocator()));

  predecessor->_successors.prependUnsafe(successor);
  successor->_predecessors.prependUnsafe(predecessor);

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - Construction / Destruction]
// ============================================================================

BaseRAPass::BaseRAPass() noexcept : FuncPass("BaseRAPass") {}
BaseRAPass::~BaseRAPass() noexcept {}

// ============================================================================
// [asmjit::BaseRAPass - RunOnFunction]
// ============================================================================

static void RAPass_reset(BaseRAPass* self, FuncDetail* funcDetail) noexcept {
  ZoneAllocator* allocator = self->allocator();

  self->_blocks.reset();
  self->_exits.reset();
  self->_pov.reset();
  self->_workRegs.reset();
  self->_instructionCount = 0;
  self->_createdBlockCount = 0;

  self->_sharedAssignments.reset();
  self->_lastTimestamp = 0;

  self->_archTraits = nullptr;
  self->_physRegIndex.reset();
  self->_physRegCount.reset();
  self->_physRegTotal = 0;

  for (size_t i = 0; i < ASMJIT_ARRAY_SIZE(self->_scratchRegIndexes); i++)
    self->_scratchRegIndexes[i] = BaseReg::kIdBad;

  self->_availableRegs.reset();
  self->_availableRegCount.reset();
  self->_clobberedRegs.reset();

  self->_workRegs.reset();
  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    self->_workRegsOfGroup[group].reset();
    self->_strategy[group].reset();
    self->_globalLiveSpans[group] = nullptr;
  }
  self->_globalMaxLiveCount.reset();
  self->_temporaryMem.reset();

  self->_stackAllocator.reset(allocator);
  self->_argsAssignment.reset(funcDetail);
  self->_numStackArgsToStackSlots = 0;
  self->_maxWorkRegNameSize = 0;
}

static void RAPass_resetVirtRegData(BaseRAPass* self) noexcept {
  // Zero everything so it cannot be used by accident.
  for (RAWorkReg* wReg : self->_workRegs) {
    VirtReg* vReg = wReg->virtReg();
    vReg->_workReg = nullptr;
  }
}

Error BaseRAPass::runOnFunction(Zone* zone, Logger* logger, FuncNode* func) {
  _allocator.reset(zone);

#ifndef ASMJIT_NO_LOGGING
  _logger = logger;
  _debugLogger = nullptr;

  if (logger) {
    _loggerFlags = logger->flags();
    if (_loggerFlags & FormatOptions::kFlagDebugPasses)
      _debugLogger = logger;
  }
#else
  DebugUtils::unused(logger);
#endif

  // Initialize all core structures to use `zone` and `func`.
  BaseNode* end = func->endNode();
  _func = func;
  _stop = end->next();
  _extraBlock = end;

  RAPass_reset(this, &_func->_funcDetail);

  // Initialize architecture-specific members.
  onInit();

  // Perform all allocation steps required.
  Error err = onPerformAllSteps();

  // Must be called regardless of the allocation status.
  onDone();

  // Reset possible connections introduced by the register allocator.
  RAPass_resetVirtRegData(this);

  // Reset all core structures and everything that depends on the passed `Zone`.
  RAPass_reset(this, nullptr);
  _allocator.reset(nullptr);

#ifndef ASMJIT_NO_LOGGING
  _logger = nullptr;
  _debugLogger = nullptr;
  _loggerFlags = 0;
#endif

  _func = nullptr;
  _stop = nullptr;
  _extraBlock = nullptr;

  // Reset `Zone` as nothing should persist between `runOnFunction()` calls.
  zone->reset();

  // We alter the compiler cursor, because it doesn't make sense to reference
  // it after the compilation - some nodes may disappear and the old cursor
  // can go out anyway.
  cc()->_setCursor(cc()->lastNode());

  return err;
}

Error BaseRAPass::onPerformAllSteps() noexcept {
  ASMJIT_PROPAGATE(buildCFG());
  ASMJIT_PROPAGATE(buildViews());
  ASMJIT_PROPAGATE(removeUnreachableBlocks());

  ASMJIT_PROPAGATE(buildDominators());
  ASMJIT_PROPAGATE(buildLiveness());
  ASMJIT_PROPAGATE(assignArgIndexToWorkRegs());

#ifndef ASMJIT_NO_LOGGING
  if (logger() && logger()->hasFlag(FormatOptions::kFlagAnnotations))
    ASMJIT_PROPAGATE(annotateCode());
#endif

  ASMJIT_PROPAGATE(runGlobalAllocator());
  ASMJIT_PROPAGATE(runLocalAllocator());

  ASMJIT_PROPAGATE(updateStackFrame());
  ASMJIT_PROPAGATE(insertPrologEpilog());

  ASMJIT_PROPAGATE(rewrite());

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - CFG - Basic Block Management]
// ============================================================================

RABlock* BaseRAPass::newBlock(BaseNode* initialNode) noexcept {
  RABlock* block = zone()->newT<RABlock>(this);
  if (ASMJIT_UNLIKELY(!block))
    return nullptr;

  block->setFirst(initialNode);
  block->setLast(initialNode);

  _createdBlockCount++;
  return block;
}

RABlock* BaseRAPass::newBlockOrExistingAt(LabelNode* cbLabel, BaseNode** stoppedAt) noexcept {
  if (cbLabel->hasPassData())
    return cbLabel->passData<RABlock>();

  FuncNode* func = this->func();
  BaseNode* node = cbLabel->prev();
  RABlock* block = nullptr;

  // Try to find some label, but terminate the loop on any code. We try hard to
  // coalesce code that contains two consecutive labels or a combination of
  // non-code nodes between 2 or more labels.
  //
  // Possible cases that would share the same basic block:
  //
  //   1. Two or more consecutive labels:
  //     Label1:
  //     Label2:
  //
  //   2. Two or more labels separated by non-code nodes:
  //     Label1:
  //     ; Some comment...
  //     .align 16
  //     Label2:
  size_t nPendingLabels = 0;

  while (node) {
    if (node->type() == BaseNode::kNodeLabel) {
      // Function has a different NodeType, just make sure this was not messed
      // up as we must never associate BasicBlock with a `func` itself.
      ASMJIT_ASSERT(node != func);

      block = node->passData<RABlock>();
      if (block) {
        // Exit node has always a block associated with it. If we went here it
        // means that `cbLabel` passed here is after the end of the function
        // and cannot be merged with the function exit block.
        if (node == func->exitNode())
          block = nullptr;
        break;
      }

      nPendingLabels++;
    }
    else if (node->type() == BaseNode::kNodeAlign) {
      // Align node is fine.
    }
    else {
      break;
    }

    node = node->prev();
  }

  if (stoppedAt)
    *stoppedAt = node;

  if (!block) {
    block = newBlock();
    if (ASMJIT_UNLIKELY(!block))
      return nullptr;
  }

  cbLabel->setPassData<RABlock>(block);
  node = cbLabel;

  while (nPendingLabels) {
    node = node->prev();
    for (;;) {
      if (node->type() == BaseNode::kNodeLabel) {
        node->setPassData<RABlock>(block);
        nPendingLabels--;
        break;
      }

      node = node->prev();
      ASMJIT_ASSERT(node != nullptr);
    }
  }

  if (!block->first()) {
    block->setFirst(node);
    block->setLast(cbLabel);
  }

  return block;
}

Error BaseRAPass::addBlock(RABlock* block) noexcept {
  ASMJIT_PROPAGATE(_blocks.willGrow(allocator()));

  block->_blockId = blockCount();
  _blocks.appendUnsafe(block);
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - CFG - Build]
// ============================================================================

Error BaseRAPass::initSharedAssignments(const ZoneVector<uint32_t>& sharedAssignmentsMap) noexcept {
  if (sharedAssignmentsMap.empty())
    return kErrorOk;

  uint32_t count = 0;
  for (RABlock* block : _blocks) {
    if (block->hasSharedAssignmentId()) {
      uint32_t sharedAssignmentId = sharedAssignmentsMap[block->sharedAssignmentId()];
      block->setSharedAssignmentId(sharedAssignmentId);
      count = Support::max(count, sharedAssignmentId + 1);
    }
  }

  ASMJIT_PROPAGATE(_sharedAssignments.resize(allocator(), count));

  // Aggregate all entry scratch GP regs from blocks of the same assignment to
  // the assignment itself. It will then be used instead of RABlock's own scratch
  // regs mask, as shared assignments have precedence.
  for (RABlock* block : _blocks) {
    if (block->hasJumpTable()) {
      const RABlocks& successors = block->successors();
      if (!successors.empty()) {
        RABlock* firstSuccessor = successors[0];
        // NOTE: Shared assignments connect all possible successors so we only
        // need the first to propagate exit scratch gp registers.
        ASMJIT_ASSERT(firstSuccessor->hasSharedAssignmentId());
        RASharedAssignment& sa = _sharedAssignments[firstSuccessor->sharedAssignmentId()];
        sa.addEntryScratchGpRegs(block->exitScratchGpRegs());
      }
    }
    if (block->hasSharedAssignmentId()) {
      RASharedAssignment& sa = _sharedAssignments[block->sharedAssignmentId()];
      sa.addEntryScratchGpRegs(block->_entryScratchGpRegs);
    }
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - CFG - Views Order]
// ============================================================================

class RABlockVisitItem {
public:
  inline RABlockVisitItem(RABlock* block, uint32_t index) noexcept
    : _block(block),
      _index(index) {}

  inline RABlockVisitItem(const RABlockVisitItem& other) noexcept
    : _block(other._block),
      _index(other._index) {}

  inline RABlockVisitItem& operator=(const RABlockVisitItem& other) noexcept = default;

  inline RABlock* block() const noexcept { return _block; }
  inline uint32_t index() const noexcept { return _index; }

  RABlock* _block;
  uint32_t _index;
};

Error BaseRAPass::buildViews() noexcept {
#ifndef ASMJIT_NO_LOGGING
  Logger* logger = debugLogger();
  ASMJIT_RA_LOG_FORMAT("[RAPass::BuildViews]\n");
#endif

  uint32_t count = blockCount();
  if (ASMJIT_UNLIKELY(!count)) return kErrorOk;

  ASMJIT_PROPAGATE(_pov.reserve(allocator(), count));

  ZoneStack<RABlockVisitItem> stack;
  ASMJIT_PROPAGATE(stack.init(allocator()));

  ZoneBitVector visited;
  ASMJIT_PROPAGATE(visited.resize(allocator(), count));

  RABlock* current = _blocks[0];
  uint32_t i = 0;

  for (;;) {
    for (;;) {
      if (i >= current->successors().size())
        break;

      // Skip if already visited.
      RABlock* child = current->successors()[i++];
      if (visited.bitAt(child->blockId()))
        continue;

      // Mark as visited to prevent visiting the same block multiple times.
      visited.setBit(child->blockId(), true);

      // Add the current block on the stack, we will get back to it later.
      ASMJIT_PROPAGATE(stack.append(RABlockVisitItem(current, i)));
      current = child;
      i = 0;
    }

    current->makeReachable();
    current->_povOrder = _pov.size();
    _pov.appendUnsafe(current);

    if (stack.empty())
      break;

    RABlockVisitItem top = stack.pop();
    current = top.block();
    i = top.index();
  }

  ASMJIT_RA_LOG_COMPLEX({
    StringTmp<1024> sb;
    for (RABlock* block : blocks()) {
      sb.clear();
      if (block->hasSuccessors()) {
        sb.appendFormat("  #%u -> {", block->blockId());
        _dumpBlockIds(sb, block->successors());
        sb.append("}\n");
      }
      else {
        sb.appendFormat("  #%u -> {Exit}\n", block->blockId());
      }
      logger->log(sb);
    }
  });

  visited.release(allocator());
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - CFG - Dominators]
// ============================================================================

static ASMJIT_INLINE RABlock* intersectBlocks(RABlock* b1, RABlock* b2) noexcept {
  while (b1 != b2) {
    while (b2->povOrder() > b1->povOrder()) b1 = b1->iDom();
    while (b1->povOrder() > b2->povOrder()) b2 = b2->iDom();
  }
  return b1;
}

// Based on "A Simple, Fast Dominance Algorithm".
Error BaseRAPass::buildDominators() noexcept {
#ifndef ASMJIT_NO_LOGGING
  Logger* logger = debugLogger();
  ASMJIT_RA_LOG_FORMAT("[RAPass::BuildDominators]\n");
#endif

  if (_blocks.empty())
    return kErrorOk;

  RABlock* entryBlock = this->entryBlock();
  entryBlock->setIDom(entryBlock);

  bool changed = true;
  uint32_t nIters = 0;

  while (changed) {
    nIters++;
    changed = false;

    uint32_t i = _pov.size();
    while (i) {
      RABlock* block = _pov[--i];
      if (block == entryBlock)
        continue;

      RABlock* iDom = nullptr;
      const RABlocks& preds = block->predecessors();

      uint32_t j = preds.size();
      while (j) {
        RABlock* p = preds[--j];
        if (!p->iDom()) continue;
        iDom = !iDom ? p : intersectBlocks(iDom, p);
      }

      if (block->iDom() != iDom) {
        ASMJIT_RA_LOG_FORMAT("  IDom of #%u -> #%u\n", block->blockId(), iDom->blockId());
        block->setIDom(iDom);
        changed = true;
      }
    }
  }

  ASMJIT_RA_LOG_FORMAT("  Done (%u iterations)\n", nIters);
  return kErrorOk;
}

bool BaseRAPass::_strictlyDominates(const RABlock* a, const RABlock* b) const noexcept {
  ASMJIT_ASSERT(a != nullptr); // There must be at least one block if this function is
  ASMJIT_ASSERT(b != nullptr); // called, as both `a` and `b` must be valid blocks.
  ASMJIT_ASSERT(a != b);       // Checked by `dominates()` and `strictlyDominates()`.

  // Nothing strictly dominates the entry block.
  const RABlock* entryBlock = this->entryBlock();
  if (a == entryBlock)
    return false;

  const RABlock* iDom = b->iDom();
  while (iDom != a && iDom != entryBlock)
    iDom = iDom->iDom();

  return iDom != entryBlock;
}

const RABlock* BaseRAPass::_nearestCommonDominator(const RABlock* a, const RABlock* b) const noexcept {
  ASMJIT_ASSERT(a != nullptr); // There must be at least one block if this function is
  ASMJIT_ASSERT(b != nullptr); // called, as both `a` and `b` must be valid blocks.
  ASMJIT_ASSERT(a != b);       // Checked by `dominates()` and `properlyDominates()`.

  if (a == b)
    return a;

  // If `a` strictly dominates `b` then `a` is the nearest common dominator.
  if (_strictlyDominates(a, b))
    return a;

  // If `b` strictly dominates `a` then `b` is the nearest common dominator.
  if (_strictlyDominates(b, a))
    return b;

  const RABlock* entryBlock = this->entryBlock();
  uint64_t timestamp = nextTimestamp();

  // Mark all A's dominators.
  const RABlock* block = a->iDom();
  while (block != entryBlock) {
    block->setTimestamp(timestamp);
    block = block->iDom();
  }

  // Check all B's dominators against marked dominators of A.
  block = b->iDom();
  while (block != entryBlock) {
    if (block->hasTimestamp(timestamp))
      return block;
    block = block->iDom();
  }

  return entryBlock;
}

// ============================================================================
// [asmjit::BaseRAPass - CFG - Utilities]
// ============================================================================

Error BaseRAPass::removeUnreachableBlocks() noexcept {
  uint32_t numAllBlocks = blockCount();
  uint32_t numReachableBlocks = reachableBlockCount();

  // All reachable -> nothing to do.
  if (numAllBlocks == numReachableBlocks)
    return kErrorOk;

#ifndef ASMJIT_NO_LOGGING
  Logger* logger = debugLogger();
  ASMJIT_RA_LOG_FORMAT("[RAPass::RemoveUnreachableBlocks (%u of %u unreachable)]\n", numAllBlocks - numReachableBlocks, numAllBlocks);
#endif

  for (uint32_t i = 0; i < numAllBlocks; i++) {
    RABlock* block = _blocks[i];
    if (block->isReachable())
      continue;

    ASMJIT_RA_LOG_FORMAT("  Removing block {%u}\n", i);
    BaseNode* first = block->first();
    BaseNode* last = block->last();

    BaseNode* beforeFirst = first->prev();
    BaseNode* afterLast = last->next();

    BaseNode* node = first;
    while (node != afterLast) {
      BaseNode* next = node->next();

      if (node->isCode() || node->isRemovable())
        cc()->removeNode(node);
      node = next;
    }

    if (beforeFirst->next() == afterLast) {
      block->setFirst(nullptr);
      block->setLast(nullptr);
    }
    else {
      block->setFirst(beforeFirst->next());
      block->setLast(afterLast->prev());
    }
  }

  return kErrorOk;
}

BaseNode* BaseRAPass::findSuccessorStartingAt(BaseNode* node) noexcept {
  while (node && (node->isInformative() || node->hasNoEffect()))
    node = node->next();
  return node;
}

bool BaseRAPass::isNextTo(BaseNode* node, BaseNode* target) noexcept {
  for (;;) {
    node = node->next();
    if (node == target)
      return true;

    if (!node)
      return false;

    if (node->isCode() || node->isData())
      return false;
  }
}

// ============================================================================
// [asmjit::BaseRAPass - ?]
// ============================================================================

Error BaseRAPass::_asWorkReg(VirtReg* vReg, RAWorkReg** out) noexcept {
  // Checked by `asWorkReg()` - must be true.
  ASMJIT_ASSERT(vReg->_workReg == nullptr);

  uint32_t group = vReg->group();
  ASMJIT_ASSERT(group < BaseReg::kGroupVirt);

  RAWorkRegs& wRegs = workRegs();
  RAWorkRegs& wRegsByGroup = workRegs(group);

  ASMJIT_PROPAGATE(wRegs.willGrow(allocator()));
  ASMJIT_PROPAGATE(wRegsByGroup.willGrow(allocator()));

  RAWorkReg* wReg = zone()->newT<RAWorkReg>(vReg, wRegs.size());
  if (ASMJIT_UNLIKELY(!wReg))
    return DebugUtils::errored(kErrorOutOfMemory);

  vReg->setWorkReg(wReg);
  if (!vReg->isStack())
    wReg->setRegByteMask(Support::lsbMask<uint64_t>(vReg->virtSize()));
  wRegs.appendUnsafe(wReg);
  wRegsByGroup.appendUnsafe(wReg);

  // Only used by RA logging.
  _maxWorkRegNameSize = Support::max(_maxWorkRegNameSize, vReg->nameSize());

  *out = wReg;
  return kErrorOk;
}

RAAssignment::WorkToPhysMap* BaseRAPass::newWorkToPhysMap() noexcept {
  uint32_t count = workRegCount();
  size_t size = WorkToPhysMap::sizeOf(count);

  // If no registers are used it could be zero, in that case return a dummy
  // map instead of NULL.
  if (ASMJIT_UNLIKELY(!size)) {
    static const RAAssignment::WorkToPhysMap nullMap = {{ 0 }};
    return const_cast<RAAssignment::WorkToPhysMap*>(&nullMap);
  }

  WorkToPhysMap* map = zone()->allocT<WorkToPhysMap>(size);
  if (ASMJIT_UNLIKELY(!map))
    return nullptr;

  map->reset(count);
  return map;
}

RAAssignment::PhysToWorkMap* BaseRAPass::newPhysToWorkMap() noexcept {
  uint32_t count = physRegTotal();
  size_t size = PhysToWorkMap::sizeOf(count);

  PhysToWorkMap* map = zone()->allocT<PhysToWorkMap>(size);
  if (ASMJIT_UNLIKELY(!map))
    return nullptr;

  map->reset(count);
  return map;
}

// ============================================================================
// [asmjit::BaseRAPass - Registers - Liveness Analysis and Statistics]
// ============================================================================

namespace LiveOps {
  typedef ZoneBitVector::BitWord BitWord;

  struct In {
    static ASMJIT_INLINE BitWord op(BitWord dst, BitWord out, BitWord gen, BitWord kill) noexcept {
      DebugUtils::unused(dst);
      return (out | gen) & ~kill;
    }
  };

  template<typename Operator>
  static ASMJIT_INLINE bool op(BitWord* dst, const BitWord* a, uint32_t n) noexcept {
    BitWord changed = 0;

    for (uint32_t i = 0; i < n; i++) {
      BitWord before = dst[i];
      BitWord after = Operator::op(before, a[i]);

      dst[i] = after;
      changed |= (before ^ after);
    }

    return changed != 0;
  }

  template<typename Operator>
  static ASMJIT_INLINE bool op(BitWord* dst, const BitWord* a, const BitWord* b, uint32_t n) noexcept {
    BitWord changed = 0;

    for (uint32_t i = 0; i < n; i++) {
      BitWord before = dst[i];
      BitWord after = Operator::op(before, a[i], b[i]);

      dst[i] = after;
      changed |= (before ^ after);
    }

    return changed != 0;
  }

  template<typename Operator>
  static ASMJIT_INLINE bool op(BitWord* dst, const BitWord* a, const BitWord* b, const BitWord* c, uint32_t n) noexcept {
    BitWord changed = 0;

    for (uint32_t i = 0; i < n; i++) {
      BitWord before = dst[i];
      BitWord after = Operator::op(before, a[i], b[i], c[i]);

      dst[i] = after;
      changed |= (before ^ after);
    }

    return changed != 0;
  }

  static ASMJIT_INLINE bool recalcInOut(RABlock* block, uint32_t numBitWords, bool initial = false) noexcept {
    bool changed = initial;

    const RABlocks& successors = block->successors();
    uint32_t numSuccessors = successors.size();

    // Calculate `OUT` based on `IN` of all successors.
    for (uint32_t i = 0; i < numSuccessors; i++)
      changed |= op<Support::Or>(block->liveOut().data(), successors[i]->liveIn().data(), numBitWords);

    // Calculate `IN` based on `OUT`, `GEN`, and `KILL` bits.
    if (changed)
      changed = op<In>(block->liveIn().data(), block->liveOut().data(), block->gen().data(), block->kill().data(), numBitWords);

    return changed;
  }
}

ASMJIT_FAVOR_SPEED Error BaseRAPass::buildLiveness() noexcept {
#ifndef ASMJIT_NO_LOGGING
  Logger* logger = debugLogger();
  StringTmp<512> sb;
#endif

  ASMJIT_RA_LOG_FORMAT("[RAPass::BuildLiveness]\n");

  uint32_t i;

  uint32_t numAllBlocks = blockCount();
  uint32_t numReachableBlocks = reachableBlockCount();

  uint32_t numVisits = numReachableBlocks;
  uint32_t numWorkRegs = workRegCount();
  uint32_t numBitWords = ZoneBitVector::_wordsPerBits(numWorkRegs);

  if (!numWorkRegs) {
    ASMJIT_RA_LOG_FORMAT("  Done (no virtual registers)\n");
    return kErrorOk;
  }

  ZoneVector<uint32_t> nUsesPerWorkReg; // Number of USEs of each RAWorkReg.
  ZoneVector<uint32_t> nOutsPerWorkReg; // Number of OUTs of each RAWorkReg.
  ZoneVector<uint32_t> nInstsPerBlock;  // Number of instructions of each RABlock.

  ASMJIT_PROPAGATE(nUsesPerWorkReg.resize(allocator(), numWorkRegs));
  ASMJIT_PROPAGATE(nOutsPerWorkReg.resize(allocator(), numWorkRegs));
  ASMJIT_PROPAGATE(nInstsPerBlock.resize(allocator(), numAllBlocks));

  // --------------------------------------------------------------------------
  // Calculate GEN/KILL of each block.
  // --------------------------------------------------------------------------

  for (i = 0; i < numReachableBlocks; i++) {
    RABlock* block = _pov[i];
    ASMJIT_PROPAGATE(block->resizeLiveBits(numWorkRegs));

    BaseNode* node = block->last();
    BaseNode* stop = block->first();

    uint32_t nInsts = 0;
    for (;;) {
      if (node->isInst()) {
        InstNode* inst = node->as<InstNode>();
        RAInst* raInst = inst->passData<RAInst>();
        ASMJIT_ASSERT(raInst != nullptr);

        RATiedReg* tiedRegs = raInst->tiedRegs();
        uint32_t count = raInst->tiedCount();

        for (uint32_t j = 0; j < count; j++) {
          RATiedReg* tiedReg = &tiedRegs[j];
          uint32_t workId = tiedReg->workId();

          // Update `nUses` and `nOuts`.
          nUsesPerWorkReg[workId] += 1u;
          nOutsPerWorkReg[workId] += uint32_t(tiedReg->isWrite());

          // Mark as:
          //   KILL - if this VirtReg is killed afterwards.
          //   LAST - if this VirtReg is last in this basic block.
          if (block->kill().bitAt(workId))
            tiedReg->addFlags(RATiedReg::kKill);
          else if (!block->gen().bitAt(workId))
            tiedReg->addFlags(RATiedReg::kLast);

          if (tiedReg->isWriteOnly()) {
            // KILL.
            block->kill().setBit(workId, true);
          }
          else {
            // GEN.
            block->kill().setBit(workId, false);
            block->gen().setBit(workId, true);
          }
        }

        nInsts++;
      }

      if (node == stop)
        break;

      node = node->prev();
      ASMJIT_ASSERT(node != nullptr);
    }

    nInstsPerBlock[block->blockId()] = nInsts;
  }

  // --------------------------------------------------------------------------
  // Calculate IN/OUT of each block.
  // --------------------------------------------------------------------------

  {
    ZoneStack<RABlock*> workList;
    ZoneBitVector workBits;

    ASMJIT_PROPAGATE(workList.init(allocator()));
    ASMJIT_PROPAGATE(workBits.resize(allocator(), blockCount(), true));

    for (i = 0; i < numReachableBlocks; i++) {
      RABlock* block = _pov[i];
      LiveOps::recalcInOut(block, numBitWords, true);
      ASMJIT_PROPAGATE(workList.append(block));
    }

    while (!workList.empty()) {
      RABlock* block = workList.popFirst();
      uint32_t blockId = block->blockId();

      workBits.setBit(blockId, false);
      if (LiveOps::recalcInOut(block, numBitWords)) {
        const RABlocks& predecessors = block->predecessors();
        uint32_t numPredecessors = predecessors.size();

        for (uint32_t j = 0; j < numPredecessors; j++) {
          RABlock* pred = predecessors[j];
          if (!workBits.bitAt(pred->blockId())) {
            workBits.setBit(pred->blockId(), true);
            ASMJIT_PROPAGATE(workList.append(pred));
          }
        }
      }
      numVisits++;
    }

    workList.reset();
    workBits.release(allocator());
  }

  ASMJIT_RA_LOG_COMPLEX({
    logger->logf("  LiveIn/Out Done (%u visits)\n", numVisits);
    for (i = 0; i < numAllBlocks; i++) {
      RABlock* block = _blocks[i];

      ASMJIT_PROPAGATE(sb.assignFormat("  {#%u}\n", block->blockId()));
      ASMJIT_PROPAGATE(_dumpBlockLiveness(sb, block));

      logger->log(sb);
    }
  });

  // --------------------------------------------------------------------------
  // Reserve the space in each `RAWorkReg` for references.
  // --------------------------------------------------------------------------

  for (i = 0; i < numWorkRegs; i++) {
    RAWorkReg* workReg = workRegById(i);
    ASMJIT_PROPAGATE(workReg->_refs.reserve(allocator(), nUsesPerWorkReg[i]));
    ASMJIT_PROPAGATE(workReg->_writes.reserve(allocator(), nOutsPerWorkReg[i]));
  }

  // --------------------------------------------------------------------------
  // Assign block and instruction positions, build LiveCount and LiveSpans.
  // --------------------------------------------------------------------------

  uint32_t position = 2;
  for (i = 0; i < numAllBlocks; i++) {
    RABlock* block = _blocks[i];
    if (!block->isReachable())
      continue;

    BaseNode* node = block->first();
    BaseNode* stop = block->last();

    uint32_t endPosition = position + nInstsPerBlock[i] * 2;
    block->setFirstPosition(position);
    block->setEndPosition(endPosition);

    RALiveCount curLiveCount;
    RALiveCount maxLiveCount;

    // Process LIVE-IN.
    ZoneBitVector::ForEachBitSet it(block->liveIn());
    while (it.hasNext()) {
      RAWorkReg* workReg = _workRegs[uint32_t(it.next())];
      curLiveCount[workReg->group()]++;
      ASMJIT_PROPAGATE(workReg->liveSpans().openAt(allocator(), position, endPosition));
    }

    for (;;) {
      if (node->isInst()) {
        InstNode* inst = node->as<InstNode>();
        RAInst* raInst = inst->passData<RAInst>();
        ASMJIT_ASSERT(raInst != nullptr);

        RATiedReg* tiedRegs = raInst->tiedRegs();
        uint32_t count = raInst->tiedCount();

        inst->setPosition(position);
        raInst->_liveCount = curLiveCount;

        for (uint32_t j = 0; j < count; j++) {
          RATiedReg* tiedReg = &tiedRegs[j];
          uint32_t workId = tiedReg->workId();

          // Create refs and writes.
          RAWorkReg* workReg = workRegById(workId);
          workReg->_refs.appendUnsafe(node);
          if (tiedReg->isWrite())
            workReg->_writes.appendUnsafe(node);

          // We couldn't calculate this in previous steps, but since we know all LIVE-OUT
          // at this point it becomes trivial. If this is the last instruction that uses
          // this `workReg` and it's not LIVE-OUT then it is KILLed here.
          if (tiedReg->isLast() && !block->liveOut().bitAt(workId))
            tiedReg->addFlags(RATiedReg::kKill);

          LiveRegSpans& liveSpans = workReg->liveSpans();
          bool wasOpen;
          ASMJIT_PROPAGATE(liveSpans.openAt(allocator(), position + !tiedReg->isRead(), endPosition, wasOpen));

          uint32_t group = workReg->group();
          if (!wasOpen) {
            curLiveCount[group]++;
            raInst->_liveCount[group]++;
          }

          if (tiedReg->isKill()) {
            liveSpans.closeAt(position + !tiedReg->isRead() + 1);
            curLiveCount[group]--;
          }

          // Update `RAWorkReg::hintRegId`.
          if (tiedReg->hasUseId() && !workReg->hasHintRegId()) {
            uint32_t useId = tiedReg->useId();
            if (!(raInst->_clobberedRegs[group] & Support::bitMask(useId)))
              workReg->setHintRegId(useId);
          }

          // Update `RAWorkReg::clobberedSurvivalMask`.
          if (raInst->_clobberedRegs[group] && !tiedReg->isOutOrKill())
            workReg->addClobberSurvivalMask(raInst->_clobberedRegs[group]);
        }

        position += 2;
        maxLiveCount.op<Support::Max>(raInst->_liveCount);
      }

      if (node == stop)
        break;

      node = node->next();
      ASMJIT_ASSERT(node != nullptr);
    }

    block->_maxLiveCount = maxLiveCount;
    _globalMaxLiveCount.op<Support::Max>(maxLiveCount);
    ASMJIT_ASSERT(position == block->endPosition());
  }

  // --------------------------------------------------------------------------
  // Calculate WorkReg statistics.
  // --------------------------------------------------------------------------

  for (i = 0; i < numWorkRegs; i++) {
    RAWorkReg* workReg = _workRegs[i];

    LiveRegSpans& spans = workReg->liveSpans();
    uint32_t width = spans.width();
    float freq = width ? float(double(workReg->_refs.size()) / double(width)) : float(0);

    RALiveStats& stats = workReg->liveStats();
    stats._width = width;
    stats._freq = freq;
    stats._priority = freq + float(int(workReg->virtReg()->weight())) * 0.01f;
  }

  ASMJIT_RA_LOG_COMPLEX({
    sb.clear();
    _dumpLiveSpans(sb);
    logger->log(sb);
  });

  nUsesPerWorkReg.release(allocator());
  nOutsPerWorkReg.release(allocator());
  nInstsPerBlock.release(allocator());

  return kErrorOk;
}

Error BaseRAPass::assignArgIndexToWorkRegs() noexcept {
  ZoneBitVector& liveIn = entryBlock()->liveIn();
  uint32_t argCount = func()->argCount();

  for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
    for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
      // Unassigned argument.
      VirtReg* virtReg = func()->argPack(argIndex)[valueIndex];
      if (!virtReg)
        continue;

      // Unreferenced argument.
      RAWorkReg* workReg = virtReg->workReg();
      if (!workReg)
        continue;

      // Overwritten argument.
      uint32_t workId = workReg->workId();
      if (!liveIn.bitAt(workId))
        continue;

      workReg->setArgIndex(argIndex, valueIndex);
      const FuncValue& arg = func()->detail().arg(argIndex, valueIndex);

      if (arg.isReg() && _archTraits->regTypeToGroup(arg.regType()) == workReg->group()) {
        workReg->setHintRegId(arg.regId());
      }
    }
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - Allocation - Global]
// ============================================================================

#ifndef ASMJIT_NO_LOGGING
static void RAPass_dumpSpans(String& sb, uint32_t index, const LiveRegSpans& liveSpans) noexcept {
  sb.appendFormat("  %02u: ", index);

  for (uint32_t i = 0; i < liveSpans.size(); i++) {
    const LiveRegSpan& liveSpan = liveSpans[i];
    if (i) sb.append(", ");
    sb.appendFormat("[%u:%u@%u]", liveSpan.a, liveSpan.b, liveSpan.id);
  }

  sb.append('\n');
}
#endif

Error BaseRAPass::runGlobalAllocator() noexcept {
  ASMJIT_PROPAGATE(initGlobalLiveSpans());

  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    ASMJIT_PROPAGATE(binPack(group));
  }

  return kErrorOk;
}

ASMJIT_FAVOR_SPEED Error BaseRAPass::initGlobalLiveSpans() noexcept {
  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    size_t physCount = _physRegCount[group];
    LiveRegSpans* liveSpans = nullptr;

    if (physCount) {
      liveSpans = allocator()->allocT<LiveRegSpans>(physCount * sizeof(LiveRegSpans));
      if (ASMJIT_UNLIKELY(!liveSpans))
        return DebugUtils::errored(kErrorOutOfMemory);

      for (size_t physId = 0; physId < physCount; physId++)
        new(&liveSpans[physId]) LiveRegSpans();
    }

    _globalLiveSpans[group] = liveSpans;
  }

  return kErrorOk;
}

ASMJIT_FAVOR_SPEED Error BaseRAPass::binPack(uint32_t group) noexcept {
  if (workRegCount(group) == 0)
    return kErrorOk;

#ifndef ASMJIT_NO_LOGGING
  Logger* logger = debugLogger();
  StringTmp<512> sb;

  ASMJIT_RA_LOG_FORMAT("[RAPass::BinPack] Available=%u (0x%08X) Count=%u\n",
    Support::popcnt(_availableRegs[group]),
    _availableRegs[group],
    workRegCount(group));
#endif

  uint32_t i;
  uint32_t physCount = _physRegCount[group];

  RAWorkRegs workRegs;
  LiveRegSpans tmpSpans;

  ASMJIT_PROPAGATE(workRegs.concat(allocator(), this->workRegs(group)));
  workRegs.sort([](const RAWorkReg* a, const RAWorkReg* b) noexcept {
    return b->liveStats().priority() - a->liveStats().priority();
  });

  uint32_t numWorkRegs = workRegs.size();
  uint32_t availableRegs = _availableRegs[group];

  // First try to pack everything that provides register-id hint as these are
  // most likely function arguments and fixed (precolored) virtual registers.
  if (!workRegs.empty()) {
    uint32_t dstIndex = 0;

    for (i = 0; i < numWorkRegs; i++) {
      RAWorkReg* workReg = workRegs[i];
      if (workReg->hasHintRegId()) {
        uint32_t physId = workReg->hintRegId();
        if (availableRegs & Support::bitMask(physId)) {
          LiveRegSpans& live = _globalLiveSpans[group][physId];
          Error err = tmpSpans.nonOverlappingUnionOf(allocator(), live, workReg->liveSpans(), LiveRegData(workReg->virtId()));

          if (err == kErrorOk) {
            workReg->setHomeRegId(physId);
            live.swap(tmpSpans);
            continue;
          }

          if (ASMJIT_UNLIKELY(err != 0xFFFFFFFFu))
            return err;
        }
      }

      workRegs[dstIndex++] = workReg;
    }

    workRegs._setSize(dstIndex);
    numWorkRegs = dstIndex;
  }

  // Try to pack the rest.
  if (!workRegs.empty()) {
    uint32_t dstIndex = 0;

    for (i = 0; i < numWorkRegs; i++) {
      RAWorkReg* workReg = workRegs[i];
      uint32_t physRegs = availableRegs;

      while (physRegs) {
        uint32_t physId = Support::ctz(physRegs);
        if (workReg->clobberSurvivalMask()) {
          uint32_t preferredMask = physRegs & workReg->clobberSurvivalMask();
          if (preferredMask)
            physId = Support::ctz(preferredMask);
        }

        LiveRegSpans& live = _globalLiveSpans[group][physId];
        Error err = tmpSpans.nonOverlappingUnionOf(allocator(), live, workReg->liveSpans(), LiveRegData(workReg->virtId()));

        if (err == kErrorOk) {
          workReg->setHomeRegId(physId);
          live.swap(tmpSpans);
          break;
        }

        if (ASMJIT_UNLIKELY(err != 0xFFFFFFFFu))
          return err;

        physRegs ^= Support::bitMask(physId);
      }

      // Keep it in `workRegs` if it was not allocated.
      if (!physRegs)
        workRegs[dstIndex++] = workReg;
    }

    workRegs._setSize(dstIndex);
    numWorkRegs = dstIndex;
  }

  ASMJIT_RA_LOG_COMPLEX({
    for (uint32_t physId = 0; physId < physCount; physId++) {
      LiveRegSpans& live = _globalLiveSpans[group][physId];
      if (live.empty())
        continue;

      sb.clear();
      RAPass_dumpSpans(sb, physId, live);
      logger->log(sb);
    }
  });

  // Maybe unused if logging is disabled.
  DebugUtils::unused(physCount);

  if (workRegs.empty()) {
    ASMJIT_RA_LOG_FORMAT("  Completed.\n");
  }
  else {
    _strategy[group].setType(RAStrategy::kStrategyComplex);
    for (RAWorkReg* workReg : workRegs)
      workReg->markStackPreferred();

    ASMJIT_RA_LOG_COMPLEX({
      uint32_t count = workRegs.size();
      sb.clear();
      sb.appendFormat("  Unassigned (%u): ", count);
      for (i = 0; i < numWorkRegs; i++) {
        RAWorkReg* workReg = workRegs[i];
        if (i) sb.append(", ");
        sb.append(workReg->name());
      }
      sb.append('\n');
      logger->log(sb);
    });
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - Allocation - Local]
// ============================================================================

Error BaseRAPass::runLocalAllocator() noexcept {
  RALocalAllocator lra(this);
  ASMJIT_PROPAGATE(lra.init());

  if (!blockCount())
    return kErrorOk;

  // The allocation is done when this reaches zero.
  uint32_t blocksRemaining = reachableBlockCount();

  // Current block.
  uint32_t blockId = 0;
  RABlock* block = _blocks[blockId];

  // The first block (entry) must always be reachable.
  ASMJIT_ASSERT(block->isReachable());

  // Assign function arguments for the initial block. The `lra` is valid now.
  lra.makeInitialAssignment();
  ASMJIT_PROPAGATE(setBlockEntryAssignment(block, block, lra._curAssignment));

  // The loop starts from the first block and iterates blocks in order, however,
  // the algorithm also allows to jump to any other block when finished if it's
  // a jump target. In-order iteration just makes sure that all blocks are visited.
  for (;;) {
    BaseNode* first = block->first();
    BaseNode* last = block->last();
    BaseNode* terminator = block->hasTerminator() ? last : nullptr;

    BaseNode* beforeFirst = first->prev();
    BaseNode* afterLast = last->next();

    bool unconditionalJump = false;
    RABlock* consecutive = nullptr;

    if (block->hasSuccessors())
      consecutive = block->successors()[0];

    lra.setBlock(block);
    block->makeAllocated();

    BaseNode* node = first;
    while (node != afterLast) {
      BaseNode* next = node->next();
      if (node->isInst()) {
        InstNode* inst = node->as<InstNode>();

        if (ASMJIT_UNLIKELY(inst == terminator)) {
          const RABlocks& successors = block->successors();
          if (block->hasConsecutive()) {
            ASMJIT_PROPAGATE(lra.allocBranch(inst, successors.last(), successors.first()));

            node = next;
            continue;
          }
          else if (successors.size() > 1) {
            RABlock* cont = block->hasConsecutive() ? successors.first() : nullptr;
            ASMJIT_PROPAGATE(lra.allocJumpTable(inst, successors, cont));

            node = next;
            continue;
          }
          else {
            // Otherwise this is an unconditional jump, special handling isn't required.
            unconditionalJump = true;
          }
        }

        ASMJIT_PROPAGATE(lra.allocInst(inst));
        if (inst->type() == BaseNode::kNodeInvoke)
          ASMJIT_PROPAGATE(emitPreCall(inst->as<InvokeNode>()));
        else
          ASMJIT_PROPAGATE(lra.spillAfterAllocation(inst));
      }
      node = next;
    }

    if (consecutive) {
      BaseNode* prev = afterLast ? afterLast->prev() : cc()->lastNode();
      cc()->_setCursor(unconditionalJump ? prev->prev() : prev);

      if (consecutive->hasEntryAssignment()) {
        ASMJIT_PROPAGATE(
          lra.switchToAssignment(
            consecutive->entryPhysToWorkMap(),
            consecutive->entryWorkToPhysMap(),
            consecutive->liveIn(),
            consecutive->isAllocated(),
            false));
      }
      else {
        ASMJIT_PROPAGATE(lra.spillRegsBeforeEntry(consecutive));
        ASMJIT_PROPAGATE(setBlockEntryAssignment(consecutive, block, lra._curAssignment));
        lra._curAssignment.copyFrom(consecutive->entryPhysToWorkMap(), consecutive->entryWorkToPhysMap());
      }
    }

    // Important as the local allocator can insert instructions before
    // and after any instruction within the basic block.
    block->setFirst(beforeFirst->next());
    block->setLast(afterLast ? afterLast->prev() : cc()->lastNode());

    if (--blocksRemaining == 0)
      break;

    // Switch to the next consecutive block, if any.
    if (consecutive) {
      block = consecutive;
      if (!block->isAllocated())
        continue;
    }

    // Get the next block.
    for (;;) {
      if (++blockId >= blockCount())
        blockId = 0;

      block = _blocks[blockId];
      if (!block->isReachable() || block->isAllocated() || !block->hasEntryAssignment())
        continue;

      break;
    }

    // If we switched to some block we have to update the local allocator.
    lra.replaceAssignment(block->entryPhysToWorkMap(), block->entryWorkToPhysMap());
  }

  _clobberedRegs.op<Support::Or>(lra._clobberedRegs);
  return kErrorOk;
}

Error BaseRAPass::setBlockEntryAssignment(RABlock* block, const RABlock* fromBlock, const RAAssignment& fromAssignment) noexcept {
  if (block->hasSharedAssignmentId()) {
    uint32_t sharedAssignmentId = block->sharedAssignmentId();

    // Shouldn't happen. Entry assignment of a block that has a shared-state
    // will assign to all blocks with the same sharedAssignmentId. It's a bug if
    // the shared state has been already assigned.
    if (!_sharedAssignments[sharedAssignmentId].empty())
      return DebugUtils::errored(kErrorInvalidState);

    return setSharedAssignment(sharedAssignmentId, fromAssignment);
  }

  PhysToWorkMap* physToWorkMap = clonePhysToWorkMap(fromAssignment.physToWorkMap());
  WorkToPhysMap* workToPhysMap = cloneWorkToPhysMap(fromAssignment.workToPhysMap());

  if (ASMJIT_UNLIKELY(!physToWorkMap || !workToPhysMap))
    return DebugUtils::errored(kErrorOutOfMemory);

  block->setEntryAssignment(physToWorkMap, workToPhysMap);

  // True if this is the first (entry) block, nothing to do in this case.
  if (block == fromBlock) {
    // Entry block should never have a shared state.
    if (block->hasSharedAssignmentId())
      return DebugUtils::errored(kErrorInvalidState);

    return kErrorOk;
  }

  RAAssignment as;
  as.initLayout(_physRegCount, workRegs());
  as.initMaps(physToWorkMap, workToPhysMap);

  const ZoneBitVector& liveOut = fromBlock->liveOut();
  const ZoneBitVector& liveIn = block->liveIn();

  // It's possible that `fromBlock` has LIVE-OUT regs that `block` doesn't
  // have in LIVE-IN, these have to be unassigned.
  {
    ZoneBitVector::ForEachBitOp<Support::AndNot> it(liveOut, liveIn);
    while (it.hasNext()) {
      uint32_t workId = uint32_t(it.next());
      RAWorkReg* workReg = workRegById(workId);

      uint32_t group = workReg->group();
      uint32_t physId = as.workToPhysId(group, workId);

      if (physId != RAAssignment::kPhysNone)
        as.unassign(group, workId, physId);
    }
  }

  return blockEntryAssigned(as);
}

Error BaseRAPass::setSharedAssignment(uint32_t sharedAssignmentId, const RAAssignment& fromAssignment) noexcept {
  ASMJIT_ASSERT(_sharedAssignments[sharedAssignmentId].empty());

  PhysToWorkMap* physToWorkMap = clonePhysToWorkMap(fromAssignment.physToWorkMap());
  WorkToPhysMap* workToPhysMap = cloneWorkToPhysMap(fromAssignment.workToPhysMap());

  if (ASMJIT_UNLIKELY(!physToWorkMap || !workToPhysMap))
    return DebugUtils::errored(kErrorOutOfMemory);

  _sharedAssignments[sharedAssignmentId].assignMaps(physToWorkMap, workToPhysMap);
  ZoneBitVector& sharedLiveIn = _sharedAssignments[sharedAssignmentId]._liveIn;
  ASMJIT_PROPAGATE(sharedLiveIn.resize(allocator(), workRegCount()));

  RAAssignment as;
  as.initLayout(_physRegCount, workRegs());

  uint32_t sharedAssigned[BaseReg::kGroupVirt] {};

  for (RABlock* block : blocks()) {
    if (block->sharedAssignmentId() == sharedAssignmentId) {
      ASMJIT_ASSERT(!block->hasEntryAssignment());

      PhysToWorkMap* entryPhysToWorkMap = clonePhysToWorkMap(fromAssignment.physToWorkMap());
      WorkToPhysMap* entryWorkToPhysMap = cloneWorkToPhysMap(fromAssignment.workToPhysMap());

      if (ASMJIT_UNLIKELY(!entryPhysToWorkMap || !entryWorkToPhysMap))
        return DebugUtils::errored(kErrorOutOfMemory);

      block->setEntryAssignment(entryPhysToWorkMap, entryWorkToPhysMap);
      as.initMaps(entryPhysToWorkMap, entryWorkToPhysMap);

      const ZoneBitVector& liveIn = block->liveIn();
      sharedLiveIn.or_(liveIn);

      for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
        sharedAssigned[group] |= entryPhysToWorkMap->assigned[group];
        Support::BitWordIterator<uint32_t> it(entryPhysToWorkMap->assigned[group]);

        while (it.hasNext()) {
          uint32_t physId = it.next();
          uint32_t workId = as.physToWorkId(group, physId);

          if (!liveIn.bitAt(workId))
            as.unassign(group, workId, physId);
        }
      }
    }
  }

  {
    as.initMaps(physToWorkMap, workToPhysMap);

    for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
      Support::BitWordIterator<uint32_t> it(_availableRegs[group] & ~sharedAssigned[group]);

      while (it.hasNext()) {
        uint32_t physId = it.next();
        if (as.isPhysAssigned(group, physId)) {
          uint32_t workId = as.physToWorkId(group, physId);
          as.unassign(group, workId, physId);
        }
      }
    }
  }

  return blockEntryAssigned(as);
}

Error BaseRAPass::blockEntryAssigned(const RAAssignment& as) noexcept {
  // Complex allocation strategy requires to record register assignments upon
  // block entry (or per shared state).
  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    if (!_strategy[group].isComplex())
      continue;

    Support::BitWordIterator<uint32_t> it(as.assigned(group));
    while (it.hasNext()) {
      uint32_t physId = it.next();
      uint32_t workId = as.physToWorkId(group, physId);

      RAWorkReg* workReg = workRegById(workId);
      workReg->addAllocatedMask(Support::bitMask(physId));
    }
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - Allocation - Utilities]
// ============================================================================

Error BaseRAPass::useTemporaryMem(BaseMem& out, uint32_t size, uint32_t alignment) noexcept {
  ASMJIT_ASSERT(alignment <= 64);

  if (_temporaryMem.isNone()) {
    ASMJIT_PROPAGATE(cc()->_newStack(&_temporaryMem.as<BaseMem>(), size, alignment));
  }
  else {
    ASMJIT_ASSERT(_temporaryMem.as<BaseMem>().isRegHome());

    uint32_t virtId = _temporaryMem.as<BaseMem>().baseId();
    VirtReg* virtReg = cc()->virtRegById(virtId);

    cc()->setStackSize(virtId, Support::max(virtReg->virtSize(), size),
                               Support::max(virtReg->alignment(), alignment));
  }

  out = _temporaryMem.as<BaseMem>();
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - Allocation - Prolog / Epilog]
// ============================================================================

Error BaseRAPass::updateStackFrame() noexcept {
  // Update some StackFrame information that we updated during allocation. The
  // only information we don't have at the moment is final local stack size,
  // which is calculated last.
  FuncFrame& frame = func()->frame();
  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++)
    frame.addDirtyRegs(group, _clobberedRegs[group]);
  frame.setLocalStackAlignment(_stackAllocator.alignment());

  // If there are stack arguments that are not assigned to registers upon entry
  // and the function doesn't require dynamic stack alignment we keep these
  // arguments where they are. This will also mark all stack slots that match
  // these arguments as allocated.
  if (_numStackArgsToStackSlots)
    ASMJIT_PROPAGATE(_markStackArgsToKeep());

  // Calculate offsets of all stack slots and update StackSize to reflect the calculated local stack size.
  ASMJIT_PROPAGATE(_stackAllocator.calculateStackFrame());
  frame.setLocalStackSize(_stackAllocator.stackSize());

  // Update the stack frame based on `_argsAssignment` and finalize it.
  // Finalization means to apply final calculation to the stack layout.
  ASMJIT_PROPAGATE(_argsAssignment.updateFuncFrame(frame));
  ASMJIT_PROPAGATE(frame.finalize());

  // StackAllocator allocates all stots starting from [0], adjust them when necessary.
  if (frame.localStackOffset() != 0)
    ASMJIT_PROPAGATE(_stackAllocator.adjustSlotOffsets(int32_t(frame.localStackOffset())));

  // Again, if there are stack arguments allocated in function's stack we have
  // to handle them. This handles all cases (either regular or dynamic stack
  // alignment).
  if (_numStackArgsToStackSlots)
    ASMJIT_PROPAGATE(_updateStackArgs());

  return kErrorOk;
}

Error BaseRAPass::_markStackArgsToKeep() noexcept {
  FuncFrame& frame = func()->frame();
  bool hasSAReg = frame.hasPreservedFP() || !frame.hasDynamicAlignment();

  RAWorkRegs& workRegs = _workRegs;
  uint32_t numWorkRegs = workRegCount();

  for (uint32_t workId = 0; workId < numWorkRegs; workId++) {
    RAWorkReg* workReg = workRegs[workId];
    if (workReg->hasFlag(RAWorkReg::kFlagStackArgToStack)) {
      ASMJIT_ASSERT(workReg->hasArgIndex());
      const FuncValue& srcArg = _func->detail().arg(workReg->argIndex());

      // If the register doesn't have stack slot then we failed. It doesn't
      // make much sense as it was marked as `kFlagStackArgToStack`, which
      // requires the WorkReg was live-in upon function entry.
      RAStackSlot* slot = workReg->stackSlot();
      if (ASMJIT_UNLIKELY(!slot))
        return DebugUtils::errored(kErrorInvalidState);

      if (hasSAReg && srcArg.isStack() && !srcArg.isIndirect()) {
        uint32_t typeSize = Type::sizeOf(srcArg.typeId());
        if (typeSize == slot->size()) {
          slot->addFlags(RAStackSlot::kFlagStackArg);
          continue;
        }
      }

      // NOTE: Update StackOffset here so when `_argsAssignment.updateFuncFrame()`
      // is called it will take into consideration moving to stack slots. Without
      // this we may miss some scratch registers later.
      FuncValue& dstArg = _argsAssignment.arg(workReg->argIndex(), workReg->argValueIndex());
      dstArg.assignStackOffset(0);
    }
  }

  return kErrorOk;
}

Error BaseRAPass::_updateStackArgs() noexcept {
  FuncFrame& frame = func()->frame();
  RAWorkRegs& workRegs = _workRegs;
  uint32_t numWorkRegs = workRegCount();

  for (uint32_t workId = 0; workId < numWorkRegs; workId++) {
    RAWorkReg* workReg = workRegs[workId];
    if (workReg->hasFlag(RAWorkReg::kFlagStackArgToStack)) {
      ASMJIT_ASSERT(workReg->hasArgIndex());
      RAStackSlot* slot = workReg->stackSlot();

      if (ASMJIT_UNLIKELY(!slot))
        return DebugUtils::errored(kErrorInvalidState);

      if (slot->isStackArg()) {
        const FuncValue& srcArg = _func->detail().arg(workReg->argIndex());
        if (frame.hasPreservedFP()) {
          slot->setBaseRegId(_fp.id());
          slot->setOffset(int32_t(frame.saOffsetFromSA()) + srcArg.stackOffset());
        }
        else {
          slot->setOffset(int32_t(frame.saOffsetFromSP()) + srcArg.stackOffset());
        }
      }
      else {
        FuncValue& dstArg = _argsAssignment.arg(workReg->argIndex(), workReg->argValueIndex());
        dstArg.setStackOffset(slot->offset());
      }
    }
  }

  return kErrorOk;
}

Error BaseRAPass::insertPrologEpilog() noexcept {
  FuncFrame& frame = _func->frame();

  cc()->_setCursor(func());
  ASMJIT_PROPAGATE(cc()->emitProlog(frame));
  ASMJIT_PROPAGATE(_iEmitHelper->emitArgsAssignment(frame, _argsAssignment));

  cc()->_setCursor(func()->exitNode());
  ASMJIT_PROPAGATE(cc()->emitEpilog(frame));

  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseRAPass - Rewriter]
// ============================================================================

Error BaseRAPass::rewrite() noexcept {
#ifndef ASMJIT_NO_LOGGING
  Logger* logger = debugLogger();
  ASMJIT_RA_LOG_FORMAT("[RAPass::Rewrite]\n");
#endif

  return _rewrite(_func, _stop);
}

// ============================================================================
// [asmjit::BaseRAPass - Logging]
// ============================================================================

#ifndef ASMJIT_NO_LOGGING
static void RAPass_dumpRAInst(BaseRAPass* pass, String& sb, const RAInst* raInst) noexcept {
  const RATiedReg* tiedRegs = raInst->tiedRegs();
  uint32_t tiedCount = raInst->tiedCount();

  for (uint32_t i = 0; i < tiedCount; i++) {
    const RATiedReg& tiedReg = tiedRegs[i];

    if (i != 0)
      sb.append(' ');

    sb.appendFormat("%s{", pass->workRegById(tiedReg.workId())->name());
    sb.append(tiedReg.isReadWrite() ? 'X' :
              tiedReg.isRead()      ? 'R' :
              tiedReg.isWrite()     ? 'W' : '?');

    if (tiedReg.hasUseId())
      sb.appendFormat("|Use=%u", tiedReg.useId());
    else if (tiedReg.isUse())
      sb.append("|Use");

    if (tiedReg.hasOutId())
      sb.appendFormat("|Out=%u", tiedReg.outId());
    else if (tiedReg.isOut())
      sb.append("|Out");

    if (tiedReg.isLast())
      sb.append("|Last");

    if (tiedReg.isKill())
      sb.append("|Kill");

    sb.append("}");
  }
}

ASMJIT_FAVOR_SIZE Error BaseRAPass::annotateCode() noexcept {
  uint32_t loggerFlags = _loggerFlags;
  StringTmp<1024> sb;

  for (const RABlock* block : _blocks) {
    BaseNode* node = block->first();
    if (!node) continue;

    BaseNode* last = block->last();
    for (;;) {
      sb.clear();
      Formatter::formatNode(sb, loggerFlags, cc(), node);

      if ((loggerFlags & FormatOptions::kFlagDebugRA) != 0 && node->isInst() && node->hasPassData()) {
        const RAInst* raInst = node->passData<RAInst>();
        if (raInst->tiedCount() > 0) {
          sb.padEnd(40);
          sb.append(" | ");
          RAPass_dumpRAInst(this, sb, raInst);
        }
      }

      node->setInlineComment(
        static_cast<char*>(
          cc()->_dataZone.dup(sb.data(), sb.size(), true)));

      if (node == last)
        break;
      node = node->next();
    }
  }

  return kErrorOk;
}

ASMJIT_FAVOR_SIZE Error BaseRAPass::_dumpBlockIds(String& sb, const RABlocks& blocks) noexcept {
  for (uint32_t i = 0, size = blocks.size(); i < size; i++) {
    const RABlock* block = blocks[i];
    if (i != 0)
      ASMJIT_PROPAGATE(sb.appendFormat(", #%u", block->blockId()));
    else
      ASMJIT_PROPAGATE(sb.appendFormat("#%u", block->blockId()));
  }
  return kErrorOk;
}

ASMJIT_FAVOR_SIZE Error BaseRAPass::_dumpBlockLiveness(String& sb, const RABlock* block) noexcept {
  for (uint32_t liveType = 0; liveType < RABlock::kLiveCount; liveType++) {
    const char* bitsName = liveType == RABlock::kLiveIn  ? "IN  " :
                           liveType == RABlock::kLiveOut ? "OUT " :
                           liveType == RABlock::kLiveGen ? "GEN " : "KILL";

    const ZoneBitVector& bits = block->_liveBits[liveType];
    uint32_t size = bits.size();
    ASMJIT_ASSERT(size <= workRegCount());

    uint32_t n = 0;
    for (uint32_t workId = 0; workId < size; workId++) {
      if (bits.bitAt(workId)) {
        RAWorkReg* wReg = workRegById(workId);

        if (!n)
          sb.appendFormat("    %s [", bitsName);
        else
          sb.append(", ");

        sb.append(wReg->name());
        n++;
      }
    }

    if (n)
      sb.append("]\n");
  }

  return kErrorOk;
}

ASMJIT_FAVOR_SIZE Error BaseRAPass::_dumpLiveSpans(String& sb) noexcept {
  uint32_t numWorkRegs = _workRegs.size();
  uint32_t maxSize = _maxWorkRegNameSize;

  for (uint32_t workId = 0; workId < numWorkRegs; workId++) {
    RAWorkReg* workReg = _workRegs[workId];

    sb.append("  ");

    size_t oldSize = sb.size();
    sb.append(workReg->name());
    sb.padEnd(oldSize + maxSize);

    RALiveStats& stats = workReg->liveStats();
    sb.appendFormat(" {id:%04u width: %-4u freq: %0.4f priority=%0.4f}",
      workReg->virtId(),
      stats.width(),
      stats.freq(),
      stats.priority());
    sb.append(": ");

    LiveRegSpans& liveSpans = workReg->liveSpans();
    for (uint32_t x = 0; x < liveSpans.size(); x++) {
      const LiveRegSpan& liveSpan = liveSpans[x];
      if (x)
        sb.append(", ");
      sb.appendFormat("[%u:%u]", liveSpan.a, liveSpan.b);
    }

    sb.append('\n');
  }

  return kErrorOk;
}
#endif

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_COMPILER
