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

#ifndef ASMJIT_CORE_RABUILDERS_P_H_INCLUDED
#define ASMJIT_CORE_RABUILDERS_P_H_INCLUDED

#include "../core/api-config.h"
#ifndef ASMJIT_NO_COMPILER

#include "../core/formatter.h"
#include "../core/rapass_p.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_ra
//! \{

// ============================================================================
// [asmjit::RACFGBuilderT]
// ============================================================================

template<typename This>
class RACFGBuilderT {
public:
  BaseRAPass* _pass = nullptr;
  BaseCompiler* _cc = nullptr;
  RABlock* _curBlock = nullptr;
  RABlock* _retBlock = nullptr;
  FuncNode* _funcNode = nullptr;
  RARegsStats _blockRegStats {};
  uint32_t _exitLabelId = Globals::kInvalidId;
  ZoneVector<uint32_t> _sharedAssignmentsMap {};

  // Only used by logging, it's fine to be here to prevent more #ifdefs...
  bool _hasCode = false;
  RABlock* _lastLoggedBlock = nullptr;

#ifndef ASMJIT_NO_LOGGING
  Logger* _logger = nullptr;
  uint32_t _logFlags = FormatOptions::kFlagPositions;
  StringTmp<512> _sb;
#endif

  static constexpr uint32_t kRootIndentation = 2;
  static constexpr uint32_t kCodeIndentation = 4;

  // NOTE: This is a bit hacky. There are some nodes which are processed twice
  // (see `onBeforeInvoke()` and `onBeforeRet()`) as they can insert some nodes
  // around them. Since we don't have any flags to mark these we just use their
  // position that is [at that time] unassigned.
  static constexpr uint32_t kNodePositionDidOnBefore = 0xFFFFFFFFu;

  inline RACFGBuilderT(BaseRAPass* pass) noexcept
    : _pass(pass),
      _cc(pass->cc()) {
#ifndef ASMJIT_NO_LOGGING
    _logger = _pass->debugLogger();
    if (_logger)
      _logFlags |= _logger->flags();
#endif
  }

  inline BaseCompiler* cc() const noexcept { return _cc; }

  // --------------------------------------------------------------------------
  // [Run]
  // --------------------------------------------------------------------------

  //! Called per function by an architecture-specific CFG builder.
  Error run() noexcept {
    log("[RAPass::BuildCFG]\n");
    ASMJIT_PROPAGATE(prepare());

    logNode(_funcNode, kRootIndentation);
    logBlock(_curBlock, kRootIndentation);

    RABlock* entryBlock = _curBlock;
    BaseNode* node = _funcNode->next();
    if (ASMJIT_UNLIKELY(!node))
      return DebugUtils::errored(kErrorInvalidState);

    _curBlock->setFirst(_funcNode);
    _curBlock->setLast(_funcNode);

    RAInstBuilder ib;
    ZoneVector<RABlock*> blocksWithUnknownJumps;

    for (;;) {
      BaseNode* next = node->next();
      ASMJIT_ASSERT(node->position() == 0 || node->position() == kNodePositionDidOnBefore);

      if (node->isInst()) {
        // Instruction | Jump | Invoke | Return
        // ------------------------------------

        // Handle `InstNode`, `InvokeNode`, and `FuncRetNode`. All of them
        // share the same interface that provides operands that have read/write
        // semantics.
        if (ASMJIT_UNLIKELY(!_curBlock)) {
          // Unreachable code has to be removed, we cannot allocate registers
          // in such code as we cannot do proper liveness analysis in such case.
          removeNode(node);
          node = next;
          continue;
        }

        _hasCode = true;

        if (node->isInvoke() || node->isFuncRet()) {
          if (node->position() != kNodePositionDidOnBefore) {
            // Call and Reg are complicated as they may insert some surrounding
            // code around them. The simplest approach is to get the previous
            // node, call the `onBefore()` handlers and then check whether
            // anything changed and restart if so. By restart we mean that the
            // current `node` would go back to the first possible inserted node
            // by `onBeforeInvoke()` or `onBeforeRet()`.
            BaseNode* prev = node->prev();

            if (node->type() == BaseNode::kNodeInvoke)
              ASMJIT_PROPAGATE(static_cast<This*>(this)->onBeforeInvoke(node->as<InvokeNode>()));
            else
              ASMJIT_PROPAGATE(static_cast<This*>(this)->onBeforeRet(node->as<FuncRetNode>()));

            if (prev != node->prev()) {
              // If this was the first node in the block and something was
              // inserted before it then we have to update the first block.
              if (_curBlock->first() == node)
                _curBlock->setFirst(prev->next());

              node->setPosition(kNodePositionDidOnBefore);
              node = prev->next();

              // `onBeforeInvoke()` and `onBeforeRet()` can only insert instructions.
              ASMJIT_ASSERT(node->isInst());
            }

            // Necessary if something was inserted after `node`, but nothing before.
            next = node->next();
          }
          else {
            // Change the position back to its original value.
            node->setPosition(0);
          }
        }

        InstNode* inst = node->as<InstNode>();
        logNode(inst, kCodeIndentation);

        uint32_t controlType = BaseInst::kControlNone;
        ib.reset();
        ASMJIT_PROPAGATE(static_cast<This*>(this)->onInst(inst, controlType, ib));

        if (node->isInvoke()) {
          ASMJIT_PROPAGATE(static_cast<This*>(this)->onInvoke(inst->as<InvokeNode>(), ib));
        }

        if (node->isFuncRet()) {
          ASMJIT_PROPAGATE(static_cast<This*>(this)->onRet(inst->as<FuncRetNode>(), ib));
          controlType = BaseInst::kControlReturn;
        }

        if (controlType == BaseInst::kControlJump) {
          uint32_t fixedRegCount = 0;
          for (RATiedReg& tiedReg : ib) {
            RAWorkReg* workReg = _pass->workRegById(tiedReg.workId());
            if (workReg->group() == BaseReg::kGroupGp) {
              uint32_t useId = tiedReg.useId();
              if (useId == BaseReg::kIdBad) {
                useId = _pass->_scratchRegIndexes[fixedRegCount++];
                tiedReg.setUseId(useId);
              }
              _curBlock->addExitScratchGpRegs(Support::bitMask<uint32_t>(useId));
            }
          }
        }

        ASMJIT_PROPAGATE(_pass->assignRAInst(inst, _curBlock, ib));
        _blockRegStats.combineWith(ib._stats);

        if (controlType != BaseInst::kControlNone) {
          // Support for conditional and unconditional jumps.
          if (controlType == BaseInst::kControlJump || controlType == BaseInst::kControlBranch) {
            _curBlock->setLast(node);
            _curBlock->addFlags(RABlock::kFlagHasTerminator);
            _curBlock->makeConstructed(_blockRegStats);

            if (!(inst->instOptions() & BaseInst::kOptionUnfollow)) {
              // Jmp/Jcc/Call/Loop/etc...
              uint32_t opCount = inst->opCount();
              const Operand* opArray = inst->operands();

              // Cannot jump anywhere without operands.
              if (ASMJIT_UNLIKELY(!opCount))
                return DebugUtils::errored(kErrorInvalidState);

              if (opArray[opCount - 1].isLabel()) {
                // Labels are easy for constructing the control flow.
                LabelNode* labelNode;
                ASMJIT_PROPAGATE(cc()->labelNodeOf(&labelNode, opArray[opCount - 1].as<Label>()));

                RABlock* targetBlock = _pass->newBlockOrExistingAt(labelNode);
                if (ASMJIT_UNLIKELY(!targetBlock))
                  return DebugUtils::errored(kErrorOutOfMemory);

                targetBlock->makeTargetable();
                ASMJIT_PROPAGATE(_curBlock->appendSuccessor(targetBlock));
              }
              else {
                // Not a label - could be jump with reg/mem operand, which
                // means that it can go anywhere. Such jumps must either be
                // annotated so the CFG can be properly constructed, otherwise
                // we assume the worst case - can jump to any basic block.
                JumpAnnotation* jumpAnnotation = nullptr;
                _curBlock->addFlags(RABlock::kFlagHasJumpTable);

                if (inst->type() == BaseNode::kNodeJump)
                  jumpAnnotation = inst->as<JumpNode>()->annotation();

                if (jumpAnnotation) {
                  uint64_t timestamp = _pass->nextTimestamp();
                  for (uint32_t id : jumpAnnotation->labelIds()) {
                    LabelNode* labelNode;
                    ASMJIT_PROPAGATE(cc()->labelNodeOf(&labelNode, id));

                    RABlock* targetBlock = _pass->newBlockOrExistingAt(labelNode);
                    if (ASMJIT_UNLIKELY(!targetBlock))
                      return DebugUtils::errored(kErrorOutOfMemory);

                    // Prevents adding basic-block successors multiple times.
                    if (!targetBlock->hasTimestamp(timestamp)) {
                      targetBlock->setTimestamp(timestamp);
                      targetBlock->makeTargetable();
                      ASMJIT_PROPAGATE(_curBlock->appendSuccessor(targetBlock));
                    }
                  }
                  ASMJIT_PROPAGATE(shareAssignmentAcrossSuccessors(_curBlock));
                }
                else {
                  ASMJIT_PROPAGATE(blocksWithUnknownJumps.append(_pass->allocator(), _curBlock));
                }
              }
            }

            if (controlType == BaseInst::kControlJump) {
              // Unconditional jump makes the code after the jump unreachable,
              // which will be removed instantly during the CFG construction;
              // as we cannot allocate registers for instructions that are not
              // part of any block. Of course we can leave these instructions
              // as they are, however, that would only postpone the problem as
              // assemblers can't encode instructions that use virtual registers.
              _curBlock = nullptr;
            }
            else {
              node = next;
              if (ASMJIT_UNLIKELY(!node))
                return DebugUtils::errored(kErrorInvalidState);

              RABlock* consecutiveBlock;
              if (node->type() == BaseNode::kNodeLabel) {
                if (node->hasPassData()) {
                  consecutiveBlock = node->passData<RABlock>();
                }
                else {
                  consecutiveBlock = _pass->newBlock(node);
                  if (ASMJIT_UNLIKELY(!consecutiveBlock))
                    return DebugUtils::errored(kErrorOutOfMemory);
                  node->setPassData<RABlock>(consecutiveBlock);
                }
              }
              else {
                consecutiveBlock = _pass->newBlock(node);
                if (ASMJIT_UNLIKELY(!consecutiveBlock))
                  return DebugUtils::errored(kErrorOutOfMemory);
              }

              _curBlock->addFlags(RABlock::kFlagHasConsecutive);
              ASMJIT_PROPAGATE(_curBlock->prependSuccessor(consecutiveBlock));

              _curBlock = consecutiveBlock;
              _hasCode = false;
              _blockRegStats.reset();

              if (_curBlock->isConstructed())
                break;
              ASMJIT_PROPAGATE(_pass->addBlock(consecutiveBlock));

              logBlock(_curBlock, kRootIndentation);
              continue;
            }
          }

          if (controlType == BaseInst::kControlReturn) {
            _curBlock->setLast(node);
            _curBlock->makeConstructed(_blockRegStats);
            ASMJIT_PROPAGATE(_curBlock->appendSuccessor(_retBlock));

            _curBlock = nullptr;
          }
        }
      }
      else if (node->type() == BaseNode::kNodeLabel) {
        // Label - Basic-Block Management
        // ------------------------------

        if (!_curBlock) {
          // If the current code is unreachable the label makes it reachable
          // again. We may remove the whole block in the future if it's not
          // referenced though.
          _curBlock = node->passData<RABlock>();

          if (_curBlock) {
            // If the label has a block assigned we can either continue with
            // it or skip it if the block has been constructed already.
            if (_curBlock->isConstructed())
              break;
          }
          else {
            // No block assigned - create a new one and assign it.
            _curBlock = _pass->newBlock(node);
            if (ASMJIT_UNLIKELY(!_curBlock))
              return DebugUtils::errored(kErrorOutOfMemory);
            node->setPassData<RABlock>(_curBlock);
          }

          _curBlock->makeTargetable();
          _hasCode = false;
          _blockRegStats.reset();
          ASMJIT_PROPAGATE(_pass->addBlock(_curBlock));
        }
        else {
          if (node->hasPassData()) {
            RABlock* consecutive = node->passData<RABlock>();
            consecutive->makeTargetable();

            if (_curBlock == consecutive) {
              // The label currently processed is part of the current block. This
              // is only possible for multiple labels that are right next to each
              // other or labels that are separated by non-code nodes like directives
              // and comments.
              if (ASMJIT_UNLIKELY(_hasCode))
                return DebugUtils::errored(kErrorInvalidState);
            }
            else {
              // Label makes the current block constructed. There is a chance that the
              // Label is not used, but we don't know that at this point. In the worst
              // case there would be two blocks next to each other, it's just fine.
              ASMJIT_ASSERT(_curBlock->last() != node);
              _curBlock->setLast(node->prev());
              _curBlock->addFlags(RABlock::kFlagHasConsecutive);
              _curBlock->makeConstructed(_blockRegStats);

              ASMJIT_PROPAGATE(_curBlock->appendSuccessor(consecutive));
              ASMJIT_PROPAGATE(_pass->addBlock(consecutive));

              _curBlock = consecutive;
              _hasCode = false;
              _blockRegStats.reset();
            }
          }
          else {
            // First time we see this label.
            if (_hasCode || _curBlock == entryBlock) {
              // Cannot continue the current block if it already contains some
              // code or it's a block entry. We need to create a new block and
              // make it a successor.
              ASMJIT_ASSERT(_curBlock->last() != node);
              _curBlock->setLast(node->prev());
              _curBlock->addFlags(RABlock::kFlagHasConsecutive);
              _curBlock->makeConstructed(_blockRegStats);

              RABlock* consecutive = _pass->newBlock(node);
              if (ASMJIT_UNLIKELY(!consecutive))
                return DebugUtils::errored(kErrorOutOfMemory);
              consecutive->makeTargetable();

              ASMJIT_PROPAGATE(_curBlock->appendSuccessor(consecutive));
              ASMJIT_PROPAGATE(_pass->addBlock(consecutive));

              _curBlock = consecutive;
              _hasCode = false;
              _blockRegStats.reset();
            }

            node->setPassData<RABlock>(_curBlock);
          }
        }

        if (_curBlock && _curBlock != _lastLoggedBlock)
          logBlock(_curBlock, kRootIndentation);
        logNode(node, kRootIndentation);

        // Unlikely: Assume that the exit label is reached only once per function.
        if (ASMJIT_UNLIKELY(node->as<LabelNode>()->labelId() == _exitLabelId)) {
          _curBlock->setLast(node);
          _curBlock->makeConstructed(_blockRegStats);
          ASMJIT_PROPAGATE(_pass->addExitBlock(_curBlock));

          _curBlock = nullptr;
        }
      }
      else {
        // Other Nodes | Function Exit
        // ---------------------------

        logNode(node, kCodeIndentation);

        if (node->type() == BaseNode::kNodeSentinel) {
          if (node == _funcNode->endNode()) {
            // Make sure we didn't flow here if this is the end of the function sentinel.
            if (ASMJIT_UNLIKELY(_curBlock))
              return DebugUtils::errored(kErrorInvalidState);
            break;
          }
        }
        else if (node->type() == BaseNode::kNodeFunc) {
          // RAPass can only compile a single function at a time. If we
          // encountered a function it must be the current one, bail if not.
          if (ASMJIT_UNLIKELY(node != _funcNode))
            return DebugUtils::errored(kErrorInvalidState);
          // PASS if this is the first node.
        }
        else {
          // PASS if this is a non-interesting or unknown node.
        }
      }

      // Advance to the next node.
      node = next;

      // NOTE: We cannot encounter a NULL node, because every function must be
      // terminated by a sentinel (`stop`) node. If we encountered a NULL node it
      // means that something went wrong and this node list is corrupted; bail in
      // such case.
      if (ASMJIT_UNLIKELY(!node))
        return DebugUtils::errored(kErrorInvalidState);
    }

    if (_pass->hasDanglingBlocks())
      return DebugUtils::errored(kErrorInvalidState);

    for (RABlock* block : blocksWithUnknownJumps)
      handleBlockWithUnknownJump(block);

    return _pass->initSharedAssignments(_sharedAssignmentsMap);
  }

  // --------------------------------------------------------------------------
  // [Prepare]
  // --------------------------------------------------------------------------

  //! Prepares the CFG builder of the current function.
  Error prepare() noexcept {
    FuncNode* func = _pass->func();
    BaseNode* node = nullptr;

    // Create entry and exit blocks.
    _funcNode = func;
    _retBlock = _pass->newBlockOrExistingAt(func->exitNode(), &node);

    if (ASMJIT_UNLIKELY(!_retBlock))
      return DebugUtils::errored(kErrorOutOfMemory);

    _retBlock->makeTargetable();
    ASMJIT_PROPAGATE(_pass->addExitBlock(_retBlock));

    if (node != func) {
      _curBlock = _pass->newBlock();
      if (ASMJIT_UNLIKELY(!_curBlock))
        return DebugUtils::errored(kErrorOutOfMemory);
    }
    else {
      // Function that has no code at all.
      _curBlock = _retBlock;
    }

    // Reset everything we may need.
    _blockRegStats.reset();
    _exitLabelId = func->exitNode()->labelId();

    // Initially we assume there is no code in the function body.
    _hasCode = false;

    return _pass->addBlock(_curBlock);
  }

  // --------------------------------------------------------------------------
  // [Utilities]
  // --------------------------------------------------------------------------

  //! Called when a `node` is removed, e.g. bacause of a dead code elimination.
  void removeNode(BaseNode* node) noexcept {
    logNode(node, kRootIndentation, "<Removed>");
    cc()->removeNode(node);
  }

  //! Handles block with unknown jump, which could be a jump to a jump table.
  //!
  //! If we encounter such block we basically insert all existing blocks as
  //! successors except the function entry block and a natural successor, if
  //! such block exists.
  Error handleBlockWithUnknownJump(RABlock* block) noexcept {
    RABlocks& blocks = _pass->blocks();
    size_t blockCount = blocks.size();

    // NOTE: Iterate from `1` as the first block is the entry block, we don't
    // allow the entry to be a successor of any block.
    RABlock* consecutive = block->consecutive();
    for (size_t i = 1; i < blockCount; i++) {
      RABlock* candidate = blocks[i];
      if (candidate == consecutive || !candidate->isTargetable())
        continue;
      block->appendSuccessor(candidate);
    }

    return shareAssignmentAcrossSuccessors(block);
  }

  Error shareAssignmentAcrossSuccessors(RABlock* block) noexcept {
    if (block->successors().size() <= 1)
      return kErrorOk;

    RABlock* consecutive = block->consecutive();
    uint32_t sharedAssignmentId = Globals::kInvalidId;

    for (RABlock* successor : block->successors()) {
      if (successor == consecutive)
        continue;

      if (successor->hasSharedAssignmentId()) {
        if (sharedAssignmentId == Globals::kInvalidId)
          sharedAssignmentId = successor->sharedAssignmentId();
        else
          _sharedAssignmentsMap[successor->sharedAssignmentId()] = sharedAssignmentId;
      }
      else {
        if (sharedAssignmentId == Globals::kInvalidId)
          ASMJIT_PROPAGATE(newSharedAssignmentId(&sharedAssignmentId));
        successor->setSharedAssignmentId(sharedAssignmentId);
      }
    }
    return kErrorOk;
  }

  Error newSharedAssignmentId(uint32_t* out) noexcept {
    uint32_t id = _sharedAssignmentsMap.size();
    ASMJIT_PROPAGATE(_sharedAssignmentsMap.append(_pass->allocator(), id));

    *out = id;
    return kErrorOk;
  }

  // --------------------------------------------------------------------------
  // [Logging]
  // --------------------------------------------------------------------------

#ifndef ASMJIT_NO_LOGGING
  template<typename... Args>
  inline void log(const char* fmt, Args&&... args) noexcept {
    if (_logger)
      _logger->logf(fmt, std::forward<Args>(args)...);
  }

  inline void logBlock(RABlock* block, uint32_t indentation = 0) noexcept {
    if (_logger)
      _logBlock(block, indentation);
  }

  inline void logNode(BaseNode* node, uint32_t indentation = 0, const char* action = nullptr) noexcept {
    if (_logger)
      _logNode(node, indentation, action);
  }

  void _logBlock(RABlock* block, uint32_t indentation) noexcept {
    _sb.clear();
    _sb.appendChars(' ', indentation);
    _sb.appendFormat("{#%u}\n", block->blockId());
    _logger->log(_sb);
    _lastLoggedBlock = block;
  }

  void _logNode(BaseNode* node, uint32_t indentation, const char* action) noexcept {
    _sb.clear();
    _sb.appendChars(' ', indentation);
    if (action) {
      _sb.append(action);
      _sb.append(' ');
    }
    Formatter::formatNode(_sb, _logFlags, cc(), node);
    _sb.append('\n');
    _logger->log(_sb);
  }
#else
  template<typename... Args>
  inline void log(const char* fmt, Args&&... args) noexcept {
    DebugUtils::unused(fmt);
    DebugUtils::unused(std::forward<Args>(args)...);
  }

  inline void logBlock(RABlock* block, uint32_t indentation = 0) noexcept {
    DebugUtils::unused(block, indentation);
  }

  inline void logNode(BaseNode* node, uint32_t indentation = 0, const char* action = nullptr) noexcept {
    DebugUtils::unused(node, indentation, action);
  }
#endif
};

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_COMPILER
#endif // ASMJIT_CORE_RABUILDERS_P_H_INCLUDED
