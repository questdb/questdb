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

#include "../core/ralocal_p.h"
#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::RALocalAllocator - Utilities]
// ============================================================================

static ASMJIT_INLINE RATiedReg* RALocal_findTiedRegByWorkId(RATiedReg* tiedRegs, size_t count, uint32_t workId) noexcept {
  for (size_t i = 0; i < count; i++)
    if (tiedRegs[i].workId() == workId)
      return &tiedRegs[i];
  return nullptr;
}

// ============================================================================
// [asmjit::RALocalAllocator - Init / Reset]
// ============================================================================

Error RALocalAllocator::init() noexcept {
  PhysToWorkMap* physToWorkMap;
  WorkToPhysMap* workToPhysMap;

  physToWorkMap = _pass->newPhysToWorkMap();
  workToPhysMap = _pass->newWorkToPhysMap();
  if (!physToWorkMap || !workToPhysMap)
    return DebugUtils::errored(kErrorOutOfMemory);

  _curAssignment.initLayout(_pass->_physRegCount, _pass->workRegs());
  _curAssignment.initMaps(physToWorkMap, workToPhysMap);

  physToWorkMap = _pass->newPhysToWorkMap();
  workToPhysMap = _pass->newWorkToPhysMap();
  if (!physToWorkMap || !workToPhysMap)
    return DebugUtils::errored(kErrorOutOfMemory);

  _tmpAssignment.initLayout(_pass->_physRegCount, _pass->workRegs());
  _tmpAssignment.initMaps(physToWorkMap, workToPhysMap);

  return kErrorOk;
}

// ============================================================================
// [asmjit::RALocalAllocator - Assignment]
// ============================================================================

Error RALocalAllocator::makeInitialAssignment() noexcept {
  FuncNode* func = _pass->func();
  RABlock* entry = _pass->entryBlock();

  ZoneBitVector& liveIn = entry->liveIn();
  uint32_t argCount = func->argCount();
  uint32_t numIter = 1;

  for (uint32_t iter = 0; iter < numIter; iter++) {
    for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
      for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
        // Unassigned argument.
        VirtReg* virtReg = func->argPack(argIndex)[valueIndex];
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

        uint32_t group = workReg->group();
        if (_curAssignment.workToPhysId(group, workId) != RAAssignment::kPhysNone)
          continue;

        uint32_t allocableRegs = _availableRegs[group] & ~_curAssignment.assigned(group);
        if (iter == 0) {
          // First iteration: Try to allocate to home RegId.
          if (workReg->hasHomeRegId()) {
            uint32_t physId = workReg->homeRegId();
            if (Support::bitTest(allocableRegs, physId)) {
              _curAssignment.assign(group, workId, physId, true);
              _pass->_argsAssignment.assignRegInPack(argIndex, valueIndex, workReg->info().type(), physId, workReg->typeId());
              continue;
            }
          }

          numIter = 2;
        }
        else {
          // Second iteration: Pick any other register if the is an unassigned one or assign to stack.
          if (allocableRegs) {
            uint32_t physId = Support::ctz(allocableRegs);
            _curAssignment.assign(group, workId, physId, true);
            _pass->_argsAssignment.assignRegInPack(argIndex, valueIndex, workReg->info().type(), physId, workReg->typeId());
          }
          else {
            // This register will definitely need stack, create the slot now and assign also `argIndex`
            // to it. We will patch `_argsAssignment` later after RAStackAllocator finishes.
            RAStackSlot* slot = _pass->getOrCreateStackSlot(workReg);
            if (ASMJIT_UNLIKELY(!slot))
              return DebugUtils::errored(kErrorOutOfMemory);

            // This means STACK_ARG may be moved to STACK.
            workReg->addFlags(RAWorkReg::kFlagStackArgToStack);
            _pass->_numStackArgsToStackSlots++;
          }
        }
      }
    }
  }

  return kErrorOk;
}

Error RALocalAllocator::replaceAssignment(
  const PhysToWorkMap* physToWorkMap,
  const WorkToPhysMap* workToPhysMap) noexcept {

  _curAssignment.copyFrom(physToWorkMap, workToPhysMap);
  return kErrorOk;
}

Error RALocalAllocator::switchToAssignment(
  PhysToWorkMap* dstPhysToWorkMap,
  WorkToPhysMap* dstWorkToPhysMap,
  const ZoneBitVector& liveIn,
  bool dstReadOnly,
  bool tryMode) noexcept {

  RAAssignment dst;
  RAAssignment& cur = _curAssignment;

  dst.initLayout(_pass->_physRegCount, _pass->workRegs());
  dst.initMaps(dstPhysToWorkMap, dstWorkToPhysMap);

  if (tryMode)
    return kErrorOk;

  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    // ------------------------------------------------------------------------
    // STEP 1:
    //   - KILL all registers that are not live at `dst`,
    //   - SPILL all registers that are not assigned at `dst`.
    // ------------------------------------------------------------------------

    if (!tryMode) {
      Support::BitWordIterator<uint32_t> it(cur.assigned(group));
      while (it.hasNext()) {
        uint32_t physId = it.next();
        uint32_t workId = cur.physToWorkId(group, physId);

        // Must be true as we iterate over assigned registers.
        ASMJIT_ASSERT(workId != RAAssignment::kWorkNone);

        // KILL if it's not live on entry.
        if (!liveIn.bitAt(workId)) {
          onKillReg(group, workId, physId);
          continue;
        }

        // SPILL if it's not assigned on entry.
        uint32_t altId = dst.workToPhysId(group, workId);
        if (altId == RAAssignment::kPhysNone) {
          ASMJIT_PROPAGATE(onSpillReg(group, workId, physId));
        }
      }
    }

    // ------------------------------------------------------------------------
    // STEP 2:
    //   - MOVE and SWAP registers from their current assignments into their
    //     DST assignments.
    //   - Build `willLoadRegs` mask of registers scheduled for `onLoadReg()`.
    // ------------------------------------------------------------------------

    // Current run-id (1 means more aggressive decisions).
    int32_t runId = -1;
    // Remaining registers scheduled for `onLoadReg()`.
    uint32_t willLoadRegs = 0;
    // Remaining registers to be allocated in this loop.
    uint32_t affectedRegs = dst.assigned(group);

    while (affectedRegs) {
      if (++runId == 2) {
        if (!tryMode)
          return DebugUtils::errored(kErrorInvalidState);

        // Stop in `tryMode` if we haven't done anything in past two rounds.
        break;
      }

      Support::BitWordIterator<uint32_t> it(affectedRegs);
      while (it.hasNext()) {
        uint32_t physId = it.next();
        uint32_t physMask = Support::bitMask(physId);

        uint32_t curWorkId = cur.physToWorkId(group, physId);
        uint32_t dstWorkId = dst.physToWorkId(group, physId);

        // The register must have assigned `dstWorkId` as we only iterate over assigned regs.
        ASMJIT_ASSERT(dstWorkId != RAAssignment::kWorkNone);

        if (curWorkId != RAAssignment::kWorkNone) {
          // Both assigned.
          if (curWorkId != dstWorkId) {
            // Wait a bit if this is the first run, we may avoid this if `curWorkId` moves out.
            if (runId <= 0)
              continue;

            uint32_t altPhysId = cur.workToPhysId(group, dstWorkId);
            if (altPhysId == RAAssignment::kPhysNone)
              continue;

            // Reset as we will do some changes to the current assignment.
            runId = -1;

            if (_archTraits->hasSwap(group)) {
              ASMJIT_PROPAGATE(onSwapReg(group, curWorkId, physId, dstWorkId, altPhysId));
            }
            else {
              // SPILL the reg if it's not dirty in DST, otherwise try to MOVE.
              if (!cur.isPhysDirty(group, physId)) {
                ASMJIT_PROPAGATE(onKillReg(group, curWorkId, physId));
              }
              else {
                uint32_t allocableRegs = _pass->_availableRegs[group] & ~cur.assigned(group);

                // If possible don't conflict with assigned regs at DST.
                if (allocableRegs & ~dst.assigned(group))
                  allocableRegs &= ~dst.assigned(group);

                if (allocableRegs) {
                  // MOVE is possible, thus preferred.
                  uint32_t tmpPhysId = Support::ctz(allocableRegs);

                  ASMJIT_PROPAGATE(onMoveReg(group, curWorkId, tmpPhysId, physId));
                  _pass->_clobberedRegs[group] |= Support::bitMask(tmpPhysId);
                }
                else {
                  // MOVE is impossible, must SPILL.
                  ASMJIT_PROPAGATE(onSpillReg(group, curWorkId, physId));
                }
              }

              goto Cleared;
            }
          }
        }
        else {
Cleared:
          // DST assigned, CUR unassigned.
          uint32_t altPhysId = cur.workToPhysId(group, dstWorkId);
          if (altPhysId == RAAssignment::kPhysNone) {
            if (liveIn.bitAt(dstWorkId))
              willLoadRegs |= physMask; // Scheduled for `onLoadReg()`.
            affectedRegs &= ~physMask;  // Unaffected from now.
            continue;
          }
          ASMJIT_PROPAGATE(onMoveReg(group, dstWorkId, physId, altPhysId));
        }

        // Both DST and CUR assigned to the same reg or CUR just moved to DST.
        if ((dst.dirty(group) & physMask) != (cur.dirty(group) & physMask)) {
          if ((dst.dirty(group) & physMask) == 0) {
            // CUR dirty, DST not dirty (the assert is just to visualize the condition).
            ASMJIT_ASSERT(!dst.isPhysDirty(group, physId) && cur.isPhysDirty(group, physId));

            // If `dstReadOnly` is true it means that that block was already
            // processed and we cannot change from CLEAN to DIRTY. In that case
            // the register has to be saved as it cannot enter the block DIRTY.
            if (dstReadOnly)
              ASMJIT_PROPAGATE(onSaveReg(group, dstWorkId, physId));
            else
              dst.makeDirty(group, dstWorkId, physId);
          }
          else {
            // DST dirty, CUR not dirty (the assert is just to visualize the condition).
            ASMJIT_ASSERT(dst.isPhysDirty(group, physId) && !cur.isPhysDirty(group, physId));

            cur.makeDirty(group, dstWorkId, physId);
          }
        }

        // Must match now...
        ASMJIT_ASSERT(dst.physToWorkId(group, physId) == cur.physToWorkId(group, physId));
        ASMJIT_ASSERT(dst.isPhysDirty(group, physId) == cur.isPhysDirty(group, physId));

        runId = -1;
        affectedRegs &= ~physMask;
      }
    }

    // ------------------------------------------------------------------------
    // STEP 3:
    //   - Load registers specified by `willLoadRegs`.
    // ------------------------------------------------------------------------

    {
      Support::BitWordIterator<uint32_t> it(willLoadRegs);
      while (it.hasNext()) {
        uint32_t physId = it.next();

        if (!cur.isPhysAssigned(group, physId)) {
          uint32_t workId = dst.physToWorkId(group, physId);

          // The algorithm is broken if it tries to load a register that is not in LIVE-IN.
          ASMJIT_ASSERT(liveIn.bitAt(workId) == true);

          ASMJIT_PROPAGATE(onLoadReg(group, workId, physId));
          if (dst.isPhysDirty(group, physId))
            cur.makeDirty(group, workId, physId);
          ASMJIT_ASSERT(dst.isPhysDirty(group, physId) == cur.isPhysDirty(group, physId));
        }
        else {
          // Not possible otherwise.
          ASMJIT_ASSERT(tryMode == true);
        }
      }
    }
  }

  if (!tryMode) {
    // Hre is a code that dumps the conflicting part if something fails here:
    //   if (!dst.equals(cur)) {
    //     uint32_t physTotal = dst._layout.physTotal;
    //     uint32_t workCount = dst._layout.workCount;
    //
    //     for (uint32_t physId = 0; physId < physTotal; physId++) {
    //       uint32_t dstWorkId = dst._physToWorkMap->workIds[physId];
    //       uint32_t curWorkId = cur._physToWorkMap->workIds[physId];
    //       if (dstWorkId != curWorkId)
    //         fprintf(stderr, "[PhysIdWork] PhysId=%u WorkId[DST(%u) != CUR(%u)]\n", physId, dstWorkId, curWorkId);
    //     }
    //
    //     for (uint32_t workId = 0; workId < workCount; workId++) {
    //       uint32_t dstPhysId = dst._workToPhysMap->physIds[workId];
    //       uint32_t curPhysId = cur._workToPhysMap->physIds[workId];
    //       if (dstPhysId != curPhysId)
    //         fprintf(stderr, "[WorkToPhys] WorkId=%u PhysId[DST(%u) != CUR(%u)]\n", workId, dstPhysId, curPhysId);
    //     }
    //   }
    ASMJIT_ASSERT(dst.equals(cur));
  }

  return kErrorOk;
}

Error RALocalAllocator::spillScratchGpRegsBeforeEntry(uint32_t scratchRegs) noexcept {
  uint32_t group = BaseReg::kGroupGp;
  Support::BitWordIterator<uint32_t> it(scratchRegs);

  while (it.hasNext()) {
    uint32_t physId = it.next();
    if (_curAssignment.isPhysAssigned(group, physId)) {
      uint32_t workId = _curAssignment.physToWorkId(group, physId);
      ASMJIT_PROPAGATE(onSpillReg(group, workId, physId));
    }
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::RALocalAllocator - Allocation]
// ============================================================================

Error RALocalAllocator::allocInst(InstNode* node) noexcept {
  RAInst* raInst = node->passData<RAInst>();

  RATiedReg* outTiedRegs[Globals::kMaxPhysRegs];
  RATiedReg* dupTiedRegs[Globals::kMaxPhysRegs];

  // The cursor must point to the previous instruction for a possible instruction insertion.
  _cc->_setCursor(node->prev());

  _node = node;
  _raInst = raInst;
  _tiedTotal = raInst->_tiedTotal;
  _tiedCount = raInst->_tiedCount;

  // Whether we already replaced register operand with memory operand.
  bool rmAllocated = false;

  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    uint32_t i, count = this->tiedCount(group);
    RATiedReg* tiedRegs = this->tiedRegs(group);

    uint32_t willUse = _raInst->_usedRegs[group];
    uint32_t willOut = _raInst->_clobberedRegs[group];
    uint32_t willFree = 0;
    uint32_t usePending = count;

    uint32_t outTiedCount = 0;
    uint32_t dupTiedCount = 0;

    // ------------------------------------------------------------------------
    // STEP 1:
    //
    // Calculate `willUse` and `willFree` masks based on tied registers we have.
    //
    // We don't do any assignment decisions at this stage as we just need to
    // collect some information first. Then, after we populate all masks needed
    // we can finally make some decisions in the second loop. The main reason
    // for this is that we really need `willFree` to make assignment decisions
    // for `willUse`, because if we mark some registers that will be freed, we
    // can consider them in decision making afterwards.
    // ------------------------------------------------------------------------

    for (i = 0; i < count; i++) {
      RATiedReg* tiedReg = &tiedRegs[i];

      // Add OUT and KILL to `outPending` for CLOBBERing and/or OUT assignment.
      if (tiedReg->isOutOrKill())
        outTiedRegs[outTiedCount++] = tiedReg;

      if (tiedReg->isDuplicate())
        dupTiedRegs[dupTiedCount++] = tiedReg;

      if (!tiedReg->isUse()) {
        tiedReg->markUseDone();
        usePending--;
        continue;
      }

      uint32_t workId = tiedReg->workId();
      uint32_t assignedId = _curAssignment.workToPhysId(group, workId);

      if (tiedReg->hasUseId()) {
        // If the register has `useId` it means it can only be allocated in that register.
        uint32_t useMask = Support::bitMask(tiedReg->useId());

        // RAInstBuilder must have collected `usedRegs` on-the-fly.
        ASMJIT_ASSERT((willUse & useMask) != 0);

        if (assignedId == tiedReg->useId()) {
          // If the register is already allocated in this one, mark it done and continue.
          tiedReg->markUseDone();
          if (tiedReg->isWrite())
            _curAssignment.makeDirty(group, workId, assignedId);
          usePending--;
          willUse |= useMask;
        }
        else {
          willFree |= useMask & _curAssignment.assigned(group);
        }
      }
      else {
        // Check if the register must be moved to `allocableRegs`.
        uint32_t allocableRegs = tiedReg->allocableRegs();
        if (assignedId != RAAssignment::kPhysNone) {
          uint32_t assignedMask = Support::bitMask(assignedId);
          if ((allocableRegs & ~willUse) & assignedMask) {
            tiedReg->setUseId(assignedId);
            tiedReg->markUseDone();
            if (tiedReg->isWrite())
              _curAssignment.makeDirty(group, workId, assignedId);
            usePending--;
            willUse |= assignedMask;
          }
          else {
            willFree |= assignedMask;
          }
        }
      }
    }

    // ------------------------------------------------------------------------
    // STEP 2:
    //
    // Do some decision making to find the best candidates of registers that
    // need to be assigned, moved, and/or spilled. Only USE registers are
    // considered here, OUT will be decided later after all CLOBBERed and OUT
    // registers are unassigned.
    // ------------------------------------------------------------------------

    if (usePending) {
      // TODO: Not sure `liveRegs` should be used, maybe willUse and willFree would be enough and much more clear.

      // All registers that are currently alive without registers that will be freed.
      uint32_t liveRegs = _curAssignment.assigned(group) & ~willFree;

      for (i = 0; i < count; i++) {
        RATiedReg* tiedReg = &tiedRegs[i];
        if (tiedReg->isUseDone()) continue;

        uint32_t workId = tiedReg->workId();
        uint32_t assignedId = _curAssignment.workToPhysId(group, workId);

        // REG/MEM: Patch register operand to memory operand if not allocated.
        if (!rmAllocated && tiedReg->hasUseRM()) {
          if (assignedId == RAAssignment::kPhysNone && Support::isPowerOf2(tiedReg->useRewriteMask())) {
            RAWorkReg* workReg = workRegById(tiedReg->workId());
            uint32_t opIndex = Support::ctz(tiedReg->useRewriteMask()) / uint32_t(sizeof(Operand) / sizeof(uint32_t));
            uint32_t rmSize = tiedReg->rmSize();

            if (rmSize <= workReg->virtReg()->virtSize()) {
              Operand& op = node->operands()[opIndex];
              op = _pass->workRegAsMem(workReg);
              op.as<BaseMem>().setSize(rmSize);
              tiedReg->_useRewriteMask = 0;

              tiedReg->markUseDone();
              usePending--;

              rmAllocated = true;
              continue;
            }
          }
        }

        if (!tiedReg->hasUseId()) {
          uint32_t allocableRegs = tiedReg->allocableRegs() & ~(willFree | willUse);

          // DECIDE where to assign the USE register.
          uint32_t useId = decideOnAssignment(group, workId, assignedId, allocableRegs);
          uint32_t useMask = Support::bitMask(useId);

          willUse |= useMask;
          willFree |= useMask & liveRegs;
          tiedReg->setUseId(useId);

          if (assignedId != RAAssignment::kPhysNone) {
            uint32_t assignedMask = Support::bitMask(assignedId);

            willFree |= assignedMask;
            liveRegs &= ~assignedMask;

            // OPTIMIZATION: Assign the USE register here if it's possible.
            if (!(liveRegs & useMask)) {
              ASMJIT_PROPAGATE(onMoveReg(group, workId, useId, assignedId));
              tiedReg->markUseDone();
              if (tiedReg->isWrite())
                _curAssignment.makeDirty(group, workId, useId);
              usePending--;
            }
          }
          else {
            // OPTIMIZATION: Assign the USE register here if it's possible.
            if (!(liveRegs & useMask)) {
              ASMJIT_PROPAGATE(onLoadReg(group, workId, useId));
              tiedReg->markUseDone();
              if (tiedReg->isWrite())
                _curAssignment.makeDirty(group, workId, useId);
              usePending--;
            }
          }

          liveRegs |= useMask;
        }
      }
    }

    // Initially all used regs will be marked clobbered.
    uint32_t clobberedByInst = willUse | willOut;

    // ------------------------------------------------------------------------
    // STEP 3:
    //
    // Free all registers that we marked as `willFree`. Only registers that are not
    // USEd by the instruction are considered as we don't want to free regs we need.
    // ------------------------------------------------------------------------

    if (willFree) {
      uint32_t allocableRegs = _availableRegs[group] & ~(_curAssignment.assigned(group) | willFree | willUse | willOut);
      Support::BitWordIterator<uint32_t> it(willFree);

      do {
        uint32_t assignedId = it.next();
        if (_curAssignment.isPhysAssigned(group, assignedId)) {
          uint32_t workId = _curAssignment.physToWorkId(group, assignedId);

          // DECIDE whether to MOVE or SPILL.
          if (allocableRegs) {
            uint32_t reassignedId = decideOnReassignment(group, workId, assignedId, allocableRegs);
            if (reassignedId != RAAssignment::kPhysNone) {
              ASMJIT_PROPAGATE(onMoveReg(group, workId, reassignedId, assignedId));
              allocableRegs ^= Support::bitMask(reassignedId);
              continue;
            }
          }

          ASMJIT_PROPAGATE(onSpillReg(group, workId, assignedId));
        }
      } while (it.hasNext());
    }

    // ------------------------------------------------------------------------
    // STEP 4:
    //
    // ALLOCATE / SHUFFLE all registers that we marked as `willUse` and weren't
    // allocated yet. This is a bit complicated as the allocation is iterative.
    // In some cases we have to wait before allocating a particual physical
    // register as it's still occupied by some other one, which we need to move
    // before we can use it. In this case we skip it and allocate another some
    // other instead (making it free for another iteration).
    //
    // NOTE: Iterations are mostly important for complicated allocations like
    // function calls, where there can be up to N registers used at once. Asm
    // instructions won't run the loop more than once in 99.9% of cases as they
    // use 2..3 registers in average.
    // ------------------------------------------------------------------------

    if (usePending) {
      bool mustSwap = false;
      do {
        uint32_t oldPending = usePending;

        for (i = 0; i < count; i++) {
          RATiedReg* thisTiedReg = &tiedRegs[i];
          if (thisTiedReg->isUseDone()) continue;

          uint32_t thisWorkId = thisTiedReg->workId();
          uint32_t thisPhysId = _curAssignment.workToPhysId(group, thisWorkId);

          // This would be a bug, fatal one!
          uint32_t targetPhysId = thisTiedReg->useId();
          ASMJIT_ASSERT(targetPhysId != thisPhysId);

          uint32_t targetWorkId = _curAssignment.physToWorkId(group, targetPhysId);
          if (targetWorkId != RAAssignment::kWorkNone) {
            RAWorkReg* targetWorkReg = workRegById(targetWorkId);

            // Swapping two registers can solve two allocation tasks by emitting
            // just a single instruction. However, swap is only available on few
            // architectures and it's definitely not available for each register
            // group. Calling `onSwapReg()` before checking these would be fatal.
            if (_archTraits->hasSwap(group) && thisPhysId != RAAssignment::kPhysNone) {
              ASMJIT_PROPAGATE(onSwapReg(group, thisWorkId, thisPhysId, targetWorkId, targetPhysId));

              thisTiedReg->markUseDone();
              if (thisTiedReg->isWrite())
                _curAssignment.makeDirty(group, thisWorkId, targetPhysId);
              usePending--;

              // Double-hit.
              RATiedReg* targetTiedReg = RALocal_findTiedRegByWorkId(tiedRegs, count, targetWorkReg->workId());
              if (targetTiedReg && targetTiedReg->useId() == thisPhysId) {
                targetTiedReg->markUseDone();
                if (targetTiedReg->isWrite())
                  _curAssignment.makeDirty(group, targetWorkId, thisPhysId);
                usePending--;
              }
              continue;
            }

            if (!mustSwap)
              continue;

            // Only branched here if the previous iteration did nothing. This is
            // essentially a SWAP operation without having a dedicated instruction
            // for that purpose (vector registers, etc). The simplest way to
            // handle such case is to SPILL the target register.
            ASMJIT_PROPAGATE(onSpillReg(group, targetWorkId, targetPhysId));
          }

          if (thisPhysId != RAAssignment::kPhysNone) {
            ASMJIT_PROPAGATE(onMoveReg(group, thisWorkId, targetPhysId, thisPhysId));

            thisTiedReg->markUseDone();
            if (thisTiedReg->isWrite())
              _curAssignment.makeDirty(group, thisWorkId, targetPhysId);
            usePending--;
          }
          else {
            ASMJIT_PROPAGATE(onLoadReg(group, thisWorkId, targetPhysId));

            thisTiedReg->markUseDone();
            if (thisTiedReg->isWrite())
              _curAssignment.makeDirty(group, thisWorkId, targetPhysId);
            usePending--;
          }
        }

        mustSwap = (oldPending == usePending);
      } while (usePending);
    }

    // ------------------------------------------------------------------------
    // STEP 5:
    //
    // KILL registers marked as KILL/OUT.
    // ------------------------------------------------------------------------

    uint32_t outPending = outTiedCount;
    if (outTiedCount) {
      for (i = 0; i < outTiedCount; i++) {
        RATiedReg* tiedReg = outTiedRegs[i];

        uint32_t workId = tiedReg->workId();
        uint32_t physId = _curAssignment.workToPhysId(group, workId);

        // Must check if it's allocated as KILL can be related to OUT (like KILL
        // immediately after OUT, which could mean the register is not assigned).
        if (physId != RAAssignment::kPhysNone) {
          ASMJIT_PROPAGATE(onKillReg(group, workId, physId));
          willOut &= ~Support::bitMask(physId);
        }

        // We still maintain number of pending registers for OUT assignment.
        // So, if this is only KILL, not OUT, we can safely decrement it.
        outPending -= !tiedReg->isOut();
      }
    }

    // ------------------------------------------------------------------------
    // STEP 6:
    //
    // SPILL registers that will be CLOBBERed. Since OUT and KILL were
    // already processed this is used mostly to handle function CALLs.
    // ------------------------------------------------------------------------

    if (willOut) {
      Support::BitWordIterator<uint32_t> it(willOut);
      do {
        uint32_t physId = it.next();
        uint32_t workId = _curAssignment.physToWorkId(group, physId);

        if (workId == RAAssignment::kWorkNone)
          continue;

        ASMJIT_PROPAGATE(onSpillReg(group, workId, physId));
      } while (it.hasNext());
    }

    // ------------------------------------------------------------------------
    // STEP 7:
    //
    // Duplication.
    // ------------------------------------------------------------------------

    for (i = 0; i < dupTiedCount; i++) {
      RATiedReg* tiedReg = dupTiedRegs[i];
      uint32_t workId = tiedReg->workId();
      uint32_t srcId = tiedReg->useId();

      Support::BitWordIterator<uint32_t> it(tiedReg->_allocableRegs);
      while (it.hasNext()) {
        uint32_t dstId = it.next();
        if (dstId == srcId)
          continue;
        _pass->emitMove(workId, dstId, srcId);
      }
    }

    // ------------------------------------------------------------------------
    // STEP 8:
    //
    // Assign OUT registers.
    // ------------------------------------------------------------------------

    if (outPending) {
      // Live registers, we need a separate variable (outside of `_curAssignment)
      // to hold these because of KILLed registers. If we KILL a register here it
      // will go out from `_curAssignment`, but we cannot assign to it in here.
      uint32_t liveRegs = _curAssignment.assigned(group);

      // Must avoid as they have been already OUTed (added during the loop).
      uint32_t outRegs = 0;

      // Must avoid as they collide with already allocated ones.
      uint32_t avoidRegs = willUse & ~clobberedByInst;

      for (i = 0; i < outTiedCount; i++) {
        RATiedReg* tiedReg = outTiedRegs[i];
        if (!tiedReg->isOut()) continue;

        uint32_t workId = tiedReg->workId();
        uint32_t assignedId = _curAssignment.workToPhysId(group, workId);

        if (assignedId != RAAssignment::kPhysNone)
          ASMJIT_PROPAGATE(onKillReg(group, workId, assignedId));

        uint32_t physId = tiedReg->outId();
        if (physId == RAAssignment::kPhysNone) {
          uint32_t allocableRegs = tiedReg->_allocableRegs & ~(outRegs | avoidRegs);

          if (!(allocableRegs & ~liveRegs)) {
            // There are no more registers, decide which one to spill.
            uint32_t spillWorkId;
            physId = decideOnSpillFor(group, workId, allocableRegs & liveRegs, &spillWorkId);
            ASMJIT_PROPAGATE(onSpillReg(group, spillWorkId, physId));
          }
          else {
            physId = decideOnAssignment(group, workId, RAAssignment::kPhysNone, allocableRegs & ~liveRegs);
          }
        }

        // OUTs are CLOBBERed thus cannot be ASSIGNed right now.
        ASMJIT_ASSERT(!_curAssignment.isPhysAssigned(group, physId));

        if (!tiedReg->isKill())
          ASMJIT_PROPAGATE(onAssignReg(group, workId, physId, true));

        tiedReg->setOutId(physId);
        tiedReg->markOutDone();

        outRegs |= Support::bitMask(physId);
        liveRegs &= ~Support::bitMask(physId);
        outPending--;
      }

      clobberedByInst |= outRegs;
      ASMJIT_ASSERT(outPending == 0);
    }

    _clobberedRegs[group] |= clobberedByInst;
  }

  return kErrorOk;
}

Error RALocalAllocator::spillAfterAllocation(InstNode* node) noexcept {
  // This is experimental feature that would spill registers that don't have
  // home-id and are last in this basic block. This prevents saving these regs
  // in other basic blocks and then restoring them (mostly relevant for loops).
  RAInst* raInst = node->passData<RAInst>();
  uint32_t count = raInst->tiedCount();

  for (uint32_t i = 0; i < count; i++) {
    RATiedReg* tiedReg = raInst->tiedAt(i);
    if (tiedReg->isLast()) {
      uint32_t workId = tiedReg->workId();
      RAWorkReg* workReg = workRegById(workId);
      if (!workReg->hasHomeRegId()) {
        uint32_t group = workReg->group();
        uint32_t assignedId = _curAssignment.workToPhysId(group, workId);
        if (assignedId != RAAssignment::kPhysNone) {
          _cc->_setCursor(node);
          ASMJIT_PROPAGATE(onSpillReg(group, workId, assignedId));
        }
      }
    }
  }

  return kErrorOk;
}

Error RALocalAllocator::allocBranch(InstNode* node, RABlock* target, RABlock* cont) noexcept {
  // TODO: This should be used to make the branch allocation better.
  DebugUtils::unused(cont);

  // The cursor must point to the previous instruction for a possible instruction insertion.
  _cc->_setCursor(node->prev());

  // Use TryMode of `switchToAssignment()` if possible.
  if (target->hasEntryAssignment()) {
    ASMJIT_PROPAGATE(switchToAssignment(
      target->entryPhysToWorkMap(),
      target->entryWorkToPhysMap(),
      target->liveIn(),
      target->isAllocated(),
      true));
  }

  ASMJIT_PROPAGATE(allocInst(node));
  ASMJIT_PROPAGATE(spillRegsBeforeEntry(target));

  if (target->hasEntryAssignment()) {
    BaseNode* injectionPoint = _pass->extraBlock()->prev();
    BaseNode* prevCursor = _cc->setCursor(injectionPoint);

    _tmpAssignment.copyFrom(_curAssignment);
    ASMJIT_PROPAGATE(switchToAssignment(
      target->entryPhysToWorkMap(),
      target->entryWorkToPhysMap(),
      target->liveIn(),
      target->isAllocated(),
      false));

    BaseNode* curCursor = _cc->cursor();
    if (curCursor != injectionPoint) {
      // Additional instructions emitted to switch from the current state to
      // the `target` state. This means that we have to move these instructions
      // into an independent code block and patch the jump location.
      Operand& targetOp = node->op(node->opCount() - 1);
      if (ASMJIT_UNLIKELY(!targetOp.isLabel()))
        return DebugUtils::errored(kErrorInvalidState);

      Label trampoline = _cc->newLabel();
      Label savedTarget = targetOp.as<Label>();

      // Patch `target` to point to the `trampoline` we just created.
      targetOp = trampoline;

      // Clear a possible SHORT form as we have no clue now if the SHORT form would
      // be encodable after patching the target to `trampoline` (X86 specific).
      node->clearInstOptions(BaseInst::kOptionShortForm);

      // Finalize the switch assignment sequence.
      ASMJIT_PROPAGATE(_pass->emitJump(savedTarget));
      _cc->_setCursor(injectionPoint);
      _cc->bind(trampoline);
    }

    _cc->_setCursor(prevCursor);
    _curAssignment.swap(_tmpAssignment);
  }
  else {
    ASMJIT_PROPAGATE(_pass->setBlockEntryAssignment(target, block(), _curAssignment));
  }

  return kErrorOk;
}

Error RALocalAllocator::allocJumpTable(InstNode* node, const RABlocks& targets, RABlock* cont) noexcept {
  // TODO: Do we really need to use `cont`?
  DebugUtils::unused(cont);

  if (targets.empty())
    return DebugUtils::errored(kErrorInvalidState);

  // The cursor must point to the previous instruction for a possible instruction insertion.
  _cc->_setCursor(node->prev());

  // All `targets` should have the same sharedAssignmentId, we just read the first.
  RABlock* anyTarget = targets[0];
  if (!anyTarget->hasSharedAssignmentId())
    return DebugUtils::errored(kErrorInvalidState);

  RASharedAssignment& sharedAssignment = _pass->_sharedAssignments[anyTarget->sharedAssignmentId()];

  ASMJIT_PROPAGATE(allocInst(node));

  if (!sharedAssignment.empty()) {
    ASMJIT_PROPAGATE(switchToAssignment(
      sharedAssignment.physToWorkMap(),
      sharedAssignment.workToPhysMap(),
      sharedAssignment.liveIn(),
      true,  // Read-only.
      false  // Try-mode.
    ));
  }

  ASMJIT_PROPAGATE(spillRegsBeforeEntry(anyTarget));

  if (sharedAssignment.empty()) {
    ASMJIT_PROPAGATE(_pass->setBlockEntryAssignment(anyTarget, block(), _curAssignment));
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::RALocalAllocator - Decision Making]
// ============================================================================

uint32_t RALocalAllocator::decideOnAssignment(uint32_t group, uint32_t workId, uint32_t physId, uint32_t allocableRegs) const noexcept {
  ASMJIT_ASSERT(allocableRegs != 0);
  DebugUtils::unused(group, physId);

  RAWorkReg* workReg = workRegById(workId);

  // Prefer home register id, if possible.
  if (workReg->hasHomeRegId()) {
    uint32_t homeId = workReg->homeRegId();
    if (Support::bitTest(allocableRegs, homeId))
      return homeId;
  }

  // Prefer registers used upon block entries.
  uint32_t previouslyAssignedRegs = workReg->allocatedMask();
  if (allocableRegs & previouslyAssignedRegs)
    allocableRegs &= previouslyAssignedRegs;

  return Support::ctz(allocableRegs);
}

uint32_t RALocalAllocator::decideOnReassignment(uint32_t group, uint32_t workId, uint32_t physId, uint32_t allocableRegs) const noexcept {
  ASMJIT_ASSERT(allocableRegs != 0);
  DebugUtils::unused(group, physId);

  RAWorkReg* workReg = workRegById(workId);

  // Prefer allocating back to HomeId, if possible.
  if (workReg->hasHomeRegId()) {
    if (Support::bitTest(allocableRegs, workReg->homeRegId()))
      return workReg->homeRegId();
  }

  // TODO: [Register Allocator] This could be improved.

  // Decided to SPILL.
  return RAAssignment::kPhysNone;
}

uint32_t RALocalAllocator::decideOnSpillFor(uint32_t group, uint32_t workId, uint32_t spillableRegs, uint32_t* spillWorkId) const noexcept {
  // May be used in the future to decide which register would be best to spill so `workId` can be assigned.
  DebugUtils::unused(workId);
  ASMJIT_ASSERT(spillableRegs != 0);

  Support::BitWordIterator<uint32_t> it(spillableRegs);
  uint32_t bestPhysId = it.next();
  uint32_t bestWorkId = _curAssignment.physToWorkId(group, bestPhysId);

  // Avoid calculating the cost model if there is only one spillable register.
  if (it.hasNext()) {
    uint32_t bestCost = calculateSpillCost(group, bestWorkId, bestPhysId);
    do {
      uint32_t localPhysId = it.next();
      uint32_t localWorkId = _curAssignment.physToWorkId(group, localPhysId);
      uint32_t localCost = calculateSpillCost(group, localWorkId, localPhysId);

      if (localCost < bestCost) {
        bestCost = localCost;
        bestPhysId = localPhysId;
        bestWorkId = localWorkId;
      }
    } while (it.hasNext());
  }

  *spillWorkId = bestWorkId;
  return bestPhysId;
}

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_COMPILER
