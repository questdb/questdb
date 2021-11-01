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
#include "../core/archtraits.h"
#include "../core/emithelper_p.h"
#include "../core/formatter.h"
#include "../core/funcargscontext_p.h"
#include "../core/radefs_p.h"

// Can be used for debugging...
// #define ASMJIT_DUMP_ARGS_ASSIGNMENT

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::BaseEmitHelper - Formatting]
// ============================================================================

#ifdef ASMJIT_DUMP_ARGS_ASSIGNMENT
static void dumpFuncValue(String& sb, uint32_t arch, const FuncValue& value) noexcept {
  Formatter::formatTypeId(sb, value.typeId());
  sb.append('@');

  if (value.isIndirect())
    sb.append('[');

  if (value.isReg())
    Formatter::formatRegister(sb, 0, nullptr, arch, value.regType(), value.regId());
  else if (value.isStack())
    sb.appendFormat("[%d]", value.stackOffset());
  else
    sb.append("<none>");

  if (value.isIndirect())
    sb.append(']');
}

static void dumpAssignment(String& sb, const FuncArgsContext& ctx) noexcept {
  typedef FuncArgsContext::Var Var;

  uint32_t arch = ctx.arch();
  uint32_t varCount = ctx.varCount();

  for (uint32_t i = 0; i < varCount; i++) {
    const Var& var = ctx.var(i);
    const FuncValue& dst = var.out;
    const FuncValue& cur = var.cur;

    sb.appendFormat("Var%u: ", i);
    dumpFuncValue(sb, arch, dst);
    sb.append(" <- ");
    dumpFuncValue(sb, arch, cur);

    if (var.isDone())
      sb.append(" {Done}");

    sb.append('\n');
  }
}
#endif

// ============================================================================
// [asmjit::BaseEmitHelper - EmitArgsAssignment]
// ============================================================================

ASMJIT_FAVOR_SIZE Error BaseEmitHelper::emitArgsAssignment(const FuncFrame& frame, const FuncArgsAssignment& args) {
  typedef FuncArgsContext::Var Var;
  typedef FuncArgsContext::WorkData WorkData;

  enum WorkFlags : uint32_t {
    kWorkNone      = 0x00,
    kWorkDidSome   = 0x01,
    kWorkPending   = 0x02,
    kWorkPostponed = 0x04
  };

  uint32_t arch = frame.arch();
  const ArchTraits& archTraits = ArchTraits::byArch(arch);

  RAConstraints constraints;
  FuncArgsContext ctx;

  ASMJIT_PROPAGATE(constraints.init(arch));
  ASMJIT_PROPAGATE(ctx.initWorkData(frame, args, &constraints));

#ifdef ASMJIT_DUMP_ARGS_ASSIGNMENT
  {
    String sb;
    dumpAssignment(sb, ctx);
    printf("%s\n", sb.data());
  }
#endif

  uint32_t varCount = ctx._varCount;
  WorkData* workData = ctx._workData;

  uint32_t saVarId = ctx._saVarId;
  BaseReg sp = BaseReg::fromSignatureAndId(_emitter->_gpRegInfo.signature(), archTraits.spRegId());
  BaseReg sa = sp;

  if (frame.hasDynamicAlignment()) {
    if (frame.hasPreservedFP())
      sa.setId(archTraits.fpRegId());
    else
      sa.setId(saVarId < varCount ? ctx._vars[saVarId].cur.regId() : frame.saRegId());
  }

  // --------------------------------------------------------------------------
  // Register to stack and stack to stack moves must be first as now we have
  // the biggest chance of having as many as possible unassigned registers.
  // --------------------------------------------------------------------------

  if (ctx._stackDstMask) {
    // Base address of all arguments passed by stack.
    BaseMem baseArgPtr(sa, int32_t(frame.saOffset(sa.id())));
    BaseMem baseStackPtr(sp, 0);

    for (uint32_t varId = 0; varId < varCount; varId++) {
      Var& var = ctx._vars[varId];

      if (!var.out.isStack())
        continue;

      FuncValue& cur = var.cur;
      FuncValue& out = var.out;

      ASMJIT_ASSERT(cur.isReg() || cur.isStack());
      BaseReg reg;

      BaseMem dstStackPtr = baseStackPtr.cloneAdjusted(out.stackOffset());
      BaseMem srcStackPtr = baseArgPtr.cloneAdjusted(cur.stackOffset());

      if (cur.isIndirect()) {
        if (cur.isStack()) {
          // TODO: Indirect stack.
          return DebugUtils::errored(kErrorInvalidAssignment);
        }
        else {
          srcStackPtr.setBaseId(cur.regId());
        }
      }

      if (cur.isReg() && !cur.isIndirect()) {
        WorkData& wd = workData[archTraits.regTypeToGroup(cur.regType())];
        uint32_t rId = cur.regId();

        reg.setSignatureAndId(archTraits.regTypeToSignature(cur.regType()), rId);
        wd.unassign(varId, rId);
      }
      else {
        // Stack to reg move - tricky since we move stack to stack we can decide which
        // register to use. In general we follow the rule that IntToInt moves will use
        // GP regs with possibility to signature or zero extend, and all other moves will
        // either use GP or VEC regs depending on the size of the move.
        RegInfo rInfo = getSuitableRegForMemToMemMove(arch, out.typeId(), cur.typeId());
        if (ASMJIT_UNLIKELY(!rInfo.isValid()))
          return DebugUtils::errored(kErrorInvalidState);

        WorkData& wd = workData[rInfo.group()];
        uint32_t availableRegs = wd.availableRegs();
        if (ASMJIT_UNLIKELY(!availableRegs))
          return DebugUtils::errored(kErrorInvalidState);

        uint32_t rId = Support::ctz(availableRegs);
        reg.setSignatureAndId(rInfo.signature(), rId);

        ASMJIT_PROPAGATE(emitArgMove(reg, out.typeId(), srcStackPtr, cur.typeId()));
      }

      if (cur.isIndirect() && cur.isReg())
        workData[BaseReg::kGroupGp].unassign(varId, cur.regId());

      // Register to stack move.
      ASMJIT_PROPAGATE(emitRegMove(dstStackPtr, reg, cur.typeId()));
      var.markDone();
    }
  }

  // --------------------------------------------------------------------------
  // Shuffle all registers that are currently assigned accordingly to target
  // assignment.
  // --------------------------------------------------------------------------

  uint32_t workFlags = kWorkNone;
  for (;;) {
    for (uint32_t varId = 0; varId < varCount; varId++) {
      Var& var = ctx._vars[varId];
      if (var.isDone() || !var.cur.isReg())
        continue;

      FuncValue& cur = var.cur;
      FuncValue& out = var.out;

      uint32_t curGroup = archTraits.regTypeToGroup(cur.regType());
      uint32_t outGroup = archTraits.regTypeToGroup(out.regType());

      uint32_t curId = cur.regId();
      uint32_t outId = out.regId();

      if (curGroup != outGroup) {
        // TODO: Conversion is not supported.
        return DebugUtils::errored(kErrorInvalidAssignment);
      }
      else {
        WorkData& wd = workData[outGroup];
        if (!wd.isAssigned(outId)) {
EmitMove:
          ASMJIT_PROPAGATE(
            emitArgMove(
              BaseReg::fromSignatureAndId(archTraits.regTypeToSignature(out.regType()), outId), out.typeId(),
              BaseReg::fromSignatureAndId(archTraits.regTypeToSignature(cur.regType()), curId), cur.typeId()));

          wd.reassign(varId, outId, curId);
          cur.initReg(out.regType(), outId, out.typeId());

          if (outId == out.regId())
            var.markDone();
          workFlags |= kWorkDidSome | kWorkPending;
        }
        else {
          uint32_t altId = wd._physToVarId[outId];
          Var& altVar = ctx._vars[altId];

          if (!altVar.out.isInitialized() || (altVar.out.isReg() && altVar.out.regId() == curId)) {
            // Only few architectures provide swap operations, and only for few register groups.
            if (archTraits.hasSwap(curGroup)) {
              uint32_t highestType = Support::max(cur.regType(), altVar.cur.regType());
              if (Support::isBetween<uint32_t>(highestType, BaseReg::kTypeGp8Lo, BaseReg::kTypeGp16))
                highestType = BaseReg::kTypeGp32;

              uint32_t signature = archTraits.regTypeToSignature(highestType);
              ASMJIT_PROPAGATE(
                emitRegSwap(BaseReg::fromSignatureAndId(signature, outId),
                            BaseReg::fromSignatureAndId(signature, curId)));
              wd.swap(varId, curId, altId, outId);
              cur.setRegId(outId);
              var.markDone();
              altVar.cur.setRegId(curId);

              if (altVar.out.isInitialized())
                altVar.markDone();
              workFlags |= kWorkDidSome;
            }
            else {
              // If there is a scratch register it can be used to perform the swap.
              uint32_t availableRegs = wd.availableRegs();
              if (availableRegs) {
                uint32_t inOutRegs = wd.dstRegs();
                if (availableRegs & ~inOutRegs)
                  availableRegs &= ~inOutRegs;
                outId = Support::ctz(availableRegs);
                goto EmitMove;
              }
              else {
                workFlags |= kWorkPending;
              }
            }
          }
          else {
            workFlags |= kWorkPending;
          }
        }
      }
    }

    if (!(workFlags & kWorkPending))
      break;

    // If we did nothing twice it means that something is really broken.
    if ((workFlags & (kWorkDidSome | kWorkPostponed)) == kWorkPostponed)
      return DebugUtils::errored(kErrorInvalidState);

    workFlags = (workFlags & kWorkDidSome) ? kWorkNone : kWorkPostponed;
  }

  // --------------------------------------------------------------------------
  // Load arguments passed by stack into registers. This is pretty simple and
  // it never requires multiple iterations like the previous phase.
  // --------------------------------------------------------------------------

  if (ctx._hasStackSrc) {
    uint32_t iterCount = 1;
    if (frame.hasDynamicAlignment() && !frame.hasPreservedFP())
      sa.setId(saVarId < varCount ? ctx._vars[saVarId].cur.regId() : frame.saRegId());

    // Base address of all arguments passed by stack.
    BaseMem baseArgPtr(sa, int32_t(frame.saOffset(sa.id())));

    for (uint32_t iter = 0; iter < iterCount; iter++) {
      for (uint32_t varId = 0; varId < varCount; varId++) {
        Var& var = ctx._vars[varId];
        if (var.isDone())
          continue;

        if (var.cur.isStack()) {
          ASMJIT_ASSERT(var.out.isReg());

          uint32_t outId = var.out.regId();
          uint32_t outType = var.out.regType();

          uint32_t group = archTraits.regTypeToGroup(outType);
          WorkData& wd = ctx._workData[group];

          if (outId == sa.id() && group == BaseReg::kGroupGp) {
            // This register will be processed last as we still need `saRegId`.
            if (iterCount == 1) {
              iterCount++;
              continue;
            }
            wd.unassign(wd._physToVarId[outId], outId);
          }

          BaseReg dstReg = BaseReg::fromSignatureAndId(archTraits.regTypeToSignature(outType), outId);
          BaseMem srcMem = baseArgPtr.cloneAdjusted(var.cur.stackOffset());

          ASMJIT_PROPAGATE(emitArgMove(
            dstReg, var.out.typeId(),
            srcMem, var.cur.typeId()));

          wd.assign(varId, outId);
          var.cur.initReg(outType, outId, var.cur.typeId(), FuncValue::kFlagIsDone);
        }
      }
    }
  }

  return kErrorOk;
}

ASMJIT_END_NAMESPACE
