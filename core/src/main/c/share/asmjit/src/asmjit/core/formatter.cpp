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
#ifndef ASMJIT_NO_LOGGING

#include "../core/archtraits.h"
#include "../core/builder.h"
#include "../core/codeholder.h"
#include "../core/compiler.h"
#include "../core/emitter.h"
#include "../core/formatter.h"
#include "../core/string.h"
#include "../core/support.h"
#include "../core/type.h"

#if !defined(ASMJIT_NO_X86)
  #include "../x86/x86formatter_p.h"
#endif

#ifdef ASMJIT_BUILD_ARM
  #include "../arm/armformatter_p.h"
#endif

ASMJIT_BEGIN_NAMESPACE

#if defined(ASMJIT_NO_COMPILER)
class VirtReg;
#endif

// ============================================================================
// [asmjit::Formatter]
// ============================================================================

namespace Formatter {

static const char wordNameTable[][8] = {
  "db",
  "dw",
  "dd",
  "dq",
  "byte",
  "half",
  "word",
  "hword",
  "dword",
  "qword",
  "xword",
  "short",
  "long",
  "quad"
};


Error formatTypeId(String& sb, uint32_t typeId) noexcept {
  if (typeId == Type::kIdVoid)
    return sb.append("void");

  if (!Type::isValid(typeId))
    return sb.append("unknown");

  const char* typeName = "unknown";
  uint32_t typeSize = Type::sizeOf(typeId);

  uint32_t baseId = Type::baseOf(typeId);
  switch (baseId) {
    case Type::kIdIntPtr : typeName = "iptr"  ; break;
    case Type::kIdUIntPtr: typeName = "uptr"  ; break;
    case Type::kIdI8     : typeName = "i8"    ; break;
    case Type::kIdU8     : typeName = "u8"    ; break;
    case Type::kIdI16    : typeName = "i16"   ; break;
    case Type::kIdU16    : typeName = "u16"   ; break;
    case Type::kIdI32    : typeName = "i32"   ; break;
    case Type::kIdU32    : typeName = "u32"   ; break;
    case Type::kIdI64    : typeName = "i64"   ; break;
    case Type::kIdU64    : typeName = "u64"   ; break;
    case Type::kIdF32    : typeName = "f32"   ; break;
    case Type::kIdF64    : typeName = "f64"   ; break;
    case Type::kIdF80    : typeName = "f80"   ; break;
    case Type::kIdMask8  : typeName = "mask8" ; break;
    case Type::kIdMask16 : typeName = "mask16"; break;
    case Type::kIdMask32 : typeName = "mask32"; break;
    case Type::kIdMask64 : typeName = "mask64"; break;
    case Type::kIdMmx32  : typeName = "mmx32" ; break;
    case Type::kIdMmx64  : typeName = "mmx64" ; break;
  }

  uint32_t baseSize = Type::sizeOf(baseId);
  if (typeSize > baseSize) {
    uint32_t count = typeSize / baseSize;
    return sb.appendFormat("%sx%u", typeName, unsigned(count));
  }
  else {
    return sb.append(typeName);
  }
}

Error formatFeature(
  String& sb,
  uint32_t arch,
  uint32_t featureId) noexcept {

#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::FormatterInternal::formatFeature(sb, featureId);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isFamilyARM(arch))
    return arm::FormatterInternal::formatFeature(sb, featureId);
#endif

  return kErrorInvalidArch;
}

Error formatLabel(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t labelId) noexcept {

  DebugUtils::unused(formatFlags);

  const LabelEntry* le = emitter->code()->labelEntry(labelId);
  if (ASMJIT_UNLIKELY(!le))
    return sb.appendFormat("<InvalidLabel:%u>", labelId);

  if (le->hasName()) {
    if (le->hasParent()) {
      uint32_t parentId = le->parentId();
      const LabelEntry* pe = emitter->code()->labelEntry(parentId);

      if (ASMJIT_UNLIKELY(!pe))
        ASMJIT_PROPAGATE(sb.appendFormat("<InvalidLabel:%u>", labelId));
      else if (ASMJIT_UNLIKELY(!pe->hasName()))
        ASMJIT_PROPAGATE(sb.appendFormat("L%u", parentId));
      else
        ASMJIT_PROPAGATE(sb.append(pe->name()));

      ASMJIT_PROPAGATE(sb.append('.'));
    }
    return sb.append(le->name());
  }
  else {
    return sb.appendFormat("L%u", labelId);
  }
}

Error formatRegister(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t arch,
  uint32_t regType,
  uint32_t regId) noexcept {

#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::FormatterInternal::formatRegister(sb, formatFlags, emitter, arch, regType, regId);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isFamilyARM(arch))
    return arm::FormatterInternal::formatRegister(sb, formatFlags, emitter, arch, regType, regId);
#endif

  return kErrorInvalidArch;
}

Error formatOperand(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t arch,
  const Operand_& op) noexcept {

#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::FormatterInternal::formatOperand(sb, formatFlags, emitter, arch, op);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isFamilyARM(arch))
    return arm::FormatterInternal::formatOperand(sb, formatFlags, emitter, arch, op);
#endif

  return kErrorInvalidArch;
}

ASMJIT_API Error formatDataType(
  String& sb,
  uint32_t formatFlags,
  uint32_t arch,
  uint32_t typeId) noexcept
{
  DebugUtils::unused(formatFlags);

  if (ASMJIT_UNLIKELY(arch >= Environment::kArchCount))
    return DebugUtils::errored(kErrorInvalidArch);

  uint32_t typeSize = Type::sizeOf(typeId);
  if (typeSize == 0 || typeSize > 8)
    return DebugUtils::errored(kErrorInvalidState);

  uint32_t typeSizeLog2 = Support::ctz(typeSize);
  return sb.append(wordNameTable[size_t(_archTraits[arch].isaWordNameId(typeSizeLog2))]);
}

static Error formatDataHelper(String& sb, const char* typeName, uint32_t typeSize, const uint8_t* data, size_t itemCount) noexcept {
  sb.append('.');
  sb.append(typeName);
  sb.append(' ');

  for (size_t i = 0; i < itemCount; i++) {
    uint64_t v;

    if (i != 0)
      ASMJIT_PROPAGATE(sb.append(", ", 2));

    switch (typeSize) {
      case 1: v = data[0]; break;
      case 2: v = Support::readU16u(data); break;
      case 4: v = Support::readU32u(data); break;
      case 8: v = Support::readU64u(data); break;
    }

    ASMJIT_PROPAGATE(sb.appendUInt(v, 16, typeSize * 2, String::kFormatAlternate));
    data += typeSize;
  }

  return kErrorOk;
}

Error formatData(
  String& sb,
  uint32_t formatFlags,
  uint32_t arch,
  uint32_t typeId, const void* data, size_t itemCount, size_t repeatCount) noexcept
{
  DebugUtils::unused(formatFlags);

  if (ASMJIT_UNLIKELY(arch >= Environment::kArchCount))
    return DebugUtils::errored(kErrorInvalidArch);

  uint32_t typeSize = Type::sizeOf(typeId);
  if (typeSize == 0)
    return DebugUtils::errored(kErrorInvalidState);

  if (!Support::isPowerOf2(typeSize)) {
    itemCount *= typeSize;
    typeSize = 1;
  }

  while (typeSize > 8u) {
    typeSize >>= 1;
    itemCount <<= 1;
  }

  uint32_t typeSizeLog2 = Support::ctz(typeSize);
  const char* wordName = wordNameTable[size_t(_archTraits[arch].isaWordNameId(typeSizeLog2))];

  if (repeatCount > 1)
    ASMJIT_PROPAGATE(sb.appendFormat(".repeat %zu ", repeatCount));

  return formatDataHelper(sb, wordName, typeSize, static_cast<const uint8_t*>(data), itemCount);
}

Error formatInstruction(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  uint32_t arch,
  const BaseInst& inst, const Operand_* operands, size_t opCount) noexcept {

#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::FormatterInternal::formatInstruction(sb, formatFlags, emitter, arch, inst, operands, opCount);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isFamilyARM(arch))
    return arm::FormatterInternal::formatInstruction(sb, formatFlags, emitter, arch, inst, operands, opCount);
#endif

  return kErrorInvalidArch;
}

#ifndef ASMJIT_NO_BUILDER

#ifndef ASMJIT_NO_COMPILER
static Error formatFuncValue(String& sb, uint32_t formatFlags, const BaseEmitter* emitter, FuncValue value) noexcept {
  uint32_t typeId = value.typeId();
  ASMJIT_PROPAGATE(formatTypeId(sb, typeId));

  if (value.isAssigned()) {
    ASMJIT_PROPAGATE(sb.append('@'));

    if (value.isIndirect())
      ASMJIT_PROPAGATE(sb.append('['));

    // NOTE: It should be either reg or stack, but never both. We
    // use two IFs on purpose so if the FuncValue is both it would
    // show in logs.
    if (value.isReg()) {
      ASMJIT_PROPAGATE(formatRegister(sb, formatFlags, emitter, emitter->arch(), value.regType(), value.regId()));
    }

    if (value.isStack()) {
      ASMJIT_PROPAGATE(sb.appendFormat("[%d]", int(value.stackOffset())));
    }

    if (value.isIndirect())
      ASMJIT_PROPAGATE(sb.append(']'));
  }

  return kErrorOk;
}

static Error formatFuncValuePack(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  const FuncValuePack& pack,
  VirtReg* const* vRegs) noexcept {

  size_t count = pack.count();
  if (!count)
    return sb.append("void");

  if (count > 1)
    sb.append('[');

  for (uint32_t valueIndex = 0; valueIndex < count; valueIndex++) {
    const FuncValue& value = pack[valueIndex];
    if (!value)
      break;

    if (valueIndex)
      ASMJIT_PROPAGATE(sb.append(", "));

    ASMJIT_PROPAGATE(formatFuncValue(sb, formatFlags, emitter, value));

    if (vRegs) {
      static const char nullRet[] = "<none>";
      ASMJIT_PROPAGATE(sb.appendFormat(" %s", vRegs[valueIndex] ? vRegs[valueIndex]->name() : nullRet));
    }
  }

  if (count > 1)
    sb.append(']');

  return kErrorOk;
}

static Error formatFuncRets(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  const FuncDetail& fd) noexcept {

  return formatFuncValuePack(sb, formatFlags, emitter, fd.retPack(), nullptr);
}

static Error formatFuncArgs(
  String& sb,
  uint32_t formatFlags,
  const BaseEmitter* emitter,
  const FuncDetail& fd,
  const FuncNode::ArgPack* argPacks) noexcept {

  uint32_t argCount = fd.argCount();
  if (!argCount)
    return sb.append("void");

  for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
    if (argIndex)
      ASMJIT_PROPAGATE(sb.append(", "));

    ASMJIT_PROPAGATE(formatFuncValuePack(sb, formatFlags, emitter, fd.argPack(argIndex), argPacks[argIndex]._data));
  }

  return kErrorOk;
}
#endif

Error formatNode(
  String& sb,
  uint32_t formatFlags,
  const BaseBuilder* builder,
  const BaseNode* node) noexcept {

  if (node->hasPosition() && (formatFlags & FormatOptions::kFlagPositions) != 0)
    ASMJIT_PROPAGATE(sb.appendFormat("<%05u> ", node->position()));

  switch (node->type()) {
    case BaseNode::kNodeInst:
    case BaseNode::kNodeJump: {
      const InstNode* instNode = node->as<InstNode>();
      ASMJIT_PROPAGATE(
        formatInstruction(sb, formatFlags, builder,
          builder->arch(),
          instNode->baseInst(), instNode->operands(), instNode->opCount()));
      break;
    }

    case BaseNode::kNodeSection: {
      const SectionNode* sectionNode = node->as<SectionNode>();
      if (builder->_code->isSectionValid(sectionNode->id())) {
        const Section* section = builder->_code->sectionById(sectionNode->id());
        ASMJIT_PROPAGATE(sb.appendFormat(".section %s", section->name()));
      }
      break;
    }

    case BaseNode::kNodeLabel: {
      const LabelNode* labelNode = node->as<LabelNode>();
      ASMJIT_PROPAGATE(formatLabel(sb, formatFlags, builder, labelNode->labelId()));
      ASMJIT_PROPAGATE(sb.append(":"));
      break;
    }

    case BaseNode::kNodeAlign: {
      const AlignNode* alignNode = node->as<AlignNode>();
      ASMJIT_PROPAGATE(
        sb.appendFormat(".align %u (%s)",
          alignNode->alignment(),
          alignNode->alignMode() == kAlignCode ? "code" : "data"));
      break;
    }

    case BaseNode::kNodeEmbedData: {
      const EmbedDataNode* embedNode = node->as<EmbedDataNode>();
      ASMJIT_PROPAGATE(sb.append('.'));
      ASMJIT_PROPAGATE(formatDataType(sb, formatFlags, builder->arch(), embedNode->typeId()));
      ASMJIT_PROPAGATE(sb.appendFormat(" {Count=%zu Repeat=%zu TotalSize=%zu}", embedNode->itemCount(), embedNode->repeatCount(), embedNode->dataSize()));
      break;
    }

    case BaseNode::kNodeEmbedLabel: {
      const EmbedLabelNode* embedNode = node->as<EmbedLabelNode>();
      ASMJIT_PROPAGATE(sb.append(".label "));
      ASMJIT_PROPAGATE(formatLabel(sb, formatFlags, builder, embedNode->labelId()));
      break;
    }

    case BaseNode::kNodeEmbedLabelDelta: {
      const EmbedLabelDeltaNode* embedNode = node->as<EmbedLabelDeltaNode>();
      ASMJIT_PROPAGATE(sb.append(".label ("));
      ASMJIT_PROPAGATE(formatLabel(sb, formatFlags, builder, embedNode->labelId()));
      ASMJIT_PROPAGATE(sb.append(" - "));
      ASMJIT_PROPAGATE(formatLabel(sb, formatFlags, builder, embedNode->baseLabelId()));
      ASMJIT_PROPAGATE(sb.append(")"));
      break;
    }

    case BaseNode::kNodeConstPool: {
      const ConstPoolNode* constPoolNode = node->as<ConstPoolNode>();
      ASMJIT_PROPAGATE(sb.appendFormat("[ConstPool Size=%zu Alignment=%zu]", constPoolNode->size(), constPoolNode->alignment()));
      break;
    };

    case BaseNode::kNodeComment: {
      const CommentNode* commentNode = node->as<CommentNode>();
      ASMJIT_PROPAGATE(sb.appendFormat("; %s", commentNode->inlineComment()));
      break;
    }

    case BaseNode::kNodeSentinel: {
      const SentinelNode* sentinelNode = node->as<SentinelNode>();
      const char* sentinelName = nullptr;

      switch (sentinelNode->sentinelType()) {
        case SentinelNode::kSentinelFuncEnd:
          sentinelName = "[FuncEnd]";
          break;

        default:
          sentinelName = "[Sentinel]";
          break;
      }

      ASMJIT_PROPAGATE(sb.append(sentinelName));
      break;
    }

#ifndef ASMJIT_NO_COMPILER
    case BaseNode::kNodeFunc: {
      const FuncNode* funcNode = node->as<FuncNode>();

      ASMJIT_PROPAGATE(formatLabel(sb, formatFlags, builder, funcNode->labelId()));
      ASMJIT_PROPAGATE(sb.append(": "));

      ASMJIT_PROPAGATE(formatFuncRets(sb, formatFlags, builder, funcNode->detail()));
      ASMJIT_PROPAGATE(sb.append(" Func("));
      ASMJIT_PROPAGATE(formatFuncArgs(sb, formatFlags, builder, funcNode->detail(), funcNode->argPacks()));
      ASMJIT_PROPAGATE(sb.append(")"));
      break;
    }

    case BaseNode::kNodeFuncRet: {
      const FuncRetNode* retNode = node->as<FuncRetNode>();
      ASMJIT_PROPAGATE(sb.append("[FuncRet]"));

      for (uint32_t i = 0; i < 2; i++) {
        const Operand_& op = retNode->_opArray[i];
        if (!op.isNone()) {
          ASMJIT_PROPAGATE(sb.append(i == 0 ? " " : ", "));
          ASMJIT_PROPAGATE(formatOperand(sb, formatFlags, builder, builder->arch(), op));
        }
      }
      break;
    }

    case BaseNode::kNodeInvoke: {
      const InvokeNode* invokeNode = node->as<InvokeNode>();
      ASMJIT_PROPAGATE(
        formatInstruction(sb, formatFlags, builder,
          builder->arch(),
          invokeNode->baseInst(), invokeNode->operands(), invokeNode->opCount()));
      break;
    }
#endif

    default: {
      ASMJIT_PROPAGATE(sb.appendFormat("[UserNode:%u]", node->type()));
      break;
    }
  }

  return kErrorOk;
}


Error formatNodeList(
  String& sb,
  uint32_t formatFlags,
  const BaseBuilder* builder) noexcept {

  return formatNodeList(sb, formatFlags, builder, builder->firstNode(), nullptr);
}

Error formatNodeList(
  String& sb,
  uint32_t formatFlags,
  const BaseBuilder* builder,
  const BaseNode* begin,
  const BaseNode* end) noexcept {

  const BaseNode* node = begin;
  while (node != end) {
    ASMJIT_PROPAGATE(formatNode(sb, formatFlags, builder, node));
    ASMJIT_PROPAGATE(sb.append('\n'));
    node = node->next();
  }
  return kErrorOk;
}
#endif

} // {Formatter}

ASMJIT_END_NAMESPACE

#endif
