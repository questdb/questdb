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
#include "../core/emitterutils_p.h"
#include "../core/errorhandler.h"
#include "../core/logger.h"
#include "../core/support.h"

#if !defined(ASMJIT_NO_X86)
  #include "../x86/x86emithelper_p.h"
  #include "../x86/x86instdb_p.h"
#endif // !ASMJIT_NO_X86

#ifdef ASMJIT_BUILD_ARM
  #include "../arm/a64emithelper_p.h"
  #include "../arm/a64instdb.h"
#endif // ASMJIT_BUILD_ARM

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::BaseEmitter - Construction / Destruction]
// ============================================================================

BaseEmitter::BaseEmitter(uint32_t emitterType) noexcept
  : _emitterType(uint8_t(emitterType)) {}

BaseEmitter::~BaseEmitter() noexcept {
  if (_code) {
    _addEmitterFlags(kFlagDestroyed);
    _code->detach(this);
  }
}

// ============================================================================
// [asmjit::BaseEmitter - Finalize]
// ============================================================================

Error BaseEmitter::finalize() {
  // Does nothing by default, overridden by `BaseBuilder` and `BaseCompiler`.
  return kErrorOk;
}

// ============================================================================
// [asmjit::BaseEmitter - Internals]
// ============================================================================

static constexpr uint32_t kEmitterPreservedFlags = BaseEmitter::kFlagOwnLogger | BaseEmitter::kFlagOwnErrorHandler;

static ASMJIT_NOINLINE void BaseEmitter_updateForcedOptions(BaseEmitter* self) noexcept {
  bool emitComments = false;
  bool hasValidationOptions = false;

  if (self->emitterType() == BaseEmitter::kTypeAssembler) {
    // Assembler: Don't emit comments if logger is not attached.
    emitComments = self->_code != nullptr && self->_logger != nullptr;
    hasValidationOptions = self->hasValidationOption(BaseEmitter::kValidationOptionAssembler);
  }
  else {
    // Builder/Compiler: Always emit comments, we cannot assume they won't be used.
    emitComments = self->_code != nullptr;
    hasValidationOptions = self->hasValidationOption(BaseEmitter::kValidationOptionIntermediate);
  }

  if (emitComments)
    self->_addEmitterFlags(BaseEmitter::kFlagLogComments);
  else
    self->_clearEmitterFlags(BaseEmitter::kFlagLogComments);

  // The reserved option tells emitter (Assembler/Builder/Compiler) that there
  // may be either a border case (CodeHolder not attached, for example) or that
  // logging or validation is required.
  if (self->_code == nullptr || self->_logger || hasValidationOptions)
    self->_forcedInstOptions |= BaseInst::kOptionReserved;
  else
    self->_forcedInstOptions &= ~BaseInst::kOptionReserved;
}

// ============================================================================
// [asmjit::BaseEmitter - Validation Options]
// ============================================================================

void BaseEmitter::addValidationOptions(uint32_t options) noexcept {
  _validationOptions = uint8_t(_validationOptions | options);
  BaseEmitter_updateForcedOptions(this);
}

void BaseEmitter::clearValidationOptions(uint32_t options) noexcept {
  _validationOptions = uint8_t(_validationOptions | options);
  BaseEmitter_updateForcedOptions(this);
}

// ============================================================================
// [asmjit::BaseEmitter - Logging]
// ============================================================================

void BaseEmitter::setLogger(Logger* logger) noexcept {
#ifndef ASMJIT_NO_LOGGING
  if (logger) {
    _logger = logger;
    _addEmitterFlags(kFlagOwnLogger);
  }
  else {
    _logger = nullptr;
    _clearEmitterFlags(kFlagOwnLogger);
    if (_code)
      _logger = _code->logger();
  }
  BaseEmitter_updateForcedOptions(this);
#else
  DebugUtils::unused(logger);
#endif
}

// ============================================================================
// [asmjit::BaseEmitter - Error Handling]
// ============================================================================

void BaseEmitter::setErrorHandler(ErrorHandler* errorHandler) noexcept {
  if (errorHandler) {
    _errorHandler = errorHandler;
    _addEmitterFlags(kFlagOwnErrorHandler);
  }
  else {
    _errorHandler = nullptr;
    _clearEmitterFlags(kFlagOwnErrorHandler);
    if (_code)
      _errorHandler = _code->errorHandler();
  }
}

Error BaseEmitter::reportError(Error err, const char* message) {
  ErrorHandler* eh = _errorHandler;
  if (eh) {
    if (!message)
      message = DebugUtils::errorAsString(err);
    eh->handleError(err, message, this);
  }
  return err;
}

// ============================================================================
// [asmjit::BaseEmitter - Labels]
// ============================================================================

Label BaseEmitter::labelByName(const char* name, size_t nameSize, uint32_t parentId) noexcept {
  return Label(_code ? _code->labelIdByName(name, nameSize, parentId) : uint32_t(Globals::kInvalidId));
}

bool BaseEmitter::isLabelValid(uint32_t labelId) const noexcept {
  return _code && labelId < _code->labelCount();
}

// ============================================================================
// [asmjit::BaseEmitter - Emit (Low-Level)]
// ============================================================================

using EmitterUtils::noExt;

Error BaseEmitter::_emitI(uint32_t instId) {
  return _emit(instId, noExt[0], noExt[1], noExt[2], noExt);
}

Error BaseEmitter::_emitI(uint32_t instId, const Operand_& o0) {
  return _emit(instId, o0, noExt[1], noExt[2], noExt);
}

Error BaseEmitter::_emitI(uint32_t instId, const Operand_& o0, const Operand_& o1) {
  return _emit(instId, o0, o1, noExt[2], noExt);
}

Error BaseEmitter::_emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2) {
  return _emit(instId, o0, o1, o2, noExt);
}

Error BaseEmitter::_emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_& o3) {
  Operand_ opExt[3] = { o3 };
  return _emit(instId, o0, o1, o2, opExt);
}

Error BaseEmitter::_emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_& o3, const Operand_& o4) {
  Operand_ opExt[3] = { o3, o4 };
  return _emit(instId, o0, o1, o2, opExt);
}

Error BaseEmitter::_emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_& o3, const Operand_& o4, const Operand_& o5) {
  Operand_ opExt[3] = { o3, o4, o5 };
  return _emit(instId, o0, o1, o2, opExt);
}

Error BaseEmitter::_emitOpArray(uint32_t instId, const Operand_* operands, size_t opCount) {
  const Operand_* op = operands;

  Operand_ opExt[3];

  switch (opCount) {
    case 0:
      return _emit(instId, noExt[0], noExt[1], noExt[2], noExt);

    case 1:
      return _emit(instId, op[0], noExt[1], noExt[2], noExt);

    case 2:
      return _emit(instId, op[0], op[1], noExt[2], noExt);

    case 3:
      return _emit(instId, op[0], op[1], op[2], noExt);

    case 4:
      opExt[0] = op[3];
      opExt[1].reset();
      opExt[2].reset();
      return _emit(instId, op[0], op[1], op[2], opExt);

    case 5:
      opExt[0] = op[3];
      opExt[1] = op[4];
      opExt[2].reset();
      return _emit(instId, op[0], op[1], op[2], opExt);

    case 6:
      return _emit(instId, op[0], op[1], op[2], op + 3);

    default:
      return DebugUtils::errored(kErrorInvalidArgument);
  }
}

// ============================================================================
// [asmjit::BaseEmitter - Emit (High-Level)]
// ============================================================================

ASMJIT_FAVOR_SIZE Error BaseEmitter::emitProlog(const FuncFrame& frame) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

#if !defined(ASMJIT_NO_X86)
  if (environment().isFamilyX86()) {
    x86::EmitHelper emitHelper(this, frame.isAvxEnabled(), frame.isAvx512Enabled());
    return emitHelper.emitProlog(frame);
  }
#endif

#ifdef ASMJIT_BUILD_ARM
  if (environment().isArchAArch64()) {
    a64::EmitHelper emitHelper(this);
    return emitHelper.emitProlog(frame);
  }
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}

ASMJIT_FAVOR_SIZE Error BaseEmitter::emitEpilog(const FuncFrame& frame) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

#if !defined(ASMJIT_NO_X86)
  if (environment().isFamilyX86()) {
    x86::EmitHelper emitHelper(this, frame.isAvxEnabled(), frame.isAvx512Enabled());
    return emitHelper.emitEpilog(frame);
  }
#endif

#ifdef ASMJIT_BUILD_ARM
  if (environment().isArchAArch64()) {
    a64::EmitHelper emitHelper(this);
    return emitHelper.emitEpilog(frame);
  }
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}

ASMJIT_FAVOR_SIZE Error BaseEmitter::emitArgsAssignment(const FuncFrame& frame, const FuncArgsAssignment& args) {
  if (ASMJIT_UNLIKELY(!_code))
    return DebugUtils::errored(kErrorNotInitialized);

#if !defined(ASMJIT_NO_X86)
  if (environment().isFamilyX86()) {
    x86::EmitHelper emitHelper(this, frame.isAvxEnabled(), frame.isAvx512Enabled());
    return emitHelper.emitArgsAssignment(frame, args);
  }
#endif

#ifdef ASMJIT_BUILD_ARM
  if (environment().isArchAArch64()) {
    a64::EmitHelper emitHelper(this);
    return emitHelper.emitArgsAssignment(frame, args);
  }
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}

// ============================================================================
// [asmjit::BaseEmitter - Comment]
// ============================================================================

Error BaseEmitter::commentf(const char* fmt, ...) {
  if (!hasEmitterFlag(kFlagLogComments)) {
    if (!hasEmitterFlag(kFlagAttached))
      return reportError(DebugUtils::errored(kErrorNotInitialized));
    return kErrorOk;
  }

#ifndef ASMJIT_NO_LOGGING
  StringTmp<1024> sb;

  va_list ap;
  va_start(ap, fmt);
  Error err = sb.appendVFormat(fmt, ap);
  va_end(ap);

  ASMJIT_PROPAGATE(err);
  return comment(sb.data(), sb.size());
#else
  DebugUtils::unused(fmt);
  return kErrorOk;
#endif
}

Error BaseEmitter::commentv(const char* fmt, va_list ap) {
  if (!hasEmitterFlag(kFlagLogComments)) {
    if (!hasEmitterFlag(kFlagAttached))
      return reportError(DebugUtils::errored(kErrorNotInitialized));
    return kErrorOk;
  }

#ifndef ASMJIT_NO_LOGGING
  StringTmp<1024> sb;
  Error err = sb.appendVFormat(fmt, ap);

  ASMJIT_PROPAGATE(err);
  return comment(sb.data(), sb.size());
#else
  DebugUtils::unused(fmt, ap);
  return kErrorOk;
#endif
}

// ============================================================================
// [asmjit::BaseEmitter - Events]
// ============================================================================

Error BaseEmitter::onAttach(CodeHolder* code) noexcept {
  _code = code;
  _environment = code->environment();
  _addEmitterFlags(kFlagAttached);

  const ArchTraits& archTraits = ArchTraits::byArch(code->arch());
  uint32_t nativeRegType = Environment::is32Bit(code->arch()) ? BaseReg::kTypeGp32 : BaseReg::kTypeGp64;
  _gpRegInfo.setSignature(archTraits._regInfo[nativeRegType].signature());

  onSettingsUpdated();
  return kErrorOk;
}

Error BaseEmitter::onDetach(CodeHolder* code) noexcept {
  DebugUtils::unused(code);

  if (!hasOwnLogger())
    _logger = nullptr;

  if (!hasOwnErrorHandler())
    _errorHandler = nullptr;

  _clearEmitterFlags(~kEmitterPreservedFlags);
  _forcedInstOptions = BaseInst::kOptionReserved;
  _privateData = 0;

  _environment.reset();
  _gpRegInfo.reset();

  _instOptions = 0;
  _extraReg.reset();
  _inlineComment = nullptr;

  return kErrorOk;
}

void BaseEmitter::onSettingsUpdated() noexcept {
  // Only called when attached to CodeHolder by CodeHolder.
  ASMJIT_ASSERT(_code != nullptr);

  if (!hasOwnLogger())
    _logger = _code->logger();

  if (!hasOwnErrorHandler())
    _errorHandler = _code->errorHandler();

  BaseEmitter_updateForcedOptions(this);
}

ASMJIT_END_NAMESPACE
