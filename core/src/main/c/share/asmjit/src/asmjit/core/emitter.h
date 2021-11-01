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

#ifndef ASMJIT_CORE_EMITTER_H_INCLUDED
#define ASMJIT_CORE_EMITTER_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/codeholder.h"
#include "../core/inst.h"
#include "../core/operand.h"
#include "../core/type.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [Forward Declarations]
// ============================================================================

class ConstPool;
class FuncFrame;
class FuncArgsAssignment;

// ============================================================================
// [asmjit::BaseEmitter]
// ============================================================================

//! Provides a base foundation to emit code - specialized by `Assembler` and
//! `BaseBuilder`.
class ASMJIT_VIRTAPI BaseEmitter {
public:
  ASMJIT_BASE_CLASS(BaseEmitter)

  //! See \ref EmitterType.
  uint8_t _emitterType = 0;
  //! See \ref BaseEmitter::EmitterFlags.
  uint8_t _emitterFlags = 0;
  //! Validation flags in case validation is used, see \ref InstAPI::ValidationFlags.
  //!
  //! \note Validation flags are specific to the emitter and they are setup at
  //! construction time and then never changed.
  uint8_t _validationFlags = 0;
  //! Validation options, see \ref ValidationOptions.
  uint8_t _validationOptions = 0;

  //! Encoding options, see \ref EncodingOptions.
  uint32_t _encodingOptions = 0;

  //! Forced instruction options, combined with \ref _instOptions by \ref emit().
  uint32_t _forcedInstOptions = BaseInst::kOptionReserved;
  //! Internal private data used freely by any emitter.
  uint32_t _privateData = 0;

  //! CodeHolder the emitter is attached to.
  CodeHolder* _code = nullptr;
  //! Attached \ref Logger.
  Logger* _logger = nullptr;
  //! Attached \ref ErrorHandler.
  ErrorHandler* _errorHandler = nullptr;

  //! Describes the target environment, matches \ref CodeHolder::environment().
  Environment _environment {};
  //! Native GP register signature and signature related information.
  RegInfo _gpRegInfo {};

  //! Next instruction options (affects the next instruction).
  uint32_t _instOptions = 0;
  //! Extra register (op-mask {k} on AVX-512) (affects the next instruction).
  RegOnly _extraReg {};
  //! Inline comment of the next instruction (affects the next instruction).
  const char* _inlineComment = nullptr;

  //! Emitter type.
  enum EmitterType : uint32_t {
    //! Unknown or uninitialized.
    kTypeNone = 0,
    //! Emitter inherits from \ref BaseAssembler.
    kTypeAssembler = 1,
    //! Emitter inherits from \ref BaseBuilder.
    kTypeBuilder = 2,
    //! Emitter inherits from \ref BaseCompiler.
    kTypeCompiler = 3,

    //! Count of emitter types.
    kTypeCount = 4
  };

  //! Emitter flags.
  enum EmitterFlags : uint32_t {
    //! Emitter is attached to CodeHolder.
    kFlagAttached = 0x01u,
    //! The emitter must emit comments.
    kFlagLogComments = 0x08u,
    //! The emitter has its own \ref Logger (not propagated from \ref CodeHolder).
    kFlagOwnLogger = 0x10u,
    //! The emitter has its own \ref ErrorHandler (not propagated from \ref CodeHolder).
    kFlagOwnErrorHandler = 0x20u,
    //! The emitter was finalized.
    kFlagFinalized = 0x40u,
    //! The emitter was destroyed.
    kFlagDestroyed = 0x80u
  };

  //! Encoding options.
  enum EncodingOptions : uint32_t {
    //! Emit instructions that are optimized for size, if possible.
    //!
    //! Default: false.
    //!
    //! X86 Specific
    //! ------------
    //!
    //! When this option is set it the assembler will try to fix instructions
    //! if possible into operation equivalent instructions that take less bytes
    //! by taking advantage of implicit zero extension. For example instruction
    //! like `mov r64, imm` and `and r64, imm` can be translated to `mov r32, imm`
    //! and `and r32, imm` when the immediate constant is lesser than `2^31`.
    kEncodingOptionOptimizeForSize = 0x00000001u,

    //! Emit optimized code-alignment sequences.
    //!
    //! Default: false.
    //!
    //! X86 Specific
    //! ------------
    //!
    //! Default align sequence used by X86 architecture is one-byte (0x90)
    //! opcode that is often shown by disassemblers as NOP. However there are
    //! more optimized align sequences for 2-11 bytes that may execute faster
    //! on certain CPUs. If this feature is enabled AsmJit will generate
    //! specialized sequences for alignment between 2 to 11 bytes.
    kEncodingOptionOptimizedAlign = 0x00000002u,

    //! Emit jump-prediction hints.
    //!
    //! Default: false.
    //!
    //! X86 Specific
    //! ------------
    //!
    //! Jump prediction is usually based on the direction of the jump. If the
    //! jump is backward it is usually predicted as taken; and if the jump is
    //! forward it is usually predicted as not-taken. The reason is that loops
    //! generally use backward jumps and conditions usually use forward jumps.
    //! However this behavior can be overridden by using instruction prefixes.
    //! If this option is enabled these hints will be emitted.
    //!
    //! This feature is disabled by default, because the only processor that
    //! used to take into consideration prediction hints was P4. Newer processors
    //! implement heuristics for branch prediction and ignore static hints. This
    //! means that this feature can be only used for annotation purposes.
    kEncodingOptionPredictedJumps = 0x00000010u
  };

#ifndef ASMJIT_NO_DEPRECATED
  enum EmitterOptions : uint32_t {
    kOptionOptimizedForSize = kEncodingOptionOptimizeForSize,
    kOptionOptimizedAlign = kEncodingOptionOptimizedAlign,
    kOptionPredictedJumps = kEncodingOptionPredictedJumps
  };
#endif

  //! Validation options are used to tell emitters to perform strict validation
  //! of instructions passed to \ref emit().
  //!
  //! \ref BaseAssembler implementation perform by default only basic checks
  //! that are necessary to identify all variations of an instruction so the
  //! correct encoding can be selected. This is fine for production-ready code
  //! as the assembler doesn't have to perform checks that would slow it down.
  //! However, sometimes these checks are beneficial especially when the project
  //! that uses AsmJit is in a development phase, in which mistakes happen often.
  //! To make the experience of using AsmJit seamless it offers validation
  //! features that can be controlled by `ValidationOptions`.
  enum ValidationOptions : uint32_t {
    //! Perform strict validation in \ref BaseAssembler::emit() implementations.
    //!
    //! This flag ensures that each instruction is checked before it's encoded
    //! into a binary representation. This flag is only relevant for \ref
    //! BaseAssembler implementations, but can be set in any other emitter type,
    //! in that case if that emitter needs to create an assembler on its own,
    //! for the purpose of \ref finalize()  it would propagate this flag to such
    //! assembler so all instructions passed to it are explicitly validated.
    //!
    //! Default: false.
    kValidationOptionAssembler = 0x00000001u,

    //! Perform strict validation in \ref BaseBuilder::emit() and \ref
    //! BaseCompiler::emit() implementations.
    //!
    //! This flag ensures that each instruction is checked before an \ref
    //! InstNode representing the instruction is created by Builder or Compiler.
    //!
    //! Default: false.
    kValidationOptionIntermediate = 0x00000002u
  };

  //! \name Construction & Destruction
  //! \{

  ASMJIT_API explicit BaseEmitter(uint32_t emitterType) noexcept;
  ASMJIT_API virtual ~BaseEmitter() noexcept;

  //! \}

  //! \name Cast
  //! \{

  template<typename T>
  inline T* as() noexcept { return reinterpret_cast<T*>(this); }

  template<typename T>
  inline const T* as() const noexcept { return reinterpret_cast<const T*>(this); }

  //! \}

  //! \name Emitter Type & Flags
  //! \{

  //! Returns the type of this emitter, see `EmitterType`.
  inline uint32_t emitterType() const noexcept { return _emitterType; }
  //! Returns emitter flags , see `Flags`.
  inline uint32_t emitterFlags() const noexcept { return _emitterFlags; }

  //! Tests whether the emitter inherits from `BaseAssembler`.
  inline bool isAssembler() const noexcept { return _emitterType == kTypeAssembler; }
  //! Tests whether the emitter inherits from `BaseBuilder`.
  //!
  //! \note Both Builder and Compiler emitters would return `true`.
  inline bool isBuilder() const noexcept { return _emitterType >= kTypeBuilder; }
  //! Tests whether the emitter inherits from `BaseCompiler`.
  inline bool isCompiler() const noexcept { return _emitterType == kTypeCompiler; }

  //! Tests whether the emitter has the given `flag` enabled.
  inline bool hasEmitterFlag(uint32_t flag) const noexcept { return (_emitterFlags & flag) != 0; }
  //! Tests whether the emitter is finalized.
  inline bool isFinalized() const noexcept { return hasEmitterFlag(kFlagFinalized); }
  //! Tests whether the emitter is destroyed (only used during destruction).
  inline bool isDestroyed() const noexcept { return hasEmitterFlag(kFlagDestroyed); }

  inline void _addEmitterFlags(uint32_t flags) noexcept { _emitterFlags = uint8_t(_emitterFlags | flags); }
  inline void _clearEmitterFlags(uint32_t flags) noexcept { _emitterFlags = uint8_t(_emitterFlags & ~flags); }

  //! \}

  //! \name Target Information
  //! \{

  //! Returns the CodeHolder this emitter is attached to.
  inline CodeHolder* code() const noexcept { return _code; }

  //! Returns the target environment, see \ref Environment.
  //!
  //! The returned \ref Environment reference matches \ref CodeHolder::environment().
  inline const Environment& environment() const noexcept { return _environment; }

  //! Tests whether the target architecture is 32-bit.
  inline bool is32Bit() const noexcept { return environment().is32Bit(); }
  //! Tests whether the target architecture is 64-bit.
  inline bool is64Bit() const noexcept { return environment().is64Bit(); }

  //! Returns the target architecture type.
  inline uint32_t arch() const noexcept { return environment().arch(); }
  //! Returns the target architecture sub-type.
  inline uint32_t subArch() const noexcept { return environment().subArch(); }

  //! Returns the target architecture's GP register size (4 or 8 bytes).
  inline uint32_t registerSize() const noexcept { return environment().registerSize(); }

  //! \}

  //! \name Initialization & Finalization
  //! \{

  //! Tests whether the emitter is initialized (i.e. attached to \ref CodeHolder).
  inline bool isInitialized() const noexcept { return _code != nullptr; }

  //! Finalizes this emitter.
  //!
  //! Materializes the content of the emitter by serializing it to the attached
  //! \ref CodeHolder through an architecture specific \ref BaseAssembler. This
  //! function won't do anything if the emitter inherits from \ref BaseAssembler
  //! as assemblers emit directly to a \ref CodeBuffer held by \ref CodeHolder.
  //! However, if this is an emitter that inherits from \ref BaseBuilder or \ref
  //! BaseCompiler then these emitters need the materialization phase as they
  //! store their content in a representation not visible to \ref CodeHolder.
  ASMJIT_API virtual Error finalize();

  //! \}

  //! \name Logging
  //! \{

  //! Tests whether the emitter has a logger.
  inline bool hasLogger() const noexcept { return _logger != nullptr; }

  //! Tests whether the emitter has its own logger.
  //!
  //! Own logger means that it overrides the possible logger that may be used
  //! by \ref CodeHolder this emitter is attached to.
  inline bool hasOwnLogger() const noexcept { return hasEmitterFlag(kFlagOwnLogger); }

  //! Returns the logger this emitter uses.
  //!
  //! The returned logger is either the emitter's own logger or it's logger
  //! used by \ref CodeHolder this emitter is attached to.
  inline Logger* logger() const noexcept { return _logger; }

  //! Sets or resets the logger of the emitter.
  //!
  //! If the `logger` argument is non-null then the logger will be considered
  //! emitter's own logger, see \ref hasOwnLogger() for more details. If the
  //! given `logger` is null then the emitter will automatically use logger
  //! that is attached to the \ref CodeHolder this emitter is attached to.
  ASMJIT_API void setLogger(Logger* logger) noexcept;

  //! Resets the logger of this emitter.
  //!
  //! The emitter will bail to using a logger attached to \ref CodeHolder this
  //! emitter is attached to, or no logger at all if \ref CodeHolder doesn't
  //! have one.
  inline void resetLogger() noexcept { return setLogger(nullptr); }

  //! \}

  //! \name Error Handling
  //! \{

  //! Tests whether the emitter has an error handler attached.
  inline bool hasErrorHandler() const noexcept { return _errorHandler != nullptr; }

  //! Tests whether the emitter has its own error handler.
  //!
  //! Own error handler means that it overrides the possible error handler that
  //! may be used by \ref CodeHolder this emitter is attached to.
  inline bool hasOwnErrorHandler() const noexcept { return hasEmitterFlag(kFlagOwnErrorHandler); }

  //! Returns the error handler this emitter uses.
  //!
  //! The returned error handler is either the emitter's own error handler or
  //! it's error handler used by \ref CodeHolder this emitter is attached to.
  inline ErrorHandler* errorHandler() const noexcept { return _errorHandler; }

  //! Sets or resets the error handler of the emitter.
  ASMJIT_API void setErrorHandler(ErrorHandler* errorHandler) noexcept;

  //! Resets the error handler.
  inline void resetErrorHandler() noexcept { setErrorHandler(nullptr); }

  //! Handles the given error in the following way:
  //!   1. If the emitter has \ref ErrorHandler attached, it calls its
  //!      \ref ErrorHandler::handleError() member function first, and
  //!      then returns the error. The `handleError()` function may throw.
  //!   2. if the emitter doesn't have \ref ErrorHandler, the error is
  //!      simply returned.
  ASMJIT_API Error reportError(Error err, const char* message = nullptr);

  //! \}

  //! \name Encoding Options
  //! \{

  //! Returns encoding options, see \ref EncodingOptions.
  inline uint32_t encodingOptions() const noexcept { return _encodingOptions; }
  //! Tests whether the encoding `option` is set.
  inline bool hasEncodingOption(uint32_t option) const noexcept { return (_encodingOptions & option) != 0; }

  //! Enables the given encoding `options`, see \ref EncodingOptions.
  inline void addEncodingOptions(uint32_t options) noexcept { _encodingOptions |= options; }
  //! Disables the given encoding `options`, see \ref EncodingOptions.
  inline void clearEncodingOptions(uint32_t options) noexcept { _encodingOptions &= ~options; }

  //! \}

  //! \name Validation Options
  //! \{

  //! Returns the emitter's validation options, see \ref ValidationOptions.
  inline uint32_t validationOptions() const noexcept {
    return _validationOptions;
  }

  //! Tests whether the given `option` is present in validation options.
  inline bool hasValidationOption(uint32_t option) const noexcept {
    return (_validationOptions & option) != 0;
  }

  //! Activates the given validation `options`, see \ref ValidationOptions.
  //!
  //! This function is used to activate explicit validation options that will
  //! be then used by all emitter implementations. There are in general two
  //! possibilities:
  //!
  //!   - Architecture specific assembler is used. In this case a
  //!     \ref kValidationOptionAssembler can be used to turn on explicit
  //!     validation that will be used before an instruction is emitted.
  //!     This means that internally an extra step will be performed to
  //!     make sure that the instruction is correct. This is needed, because
  //!     by default assemblers prefer speed over strictness.
  //!
  //!     This option should be used in debug builds as it's pretty expensive.
  //!
  //!   - Architecture specific builder or compiler is used. In this case
  //!     the user can turn on \ref kValidationOptionIntermediate option
  //!     that adds explicit validation step before the Builder or Compiler
  //!     creates an \ref InstNode to represent an emitted instruction. Error
  //!     will be returned if the instruction is ill-formed. In addition,
  //!     also \ref kValidationOptionAssembler can be used, which would not be
  //!     consumed by Builder / Compiler directly, but it would be propagated
  //!     to an architecture specific \ref BaseAssembler implementation it
  //!     creates during \ref BaseEmitter::finalize().
  ASMJIT_API void addValidationOptions(uint32_t options) noexcept;

  //! Deactivates the given validation `options`.
  //!
  //! See \ref addValidationOptions() and \ref ValidationOptions for more details.
  ASMJIT_API void clearValidationOptions(uint32_t options) noexcept;

  //! \}

  //! \name Instruction Options
  //! \{

  //! Returns forced instruction options.
  //!
  //! Forced instruction options are merged with next instruction options before
  //! the instruction is encoded. These options have some bits reserved that are
  //! used by error handling, logging, and instruction validation purposes. Other
  //! options are globals that affect each instruction.
  inline uint32_t forcedInstOptions() const noexcept { return _forcedInstOptions; }

  //! Returns options of the next instruction.
  inline uint32_t instOptions() const noexcept { return _instOptions; }
  //! Returns options of the next instruction.
  inline void setInstOptions(uint32_t options) noexcept { _instOptions = options; }
  //! Adds options of the next instruction.
  inline void addInstOptions(uint32_t options) noexcept { _instOptions |= options; }
  //! Resets options of the next instruction.
  inline void resetInstOptions() noexcept { _instOptions = 0; }

  //! Tests whether the extra register operand is valid.
  inline bool hasExtraReg() const noexcept { return _extraReg.isReg(); }
  //! Returns an extra operand that will be used by the next instruction (architecture specific).
  inline const RegOnly& extraReg() const noexcept { return _extraReg; }
  //! Sets an extra operand that will be used by the next instruction (architecture specific).
  inline void setExtraReg(const BaseReg& reg) noexcept { _extraReg.init(reg); }
  //! Sets an extra operand that will be used by the next instruction (architecture specific).
  inline void setExtraReg(const RegOnly& reg) noexcept { _extraReg.init(reg); }
  //! Resets an extra operand that will be used by the next instruction (architecture specific).
  inline void resetExtraReg() noexcept { _extraReg.reset(); }

  //! Returns comment/annotation of the next instruction.
  inline const char* inlineComment() const noexcept { return _inlineComment; }
  //! Sets comment/annotation of the next instruction.
  //!
  //! \note This string is set back to null by `_emit()`, but until that it has
  //! to remain valid as the Emitter is not required to make a copy of it (and
  //! it would be slow to do that for each instruction).
  inline void setInlineComment(const char* s) noexcept { _inlineComment = s; }
  //! Resets the comment/annotation to nullptr.
  inline void resetInlineComment() noexcept { _inlineComment = nullptr; }

  //! \}

  //! \name Sections
  //! \{

  virtual Error section(Section* section) = 0;

  //! \}

  //! \name Labels
  //! \{

  //! Creates a new label.
  virtual Label newLabel() = 0;
  //! Creates a new named label.
  virtual Label newNamedLabel(const char* name, size_t nameSize = SIZE_MAX, uint32_t type = Label::kTypeGlobal, uint32_t parentId = Globals::kInvalidId) = 0;

  //! Creates a new external label.
  inline Label newExternalLabel(const char* name, size_t nameSize = SIZE_MAX) {
    return newNamedLabel(name, nameSize, Label::kTypeExternal);
  }

  //! Returns `Label` by `name`.
  //!
  //! Returns invalid Label in case that the name is invalid or label was not found.
  //!
  //! \note This function doesn't trigger ErrorHandler in case the name is invalid
  //! or no such label exist. You must always check the validity of the `Label` returned.
  ASMJIT_API Label labelByName(const char* name, size_t nameSize = SIZE_MAX, uint32_t parentId = Globals::kInvalidId) noexcept;

  //! Binds the `label` to the current position of the current section.
  //!
  //! \note Attempt to bind the same label multiple times will return an error.
  virtual Error bind(const Label& label) = 0;

  //! Tests whether the label `id` is valid (i.e. registered).
  ASMJIT_API bool isLabelValid(uint32_t labelId) const noexcept;
  //! Tests whether the `label` is valid (i.e. registered).
  inline bool isLabelValid(const Label& label) const noexcept { return isLabelValid(label.id()); }

  //! \}

  //! \name Emit
  //! \{

  // NOTE: These `emit()` helpers are designed to address a code-bloat generated
  // by C++ compilers to call a function having many arguments. Each parameter to
  // `_emit()` requires some code to pass it, which means that if we default to
  // 5 arguments in `_emit()` and instId the C++ compiler would have to generate
  // a virtual function call having 5 parameters and additional `this` argument,
  // which is quite a lot. Since by default most instructions have 2 to 3 operands
  // it's better to introduce helpers that pass from 0 to 6 operands that help to
  // reduce the size of emit(...) function call.

  //! Emits an instruction (internal).
  ASMJIT_API Error _emitI(uint32_t instId);
  //! \overload
  ASMJIT_API Error _emitI(uint32_t instId, const Operand_& o0);
  //! \overload
  ASMJIT_API Error _emitI(uint32_t instId, const Operand_& o0, const Operand_& o1);
  //! \overload
  ASMJIT_API Error _emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2);
  //! \overload
  ASMJIT_API Error _emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_& o3);
  //! \overload
  ASMJIT_API Error _emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_& o3, const Operand_& o4);
  //! \overload
  ASMJIT_API Error _emitI(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_& o3, const Operand_& o4, const Operand_& o5);

  //! Emits an instruction `instId` with the given `operands`.
  template<typename... Args>
  ASMJIT_INLINE Error emit(uint32_t instId, Args&&... operands) {
    return _emitI(instId, Support::ForwardOp<Args>::forward(operands)...);
  }

  inline Error emitOpArray(uint32_t instId, const Operand_* operands, size_t opCount) {
    return _emitOpArray(instId, operands, opCount);
  }

  inline Error emitInst(const BaseInst& inst, const Operand_* operands, size_t opCount) {
    setInstOptions(inst.options());
    setExtraReg(inst.extraReg());
    return _emitOpArray(inst.id(), operands, opCount);
  }

  //! \cond INTERNAL
  //! Emits an instruction - all 6 operands must be defined.
  virtual Error _emit(uint32_t instId, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* oExt) = 0;
  //! Emits instruction having operands stored in array.
  ASMJIT_API virtual Error _emitOpArray(uint32_t instId, const Operand_* operands, size_t opCount);
  //! \endcond

  //! \}

  //! \name Emit Utilities
  //! \{

  ASMJIT_API Error emitProlog(const FuncFrame& frame);
  ASMJIT_API Error emitEpilog(const FuncFrame& frame);
  ASMJIT_API Error emitArgsAssignment(const FuncFrame& frame, const FuncArgsAssignment& args);

  //! \}

  //! \name Align
  //! \{

  //! Aligns the current CodeBuffer position to the `alignment` specified.
  //!
  //! The sequence that is used to fill the gap between the aligned location
  //! and the current location depends on the align `mode`, see \ref AlignMode.
  virtual Error align(uint32_t alignMode, uint32_t alignment) = 0;

  //! \}

  //! \name Embed
  //! \{

  //! Embeds raw data into the \ref CodeBuffer.
  virtual Error embed(const void* data, size_t dataSize) = 0;

  //! Embeds a typed data array.
  //!
  //! This is the most flexible function for embedding data as it allows to:
  //!   - Assign a `typeId` to the data, so the emitter knows the type of
  //!     items stored in `data`. Binary data should use \ref Type::kIdU8.
  //!   - Repeat the given data `repeatCount` times, so the data can be used
  //!     as a fill pattern for example, or as a pattern used by SIMD instructions.
  virtual Error embedDataArray(uint32_t typeId, const void* data, size_t itemCount, size_t repeatCount = 1) = 0;

  //! Embeds int8_t `value` repeated by `repeatCount`.
  inline Error embedInt8(int8_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdI8, &value, 1, repeatCount); }
  //! Embeds uint8_t `value` repeated by `repeatCount`.
  inline Error embedUInt8(uint8_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdU8, &value, 1, repeatCount); }
  //! Embeds int16_t `value` repeated by `repeatCount`.
  inline Error embedInt16(int16_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdI16, &value, 1, repeatCount); }
  //! Embeds uint16_t `value` repeated by `repeatCount`.
  inline Error embedUInt16(uint16_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdU16, &value, 1, repeatCount); }
  //! Embeds int32_t `value` repeated by `repeatCount`.
  inline Error embedInt32(int32_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdI32, &value, 1, repeatCount); }
  //! Embeds uint32_t `value` repeated by `repeatCount`.
  inline Error embedUInt32(uint32_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdU32, &value, 1, repeatCount); }
  //! Embeds int64_t `value` repeated by `repeatCount`.
  inline Error embedInt64(int64_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdI64, &value, 1, repeatCount); }
  //! Embeds uint64_t `value` repeated by `repeatCount`.
  inline Error embedUInt64(uint64_t value, size_t repeatCount = 1) { return embedDataArray(Type::kIdU64, &value, 1, repeatCount); }
  //! Embeds a floating point `value` repeated by `repeatCount`.
  inline Error embedFloat(float value, size_t repeatCount = 1) { return embedDataArray(Type::kIdF32, &value, 1, repeatCount); }
  //! Embeds a floating point `value` repeated by `repeatCount`.
  inline Error embedDouble(double value, size_t repeatCount = 1) { return embedDataArray(Type::IdOfT<double>::kTypeId, &value, 1, repeatCount); }

  //! Embeds a constant pool at the current offset by performing the following:
  //!   1. Aligns by using kAlignData to the minimum `pool` alignment.
  //!   2. Binds the ConstPool label so it's bound to an aligned location.
  //!   3. Emits ConstPool content.
  virtual Error embedConstPool(const Label& label, const ConstPool& pool) = 0;

  //! Embeds an absolute `label` address as data.
  //!
  //! The `dataSize` is an optional argument that can be used to specify the
  //! size of the address data. If it's zero (default) the address size is
  //! deduced from the target architecture (either 4 or 8 bytes).
  virtual Error embedLabel(const Label& label, size_t dataSize = 0) = 0;

  //! Embeds a delta (distance) between the `label` and `base` calculating it
  //! as `label - base`. This function was designed to make it easier to embed
  //! lookup tables where each index is a relative distance of two labels.
  virtual Error embedLabelDelta(const Label& label, const Label& base, size_t dataSize = 0) = 0;

  //! \}

  //! \name Comment
  //! \{

  //! Emits a comment stored in `data` with an optional `size` parameter.
  virtual Error comment(const char* data, size_t size = SIZE_MAX) = 0;

  //! Emits a formatted comment specified by `fmt` and variable number of arguments.
  ASMJIT_API Error commentf(const char* fmt, ...);
  //! Emits a formatted comment specified by `fmt` and `ap`.
  ASMJIT_API Error commentv(const char* fmt, va_list ap);

  //! \}

  //! \name Events
  //! \{

  //! Called after the emitter was attached to `CodeHolder`.
  virtual Error onAttach(CodeHolder* code) noexcept = 0;
  //! Called after the emitter was detached from `CodeHolder`.
  virtual Error onDetach(CodeHolder* code) noexcept = 0;

  //! Called when \ref CodeHolder has updated an important setting, which
  //! involves the following:
  //!
  //!   - \ref Logger has been changed (\ref CodeHolder::setLogger() has been
  //!     called).
  //!   - \ref ErrorHandler has been changed (\ref CodeHolder::setErrorHandler()
  //!     has been called).
  //!
  //! This function ensures that the settings are properly propagated from
  //! \ref CodeHolder to the emitter.
  //!
  //! \note This function is virtual and can be overridden, however, if you
  //! do so, always call \ref BaseEmitter::onSettingsUpdated() within your
  //! own implementation to ensure that the emitter is in a consisten state.
  ASMJIT_API virtual void onSettingsUpdated() noexcept;

  //! \}

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("Use environment() instead")
  inline CodeInfo codeInfo() const noexcept {
    return CodeInfo(_environment, _code ? _code->baseAddress() : Globals::kNoBaseAddress);
  }

  ASMJIT_DEPRECATED("Use arch() instead")
  inline uint32_t archId() const noexcept { return arch(); }

  ASMJIT_DEPRECATED("Use registerSize() instead")
  inline uint32_t gpSize() const noexcept { return registerSize(); }

  ASMJIT_DEPRECATED("Use encodingOptions() instead")
  inline uint32_t emitterOptions() const noexcept { return encodingOptions(); }

  ASMJIT_DEPRECATED("Use addEncodingOptions() instead")
  inline void addEmitterOptions(uint32_t options) noexcept { addEncodingOptions(options); }

  ASMJIT_DEPRECATED("Use clearEncodingOptions() instead")
  inline void clearEmitterOptions(uint32_t options) noexcept { clearEncodingOptions(options); }

  ASMJIT_DEPRECATED("Use forcedInstOptions() instead")
  inline uint32_t globalInstOptions() const noexcept { return forcedInstOptions(); }
#endif // !ASMJIT_NO_DEPRECATED
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_EMITTER_H_INCLUDED
