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

#ifndef ASMJIT_CORE_COMPILER_H_INCLUDED
#define ASMJIT_CORE_COMPILER_H_INCLUDED

#include "../core/api-config.h"
#ifndef ASMJIT_NO_COMPILER

#include "../core/assembler.h"
#include "../core/builder.h"
#include "../core/constpool.h"
#include "../core/compilerdefs.h"
#include "../core/func.h"
#include "../core/inst.h"
#include "../core/operand.h"
#include "../core/support.h"
#include "../core/zone.h"
#include "../core/zonevector.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [Forward Declarations]
// ============================================================================

class JumpAnnotation;
class JumpNode;
class FuncNode;
class FuncRetNode;
class InvokeNode;

//! \addtogroup asmjit_compiler
//! \{

// ============================================================================
// [asmjit::BaseCompiler]
// ============================================================================

//! Code emitter that uses virtual registers and performs register allocation.
//!
//! Compiler is a high-level code-generation tool that provides register
//! allocation and automatic handling of function calling conventions. It was
//! primarily designed for merging multiple parts of code into a function
//! without worrying about registers and function calling conventions.
//!
//! BaseCompiler can be used, with a minimum effort, to handle 32-bit and
//! 64-bit code generation within a single code base.
//!
//! BaseCompiler is based on BaseBuilder and contains all the features it
//! provides. It means that the code it stores can be modified (removed, added,
//! injected) and analyzed. When the code is finalized the compiler can emit
//! the code into an Assembler to translate the abstract representation into a
//! machine code.
//!
//! Check out architecture specific compilers for more details and examples:
//!
//!   - \ref x86::Compiler - X86/X64 compiler implementation.
class ASMJIT_VIRTAPI BaseCompiler : public BaseBuilder {
public:
  ASMJIT_NONCOPYABLE(BaseCompiler)
  typedef BaseBuilder Base;

  //! Current function.
  FuncNode* _func;
  //! Allocates `VirtReg` objects.
  Zone _vRegZone;
  //! Stores array of `VirtReg` pointers.
  ZoneVector<VirtReg*> _vRegArray;
  //! Stores jump annotations.
  ZoneVector<JumpAnnotation*> _jumpAnnotations;

  //! Local constant pool, flushed at the end of each function.
  ConstPoolNode* _localConstPool;
  //! Global constant pool, flushed by `finalize()`.
  ConstPoolNode* _globalConstPool;

  //! \name Construction & Destruction
  //! \{

  //! Creates a new `BaseCompiler` instance.
  ASMJIT_API BaseCompiler() noexcept;
  //! Destroys the `BaseCompiler` instance.
  ASMJIT_API virtual ~BaseCompiler() noexcept;

  //! \}

  //! \name Function Management
  //! \{

  //! Returns the current function.
  inline FuncNode* func() const noexcept { return _func; }

  //! Creates a new \ref FuncNode.
  ASMJIT_API Error _newFuncNode(FuncNode** out, const FuncSignature& signature);
  //! Creates a new \ref FuncNode adds it to the compiler.
  ASMJIT_API Error _addFuncNode(FuncNode** out, const FuncSignature& signature);

  //! Creates a new \ref FuncRetNode.
  ASMJIT_API Error _newRetNode(FuncRetNode** out, const Operand_& o0, const Operand_& o1);
  //! Creates a new \ref FuncRetNode and adds it to the compiler.
  ASMJIT_API Error _addRetNode(FuncRetNode** out, const Operand_& o0, const Operand_& o1);

  //! Creates a new \ref FuncNode with the given `signature` and returns it.
  inline FuncNode* newFunc(const FuncSignature& signature) {
    FuncNode* node;
    _newFuncNode(&node, signature);
    return node;
  }

  //! Creates a new \ref FuncNode with the given `signature`, adds it to the
  //! compiler by using the \ref addFunc(FuncNode*) overload, and returns it.
  inline FuncNode* addFunc(const FuncSignature& signature) {
    FuncNode* node;
    _addFuncNode(&node, signature);
    return node;
  }

  //! Adds a function `node` to the instruction stream.
  ASMJIT_API FuncNode* addFunc(FuncNode* func);
  //! Emits a sentinel that marks the end of the current function.
  ASMJIT_API Error endFunc();

  ASMJIT_API Error _setArg(size_t argIndex, size_t valueIndex, const BaseReg& reg);

  //! Sets a function argument at `argIndex` to `reg`.
  inline Error setArg(size_t argIndex, const BaseReg& reg) { return _setArg(argIndex, 0, reg); }
  //! Sets a function argument at `argIndex` at `valueIndex` to `reg`.
  inline Error setArg(size_t argIndex, size_t valueIndex, const BaseReg& reg) { return _setArg(argIndex, valueIndex, reg); }

  inline FuncRetNode* newRet(const Operand_& o0, const Operand_& o1) {
    FuncRetNode* node;
    _newRetNode(&node, o0, o1);
    return node;
  }

  inline FuncRetNode* addRet(const Operand_& o0, const Operand_& o1) {
    FuncRetNode* node;
    _addRetNode(&node, o0, o1);
    return node;
  }

  //! \}

  //! \name Function Invocation
  //! \{

  //! Creates a new \ref InvokeNode.
  ASMJIT_API Error _newInvokeNode(InvokeNode** out, uint32_t instId, const Operand_& o0, const FuncSignature& signature);
  //! Creates a new \ref InvokeNode and adds it to Compiler.
  ASMJIT_API Error _addInvokeNode(InvokeNode** out, uint32_t instId, const Operand_& o0, const FuncSignature& signature);

  //! Creates a new `InvokeNode`.
  inline InvokeNode* newCall(uint32_t instId, const Operand_& o0, const FuncSignature& signature) {
    InvokeNode* node;
    _newInvokeNode(&node, instId, o0, signature);
    return node;
  }

  //! Adds a new `InvokeNode`.
  inline InvokeNode* addCall(uint32_t instId, const Operand_& o0, const FuncSignature& signature) {
    InvokeNode* node;
    _addInvokeNode(&node, instId, o0, signature);
    return node;
  }

  //! \}

  //! \name Virtual Registers
  //! \{

  //! Creates a new virtual register representing the given `typeId` and `signature`.
  //!
  //! \note This function is public, but it's not generally recommended to be used
  //! by AsmJit users, use architecture-specific `newReg()` functionality instead
  //! or functions like \ref _newReg() and \ref _newRegFmt().
  ASMJIT_API Error newVirtReg(VirtReg** out, uint32_t typeId, uint32_t signature, const char* name);

  //! Creates a new virtual register of the given `typeId` and stores it to `out` operand.
  ASMJIT_API Error _newReg(BaseReg* out, uint32_t typeId, const char* name = nullptr);

  //! Creates a new virtual register of the given `typeId` and stores it to `out` operand.
  //!
  //! \note This version accepts a snprintf() format `fmt` followed by a variadic arguments.
  ASMJIT_API Error _newRegFmt(BaseReg* out, uint32_t typeId, const char* fmt, ...);

  //! Creates a new virtual register compatible with the provided reference register `ref`.
  ASMJIT_API Error _newReg(BaseReg* out, const BaseReg& ref, const char* name = nullptr);

  //! Creates a new virtual register compatible with the provided reference register `ref`.
  //!
  //! \note This version accepts a snprintf() format `fmt` followed by a variadic arguments.
  ASMJIT_API Error _newRegFmt(BaseReg* out, const BaseReg& ref, const char* fmt, ...);

  //! Tests whether the given `id` is a valid virtual register id.
  inline bool isVirtIdValid(uint32_t id) const noexcept {
    uint32_t index = Operand::virtIdToIndex(id);
    return index < _vRegArray.size();
  }
  //! Tests whether the given `reg` is a virtual register having a valid id.
  inline bool isVirtRegValid(const BaseReg& reg) const noexcept {
    return isVirtIdValid(reg.id());
  }

  //! Returns \ref VirtReg associated with the given `id`.
  inline VirtReg* virtRegById(uint32_t id) const noexcept {
    ASMJIT_ASSERT(isVirtIdValid(id));
    return _vRegArray[Operand::virtIdToIndex(id)];
  }

  //! Returns \ref VirtReg associated with the given `reg`.
  inline VirtReg* virtRegByReg(const BaseReg& reg) const noexcept { return virtRegById(reg.id()); }

  //! Returns \ref VirtReg associated with the given virtual register `index`.
  //!
  //! \note This is not the same as virtual register id. The conversion between
  //! id and its index is implemented by \ref Operand_::virtIdToIndex() and \ref
  //! Operand_::indexToVirtId() functions.
  inline VirtReg* virtRegByIndex(uint32_t index) const noexcept { return _vRegArray[index]; }

  //! Returns an array of all virtual registers managed by the Compiler.
  inline const ZoneVector<VirtReg*>& virtRegs() const noexcept { return _vRegArray; }

  //! \name Stack
  //! \{

  //! Creates a new stack of the given `size` and `alignment` and stores it to `out`.
  //!
  //! \note `name` can be used to give the stack a name, for debugging purposes.
  ASMJIT_API Error _newStack(BaseMem* out, uint32_t size, uint32_t alignment, const char* name = nullptr);

  //! Updates the stack size of a stack created by `_newStack()` by its `virtId`.
  ASMJIT_API Error setStackSize(uint32_t virtId, uint32_t newSize, uint32_t newAlignment = 0);

  //! Updates the stack size of a stack created by `_newStack()`.
  inline Error setStackSize(const BaseMem& mem, uint32_t newSize, uint32_t newAlignment = 0) {
    return setStackSize(mem.id(), newSize, newAlignment);
  }

  //! \}

  //! \name Constants
  //! \{

  //! Creates a new constant of the given `scope` (see \ref ConstPool::Scope).
  //!
  //! This function adds a constant of the given `size` to the built-in \ref
  //! ConstPool and stores the reference to that constant to the `out` operand.
  ASMJIT_API Error _newConst(BaseMem* out, uint32_t scope, const void* data, size_t size);

  //! \}

  //! \name Miscellaneous
  //! \{

  //! Rename the given virtual register `reg` to a formatted string `fmt`.
  ASMJIT_API void rename(const BaseReg& reg, const char* fmt, ...);

  //! \}

  //! \name Jump Annotations
  //! \{

  inline const ZoneVector<JumpAnnotation*>& jumpAnnotations() const noexcept {
    return _jumpAnnotations;
  }

  ASMJIT_API Error newJumpNode(JumpNode** out, uint32_t instId, uint32_t instOptions, const Operand_& o0, JumpAnnotation* annotation);
  ASMJIT_API Error emitAnnotatedJump(uint32_t instId, const Operand_& o0, JumpAnnotation* annotation);

  //! Returns a new `JumpAnnotation` instance, which can be used to aggregate
  //! possible targets of a jump where the target is not a label, for example
  //! to implement jump tables.
  ASMJIT_API JumpAnnotation* newJumpAnnotation();

  //! \}

#ifndef ASMJIT_NO_DEPRECATED
  ASMJIT_DEPRECATED("alloc() has no effect, it will be removed in the future")
  inline void alloc(BaseReg&) {}
  ASMJIT_DEPRECATED("spill() has no effect, it will be removed in the future")
  inline void spill(BaseReg&) {}
#endif // !ASMJIT_NO_DEPRECATED

  //! \name Events
  //! \{

  ASMJIT_API Error onAttach(CodeHolder* code) noexcept override;
  ASMJIT_API Error onDetach(CodeHolder* code) noexcept override;

  //! \}
};

// ============================================================================
// [asmjit::JumpAnnotation]
// ============================================================================

//! Jump annotation used to annotate jumps.
//!
//! \ref BaseCompiler allows to emit jumps where the target is either register
//! or memory operand. Such jumps cannot be trivially inspected, so instead of
//! doing heuristics AsmJit allows to annotate such jumps with possible targets.
//! Register allocator then use the annotation to construct control-flow, which
//! is then used by liveness analysis and other tools to prepare ground for
//! register allocation.
class JumpAnnotation {
public:
  ASMJIT_NONCOPYABLE(JumpAnnotation)

  //! Compiler that owns this JumpAnnotation.
  BaseCompiler* _compiler;
  //! Annotation identifier.
  uint32_t _annotationId;
  //! Vector of label identifiers, see \ref labelIds().
  ZoneVector<uint32_t> _labelIds;

  inline JumpAnnotation(BaseCompiler* compiler, uint32_t annotationId) noexcept
    : _compiler(compiler),
      _annotationId(annotationId) {}

  //! Returns the compiler that owns this JumpAnnotation.
  inline BaseCompiler* compiler() const noexcept { return _compiler; }
  //! Returns the annotation id.
  inline uint32_t annotationId() const noexcept { return _annotationId; }
  //! Returns a vector of label identifiers that lists all targets of the jump.
  const ZoneVector<uint32_t>& labelIds() const noexcept { return _labelIds; }

  //! Tests whether the given `label` is a target of this JumpAnnotation.
  inline bool hasLabel(const Label& label) const noexcept { return hasLabelId(label.id()); }
  //! Tests whether the given `labelId` is a target of this JumpAnnotation.
  inline bool hasLabelId(uint32_t labelId) const noexcept { return _labelIds.contains(labelId); }

  //! Adds the `label` to the list of targets of this JumpAnnotation.
  inline Error addLabel(const Label& label) noexcept { return addLabelId(label.id()); }
  //! Adds the `labelId` to the list of targets of this JumpAnnotation.
  inline Error addLabelId(uint32_t labelId) noexcept { return _labelIds.append(&_compiler->_allocator, labelId); }
};

// ============================================================================
// [asmjit::JumpNode]
// ============================================================================

//! Jump instruction with \ref JumpAnnotation.
//!
//! \note This node should be only used to represent jump where the jump target
//! cannot be deduced by examining instruction operands. For example if the jump
//! target is register or memory location. This pattern is often used to perform
//! indirect jumps that use jump table, e.g. to implement `switch{}` statement.
class JumpNode : public InstNode {
public:
  ASMJIT_NONCOPYABLE(JumpNode)

  JumpAnnotation* _annotation;

  //! \name Construction & Destruction
  //! \{

  ASMJIT_INLINE JumpNode(BaseCompiler* cc, uint32_t instId, uint32_t options, uint32_t opCount, JumpAnnotation* annotation) noexcept
    : InstNode(cc, instId, options, opCount, kBaseOpCapacity),
      _annotation(annotation) {
    setType(kNodeJump);
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Tests whether this JumpNode has associated a \ref JumpAnnotation.
  inline bool hasAnnotation() const noexcept { return _annotation != nullptr; }
  //! Returns the \ref JumpAnnotation associated with this jump, or `nullptr`.
  inline JumpAnnotation* annotation() const noexcept { return _annotation; }
  //! Sets the \ref JumpAnnotation associated with this jump to `annotation`.
  inline void setAnnotation(JumpAnnotation* annotation) noexcept { _annotation = annotation; }

  //! \}
};

// ============================================================================
// [asmjit::FuncNode]
// ============================================================================

//! Function node represents a function used by \ref BaseCompiler.
//!
//! A function is composed of the following:
//!
//!   - Function entry, \ref FuncNode acts as a label, so the entry is implicit.
//!     To get the entry, simply use \ref FuncNode::label(), which is the same
//!     as \ref LabelNode::label().
//!
//!   - Function exit, which is represented by \ref FuncNode::exitNode(). A
//!     helper function \ref FuncNode::exitLabel() exists and returns an exit
//!     label instead of node.
//!
//!   - Function \ref FuncNode::endNode() sentinel. This node marks the end of
//!     a function - there should be no code that belongs to the function after
//!     this node, but the Compiler doesn't enforce that at the moment.
//!
//!   - Function detail, see \ref FuncNode::detail().
//!
//!   - Function frame, see \ref FuncNode::frame().
//!
//!   - Function arguments mapped to virtual registers, see \ref FuncNode::args().
//!
//! In a node list, the function and its body looks like the following:
//!
//! \code{.unparsed}
//! [...]       - Anything before the function.
//!
//! [FuncNode]  - Entry point of the function, acts as a label as well.
//!   <Prolog>  - Prolog inserted by the register allocator.
//!   {...}     - Function body - user code basically.
//! [ExitLabel] - Exit label
//!   <Epilog>  - Epilog inserted by the register allocator.
//!   <Return>  - Return inserted by the register allocator.
//!   {...}     - Can contain data or user code (error handling, special cases, ...).
//! [FuncEnd]   - End sentinel
//!
//! [...]       - Anything after the function.
//! \endcode
//!
//! When a function is added to the compiler by \ref BaseCompiler::addFunc() it
//! actually inserts 3 nodes (FuncNode, ExitLabel, and FuncEnd) and sets the
//! current cursor to be FuncNode. When \ref BaseCompiler::endFunc() is called
//! the cursor is set to FuncEnd. This guarantees that user can use ExitLabel
//! as a marker after additional code or data can be placed, and it's a common
//! practice.
class FuncNode : public LabelNode {
public:
  ASMJIT_NONCOPYABLE(FuncNode)

  //! Arguments pack.
  struct ArgPack {
    VirtReg* _data[Globals::kMaxValuePack];

    inline void reset() noexcept {
      for (size_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++)
        _data[valueIndex] = nullptr;
    }

    inline VirtReg*& operator[](size_t valueIndex) noexcept { return _data[valueIndex]; }
    inline VirtReg* const& operator[](size_t valueIndex) const noexcept { return _data[valueIndex]; }
  };

  //! Function detail.
  FuncDetail _funcDetail;
  //! Function frame.
  FuncFrame _frame;
  //! Function exit label.
  LabelNode* _exitNode;
  //! Function end (sentinel).
  SentinelNode* _end;

  //! Argument packs.
  ArgPack* _args;

  //! \name Construction & Destruction
  //! \{

  //! Creates a new `FuncNode` instance.
  //!
  //! Always use `BaseCompiler::addFunc()` to create `FuncNode`.
  ASMJIT_INLINE FuncNode(BaseBuilder* cb) noexcept
    : LabelNode(cb),
      _funcDetail(),
      _frame(),
      _exitNode(nullptr),
      _end(nullptr),
      _args(nullptr) {
    setType(kNodeFunc);
  }

  //! \}

  //! \{
  //! \name Accessors

  //! Returns function exit `LabelNode`.
  inline LabelNode* exitNode() const noexcept { return _exitNode; }
  //! Returns function exit label.
  inline Label exitLabel() const noexcept { return _exitNode->label(); }

  //! Returns "End of Func" sentinel.
  inline SentinelNode* endNode() const noexcept { return _end; }

  //! Returns function declaration.
  inline FuncDetail& detail() noexcept { return _funcDetail; }
  //! Returns function declaration.
  inline const FuncDetail& detail() const noexcept { return _funcDetail; }

  //! Returns function frame.
  inline FuncFrame& frame() noexcept { return _frame; }
  //! Returns function frame.
  inline const FuncFrame& frame() const noexcept { return _frame; }

  //! Tests whether the function has a return value.
  inline bool hasRet() const noexcept { return _funcDetail.hasRet(); }
  //! Returns arguments count.
  inline uint32_t argCount() const noexcept { return _funcDetail.argCount(); }

  //! Returns argument packs.
  inline ArgPack* argPacks() const noexcept { return _args; }

  //! Returns argument pack at `argIndex`.
  inline ArgPack& argPack(size_t argIndex) const noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    return _args[argIndex];
  }

  //! Sets argument at `argIndex`.
  inline void setArg(size_t argIndex, VirtReg* vReg) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    _args[argIndex][0] = vReg;
  }

  //! Sets argument at `argIndex` and `valueIndex`.
  inline void setArg(size_t argIndex, size_t valueIndex, VirtReg* vReg) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    _args[argIndex][valueIndex] = vReg;
  }

  //! Resets argument pack at `argIndex`.
  inline void resetArg(size_t argIndex) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    _args[argIndex].reset();
  }

  //! Resets argument pack at `argIndex`.
  inline void resetArg(size_t argIndex, size_t valueIndex) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    _args[argIndex][valueIndex] = nullptr;
  }

  //! Returns function attributes.
  inline uint32_t attributes() const noexcept { return _frame.attributes(); }
  //! Adds `attrs` to the function attributes.
  inline void addAttributes(uint32_t attrs) noexcept { _frame.addAttributes(attrs); }

  //! \}
};

// ============================================================================
// [asmjit::FuncRetNode]
// ============================================================================

//! Function return, used by \ref BaseCompiler.
class FuncRetNode : public InstNode {
public:
  ASMJIT_NONCOPYABLE(FuncRetNode)

  //! \name Construction & Destruction
  //! \{

  //! Creates a new `FuncRetNode` instance.
  inline FuncRetNode(BaseBuilder* cb) noexcept : InstNode(cb, BaseInst::kIdAbstract, 0, 0) {
    _any._nodeType = kNodeFuncRet;
  }

  //! \}
};

// ============================================================================
// [asmjit::InvokeNode]
// ============================================================================

//! Function invocation, used by \ref BaseCompiler.
class InvokeNode : public InstNode {
public:
  ASMJIT_NONCOPYABLE(InvokeNode)

  //! Operand pack provides multiple operands that can be associated with a
  //! single return value of function argument. Sometims this is necessary to
  //! express an argument or return value that requires multiple registers, for
  //! example 64-bit value in 32-bit mode or passing / returning homogenous data
  //! structures.
  struct OperandPack {
    //! Operands.
    Operand_ _data[Globals::kMaxValuePack];

    //! Reset the pack by resetting all operands in the pack.
    inline void reset() noexcept {
      for (size_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++)
        _data[valueIndex].reset();
    }

    //! Returns an operand at the given `valueIndex`.
    inline Operand& operator[](size_t valueIndex) noexcept {
      ASMJIT_ASSERT(valueIndex < Globals::kMaxValuePack);
      return _data[valueIndex].as<Operand>();
    }

    //! Returns an operand at the given `valueIndex` (const).
    const inline Operand& operator[](size_t valueIndex) const noexcept {
      ASMJIT_ASSERT(valueIndex < Globals::kMaxValuePack);
      return _data[valueIndex].as<Operand>();
    }
  };

  //! Function detail.
  FuncDetail _funcDetail;
  //! Function return value(s).
  OperandPack _rets;
  //! Function arguments.
  OperandPack* _args;

  //! \name Construction & Destruction
  //! \{

  //! Creates a new `InvokeNode` instance.
  inline InvokeNode(BaseBuilder* cb, uint32_t instId, uint32_t options) noexcept
    : InstNode(cb, instId, options, kBaseOpCapacity),
      _funcDetail(),
      _args(nullptr) {
    setType(kNodeInvoke);
    _resetOps();
    _rets.reset();
    addFlags(kFlagIsRemovable);
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Sets the function signature.
  inline Error init(const FuncSignature& signature, const Environment& environment) noexcept {
    return _funcDetail.init(signature, environment);
  }

  //! Returns the function detail.
  inline FuncDetail& detail() noexcept { return _funcDetail; }
  //! Returns the function detail.
  inline const FuncDetail& detail() const noexcept { return _funcDetail; }

  //! Returns the target operand.
  inline Operand& target() noexcept { return _opArray[0].as<Operand>(); }
  //! \overload
  inline const Operand& target() const noexcept { return _opArray[0].as<Operand>(); }

  //! Returns the number of function return values.
  inline bool hasRet() const noexcept { return _funcDetail.hasRet(); }
  //! Returns the number of function arguments.
  inline uint32_t argCount() const noexcept { return _funcDetail.argCount(); }

  //! Returns operand pack representing function return value(s).
  inline OperandPack& retPack() noexcept { return _rets; }
  //! Returns operand pack representing function return value(s).
  inline const OperandPack& retPack() const noexcept { return _rets; }

  //! Returns the return value at the given `valueIndex`.
  inline Operand& ret(size_t valueIndex = 0) noexcept { return _rets[valueIndex]; }
  //! \overload
  inline const Operand& ret(size_t valueIndex = 0) const noexcept { return _rets[valueIndex]; }

  //! Returns operand pack representing function return value(s).
  inline OperandPack& argPack(size_t argIndex) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    return _args[argIndex];
  }
  //! \overload
  inline const OperandPack& argPack(size_t argIndex) const noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    return _args[argIndex];
  }

  //! Returns a function argument at the given `argIndex`.
  inline Operand& arg(size_t argIndex, size_t valueIndex) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    return _args[argIndex][valueIndex];
  }
  //! \overload
  inline const Operand& arg(size_t argIndex, size_t valueIndex) const noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    return _args[argIndex][valueIndex];
  }

  //! Sets the function return value at `i` to `op`.
  inline void _setRet(size_t valueIndex, const Operand_& op) noexcept { _rets[valueIndex] = op; }
  //! Sets the function argument at `i` to `op`.
  inline void _setArg(size_t argIndex, size_t valueIndex, const Operand_& op) noexcept {
    ASMJIT_ASSERT(argIndex < argCount());
    _args[argIndex][valueIndex] = op;
  }

  //! Sets the function return value at `valueIndex` to `reg`.
  inline void setRet(size_t valueIndex, const BaseReg& reg) noexcept { _setRet(valueIndex, reg); }

  //! Sets the first function argument in a value-pack at `argIndex` to `reg`.
  inline void setArg(size_t argIndex, const BaseReg& reg) noexcept { _setArg(argIndex, 0, reg); }
  //! Sets the first function argument in a value-pack at `argIndex` to `imm`.
  inline void setArg(size_t argIndex, const Imm& imm) noexcept { _setArg(argIndex, 0, imm); }

  //! Sets the function argument at `argIndex` and `valueIndex` to `reg`.
  inline void setArg(size_t argIndex, size_t valueIndex, const BaseReg& reg) noexcept { _setArg(argIndex, valueIndex, reg); }
  //! Sets the function argument at `argIndex` and `valueIndex` to `imm`.
  inline void setArg(size_t argIndex, size_t valueIndex, const Imm& imm) noexcept { _setArg(argIndex, valueIndex, imm); }

  //! \}
};

// ============================================================================
// [asmjit::FuncPass]
// ============================================================================

//! Function pass extends \ref Pass with \ref FuncPass::runOnFunction().
class ASMJIT_VIRTAPI FuncPass : public Pass {
public:
  ASMJIT_NONCOPYABLE(FuncPass)
  typedef Pass Base;

  //! \name Construction & Destruction
  //! \{

  ASMJIT_API FuncPass(const char* name) noexcept;

  //! \}

  //! \name Accessors
  //! \{

  //! Returns the associated `BaseCompiler`.
  inline BaseCompiler* cc() const noexcept { return static_cast<BaseCompiler*>(_cb); }

  //! \}

  //! \name Run
  //! \{

  //! Calls `runOnFunction()` on each `FuncNode` node found.
  ASMJIT_API Error run(Zone* zone, Logger* logger) override;

  //! Called once per `FuncNode`.
  virtual Error runOnFunction(Zone* zone, Logger* logger, FuncNode* func) = 0;

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_COMPILER
#endif // ASMJIT_CORE_COMPILER_H_INCLUDED
