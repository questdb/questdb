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

#ifndef ASMJIT_CORE_ZONELIST_H_INCLUDED
#define ASMJIT_CORE_ZONELIST_H_INCLUDED

#include "../core/support.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_zone
//! \{

// ============================================================================
// [asmjit::ZoneListNode]
// ============================================================================

//! Node used by \ref ZoneList template.
template<typename NodeT>
class ZoneListNode {
public:
  ASMJIT_NONCOPYABLE(ZoneListNode)

  NodeT* _listNodes[Globals::kLinkCount];

  //! \name Construction & Destruction
  //! \{

  inline ZoneListNode() noexcept
    : _listNodes { nullptr, nullptr } {}

  inline ZoneListNode(ZoneListNode&& other) noexcept
    : _listNodes { other._listNodes[0], other._listNodes[1] } {}

  //! \}

  //! \name Accessors
  //! \{

  inline bool hasPrev() const noexcept { return _listNodes[Globals::kLinkPrev] != nullptr; }
  inline bool hasNext() const noexcept { return _listNodes[Globals::kLinkNext] != nullptr; }

  inline NodeT* prev() const noexcept { return _listNodes[Globals::kLinkPrev]; }
  inline NodeT* next() const noexcept { return _listNodes[Globals::kLinkNext]; }

  //! \}
};

// ============================================================================
// [asmjit::ZoneList<T>]
// ============================================================================

//! Zone allocated list container that uses nodes of `NodeT` type.
template <typename NodeT>
class ZoneList {
public:
  ASMJIT_NONCOPYABLE(ZoneList)

  NodeT* _nodes[Globals::kLinkCount];

  //! \name Construction & Destruction
  //! \{

  inline ZoneList() noexcept
    : _nodes { nullptr, nullptr } {}

  inline ZoneList(ZoneList&& other) noexcept
    : _nodes { other._nodes[0], other._nodes[1] } {}

  inline void reset() noexcept {
    _nodes[0] = nullptr;
    _nodes[1] = nullptr;
  }

  //! \}

  //! \name Accessors
  //! \{

  inline bool empty() const noexcept { return _nodes[0] == nullptr; }
  inline NodeT* first() const noexcept { return _nodes[Globals::kLinkFirst]; }
  inline NodeT* last() const noexcept { return _nodes[Globals::kLinkLast]; }

  //! \}

  //! \name Utilities
  //! \{

  inline void swap(ZoneList& other) noexcept {
    std::swap(_nodes[0], other._nodes[0]);
    std::swap(_nodes[1], other._nodes[1]);
  }

  // Can be used to both append and prepend.
  inline void _addNode(NodeT* node, size_t dir) noexcept {
    NodeT* prev = _nodes[dir];

    node->_listNodes[!dir] = prev;
    _nodes[dir] = node;
    if (prev)
      prev->_listNodes[dir] = node;
    else
      _nodes[!dir] = node;
  }

  // Can be used to both append and prepend.
  inline void _insertNode(NodeT* ref, NodeT* node, size_t dir) noexcept {
    ASMJIT_ASSERT(ref != nullptr);

    NodeT* prev = ref;
    NodeT* next = ref->_listNodes[dir];

    prev->_listNodes[dir] = node;
    if (next)
      next->_listNodes[!dir] = node;
    else
      _nodes[dir] = node;

    node->_listNodes[!dir] = prev;
    node->_listNodes[ dir] = next;
  }

  inline void append(NodeT* node) noexcept { _addNode(node, Globals::kLinkLast); }
  inline void prepend(NodeT* node) noexcept { _addNode(node, Globals::kLinkFirst); }

  inline void insertAfter(NodeT* ref, NodeT* node) noexcept { _insertNode(ref, node, Globals::kLinkNext); }
  inline void insertBefore(NodeT* ref, NodeT* node) noexcept { _insertNode(ref, node, Globals::kLinkPrev); }

  inline NodeT* unlink(NodeT* node) noexcept {
    NodeT* prev = node->prev();
    NodeT* next = node->next();

    if (prev) { prev->_listNodes[1] = next; node->_listNodes[0] = nullptr; } else { _nodes[0] = next; }
    if (next) { next->_listNodes[0] = prev; node->_listNodes[1] = nullptr; } else { _nodes[1] = prev; }

    node->_listNodes[0] = nullptr;
    node->_listNodes[1] = nullptr;

    return node;
  }

  inline NodeT* popFirst() noexcept {
    NodeT* node = _nodes[0];
    ASMJIT_ASSERT(node != nullptr);

    NodeT* next = node->next();
    _nodes[0] = next;

    if (next) {
      next->_listNodes[0] = nullptr;
      node->_listNodes[1] = nullptr;
    }
    else {
      _nodes[1] = nullptr;
    }

    return node;
  }

  inline NodeT* pop() noexcept {
    NodeT* node = _nodes[1];
    ASMJIT_ASSERT(node != nullptr);

    NodeT* prev = node->prev();
    _nodes[1] = prev;

    if (prev) {
      prev->_listNodes[1] = nullptr;
      node->_listNodes[0] = nullptr;
    }
    else {
      _nodes[0] = nullptr;
    }

    return node;
  }

  //! \}
};

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_ZONELIST_H_INCLUDED
