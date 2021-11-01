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
#include "../core/support.h"
#include "../core/zone.h"
#include "../core/zonetree.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::ZoneTree - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
template<typename NodeT>
struct ZoneRBUnit {
  typedef ZoneTree<NodeT> Tree;

  static void verifyTree(Tree& tree) noexcept {
    EXPECT(checkHeight(static_cast<NodeT*>(tree._root)) > 0);
  }

  // Check whether the Red-Black tree is valid.
  static int checkHeight(NodeT* node) noexcept {
    if (!node) return 1;

    NodeT* ln = node->left();
    NodeT* rn = node->right();

    // Invalid tree.
    EXPECT(ln == nullptr || *ln < *node);
    EXPECT(rn == nullptr || *rn > *node);

    // Red violation.
    EXPECT(!node->isRed() ||
          (!ZoneTreeNode::_isValidRed(ln) && !ZoneTreeNode::_isValidRed(rn)));

    // Black violation.
    int lh = checkHeight(ln);
    int rh = checkHeight(rn);
    EXPECT(!lh || !rh || lh == rh);

    // Only count black links.
    return (lh && rh) ? lh + !node->isRed() : 0;
  }
};

class MyRBNode : public ZoneTreeNodeT<MyRBNode> {
public:
  ASMJIT_NONCOPYABLE(MyRBNode)

  inline explicit MyRBNode(uint32_t key) noexcept
    : _key(key) {}

  inline bool operator<(const MyRBNode& other) const noexcept { return _key < other._key; }
  inline bool operator>(const MyRBNode& other) const noexcept { return _key > other._key; }

  inline bool operator<(uint32_t queryKey) const noexcept { return _key < queryKey; }
  inline bool operator>(uint32_t queryKey) const noexcept { return _key > queryKey; }

  uint32_t _key;
};

UNIT(zone_rbtree) {
  uint32_t kCount = BrokenAPI::hasArg("--quick") ? 1000 : 10000;

  Zone zone(4096);
  ZoneTree<MyRBNode> rbTree;

  uint32_t key;
  INFO("Inserting %u elements to RBTree and validating each operation", unsigned(kCount));
  for (key = 0; key < kCount; key++) {
    rbTree.insert(zone.newT<MyRBNode>(key));
    ZoneRBUnit<MyRBNode>::verifyTree(rbTree);
  }

  uint32_t count = kCount;
  INFO("Removing %u elements from RBTree and validating each operation", unsigned(kCount));
  do {
    MyRBNode* node;

    for (key = 0; key < count; key++) {
      node = rbTree.get(key);
      EXPECT(node != nullptr);
      EXPECT(node->_key == key);
    }

    node = rbTree.get(--count);
    rbTree.remove(node);
    ZoneRBUnit<MyRBNode>::verifyTree(rbTree);
  } while (count);

  EXPECT(rbTree.empty());
}
#endif

ASMJIT_END_NAMESPACE
