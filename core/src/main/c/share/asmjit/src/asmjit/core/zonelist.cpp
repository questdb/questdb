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
#include "../core/zone.h"
#include "../core/zonelist.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::ZoneList - Unit]
// ============================================================================

#if defined(ASMJIT_TEST)
class MyListNode : public ZoneListNode<MyListNode> {};

UNIT(zone_list) {
  Zone zone(4096);
  ZoneList<MyListNode> list;

  MyListNode* a = zone.newT<MyListNode>();
  MyListNode* b = zone.newT<MyListNode>();
  MyListNode* c = zone.newT<MyListNode>();
  MyListNode* d = zone.newT<MyListNode>();

  INFO("Append / Unlink");

  // []
  EXPECT(list.empty() == true);

  // [A]
  list.append(a);
  EXPECT(list.empty() == false);
  EXPECT(list.first() == a);
  EXPECT(list.last() == a);
  EXPECT(a->prev() == nullptr);
  EXPECT(a->next() == nullptr);

  // [A, B]
  list.append(b);
  EXPECT(list.first() == a);
  EXPECT(list.last() == b);
  EXPECT(a->prev() == nullptr);
  EXPECT(a->next() == b);
  EXPECT(b->prev() == a);
  EXPECT(b->next() == nullptr);

  // [A, B, C]
  list.append(c);
  EXPECT(list.first() == a);
  EXPECT(list.last() == c);
  EXPECT(a->prev() == nullptr);
  EXPECT(a->next() == b);
  EXPECT(b->prev() == a);
  EXPECT(b->next() == c);
  EXPECT(c->prev() == b);
  EXPECT(c->next() == nullptr);

  // [B, C]
  list.unlink(a);
  EXPECT(list.first() == b);
  EXPECT(list.last() == c);
  EXPECT(a->prev() == nullptr);
  EXPECT(a->next() == nullptr);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == c);
  EXPECT(c->prev() == b);
  EXPECT(c->next() == nullptr);

  // [B]
  list.unlink(c);
  EXPECT(list.first() == b);
  EXPECT(list.last() == b);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == nullptr);
  EXPECT(c->prev() == nullptr);
  EXPECT(c->next() == nullptr);

  // []
  list.unlink(b);
  EXPECT(list.empty() == true);
  EXPECT(list.first() == nullptr);
  EXPECT(list.last() == nullptr);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == nullptr);

  INFO("Prepend / Unlink");

  // [A]
  list.prepend(a);
  EXPECT(list.empty() == false);
  EXPECT(list.first() == a);
  EXPECT(list.last() == a);
  EXPECT(a->prev() == nullptr);
  EXPECT(a->next() == nullptr);

  // [B, A]
  list.prepend(b);
  EXPECT(list.first() == b);
  EXPECT(list.last() == a);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == a);
  EXPECT(a->prev() == b);
  EXPECT(a->next() == nullptr);

  INFO("InsertAfter / InsertBefore");

  // [B, A, C]
  list.insertAfter(a, c);
  EXPECT(list.first() == b);
  EXPECT(list.last() == c);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == a);
  EXPECT(a->prev() == b);
  EXPECT(a->next() == c);
  EXPECT(c->prev() == a);
  EXPECT(c->next() == nullptr);

  // [B, D, A, C]
  list.insertBefore(a, d);
  EXPECT(list.first() == b);
  EXPECT(list.last() == c);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == d);
  EXPECT(d->prev() == b);
  EXPECT(d->next() == a);
  EXPECT(a->prev() == d);
  EXPECT(a->next() == c);
  EXPECT(c->prev() == a);
  EXPECT(c->next() == nullptr);

  INFO("PopFirst / Pop");

  // [D, A, C]
  EXPECT(list.popFirst() == b);
  EXPECT(b->prev() == nullptr);
  EXPECT(b->next() == nullptr);

  EXPECT(list.first() == d);
  EXPECT(list.last() == c);
  EXPECT(d->prev() == nullptr);
  EXPECT(d->next() == a);
  EXPECT(a->prev() == d);
  EXPECT(a->next() == c);
  EXPECT(c->prev() == a);
  EXPECT(c->next() == nullptr);

  // [D, A]
  EXPECT(list.pop() == c);
  EXPECT(c->prev() == nullptr);
  EXPECT(c->next() == nullptr);

  EXPECT(list.first() == d);
  EXPECT(list.last() == a);
  EXPECT(d->prev() == nullptr);
  EXPECT(d->next() == a);
  EXPECT(a->prev() == d);
  EXPECT(a->next() == nullptr);
}
#endif

ASMJIT_END_NAMESPACE
