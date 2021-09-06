import { html, fixture, expect, elementUpdated, nextFrame } from '@open-wc/testing';

import '../src/virtual-scroll-row.ts';
import { VirtualScrollRow } from '../src/VirtualScrollRow';

const given = beforeEach;
const when = describe;
const then = it;

when('VirtualScrollRow is initialized', () => {
  let el: VirtualScrollRow
  given( async () => {
      el = await fixture(html`
      <virtual-scroll-row></virtual-scroll-row>
    `);
  })
  then('has component virtualScrollRow initialized', async () => {
    expect(el).to.exist;
  });
});
