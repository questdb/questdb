import { html, fixture, expect, elementUpdated, nextFrame } from '@open-wc/testing';

import '../virtual-scroll.ts';
import { VirtualScroll } from '../src/VirtualScroll';

const given = beforeEach;
const when = describe;
const then = it;

when('VirtualScroll is initialized', () => {
  let el: VirtualScroll
  given( async () => {
      el = await fixture(html`
      <virtual-scroll></virtual-scroll>
    `);
  })
  then('has component virtualScroll initialized', async () => {
    expect(el).to.exist;
  });
});
