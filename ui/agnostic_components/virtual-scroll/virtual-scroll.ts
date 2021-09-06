import { VirtualScroll } from "./src/VirtualScroll";
import '@questdb_design-system/virtual-scroll-row';
import { VirtualAscensor } from "./src/VirtualAscensor";

if (!customElements.get('virtual-scroll')) customElements.define('virtual-scroll', VirtualScroll);
if (!customElements.get('virtual-ascensor')) customElements.define('virtual-ascensor', VirtualAscensor);