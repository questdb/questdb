import { LitElement, customElement, html, css, property, TemplateResult, CSSResultArray }  from 'lit-element';
import { VirtualBase } from './VirtualScrollBase';


export class VirtualScroll extends VirtualBase {

    static get styles(): CSSResultArray {
    return [
        super.styles, 
        css`
            :host {
                overflow: hidden;
                display flex;
                flex: 1;
            }
        `]
    }

    constructor() {
        super();
    }

    connectedCallback() {
        super.connectedCallback();
    }

    firstUpdated(changedProperties) {
        if(changedProperties.width) this.widthChanged(changedProperties.width);
        // this.observeSlot();
        this.evaluateScroll();
        this.deltaH = this.deltaV = 0;
    }
    
    render() {
    return html`
        ${this.containerTemplate}
        `;
    }
    
    get scrollTemplate(): TemplateResult {
        return html`
        <virtual-ascensor id="scrollElement" ?show-v="${this.needScrollHeight}" ?show-h="${this.needScrollWidth}"></virtual-ascensor>
        `
    }

    get templateItem() {
        return html`
            <div style="height: 65px; background: tan;position: absolute">Ceci est un test</div>
        `
    }

    get containerTemplate():TemplateResult {
        return html`
            <div id="superContainer">
            ${this.scrollTemplate}
                <div id="container">
                <slot id="slot"></slot>
                </div>
            </div>
        `
    }
    
    get container():HTMLElement {
        return this.shadowRoot.querySelector('#container');
    }

    static get classes() {
        return [
            css`
            #superContainer {
                position: absolute;
                overflow: hidden;
            }

            virtual-ascensor {
                position: absolute;
            }
            `
        ]
    }
}
