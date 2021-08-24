import { LitElement, property, html, css, CSSResult, TemplateResult } from "lit-element";

import { VirtualAscensor } from "../VirtualAscensor";



export class ScrollMixin extends LitElement {
    private _isScrollWidth: boolean = false;
    private _deltaH: number = 0;
    private _deltaV: number = 0;
    private _positionH: number = 0;
    private _lastIndex: number = 0;
    protected handleTrackBind: (this: HTMLElement, ev: TouchEvent)=> any;
    protected handleWheelBind: (this: HTMLElement, ev: WheelEvent)=> any;;
    protected handleChangeInDomBind: MutationCallback;

    @property({ type: Boolean}) needScrollHeight = false;
    @property({ type: Boolean}) needScrollWidth = false;
    @property({ type: Number}) ratioV = 0;
    @property({ type: Number}) ratioH = 0;
    @property({
        type: Boolean
    })
    get isScrollWidth():boolean {
        return this._isScrollWidth;
    }
    set isScrollWidth(value: boolean) {
        const old= this._isScrollWidth;
        this._isScrollWidth = value;
        this.requestUpdate('isScrollWidth', old);
    }
    
    @property({
        type: Number
    })
    get deltaH() {
        return this._deltaH;
    }
    set deltaH(value) {
        const oldValue = this._deltaH;
        this._deltaH = value;
        this.requestUpdate('deltaH', oldValue);
    }

    @property({
        type: Number
    })
    get deltaV() {
        return this._deltaV;
    }
    set deltaV(value) {
        const oldValue = this._deltaV;
        this._deltaV = value;
        this.requestUpdate('deltaV', oldValue);
    }

    set lastIndex(value) {
        const oldValue = this._lastIndex;
        this._lastIndex = value;
        this.requestUpdate('lastIndex', oldValue);
    }

    get lastIndex() {
        return this._lastIndex;
    }

    get containerTemplate() {
        return html`
        <div id="container">
            <slot id="slot"></slot>
        </div>
        `
    }

    get styleTemplate(): CSSResult {
        return css`
                :host{
                    display: flex;
                    flex: 1;
                    flex-direction: column;
                    overflow: hidden;
                    opacity: 0;
                }

                ::slotted(*){
                    -webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;
                }
        `
    }

    get scrollTemplate(): TemplateResult {
        return html`
            <pedrule-scrollbar id="scrollElement" ?show-v="${this.needScrollHeight}" ?show-h="${this.needScrollWidth}"></pedrule-scrollbar>
        `
    }

    get scrollElement(): VirtualAscensor {
        return this.shadowRoot.querySelector('#scrollElement');
    }

    get container(): HTMLElement {
        return this.shadowRoot.querySelector('#container');
    }

    private get slotElement(): HTMLSlotElement {
        return this.shadowRoot.querySelector('#slot');
    }

    /**
     * callback of mutation observer
     * @param {Array} mutationList 
     */
    protected handleChangeInDom(mutationList) {
        // we check if height of Element is higher than one of its container.
        this.evaluateScroll();
    }

    /**
     * we evaluate if element need scroll element to be used
     * @public
     */
    evaluateScroll() {
        if(!this.container) return;
        this.computeSizeOfHost();
        this.needScrollHeight = this.getBoundingClientRect().height < this.container.getBoundingClientRect().height;
        if(this.isScrollWidth)this.adjustWidthOfContainer();
        this.needScrollWidth = this.getBoundingClientRect().width < this.container.getBoundingClientRect().width;
        if(this.needScrollHeight)this.resizescrollElement();
    }

    protected computeSizeOfHost() {
        const position = this.getBoundingClientRect();
        this.superContainer.style.width = `${position.width}px`;
        this.superContainer.style.height = `${position.height}px`;
        this.superContainer.style.top = `${position.top}px`;
        this.superContainer.style.left = `${position.left}px`;
        this.style.opacity = "1";
    }

    private get superContainer():HTMLElement {
        return this.shadowRoot.querySelector('#superContainer');
    }

    private adjustWidthOfContainer() {
        const maxWidth = this.slotElement.assignedElements().reduce((previous, item) => {
            const size = item.getBoundingClientRect().width;
            if(size > previous)return size;
            return previous;
        }, this.container.getBoundingClientRect().width);
        this.container.style.width = `${maxWidth}px`;
    }

    private resizescrollElement() {
        this.scrollElement.style.width = `${this.getBoundingClientRect().width}px`;
        this.scrollElement.style.height = `${this.getBoundingClientRect().height}px`;
        this.scrollElement.sizeV = this.getBoundingClientRect().height*(this.getBoundingClientRect().height/this.container.getBoundingClientRect().height);
        this.scrollElement.sizeH = this.getBoundingClientRect().width*(this.getBoundingClientRect().width/this.container.getBoundingClientRect().width);
        this.ratioV = this.container.getBoundingClientRect().height / this.getBoundingClientRect().height;
        this.ratioH = this.container.getBoundingClientRect().width / this.getBoundingClientRect().width;
    }

    get positionV() {
        return (this.getBoundingClientRect().top - this.container.getBoundingClientRect().top) /this.ratioV;
    }

    get positionH() {
        return (this.getBoundingClientRect().left - this.container.getBoundingClientRect().left) /this.ratioH;
    }

    protected handleTrack(event) {
        if(this.needScrollHeight) {
            let deltaV = this.positionV + event.detail.dy*0.5;
            this.deltaV = this.scrollElement.checkDeltaV(deltaV);
        }
        
        if(this.needScrollWidth) {
            let deltaH = this.positionH + event.detail.dx;
            this.deltaH = this.scrollElement.checkDeltaH(deltaH);
        }
        this.container.style.transform = `translate3d(-${this.deltaH*this.ratioH}px ,-${this.deltaV*this.ratioV}px, 0)`;
        event.stopImmediatePropagation();
    }

    protected handleWheel(event) {
        if(this.needScrollHeight && !this.scrollElement.isHorizontalScroll) {
            let deltaV = this.positionV + event.deltaY*0.1;
            this.deltaV = this.scrollElement.checkDeltaV(deltaV);
        }

        if(this.needScrollHeight && this.scrollElement.isHorizontalScroll) {
            let deltaH = this.positionH + event.deltaY;
            this.deltaH = this.scrollElement.checkDeltaH(deltaH);
        }
        this.container.style.transform = `translate3d(-${this.deltaH*this.ratioH}px ,-${this.deltaV*this.ratioV}px, 0)`;
        event.stopImmediatePropagation();
    }

    protected setOnTop() {
        this.deltaV = 0;
        this.container.style.transform = `translate3d(-${this.deltaH*this.ratioH}px, 0px, 0)`;
    }
}

export interface ScrollMixin {}
