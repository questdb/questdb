// import { PolymerElement, html } from "@polymer/polymer/polymer-element";
import { LitElement, html, css, customElement, property } from "lit-element";

export class VirtualAscensor extends LitElement{

    @property({ type:Boolean, reflect: true, attribute: 'show-v' }) showV = false;
    @property({ type:Boolean, reflect: true, attribute: 'show-h' }) showH = false;
    @property({ type:Boolean }) isHorizontalScroll = false;
    @property({ type:Number }) sizeV = 0;
    @property({ type:Number }) sizeH = 0;
    @property({ type:Number }) deltaV = 0;
    @property({ type:Number }) deltaH = 0;

    static get styles() {
        return css`
            :host{
                pointer-events: none;
                display: flex;
                --weight-scroll-container: 6px;
                position: absolute;
                -webkit-user-select: none;-moz-user-select: none;-ms-user-select: none;user-select: none;
                cursor: pointer;
                --background-ascensor: #eeeeee;
                --color-ascensor: #D7D7D7;
                z-index: 9;
            }

            .container{
                background: var(--background-ascensor);
                transition: width .1s ease-out;
                overflow: hidden;
            }

            .scrollBar{
                background: var(--color-ascensor);
                border-radius: 100px;
            }

            #containerH{
                width: calc(100% - var(--weight-scroll-container));
            }

            #ascensorH{
                 height: var(--weight-scroll-container);
            }

            #containerV{
               height : 100%;
               width : var(--weight-scroll-container);
            }

            div.hide{
                opacity: 0;
                pointer-events: none;
            }

            div.show{
                opacity: 1;
                pointer-events: all;
            }

            :host(:hover){
                --weight-scroll-container: 12px;

            }

            .self-end {
                align-self: flex-end;
            }
            `
    }

    render() {
        return html`
        <div id="containerH" class="${this.hideH(this.showH)} self-end container" @mouseenter="${this.enableHorizontalScroll}" @mouseleave="${this.disableHorizontalScroll}">
            <div id="ascensorH" class="scrollBar"></div>
        </div>

        <div id="containerV" class="${this.hideV(this.showV)} container">
            <div id="ascensorV" class="scrollBar"></div>
        </div>        
        `
    }

    get ascensorV(): HTMLElement {
        return this.shadowRoot.querySelector('#ascensorV');
    }

    get ascensorH(): HTMLElement {
        return this.shadowRoot.querySelector('#ascensorH');
    }

    get containerH(): HTMLElement {
        return this.shadowRoot.querySelector('#containerH');
    }

    get containerV(): HTMLElement {
        return this.shadowRoot.querySelector('#containerV');
    }

    constructor() {
        super();
        this.isHorizontalScroll = false;
    }

    updated(changedProperties) {
        if(changedProperties.has('deltaV')) this.deltaVChanged(this.deltaV);
        if(changedProperties.has('deltaH')) this.deltaHChanged(this.deltaH);
        if(changedProperties.has('sizeH')) this.sizeHChanged(this.sizeH);
        if(changedProperties.has('sizeV')) this.sizeVChanged(this.sizeV);
    }

    private hideH(showH){
        return showH ? 'show' : 'hide';
    }

    private hideV(showV){
        return showV ? 'show' : 'hide';
    }

    private sizeVChanged(arg) {
        this.ascensorV.style.height = `${arg}px`
    }

    private sizeHChanged(arg) {
        // we set width of horizontal scroll but we diminute it from width of vertical scroll
        this.ascensorH.style.width = `${arg - this.containerV.getBoundingClientRect().width}px`; 
    }

    private deltaVChanged(arg) {
        this.ascensorV.style.transform = `translateY(${arg}px)`;
    }

    checkDeltaV(delta) {
        if (isNaN(delta) || delta === undefined || delta < 0) return 0;
        if (delta > (this.containerV.getBoundingClientRect().height - this.ascensorV.getBoundingClientRect().height)) return this.containerV.getBoundingClientRect().height - this.ascensorV.getBoundingClientRect().height;
        return delta;
    }

    private deltaHChanged(arg) {
        this.ascensorH.style.transform = `translateX(${arg}px)`;
    }

    checkDeltaH(delta) {
        if (isNaN(delta) || delta === undefined || delta < 0) return 0;
        if (delta > (this.containerH.getBoundingClientRect().width - this.ascensorH.getBoundingClientRect().width)) return this.containerH.getBoundingClientRect().width - this.ascensorH.getBoundingClientRect().width;
        return delta;
    }

    private enableHorizontalScroll(event) {
        this.isHorizontalScroll = true;
    }

    private disableHorizontalScroll() {
        this.isHorizontalScroll = false;
    }
}