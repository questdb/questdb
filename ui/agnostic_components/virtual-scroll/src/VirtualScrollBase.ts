import { LitElement, property, css, CSSResultArray,CSSResult } from "lit-element";
import { ScrollMixin } from "./behaviors/ScrollMixin";
import { applyMixins } from "./behaviors/Mixin.model";
import { InstanceMixin } from "./behaviors/InstanceMixin";
import { DomMixin } from "./behaviors/DomMixin";
// import { ContainerMixin } from "./behaviors/ContainerMixin";
import { SizeMixin } from "./behaviors/SizeMixin";
import { ItemsClass } from "./ItemsClass";



export class VirtualBase  extends ItemsClass {


    updated(changedProperties) {
        if(changedProperties.has('width')) this.widthChanged(this.width);
        if(changedProperties.has('height')) this.heightChanged(this.width, changedProperties.get('height'));
        if(changedProperties.has('items')) {
            this.observeNameContainerAndUnitarySize();
        }
        if (changedProperties.has('unitarySize')) {
            this.observeNameContainerAndUnitarySize();
        }
        if (changedProperties.has('needScrollHeight')) this.needScrollChanged();
        if (changedProperties.has('deltaH')) this.deltaHChanged();
        if (changedProperties.has('deltaV')) this.deltaVChanged();
        if (changedProperties.has('nameOfContainer')) this.computeHeightOfTemplate();
        // if (changedProperties.has('nameOfContainer')) this.observeNameContainerAndUnitarySize();
        if (changedProperties.has('lastIndex')) this.lastIndexChanged(this.lastIndex,changedProperties.get('lastIndex'));
    }

    private computeHeightOfTemplate() {
        const instance = document.createElement(this.nameOfContainer);
        instance.style.position = "absolute";
        instance.style.opacity = "0";
        this.appendChild(instance);
        setTimeout(() => {
            this.unitarySize = instance.getBoundingClientRect();
            this.removeChild(instance);
        },0);

    }

    private needScrollChanged() {
        this.container.style.position = this.needScrollHeight ? "initial" : "absolute";
    }

    private observeNameContainerAndUnitarySize() {
        if (!this.unitarySize || !this.nameOfContainer || !this.items) return;
        // this.templateForInstance = document.createElement(this.nameOfContainer);
        this.height = this.setHeight();
        this.numberOfInstance = this.computeNumberOfInstance();
        this.itemsChanged(this.items);
    }

    protected deltaHChanged() {
        if(this.scrollElement) this.scrollElement.deltaH = this.deltaH;
    }

    protected deltaVChanged() {
        if(this.scrollElement) this.scrollElement.deltaV = this.deltaV;
        if(this.unitarySize && this.deltaV != undefined)this.lastIndex = this.numberOfInstance + Math.floor(this.deltaV*this.ratioV/this.unitarySize.height);
    }

    protected widthChanged(width) {
        this.container.style.width = `${width}px`;
        this.instances.forEach(item => item.style.width = `${width}px`);
    }

    protected setHeight(): number {
        return this.items && this.unitarySize ? this.unitarySize.height*this.items.length : 0;
    }

    protected heightChanged(actual, previous) {
        if(actual != undefined && actual !== previous){ 
            this.container.style.height = `${actual}px`;
            this.evaluateScroll();
        }
    }

    constructor() {
        super();
        this.handleTrackBind = this.handleTrack.bind(this);
        this.handleWheelBind = this.handleWheel.bind(this);
        this.handleChangeInDomBind = this.handleChangeInDom.bind(this);
        this.isScrollWidth = false;
        let observer = new MutationObserver(this.handleChangeInDomBind);
        observer.observe(this, {childList: true, attributes: false});
        this.addEventListener('touchmove', this.handleTrackBind);
        this.addEventListener('wheel', this.handleWheelBind);
    }

    connectedCallback() {
        super.connectedCallback();
        window.addEventListener('resize', () => {
            this.evaluateScroll();
        })
    }

    disconnectedCallback() {
        super.disconnectedCallback();
        this.removeEventListener('touchmove', this.handleTrackBind);
        this.removeEventListener('wheel', this.handleWheelBind);
    }

    static get styles(): CSSResult|CSSResultArray {
        return [css`
            #container{
                width: inherit;
                overflow: hidden;
                transform: translateY(0px);
            }
            `
        ]
    }

    protected lastIndexChanged(arg, prev) {
        if(arg && prev){
            if(arg > prev){
                for(let i = prev; i< arg; i++) {
                    let indexOfItem = i%this.numberOfInstance;
                    let loop = Math.floor(i/this.numberOfInstance);
                    let item = this.instances[indexOfItem];
                    if(item) {
                        item.item = this.items[i];
                        item.index = i;
                        item.style.transform = `translate3d(0, ${(loop*(this.numberOfInstance*this.unitarySize.height))+(indexOfItem*this.unitarySize.height)}px, 0)`
                    }
                }
            }
    
            if(arg < prev){
                if(prev>arg+this.numberOfInstance)prev = arg + this.numberOfInstance //this is to be sure to not doing more that a loop in instances displayed to avoid to have bugs
                for(let i = arg; i< prev; i++) {
                    let indexOfItem = (i-this.numberOfInstance)%this.numberOfInstance;
                    let loop = Math.floor((i-this.numberOfInstance)/this.numberOfInstance);
                    let item = this.instances[indexOfItem];
                    if(item) {
                        item.item = this.items[(i-this.numberOfInstance)];
                        item.index = i-this.numberOfInstance;
                        item.style.transform = `translate3d(0, ${(loop*(this.numberOfInstance*this.unitarySize.height))+(indexOfItem*this.unitarySize.height)}px, 0)`
                    }
                }
            }
        }
    }
}

export interface VirtualBase extends InstanceMixin {}

applyMixins(VirtualBase, [InstanceMixin, ScrollMixin, SizeMixin])