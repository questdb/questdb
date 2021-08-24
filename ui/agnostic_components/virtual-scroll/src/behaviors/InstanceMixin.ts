import { LitElement, property } from "lit-element";
import { SizeMixin } from "./SizeMixin";
import { ScrollMixin } from "./ScrollMixin";

export interface VirtualScrollElement extends HTMLElement {
    item: {[key: string]: string};
    index: number;
}

    export class InstanceMixin extends LitElement{
        private _numberOfInstance: number;
        
        @property({
            type: Number
        })
        protected get numberOfInstance(): number {
            return this._numberOfInstance;
        }
    
        protected set numberOfInstance(value: number) {
            const oldValue = this._numberOfInstance;
            this._numberOfInstance = value;
            this.requestUpdate('numberOfInstance', oldValue);
            this.adjustNumberOfInstances(value);
        }
        
        protected get instances(): NodeListOf<VirtualScrollElement> {
            return this.querySelectorAll(this.nameOfContainer);
        }

        protected computeNumberOfInstance() {
            if(window.innerHeight && this.unitarySize && this.unitarySize.height != 0) {
                let numberOfInstance = Math.ceil(window.innerHeight/this.unitarySize.height*1.2);
                return numberOfInstance; 
            }
            return 0;
        }

        private adjustNumberOfInstances(numberInstance: Number) {
            if(numberInstance && numberInstance != Infinity){
                //if there is not enough instances of items, we add new ones
                if(this.instances.length < numberInstance) {
                    for(let i = this.instances.length; i<numberInstance; i++){
                        let instance = this.content;
                        (instance as HTMLElement).style.position = "absolute";
                        this.appendChild(instance);
                        (instance as HTMLElement).style.transform = `translate3d(0, ${i*this.unitarySize.height}px, 0)`;
                    }
                }
                //otherwise we remove instances which are no longer necessary in display area.
                else if(this.instances.length > numberInstance){
                    for(let i = this.instances.length-1; i >= numberInstance; i--){
                        this.removeChild(this.instances[i]);
                    }
                }
                this.dispatchEvent(new CustomEvent('containers-ready', {detail: {value: this.instances}, bubbles: true, composed: true}));
            }
            //finally we affect to each instances corresponding data in items array indices.
            this.lastIndex = this.instances.length;
        }

        protected resetInstances() {
            this.instances.forEach((instance, index)=>{
                if(this.unitarySize) (instance as VirtualScrollElement).style.transform = `translate3d(0, ${index*this.unitarySize.height}px, 0)`;
                instance.item = undefined;
                instance.index = undefined;
                this.deltaV = 0;
            });
            this.setOnTop();
        }

        protected removeInstances(): void {
            if (this.instances.length) this.instances.forEach(item => this.removeChild(item));
        }

        protected itemsChanged(value: any[], old: any = []) {
            this.resetInstances();
            if (value) this.instances.forEach((instance, index) => (instance.item = value[index]));
        }
    }

    export interface InstanceMixin extends LitElement, SizeMixin, ScrollMixin {}