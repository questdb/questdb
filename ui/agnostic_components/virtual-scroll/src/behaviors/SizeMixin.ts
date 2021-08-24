import { LitElement, html, css, svg, property }  from 'lit-element';
import { DomMixin } from './DomMixin';
import { VirtualScrollElement } from './InstanceMixin';



export class SizeMixin extends LitElement{

    protected async observeSlot() {
        // this.unitarySize = await this.getSizeTemplateVirtual();
    }
    protected instances:  NodeListOf<VirtualScrollElement>;

    private getSizeTemplateVirtual(): Promise<DOMRect|ClientRect> {
        return new Promise((resolve ,reject) => {
            let size;
            let slotelement = this.templateForInstance = this.querySelector(this.nameOfContainer);
            // if(!slotelement) reject('no element retrieved in lightDom');
            let observer = new MutationObserver((mutationList) => {
                if(mutationList[0] && mutationList[0].addedNodes) {
                    const addedElement: Node = mutationList[0].addedNodes[0];
                    if ((addedElement && addedElement as HTMLElement).localName === "slot") return;
                    slotelement = Array.prototype.reduce.call(this.children, (prev, crt) => crt === addedElement ? crt : prev, null);
                    if (!!!slotelement) return;
                    size = slotelement.getBoundingClientRect();
                    this.templateForInstance = slotelement;
                    // this.computeSizeOfHost();
                    // size = mutationList[0].addedNodes[0].getBoundingClientRect();
                    // this.template = mutationList[0].addedNodes[0];
                }
                if(!!slotelement) observer.disconnect();
                resolve(size);
            });
            observer.observe(this, {childList: true});
           
        })
    }

    private _templateForInstance: HTMLTemplateElement;

    protected get templateForInstance(): Element|HTMLTemplateElement {
        return this._templateForInstance;
    }

    protected set templateForInstance(value: Element|HTMLTemplateElement) {
        this.removeInstances();
        let template = document.createElement('template');
        this._templateForInstance = template;
        if (value) {
            template.content.appendChild(value);

        }
    }
    
    protected get content(): Element|HTMLTemplateElement {
        return (this.templateForInstance as HTMLTemplateElement).content.cloneNode(true)['firstElementChild'];
    }

    protected removeInstances(): void {
        if (this.instances.length) this.instances.forEach(item => this.removeChild(item));
    }
}

export interface SizeMixin extends DomMixin, LitElement{}