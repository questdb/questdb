import { LitElement, property } from "lit-element";
import { DomMixin } from "./behaviors/DomMixin";
import { VirtualBase } from "./VirtualScrollBase";


export class ItemsClass extends DomMixin {

    private _items: any[] = [];
    @property({
        type: Array
    }) 
    public get items(): any[] {
        return this._items;
    }
    public set items(value: any[]) {
        let oldValue = this._items;
        this._items = value;
        this.requestUpdate('items', oldValue);
    }
}

export interface ItemsClass {

}