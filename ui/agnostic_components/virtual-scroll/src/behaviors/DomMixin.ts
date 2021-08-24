import { LitElement, property } from "lit-element";
import { Constructor } from "./Mixin.model";
import { ContainerMixin } from "./ContainerMixin";

// export const DomMixin = (Base:  typeof LitElement extends Constructor? typeof LitElement : typeof LitElement) => class DomMixin extends Base { 
    
export class DomMixin extends LitElement{
    private _height: number;
    private _nameOfContainer: string = "";
    private _unitarySize: ClientRect|DOMRect;
    private _width: number;

    @property({
        type: String,
    })
    get nameOfContainer(): string {
        return this._nameOfContainer;
    }

    set nameOfContainer(value: string) {
        const oldValue = this._nameOfContainer;
        this._nameOfContainer = value;
        this.requestUpdate('nameOfContainer', oldValue);
    }


    @property({
        type: Object
    })
    protected get unitarySize(): DOMRect|ClientRect {
        return this._unitarySize;
    }

    protected set unitarySize(value: DOMRect|ClientRect) {
        const old = this._unitarySize;
        this._unitarySize = value;
        this.requestUpdate('unitarySize', old);
    }

    @property({
        reflect: true,
        type: Boolean
    })  show = false;


    @property({
        type: Number,
        reflect: true,
        converter(value) {
            return Number(value);
        }
    }) 
    public get width(): number {
        return this._width;
    }

    public set width(value: number) {
        let oldValue = this._width;
        this._width = value;
        this.requestUpdate('width', oldValue);
        this.widthChanged(value);
    }

    protected widthChanged(value: number) {

    }

    
    @property({
        type: Number
    }) 
    protected get height(): number {
        return this._height;
    }
    
    protected set height(value: number) {
        let oldValue = this._height;
        this._height = value;
        if(value !== undefined) {
            this.requestUpdate('height', oldValue);
            this.heightChanged(value, oldValue);
        }
    }

    protected heightChanged(actual, previous) { }
}

export interface DomMixin extends LitElement {}
