import { LitElement } from "lit-element";


export type Constructor = new (args: any[]) => HTMLElement;
export const Mixin = (Base: typeof LitElement) => class Mixin extends Base {

}

export function applyMixins(derivedCtor: any, baseCtors: any[]) {
    baseCtors.forEach(baseCtor => {
        Object.getOwnPropertyNames(baseCtor.prototype).forEach(name => {
            Object.defineProperty(derivedCtor.prototype, name, Object.getOwnPropertyDescriptor(baseCtor.prototype, name));
        });
    });
}


