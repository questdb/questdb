import { LitElement, customElement, html, css, svg, property }  from 'lit-element';


export class VirtualScrollRow extends LitElement {
    @property({
        reflect: true,
        type: Boolean
    })  show = false;

    @property({
        type: Object
    }) item = {index: ""}

    get indexItem(): string {
        return this.item.index||"";
    }

    static get styles() {
    return css`
    :host {
        min-height: 65px;
        min-width: 150px;
    }`
    }

    render() {
    return html`
        <div>ceci est un l'item n° ${this.item.index} </div>
        `;
    }
}
