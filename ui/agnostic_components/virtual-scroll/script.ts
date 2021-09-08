

const init = () => {
    const virtual = document.querySelector('virtual-scroll');
    (virtual as any).items = Array.from({length: 100000}, (item, index) => ({index}));
}

init();