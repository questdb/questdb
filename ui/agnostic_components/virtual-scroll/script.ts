

const init = () => {
    const virtual = document.querySelector('virtual-scroll');
    (virtual as any).items = Array.from({length: 10000}, (item, index) => ({test: index}));
}

init();