use criterion::{criterion_group, criterion_main, Criterion};

use parquet2::encoding::bitpacked::Decoder;

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let bytes = (0..size as u32)
            .map(|x| 0b01011011u8.rotate_left(x))
            .collect::<Vec<_>>();

        c.bench_function(&format!("bitpacking 2^{}", log2_size), |b| {
            b.iter(|| Decoder::<u32>::try_new(&bytes, 1, size).unwrap().count())
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
