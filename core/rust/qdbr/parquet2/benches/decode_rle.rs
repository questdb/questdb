use criterion::{criterion_group, criterion_main, Criterion};

use parquet2::encoding::hybrid_rle::{encode_u32, Decoder};

fn add_benchmark(c: &mut Criterion) {
    (10..=20).step_by(2).for_each(|log2_size| {
        let size = 2usize.pow(log2_size);

        let mut bytes = vec![];
        encode_u32(&mut bytes, (0..size as u32).map(|x| x % 128), 8).unwrap();

        c.bench_function(&format!("rle decode 2^{}", log2_size), |b| {
            b.iter(|| Decoder::new(&bytes, 1).count())
        });
    })
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
