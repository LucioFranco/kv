use criterion::{criterion_group, criterion_main, Criterion};
use kv::{pb::wal as pb, wal::Log};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("save_entry", |b| {
        b.iter_with_setup(
            || {
                let temp_dir = std::env::temp_dir();
                let wal = Log::new(temp_dir).unwrap();
                let data = vec![0u8; 1024];

                let entry = pb::Record {
                    data,
                    ..Default::default()
                };

                let entries = (0..100)
                    .into_iter()
                    .map(|_| entry.clone())
                    .collect::<Vec<_>>();

                (wal, entries)
            },
            |(mut wal, entries)| {
                for entry in entries {
                    wal.save(entry).unwrap();
                }
            },
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
