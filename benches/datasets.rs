use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use ugnos::TagSet;

pub const DEFAULT_SEED: u64 = 0x_5547_4E4F_535F_4245; // fixed seed for stable benchmarks

#[derive(Clone, Debug)]
pub struct InsertOp {
    pub series: String,
    pub ts: u64,
    pub val: f64,
    pub tags: TagSet,
}

pub fn generate_insert_ops(
    seed: u64,
    points: usize,
    series_count: usize,
    tag_pairs: usize,
    tag_cardinality: u32,
) -> Vec<InsertOp> {
    assert!(series_count > 0);
    assert!(tag_cardinality > 0);

    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut ops = Vec::with_capacity(points);

    for i in 0..points {
        let series = format!("series_{}", i % series_count);
        let ts = i as u64;
        let val = rng.random::<u32>() as f64 * 0.001;

        let mut tags = TagSet::new();
        for k in 0..tag_pairs {
            let key = format!("k{}", k);
            let v = rng.random_range(0..tag_cardinality);
            let value = format!("v{}", v);
            tags.insert(key, value);
        }

        ops.push(InsertOp {
            series,
            ts,
            val,
            tags,
        });
    }

    ops
}
