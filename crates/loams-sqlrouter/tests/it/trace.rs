use loams_sqlrouter::machine::{Ctx, Machine, Millis};
use loams_sqlrouter::trace::{SpecEvent, SpecValue, VecSink};
use rand::SeedableRng;

/// A toy machine: counts inputs and reports each as a `ShardMap/Reload` event.
struct Counter(u32);

impl Machine for Counter {
    type Input = &'static str;
    type Output = u32;
    fn on(&mut self, ctx: &mut Ctx<'_>, instance: &'static str) -> Vec<u32> {
        self.0 += 1;
        ctx.trace.emit(SpecEvent {
            spec: "ShardMap",
            action: "Reload",
            fields: vec![
                ("instance", instance.into()),
                ("gen", self.0.into()),
                ("ok", true.into()),
            ],
        });
        vec![self.0]
    }
}

#[test]
fn trace_sink_records_in_order() {
    let mut rng = rand::rngs::ChaCha8Rng::seed_from_u64(7);
    let mut sink = VecSink::default();
    let mut m = Counter(0);
    let mut outs = Vec::new();
    for (t, i) in ["pgdog-1", "pgdog-2", "pgdog-1"].into_iter().enumerate() {
        let mut ctx = Ctx {
            now: Millis(t as u64 * 10),
            rng: &mut rng,
            trace: &mut sink,
        };
        outs.extend(m.on(&mut ctx, i));
    }
    assert_eq!(outs, [1, 2, 3]);
    let seen: Vec<_> = sink
        .0
        .iter()
        .map(|e| (e.fields[0].1.clone(), e.fields[1].1.clone()))
        .collect();
    assert_eq!(
        seen,
        [
            (SpecValue::Str("pgdog-1".into()), SpecValue::Int(1)),
            (SpecValue::Str("pgdog-2".into()), SpecValue::Int(2)),
            (SpecValue::Str("pgdog-1".into()), SpecValue::Int(3)),
        ]
    );
    // Events serialise as the JSON lines §31 §11.3 describes.
    let json = serde_json::to_string(&sink.0[0]).unwrap();
    assert_eq!(
        json,
        r#"{"spec":"ShardMap","action":"Reload","fields":[["instance","pgdog-1"],["gen",1],["ok",true]]}"#
    );
}

#[test]
fn driver_randomness_preserves_the_legacy_seeded_stream() {
    use rand_chacha::rand_core::{RngCore as _, SeedableRng as _};
    for seed in [0, 7, u64::MAX] {
        let mut legacy = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        let mut current = rand::rngs::ChaCha8Rng::seed_from_u64(seed);
        let mut sink = VecSink::default();
        let ctx = Ctx {
            now: Millis(0),
            rng: &mut current,
            trace: &mut sink,
        };
        for _ in 0..64 {
            assert_eq!(ctx.rng.next_u64(), legacy.next_u64(), "seed {seed}");
        }
    }
}
