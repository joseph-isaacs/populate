use populate_gen_tsbs::model::Point;
use populate_gen_tsbs::sim::{AnySim, Sim};
use populate_gen_tsbs::{Config, Tsbs, UseCase};

#[test]
fn every_use_case_matches_the_sequential_simulator_and_can_be_replayed() {
    for use_case in [
        UseCase::Devops,
        UseCase::CpuOnly,
        UseCase::CpuSingle,
        UseCase::DevopsGeneric,
        UseCase::Iot,
    ] {
        for groups in [1, 3] {
            let mut cfg = Config::new(use_case, 3, 123);
            cfg.end = cfg.start + 40_000_000_000;
            cfg.groups = groups;
            cfg.group_id = groups - 1;
            let t = Tsbs::new(cfg).unwrap();
            let mut sim = AnySim::new(t.config());
            let mut point = Point::default();
            let mut expected = Vec::new();
            let mut group = 0;
            while !sim.finished() {
                if sim.next(&mut point) {
                    if group == groups - 1 {
                        expected.push(point.clone());
                    }
                    group = (group + 1) % groups;
                }
            }
            let raw = t.raw(7);
            let metadata = raw.dist().statics();
            assert!(!metadata.measurements.is_empty());
            for threads in [1, 3] {
                let mut actual = Vec::new();
                raw.for_each_batch(threads, |c| actual.extend(c.values));
                assert_eq!(
                    actual, expected,
                    "{use_case:?}, group {groups}, threads {threads}"
                );
            }
            let first = raw.batch(0, 1).values;
            assert_eq!(first, raw.batch(0, 1).values);
        }
    }
}
