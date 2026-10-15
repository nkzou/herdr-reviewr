//! Times one full world build per tab and scope, the work a pane open or a switch waits on.
//! Usage: `cargo run --release --example bench_world -- <repo-path> [runs]`

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use herdr_reviewr::app::Tab;
use herdr_reviewr::model::Scope;
use herdr_reviewr::world::{WorldInput, build};

fn main() {
    let mut args = std::env::args().skip(1);
    let repo = PathBuf::from(args.next().expect("usage: bench_world <repo> [runs]"));
    let runs: usize = args.next().and_then(|n| n.parse().ok()).unwrap_or(3);
    for (tab, scope) in [
        (Tab::Changes, Scope::Uncommitted),
        (Tab::Changes, Scope::Branch),
        (Tab::AllFiles, Scope::Uncommitted),
    ] {
        let input = WorldInput {
            repo: repo.clone(),
            tab,
            scope,
            base: None,
            base_epoch: 0,
            turn_baseline: None,
            commit_pick: None,
            toggled_dirs: HashSet::new(),
        };
        let mut times: Vec<f64> = (0..runs)
            .map(|_| {
                let t = Instant::now();
                let built = build(&input).expect("build");
                std::hint::black_box(built);
                t.elapsed().as_secs_f64() * 1000.0
            })
            .collect();
        let first = times[0];
        times.sort_by(f64::total_cmp);
        let name = format!("{tab:?}/{scope:?}");
        println!("{name:<24} first {first:>8.0}ms   median {:>8.0}ms", times[runs / 2]);
    }
    let t = Instant::now();
    herdr_reviewr::git::snapshot_worktree(&repo).expect("snapshot");
    println!("{:<24} once  {:>8.0}ms", "snapshot_worktree", t.elapsed().as_secs_f64() * 1000.0);
}
