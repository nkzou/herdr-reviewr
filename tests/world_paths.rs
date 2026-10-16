//! A refresh limited to a watcher batch's paths lands exactly what a full build of the same
//! worktree would, in every scope and on both file tabs, through the real world worker.

mod common;

use std::collections::BTreeSet;
use std::sync::mpsc;

use common::Repo;
use herdr_reviewr::app::{App, Tab};
use herdr_reviewr::model::{CommitPick, Scope};
use herdr_reviewr::wake::{Waker, channel};
use herdr_reviewr::world::{self, Refresh, WorldCompletion, WorldInput, WorldJob, WorldSnapshot};

struct Worker {
    jobs: mpsc::Sender<WorldJob>,
    done: mpsc::Receiver<WorldCompletion>,
    generation: u64,
}

impl Worker {
    fn on(repo: &Repo) -> Self {
        let (jobs, rx) = mpsc::channel();
        let (tx, done) = channel(&Waker::detached());
        world::spawn(repo.path_buf(), rx, tx);
        Self { jobs, done, generation: 0 }
    }

    /// One job through the worker: the input it built for and its snapshot, `None` when nothing
    /// needed re-reading.
    fn run(&mut self, input: &WorldInput, refresh: Refresh) -> (WorldInput, Option<WorldSnapshot>) {
        self.send(input, refresh, false)
    }

    /// One switch through the worker: `refresh` is what moved since the last job.
    fn switch(&mut self, input: &WorldInput, refresh: Refresh) -> WorldSnapshot {
        self.send(input, refresh, true).1.expect("a switch builds")
    }

    fn send(
        &mut self,
        input: &WorldInput,
        refresh: Refresh,
        switch: bool,
    ) -> (WorldInput, Option<WorldSnapshot>) {
        self.generation += 1;
        let job = WorldJob {
            generation: self.generation,
            input: input.clone(),
            reveal: false,
            refresh,
            switch,
        };
        self.jobs.send(job).unwrap();
        let mut completion = self.done.recv().unwrap();
        // A build waiting on its ignored walk is followed by the build with them.
        while completion
            .snapshot
            .as_ref()
            .is_some_and(|s| s.as_ref().is_ok_and(|s| s.ignored_pending))
        {
            let follow_up = self.done.recv().unwrap();
            assert_eq!(
                follow_up.generation, completion.generation,
                "the follow-up lands as its job"
            );
            completion = follow_up;
        }
        (completion.input, completion.snapshot.map(|s| s.expect("the build succeeds")))
    }
}

fn paths(list: &[&str]) -> Refresh {
    Refresh::paths(list.iter().map(|p| (*p).to_string()))
}

/// A repository with every kind of file a change can touch, and a feature branch off `main`.
fn fixture() -> Repo {
    let r = Repo::init();
    r.write(".gitignore", "target/\n");
    r.write(".gitattributes", "attr.txt -diff\n");
    // Each file its own content: identical files pair renames ambiguously, and a narrow diff may
    // then pair differently from a full one (both right; the next full build settles it).
    for f in ["a.txt", "b.txt", "c.txt", "sub/d.txt", "sub/e.txt", "attr.txt"] {
        r.write(f, &format!("one\ntwo\n{f}\n"));
    }
    std::fs::write(r.path().join("bin.dat"), [0_u8, 1, 2]).unwrap();
    r.commit_all("init");
    r.git(&["checkout", "-q", "-b", "feature"]);
    r.write("a.txt", "one\ntwo\nfeature\n");
    r.commit_all("feature");
    r
}

/// A named mutation of the worktree and the paths its watcher batch carries.
type Step = (&'static str, Box<dyn Fn(&Repo)>, Vec<&'static str>);

/// A name full of pathspec glob characters; Windows forbids `*` in a file name.
const GLOB_NAME: &str = if cfg!(windows) { "glob[x].txt" } else { "glob*[x].txt" };

fn steps() -> Vec<Step> {
    vec![
        ("modify", Box::new(|r: &Repo| r.write("a.txt", "changed\n")), vec!["a.txt"]),
        ("untracked add", Box::new(|r: &Repo| r.write("new.txt", "x\ny\n")), vec!["new.txt"]),
        ("tracked delete", Box::new(|r: &Repo| r.remove("b.txt")), vec!["b.txt"]),
        ("untracked delete", Box::new(|r: &Repo| r.remove("new.txt")), vec!["new.txt"]),
        (
            "binary",
            Box::new(|r: &Repo| std::fs::write(r.path().join("bin.dat"), [0_u8, 9]).unwrap()),
            vec!["bin.dat"],
        ),
        ("-diff attribute", Box::new(|r: &Repo| r.write("attr.txt", "z\n")), vec!["attr.txt"]),
        (
            "rename on disk",
            Box::new(|r: &Repo| {
                std::fs::rename(r.path().join("c.txt"), r.path().join("c2.txt")).unwrap();
            }),
            vec!["c.txt", "c2.txt"],
        ),
        // A rename's new side drifts, then a closer copy of its source appears in a batch that
        // names neither side of the rename: the source pairs with the copy.
        (
            "a rename's new side drifts",
            Box::new(|r: &Repo| r.write("c2.txt", "one\ntwo\nc.txt\nmore\n")),
            vec!["c2.txt"],
        ),
        (
            "a closer copy of a rename's source",
            Box::new(|r: &Repo| r.write("c3.txt", "one\ntwo\nc.txt\n")),
            vec!["c3.txt"],
        ),
        (
            "staged rename",
            Box::new(|r: &Repo| {
                r.git(&["mv", "a.txt", "a2.txt"]);
            }),
            vec!["a.txt", "a2.txt"],
        ),
        // A later batch that names one side of a rename re-reads its partner too.
        (
            "edit a rename's new side",
            Box::new(|r: &Repo| r.write("a2.txt", "one\ntwo\nfeature\nmore\n")),
            vec!["a2.txt"],
        ),
        (
            "recreate a rename's old side",
            Box::new(|r: &Repo| r.write("a.txt", "back\n")),
            vec!["a.txt"],
        ),
        (
            "odd names",
            Box::new(|r: &Repo| {
                r.write("sp ace.txt", "x\n");
                r.write("-dash.txt", "x\n");
                r.write(GLOB_NAME, "x\n");
            }),
            vec!["sp ace.txt", "-dash.txt", GLOB_NAME],
        ),
        (
            "folder removed",
            Box::new(|r: &Repo| std::fs::remove_dir_all(r.path().join("sub")).unwrap()),
            vec!["sub"],
        ),
        ("ignored write", Box::new(|r: &Repo| r.write("target/out.o", "x\n")), vec!["target"]),
    ]
}

fn input_for(r: &Repo, scope: Scope, tab: Tab) -> WorldInput {
    let root = herdr_reviewr::git::toplevel(r.path()).unwrap();
    let mut input = App::new(root, scope, Some("main".into())).world_input();
    input.tab = tab;
    if scope == Scope::Commits {
        let head = r.git(&["rev-parse", "HEAD"]).trim().to_string();
        input.commit_pick = Some(CommitPick::single(&head));
    }
    input
}

/// Walk every step in one scope and tab, each path-limited landing checked against a full build.
fn path_limited_equals_full(scope: Scope, tab: Tab) {
    let r = fixture();
    if scope == Scope::LastTurn {
        let tree = r.git(&["rev-parse", "HEAD^{tree}"]).trim().to_string();
        herdr_reviewr::git::write_baseline_ref(r.path(), &tree).unwrap();
    }
    let mut worker = Worker::on(&r);
    let input = input_for(&r, scope, tab);
    let (_, first) = worker.run(&input, Refresh::Full);
    assert!(first.is_some(), "the first build is full");
    for (name, mutate, batch) in steps() {
        mutate(&r);
        let (built_for, scoped) = worker.run(&input, paths(&batch));
        // A pick reads committed trees: on `Changes`, a worktree batch has nothing to re-read.
        if scope == Scope::Commits && tab == Tab::Changes {
            assert!(scoped.is_none(), "{name}: a pick ignores the worktree");
            continue;
        }
        let scoped = scoped.unwrap_or_else(|| panic!("{scope:?}/{tab:?} {name}: a batch re-reads"));
        let full = world::build(&built_for).unwrap();
        let files = |s: &WorldSnapshot| s.changeset.files.clone();
        assert_eq!(files(&scoped), files(&full), "{scope:?}/{tab:?} {name}: the changeset");
        let ends = |s: &WorldSnapshot| s.changeset.ends.clone();
        assert_eq!(ends(&scoped), ends(&full), "{scope:?}/{tab:?} {name}: the ends");
        assert_eq!(scoped.entries, full.entries, "{scope:?}/{tab:?} {name}: the navigator");
        let batch: BTreeSet<String> = batch.iter().map(|p| (*p).to_string()).collect();
        let touched = scoped.touched.unwrap_or_else(|| panic!("{scope:?}/{tab:?} {name}: by path"));
        assert!(touched.is_superset(&batch), "{scope:?}/{tab:?} {name}: re-read {touched:?}");
    }
}

#[test]
fn uncommitted_changes_tab() {
    path_limited_equals_full(Scope::Uncommitted, Tab::Changes);
}

#[test]
fn uncommitted_all_files_tab() {
    path_limited_equals_full(Scope::Uncommitted, Tab::AllFiles);
}

#[test]
fn branch_changes_tab() {
    path_limited_equals_full(Scope::Branch, Tab::Changes);
}

#[test]
fn branch_all_files_tab() {
    path_limited_equals_full(Scope::Branch, Tab::AllFiles);
}

#[test]
fn last_turn_changes_tab() {
    path_limited_equals_full(Scope::LastTurn, Tab::Changes);
}

#[test]
fn last_turn_all_files_tab() {
    path_limited_equals_full(Scope::LastTurn, Tab::AllFiles);
}

#[test]
fn commits_changes_tab() {
    path_limited_equals_full(Scope::Commits, Tab::Changes);
}

#[test]
fn an_index_rewrite_rebuilds_only_when_what_it_stages_moved() {
    let r = fixture();
    let mut worker = Worker::on(&r);
    let input = input_for(&r, Scope::Uncommitted, Tab::Changes);
    worker.run(&input, Refresh::Full);
    let index_only = Refresh::Paths { paths: BTreeSet::new(), index: true };

    // An agent's `git status` after touching files rewrites the index for stat data alone.
    let past = std::time::SystemTime::now() - std::time::Duration::from_hours(1);
    std::fs::File::options()
        .write(true)
        .open(r.path().join("a.txt"))
        .unwrap()
        .set_modified(past)
        .unwrap();
    r.git(&["status", "--porcelain"]);
    let (_, unchanged) = worker.run(&input, index_only.clone());
    assert!(unchanged.is_none(), "a stat-only rewrite re-reads nothing");

    r.write("a.txt", "staged\n");
    r.git(&["add", "a.txt"]);
    let (built_for, staged) = worker.run(&input, index_only);
    let staged = staged.expect("a real stage rebuilds");
    assert_eq!(staged.touched, None, "in full");
    assert_eq!(staged.changeset.files, world::build(&built_for).unwrap().changeset.files);
}

#[test]
fn a_batch_past_the_byte_cap_rebuilds_in_full() {
    // Long names pass the cap in bytes long before they would in count.
    let long = |i: usize| format!("{}/{i}", "deep".repeat(60));
    let fits: BTreeSet<String> = (0..60).map(long).collect();
    let mut refresh = Refresh::default();
    refresh.absorb(Refresh::paths(fits.clone()));
    assert_eq!(refresh.named_paths(), Some(&fits), "under the cap stays by path");
    let many: BTreeSet<String> = (0..80).map(long).collect();
    refresh.absorb(Refresh::paths(many));
    assert!(refresh.is_full(), "past the cap is a full rebuild");

    let mut small = paths(&["a.txt"]);
    small.absorb(Refresh::Full);
    assert!(small.is_full(), "anything full stays full");
}

#[test]
fn commits_all_files_tab() {
    path_limited_equals_full(Scope::Commits, Tab::AllFiles);
}

#[test]
fn a_batch_rereads_the_open_file_only_when_it_names_it() {
    let r = Repo::init();
    r.write("a.txt", "one\n");
    r.write("b.txt", "b\n");
    r.commit_all("init");
    r.write("a.txt", "two\n");
    r.write("b.txt", "bb\n");
    let mut app = common::app_on(&r);
    assert_eq!(app.diff_path.as_deref(), Some("a.txt"), "the first changed file is open");
    let text = |app: &App| {
        app.visible.iter().map(herdr_reviewr::diff::Row::text).collect::<Vec<_>>().join("\n")
    };
    assert!(text(&app).contains("two"));

    r.write("a.txt", "three\n");
    let mut other = world::build(&app.world_input()).unwrap();
    other.touched = Some(["b.txt".to_string()].into());
    app.reconcile_world(other);
    assert!(!text(&app).contains("three"), "a batch naming only b.txt does not reread a.txt");

    let mut named = world::build(&app.world_input()).unwrap();
    named.touched = Some(["a.txt".to_string()].into());
    app.reconcile_world(named);
    assert!(text(&app).contains("three"), "a batch naming the open file rereads it");

    r.write("a.txt", "four\n");
    app.reconcile_world(world::build(&app.world_input()).unwrap());
    assert!(text(&app).contains("four"), "a full build rereads it");
}

/// A last-turn worker past its first full build, on the fixture with a baseline at `HEAD`.
fn last_turn(r: &Repo) -> (Worker, WorldInput) {
    let tree = r.git(&["rev-parse", "HEAD^{tree}"]).trim().to_string();
    herdr_reviewr::git::write_baseline_ref(r.path(), &tree).unwrap();
    let mut worker = Worker::on(r);
    let input = input_for(r, Scope::LastTurn, Tab::Changes);
    worker.run(&input, Refresh::Full);
    (worker, input)
}

#[test]
fn a_path_limited_last_turn_tree_is_the_full_snapshots_tree() {
    let r = fixture();
    let (mut worker, input) = last_turn(&r);
    r.write("a.txt", "edited\n");
    r.write("brand new.txt", "x\n");
    r.remove("b.txt");
    let (_, scoped) = worker.run(&input, paths(&["a.txt", "brand new.txt", "b.txt"]));
    let scoped = scoped.expect("a batch re-reads");
    assert!(scoped.touched.is_some(), "by path");
    let full = herdr_reviewr::git::snapshot_worktree(r.path()).unwrap();
    let ends = scoped.changeset.ends.expect("last-turn has ends");
    assert_eq!(ends.new.as_deref(), Some(full.as_str()), "the same tree id");
}

#[test]
fn a_moved_folder_past_the_byte_cap_rebuilds_in_full() {
    let r = fixture();
    // Names long enough that the folder's files pass the cap once listed one by one.
    // Each path stays under Windows' 260-character limit; together they pass the cap.
    for i in 0..110 {
        r.write(&format!("big/{}-{i}.txt", "n".repeat(150)), &format!("{i}\n"));
    }
    r.commit_all("big");
    let (mut worker, input) = last_turn(&r);
    std::fs::rename(r.path().join("big"), r.path().join("moved")).unwrap();
    let (built_for, landed) = worker.run(&input, paths(&["big", "moved"]));
    let landed = landed.expect("a batch re-reads");
    assert_eq!(landed.touched, None, "past the cap the build is full");
    let full = world::build(&built_for).unwrap();
    assert_eq!(landed.changeset.files, full.changeset.files);
}

#[cfg(unix)]
#[test]
fn a_failed_path_limited_read_retries_in_full_at_once() {
    let r = fixture();
    let (mut worker, input) = last_turn(&r);
    // A folder replaced by a link: `git add` refuses a pathspec beyond a symbolic link.
    std::fs::remove_dir_all(r.path().join("sub")).unwrap();
    std::os::unix::fs::symlink(r.path().join("target"), r.path().join("sub")).unwrap();
    let (built_for, landed) = worker.run(&input, paths(&["sub/d.txt"]));
    let landed = landed.expect("a batch re-reads");
    assert_eq!(landed.touched, None, "the retry is a full build");
    assert_eq!(landed.changeset.files, world::build(&built_for).unwrap().changeset.files);
}

#[test]
fn a_path_limited_read_never_rewrites_the_index() {
    let r = fixture();
    let mut worker = Worker::on(&r);
    let input = input_for(&r, Scope::Uncommitted, Tab::AllFiles);
    worker.run(&input, Refresh::Full);
    let index = r.path().join(".git/index");
    let stamp = || std::fs::metadata(&index).unwrap().modified().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    // Same content, a later mtime: the stat data a refresh would write back.
    r.write("a.txt", "one\ntwo\nfeature\n");
    r.write("c.txt", "edited\n");
    let before = stamp();
    let (_, landed) = worker.run(&input, paths(&["a.txt", "c.txt"]));
    assert!(landed.expect("a batch re-reads").touched.is_some(), "by path");
    assert_eq!(stamp(), before, ".git/index was rewritten");
}

fn changed(snapshot: &WorldSnapshot) -> Vec<String> {
    snapshot.changeset.files.keys().cloned().collect()
}

fn listed(snapshot: &WorldSnapshot) -> Vec<String> {
    snapshot.entries.iter().map(|e| e.path.clone()).collect()
}

/// A switch re-reads only what a refresh said moved: the parts it shares come from the cache.
#[test]
fn a_switch_reads_only_what_moved() {
    let r = fixture();
    let mut worker = Worker::on(&r);
    let uncommitted = input_for(&r, Scope::Uncommitted, Tab::Changes);
    let branch = input_for(&r, Scope::Branch, Tab::Changes);
    assert!(changed(&worker.switch(&uncommitted, Refresh::Full)).is_empty());
    // Written behind the worker's back: no refresh names it, so the cached listing stands.
    r.write("quiet.txt", "q\n");
    assert_eq!(changed(&worker.switch(&branch, Refresh::default())), ["a.txt"]);
    // Once a batch names it, the next switch re-reads it.
    r.write("named.txt", "n\n");
    let built = worker.switch(&uncommitted, paths(&["named.txt"]));
    assert_eq!(changed(&built), ["named.txt"]);
    // A full refresh re-reads everything.
    assert_eq!(
        changed(&worker.switch(&branch, Refresh::Full)),
        ["a.txt", "named.txt", "quiet.txt"]
    );
}

/// A switch back to the last build's input, with nothing moved, is that build.
#[test]
fn a_switch_back_with_nothing_moved_is_the_last_build() {
    let r = fixture();
    let mut worker = Worker::on(&r);
    let input = input_for(&r, Scope::Uncommitted, Tab::Changes);
    r.write("new.txt", "x\n");
    let first = worker.switch(&input, Refresh::Full);
    r.write("late.txt", "x\n");
    let again = worker.switch(&input, Refresh::default());
    assert_eq!(changed(&again), changed(&first));
    assert_eq!(again.touched, None, "a whole build, reconciled whole");
}

/// What moved while the `PR` tab showed still reaches the next file tab's build.
#[test]
fn what_moved_on_the_pr_tab_reaches_the_next_build() {
    let r = fixture();
    let mut worker = Worker::on(&r);
    let changes = input_for(&r, Scope::Uncommitted, Tab::Changes);
    let pr = input_for(&r, Scope::Uncommitted, Tab::Pr);
    worker.switch(&changes, Refresh::Full);
    r.write("new.txt", "x\n");
    assert!(worker.run(&pr, paths(&["new.txt"])).1.is_none(), "the PR tab builds nothing");
    assert_eq!(changed(&worker.switch(&changes, Refresh::default())), ["new.txt"]);
}

/// `All files` reuses the cached ignored listing across switches, and a batch re-reads under it.
#[test]
fn all_files_reuses_the_ignored_listing_and_a_batch_rereads_under_it() {
    let r = fixture();
    let mut worker = Worker::on(&r);
    let uncommitted = input_for(&r, Scope::Uncommitted, Tab::AllFiles);
    let branch = input_for(&r, Scope::Branch, Tab::AllFiles);
    r.write("target/out.o", "x\n");
    assert!(listed(&worker.switch(&uncommitted, Refresh::Full)).contains(&"target".to_string()));
    r.write("build/x.o", "x\n");
    r.write(".gitignore", "target/\nbuild/\n");
    let built = worker.switch(&branch, paths(&["build", ".gitignore"]));
    let entries = listed(&built);
    assert!(entries.contains(&"target".to_string()), "{entries:?}");
    assert!(entries.contains(&"build".to_string()), "{entries:?}");
    assert!(!entries.iter().any(|e| e.starts_with("build/")), "collapsed: {entries:?}");
}

/// An `All files` build lands whole or before its ignored walk, which then lands as the same job.
#[test]
fn all_files_lands_before_its_ignored_walk_and_the_walk_follows() {
    let r = fixture();
    r.write("target/out.o", "x\n");
    let mut worker = Worker::on(&r);
    let input = input_for(&r, Scope::Uncommitted, Tab::AllFiles);
    let job = WorldJob { generation: 1, input, reveal: true, refresh: Refresh::Full, switch: true };
    worker.jobs.send(job).unwrap();
    let mut built = worker.done.recv().unwrap().snapshot.unwrap().unwrap();
    assert!(listed(&built).contains(&"a.txt".to_string()), "the tree itself is whole");
    if built.ignored_pending {
        assert!(!listed(&built).contains(&"target".to_string()), "no ignored entry yet");
        let follow_up = worker.done.recv().unwrap();
        assert_eq!((follow_up.generation, follow_up.reveal), (1, false), "the same job, no reveal");
        built = follow_up.snapshot.unwrap().unwrap();
    }
    assert!(!built.ignored_pending);
    assert!(listed(&built).contains(&"target".to_string()));
    // Known now: the next switch lands whole at once.
    worker.generation = 1;
    let again = worker.switch(&input_for(&r, Scope::Branch, Tab::AllFiles), Refresh::default());
    assert!(!again.ignored_pending);
    assert!(listed(&again).contains(&"target".to_string()));
}
