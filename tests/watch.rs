//! The watcher against real repositories and each platform's backend (`FSEvents`, inotify,
//! `ReadDirectoryChangesW`): what each change reports, and what it never reports.

mod common;

use std::collections::BTreeSet;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use common::Repo;
use herdr_reviewr::turn::{TurnFeed, TurnNews};
use herdr_reviewr::wake::{Waker, channel};
use herdr_reviewr::watch::{Batch, GitChange, Watch, WatchEvent};

/// How long nothing must arrive before a burst counts as over.
const QUIET: Duration = Duration::from_millis(400);

/// Live watchers at once: macOS refuses a fourth live `FSEvents` stream in one process, a ceiling
/// only the test binary meets. One watcher holds one stream.
const STREAMS: usize = if cfg!(target_os = "macos") { 3 } else { usize::MAX };

/// How many watchers are live, and the wait for a free slot.
static LIVE: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

/// One of [`STREAMS`] slots, held while a watcher lives.
struct Slot;

impl Slot {
    fn take() -> Self {
        let (live, freed) = &LIVE;
        let live = live.lock().unwrap_or_else(PoisonError::into_inner);
        let mut live =
            freed.wait_while(live, |n| *n == STREAMS).unwrap_or_else(PoisonError::into_inner);
        *live += 1;
        Self
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let (live, freed) = &LIVE;
        *live.lock().unwrap_or_else(PoisonError::into_inner) -= 1;
        freed.notify_one();
    }
}

struct Watched {
    watch: Watch,
    rx: Receiver<WatchEvent>,
    /// The paths each write reported to turn tracking named.
    wrote: Arc<Mutex<Vec<String>>>,
    // Declared last, so the slot frees after the watcher's stream stops.
    _slot: Slot,
}

impl Watched {
    fn start(repo: &Repo) -> Self {
        Self::start_with_config(repo, None)
    }

    fn start_with_config(repo: &Repo, config: Option<&std::path::Path>) -> Self {
        Self::start_at(repo.path(), config)
    }

    fn start_at(root: &std::path::Path, config: Option<&std::path::Path>) -> Self {
        let slot = Slot::take();
        let (tx, rx) = channel(&Waker::detached());
        let wrote = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&wrote);
        let turns = TurnFeed::new(move |news| {
            if let TurnNews::Wrote(w) = news {
                sink.lock().unwrap().extend(w.paths.into_iter().flatten());
            }
        });
        let watch = Watch::start(root, config, tx, Some(turns));
        let watched = Self { watch, rx, wrote, _slot: slot };
        // A stream starts live some time after `start`: probe until a write arrives, then drain.
        let probe = root.join(".watch-probe");
        let deadline = Instant::now() + Duration::from_secs(20);
        for n in 0.. {
            std::fs::write(&probe, format!("{n}\n")).unwrap();
            if watched.next().worktree.contains(".watch-probe") {
                break;
            }
            assert!(Instant::now() < deadline, "the watcher never went live");
        }
        std::fs::remove_file(&probe).unwrap();
        let _ = watched.next();
        watched
    }

    /// Everything that arrives until a quiet stretch, merged; panics on `Unavailable`.
    fn next(&self) -> Batch {
        let mut merged = Batch::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        while let Ok(event) = self.rx.recv_timeout(QUIET) {
            match event {
                WatchEvent::Batch(b) => {
                    merged.worktree.extend(b.worktree);
                    merged.files.extend(b.files);
                    merged.git.extend(b.git);
                    merged.rescan |= b.rescan;
                    merged.config |= b.config;
                }
                WatchEvent::Ready => {}
                WatchEvent::Unavailable(reason) => panic!("watcher unavailable: {reason}"),
            }
            assert!(Instant::now() < deadline, "the watcher never went quiet");
        }
        merged
    }
}

fn repo_with(files: &[&str]) -> Repo {
    let r = Repo::init();
    for f in files {
        r.write(f, "one\n");
    }
    r.commit_all("init");
    r
}

fn paths(b: &Batch) -> BTreeSet<&str> {
    b.worktree.iter().map(String::as_str).collect()
}

#[test]
fn edits_creations_deletions_and_atomic_saves_report_their_paths() {
    let r = repo_with(&["a.txt", "b.txt", "c.txt"]);
    let w = Watched::start(&r);

    r.write("a.txt", "two\n");
    assert!(paths(&w.next()).contains("a.txt"), "an edit");

    r.write("new.txt", "x\n");
    assert!(paths(&w.next()).contains("new.txt"), "a creation");

    r.remove("b.txt");
    assert!(paths(&w.next()).contains("b.txt"), "a deletion");

    // An editor's save: write a temp file, rename it over the original.
    r.write(".c.txt.swp", "saved\n");
    std::fs::rename(r.path().join(".c.txt.swp"), r.path().join("c.txt")).unwrap();
    assert!(paths(&w.next()).contains("c.txt"), "an atomic save names the saved file");
}

#[test]
fn folders_moved_removed_or_filled_at_once_report_what_changed() {
    let r = repo_with(&["d/x.txt", "d/y.txt", "keep.txt"]);
    let w = Watched::start(&r);

    std::fs::rename(r.path().join("d"), r.path().join("e")).unwrap();
    let moved = w.next();
    let p = paths(&moved);
    assert!(
        moved.rescan
            || (p.iter().any(|x| x.starts_with('d')) && p.iter().any(|x| x.starts_with('e'))),
        "a folder move names both sides or asks for a rescan: {moved:?}"
    );

    std::fs::remove_dir_all(r.path().join("e")).unwrap();
    let removed = w.next();
    assert!(
        removed.rescan || paths(&removed).iter().any(|x| x.starts_with('e')),
        "a removed folder reports: {removed:?}"
    );

    // A folder filled the moment it exists: on Linux its watch lands after some writes.
    let fresh = r.path().join("fresh/deep");
    std::fs::create_dir_all(&fresh).unwrap();
    for i in 0..20 {
        std::fs::write(fresh.join(format!("f{i}.txt")), "x\n").unwrap();
    }
    let filled = w.next();
    let p = paths(&filled);
    for i in 0..20 {
        let path = format!("fresh/deep/f{i}.txt");
        assert!(
            filled.rescan
                || p.contains(path.as_str())
                || p.contains("fresh")
                || p.contains("fresh/deep"),
            "{path} reaches the batch: {filled:?}"
        );
    }
}

#[test]
fn ignored_paths_stay_quiet_except_the_topmost_entry_and_what_is_on_screen() {
    let r = Repo::init();
    r.write(".gitignore", "target/\nnode_modules/\ndist/\n*.log\n");
    r.write("a.txt", "one\n");
    r.commit_all("init");
    std::fs::create_dir_all(r.path().join("target/debug")).unwrap();
    let mut w = Watched::start(&r);

    for i in 0..30 {
        r.write(&format!("target/debug/obj{i}.o"), "x\n");
    }
    let build = w.next();
    assert!(build.is_empty(), "a build writing into an ignored folder reports nothing: {build:?}");

    std::fs::create_dir(r.path().join("node_modules")).unwrap();
    let appeared = w.next();
    assert!(
        paths(&appeared).contains("node_modules"),
        "the topmost ignored entry appearing: {appeared:?}"
    );
    r.write("node_modules/pkg/index.js", "x\n");
    r.write("node_modules/pkg/.gitattributes", "* -text\n");
    let inside = w.next();
    assert!(
        inside.is_empty(),
        "a write inside it reports nothing, attribute files too: {inside:?}"
    );

    r.write("debug.log", "x\n");
    let created = w.next();
    assert!(paths(&created).contains("debug.log"), "an ignored file appearing lists in All files");
    r.write("debug.log", "x\nmore\n");
    assert!(w.next().is_empty(), "its content changing does not");

    // An ignored folder that comes back is still ignored, its insides quiet.
    r.write("dist/out.js", "x\n");
    std::fs::remove_dir_all(r.path().join("dist")).unwrap();
    let _ = w.next();
    w.wrote.lock().unwrap().clear();
    r.write("dist/again.js", "x\n");
    let back = w.next();
    assert_eq!(paths(&back), ["dist"].into(), "only the folder lists: {back:?}");
    let wrote = w.wrote.lock().unwrap().clone();
    assert!(wrote.is_empty(), "its coming back is no turn's write: {wrote:?}");

    w.watch.show(["target".to_string()].into());
    std::thread::sleep(Duration::from_millis(100));
    r.write("target/shown.o", "x\n");
    assert!(paths(&w.next()).contains("target/shown.o"), "an expanded ignored folder is watched");
    r.write("target/release/deps/lib.rlib", "x\n");
    let release = w.next();
    assert!(release.files.is_empty(), "search indexes nothing inside it: {release:?}");
}

#[test]
fn git_operations_report_what_they_move() {
    let r = repo_with(&["a.txt"]);
    let w = Watched::start(&r);

    r.write("a.txt", "two\n");
    let _ = w.next();
    r.git(&["add", "a.txt"]);
    assert!(w.next().git.contains(&GitChange::Index), "a stage");

    r.git(&["commit", "-q", "-m", "two"]);
    let commit = w.next();
    assert!(
        commit.git.contains(&GitChange::Refs) || commit.git.contains(&GitChange::Head),
        "a commit moves the branch: {commit:?}"
    );

    r.git(&["checkout", "-q", "-b", "feature"]);
    assert!(w.next().git.contains(&GitChange::Head), "a checkout");

    r.git(&["update-ref", "refs/remotes/origin/main", "HEAD"]);
    assert!(w.next().git.contains(&GitChange::Refs), "a fetch-like remote ref move");

    r.git(&["pack-refs", "--all"]);
    assert!(w.next().git.contains(&GitChange::Refs), "a packed-refs rewrite");

    r.git(&["config", "remote.origin.url", "https://example.com/x.git"]);
    assert!(w.next().git.contains(&GitChange::Config), "a config change");

    std::fs::write(r.path().join(".git/info/exclude"), "*.tmp\n").unwrap();
    assert!(w.next().git.contains(&GitChange::IgnoreRules), "an exclude rule");

    r.write(".gitignore", "*.bak\n");
    assert!(w.next().git.contains(&GitChange::IgnoreRules), "a .gitignore edit");
}

#[test]
fn git_noise_and_other_worktrees_report_nothing() {
    let r = repo_with(&["a.txt"]);
    let linked = r.add_worktree("other");
    let w = Watched::start(&r);

    r.git(&["hash-object", "-w", "a.txt"]);
    std::fs::write(r.path().join(".git/FETCH_HEAD"), "x\n").unwrap();
    assert!(w.next().is_empty(), "objects and FETCH_HEAD are noise");

    std::fs::write(linked.path().join("b.txt"), "x\n").unwrap();
    std::process::Command::new("git")
        .args(["-C", linked.path().to_str().unwrap(), "add", "b.txt"])
        .output()
        .unwrap();
    assert!(w.next().is_empty(), "another worktree's stage is not ours");

    r.git(&["update-ref", "refs/heads/someone-else", "HEAD"]);
    assert!(w.next().is_empty(), "another agent's branch is not ours");
}

#[test]
fn reviewrs_own_ref_write_is_quiet_and_another_panes_is_not() {
    let r = repo_with(&["a.txt"]);
    let w = Watched::start(&r);
    let tree = String::from_utf8(
        std::process::Command::new("git")
            .args(["-C", r.path().to_str().unwrap(), "rev-parse", "HEAD^{tree}"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();

    herdr_reviewr::git::write_baseline_ref(r.path(), tree.trim()).unwrap();
    let own = w.next();
    assert!(!own.git.contains(&GitChange::ReviewrRefs), "our own write: {own:?}");

    r.git(&["update-ref", "refs/worktree/reviewr/base-pick", tree.trim()]);
    assert!(w.next().git.contains(&GitChange::ReviewrRefs), "another pane's base pick");
}

#[test]
fn a_change_inside_a_nested_repository_reports_its_entry() {
    let r = repo_with(&["a.txt"]);
    let nested = r.path().join("vendor/lib");
    std::fs::create_dir_all(&nested).unwrap();
    std::process::Command::new("git").args(["init", "-q"]).current_dir(&nested).output().unwrap();
    let w = Watched::start(&r);

    std::fs::write(nested.join("x.rs"), "x\n").unwrap();
    let batch = w.next();
    assert!(paths(&batch).contains("vendor/lib"), "reported as the nested repo: {batch:?}");
    assert!(!paths(&batch).contains("vendor/lib/x.rs"), "never as a path git cannot diff");
    assert!(batch.files.contains("vendor/lib/x.rs"), "search hears the file itself: {batch:?}");

    // A folder made inside it, and written at once, is followed.
    std::fs::create_dir(nested.join("src")).unwrap();
    std::fs::write(nested.join("src/a.rs"), "a\n").unwrap();
    let batch = w.next();
    assert!(batch.files.contains("vendor/lib/src/a.rs"), "a new folder's file: {batch:?}");

    // What the nested repository ignores never reaches search.
    std::fs::write(nested.join(".gitignore"), "target/\n").unwrap();
    let _ = w.next();
    std::fs::create_dir_all(nested.join("target")).unwrap();
    std::fs::write(nested.join("target/out.o"), "o\n").unwrap();
    std::fs::write(nested.join("y.rs"), "y\n").unwrap();
    let batch = w.next();
    assert!(batch.files.contains("vendor/lib/y.rs"), "{batch:?}");
    assert!(
        !batch.files.iter().any(|f| f.starts_with("vendor/lib/target")),
        "the nested repo's ignored build output stays out of search: {batch:?}"
    );

    // Once the nested repository stops ignoring it, its writes reach search again.
    std::fs::write(nested.join(".gitignore"), "").unwrap();
    let _ = w.next();
    std::fs::write(nested.join("target/out2.o"), "o\n").unwrap();
    let batch = w.next();
    assert!(batch.files.contains("vendor/lib/target/out2.o"), "the rule change is seen: {batch:?}");
}

#[test]
fn a_nested_repository_the_worktree_ignores_stays_out_of_search() {
    let r = repo_with(&["a.txt"]);
    r.write(".gitignore", "vendor/\n");
    r.commit_all("ignore vendor");
    let nested = r.path().join("vendor/lib");
    std::fs::create_dir_all(&nested).unwrap();
    std::process::Command::new("git").args(["init", "-q"]).current_dir(&nested).output().unwrap();
    let w = Watched::start(&r);

    // After its own rule change, what it writes reaches no one, while the worktree's does.
    std::fs::write(nested.join(".gitignore"), "x\n").unwrap();
    let _ = w.next();
    std::fs::create_dir(nested.join("src")).unwrap();
    std::fs::write(nested.join("src/a.rs"), "a\n").unwrap();
    r.write("a.txt", "two\n");
    let batch = w.next();
    assert!(batch.files.contains("a.txt"), "the worktree's own write arrives: {batch:?}");
    assert!(!batch.files.iter().any(|f| f.starts_with("vendor/")), "{batch:?}");
}

#[test]
fn a_write_inside_a_folder_names_the_file_not_the_folder() {
    let r = repo_with(&["src/a.rs"]);
    let w = Watched::start(&r);
    r.write("src/a.rs", "two\n");
    r.write("src/b.rs", "x\n");
    let batch = w.next();
    assert!(!paths(&batch).contains("src"), "a folder's own modify is no change: {batch:?}");
    assert!(paths(&batch).is_superset(&["src/a.rs", "src/b.rs"].into()), "{batch:?}");
}

#[test]
fn a_file_named_like_a_folder_rule_reports_its_edits_after_coming_back() {
    let r = Repo::init();
    r.write(".gitignore", "build/\n");
    r.write("scripts/build", "one\n");
    r.commit_all("init");
    let w = Watched::start(&r);
    r.remove("scripts/build");
    let deleted = w.next();
    assert!(paths(&deleted).contains("scripts/build"), "a deletion");
    assert!(deleted.files.contains("scripts/build"), "search drops it: {deleted:?}");
    r.write("scripts/build", "two\n");
    let _ = w.next();
    r.write("scripts/build", "three\n");
    let edited = w.next();
    assert!(paths(&edited).contains("scripts/build"), "its edits report again: {edited:?}");
}

#[test]
fn an_ignored_folder_coming_or_going_whole_reports_itself_alone() {
    let r = Repo::init();
    r.write(".gitignore", "build/\n");
    r.commit_all("init");
    let w = Watched::start(&r);
    w.wrote.lock().unwrap().clear();
    // It appears already full, as a build makes it.
    for i in 0..20 {
        r.write(&format!("build/o{i}.o"), "x\n");
    }
    let appeared = w.next();
    assert_eq!(paths(&appeared), ["build"].into(), "only the folder lists: {appeared:?}");
    assert!(!appeared.files.iter().any(|f| f.starts_with("build/")), "{appeared:?}");
    r.write("build/o0.o", "y\n");
    let _ = w.next();
    // It goes whole, as `cargo clean` removes it.
    std::fs::remove_dir_all(r.path().join("build")).unwrap();
    let gone = w.next();
    assert_eq!(paths(&gone), ["build"].into(), "only the folder lists: {gone:?}");
    let wrote = w.wrote.lock().unwrap().clone();
    assert_eq!(wrote, ["build"], "only its going counts, as every path that goes does");
}

#[test]
fn a_path_that_turns_from_folder_to_file_or_back_is_judged_as_what_it_is() {
    let r = Repo::init();
    r.write(".gitignore", "build/\n");
    r.commit_all("init");
    let w = Watched::start(&r);
    // The folder appears while watched, so its coming is on record.
    r.write("build/out.o", "x\n");
    let _ = w.next();
    r.write("build/out.o", "y\n");
    let _ = w.next();
    // Folder to file inside one gather window.
    std::fs::remove_dir_all(r.path().join("build")).unwrap();
    r.write("build", "a file now\n");
    let _ = w.next();
    r.write("build", "edited\n");
    assert!(paths(&w.next()).contains("build"), "the file's edits report");
    // And back: the file's going reports, and the folder's insides are ignored again.
    r.remove("build");
    r.write("build/b.o", "x\n");
    let back = w.next();
    assert!(paths(&back).contains("build"), "the file's removal reports: {back:?}");
    assert!(back.files.contains("build"), "and reaches search: {back:?}");
    r.write("build/b.o", "y\n");
    let inside = w.next();
    assert!(inside.is_empty(), "inside the folder again, quiet: {inside:?}");
}

#[test]
fn a_link_to_a_folder_is_asked_as_a_file_and_never_fails_the_batch() {
    let r = Repo::init();
    r.write(".gitignore", "*.log\n");
    r.write("real/a.txt", "x\n");
    r.commit_all("init");
    let w = Watched::start(&r);
    w.wrote.lock().unwrap().clear();
    // Past the one-at-a-time fallback's 64 paths, a refused batch would keep every path.
    for i in 0..80 {
        r.write(&format!("src/f{i}.rs"), "x\n");
    }
    r.write("debug.log", "x\n");
    #[cfg(unix)]
    std::os::unix::fs::symlink(r.path().join("real"), r.path().join("lnk")).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(r.path().join("real"), r.path().join("lnk")).unwrap();
    let batch = w.next();
    assert!(batch.worktree.contains("lnk"), "the link lists: {batch:?}");
    assert!(!batch.files.contains("debug.log"), "an ignored file stays out of search");
    let wrote = w.wrote.lock().unwrap().clone();
    assert!(!wrote.iter().any(|p| p == "debug.log"), "and is no turn's write: {wrote:?}");
}

#[test]
fn a_nested_repository_judges_a_folder_turned_file_as_what_it_is() {
    let r = Repo::init();
    r.write("a.txt", "x\n");
    r.commit_all("init");
    let nested = r.path().join("vendor/lib");
    std::fs::create_dir_all(&nested).unwrap();
    let init =
        std::process::Command::new("git").arg("-C").arg(&nested).args(["init", "-q"]).output();
    assert!(init.unwrap().status.success());
    r.write("vendor/lib/.gitignore", "build/\n");
    let w = Watched::start(&r);
    r.write("vendor/lib/build/out.o", "x\n");
    let _ = w.next();
    std::fs::remove_dir_all(nested.join("build")).unwrap();
    let _ = w.next();
    r.write("vendor/lib/build", "a file now\n");
    let _ = w.next();
    r.write("vendor/lib/build", "edited\n");
    let edited = w.next();
    assert!(edited.files.contains("vendor/lib/build"), "the file's edits reach search: {edited:?}");
    r.remove("vendor/lib/build");
    let gone = w.next();
    assert!(gone.files.contains("vendor/lib/build"), "and its removal: {gone:?}");
}

#[test]
fn a_file_that_replaces_an_ignored_folder_reports_its_edits() {
    let r = Repo::init();
    r.write(".gitignore", "build/\n");
    r.commit_all("init");
    r.write("build/out.o", "x\n");
    let w = Watched::start(&r);
    r.write("build/out.o", "y\n");
    let _ = w.next();
    std::fs::remove_dir_all(r.path().join("build")).unwrap();
    let _ = w.next();
    r.write("build", "a file now\n");
    let _ = w.next();
    r.write("build", "edited\n");
    let edited = w.next();
    assert!(paths(&edited).contains("build"), "the file's edits report: {edited:?}");
}

#[test]
fn a_tracked_file_whose_ignored_folder_is_gone_leaves_the_watcher_running() {
    let r = Repo::init();
    r.write(".gitignore", "build/\n");
    r.write("build/.gitkeep", "");
    r.git(&["add", "-f", "build/.gitkeep"]);
    r.commit_all("init");
    std::fs::remove_dir_all(r.path().join("build")).unwrap();
    // Starting panics on `Unavailable`: the gone folder must not fail the start.
    let w = Watched::start(&r);
    r.write("a.txt", "x\n");
    assert!(paths(&w.next()).contains("a.txt"));
}

#[test]
fn a_repository_whose_git_dir_does_not_resolve_is_unavailable_not_a_plain_folder() {
    let _slot = Slot::take();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join(".git"), "gitdir: /nowhere/at/all\n").unwrap();
    let (tx, rx) = channel(&Waker::detached());
    let _watch = Watch::start(root.path(), None, tx, None);
    let event = rx.recv_timeout(Duration::from_secs(5)).expect("an event");
    assert!(matches!(event, WatchEvent::Unavailable(_)), "{event:?}");
}

#[test]
fn dropping_a_watch_releases_its_stream() {
    // macOS allows three live streams per process: a leaked one fails the fourth start here.
    let r = repo_with(&["a.txt"]);
    for round in 0..6 {
        let w = Watched::start(&r);
        r.write("a.txt", &format!("round {round}\n"));
        assert!(paths(&w.next()).contains("a.txt"), "round {round} watches");
    }
}

#[test]
fn a_file_force_added_inside_an_ignored_folder_reports_its_edits() {
    let r = repo_with(&["a.txt"]);
    r.write(".gitignore", "dist/\n");
    r.write("dist/bundle.js", "one\n");
    r.commit_all("rules");
    let w = Watched::start(&r);
    r.write("dist/bundle.js", "two\n");
    assert!(w.next().is_empty(), "ignored while untracked");
    r.git(&["add", "-f", "dist/bundle.js"]);
    assert!(w.next().git.contains(&GitChange::Index), "the force-add");
    r.write("dist/bundle.js", "three\n");
    assert!(paths(&w.next()).contains("dist/bundle.js"), "a tracked file's edits report");
}

#[test]
fn a_tracked_file_keeps_reporting_when_its_folder_becomes_ignored_or_leaves_the_screen() {
    let r = repo_with(&["gen/foo.rs", "a.txt"]);
    let mut w = Watched::start(&r);
    // Its folder becomes ignored: git still tracks the file.
    r.write(".gitignore", "gen/\n");
    let _ = w.next();
    r.write("gen/foo.rs", "two\n");
    assert!(paths(&w.next()).contains("gen/foo.rs"), "a tracked file under a new rule");
    // Shown, then off the screen again: its folder keeps the watch.
    w.watch.show(["gen".to_string()].into());
    std::thread::sleep(Duration::from_millis(100));
    w.watch.show(BTreeSet::new());
    std::thread::sleep(Duration::from_millis(100));
    r.write("gen/foo.rs", "three\n");
    assert!(paths(&w.next()).contains("gen/foo.rs"), "after leaving the screen");
}

#[test]
fn a_new_global_excludes_file_is_a_rule_change_and_is_followed() {
    let r = repo_with(&["a.txt"]);
    let rules = tempfile::tempdir().unwrap();
    let first = rules.path().join("first");
    std::fs::write(&first, "*.tmp\n").unwrap();
    r.git(&["config", "core.excludesFile", first.to_str().unwrap()]);
    let w = Watched::start(&r);
    r.write("x.tmp", "x\n");
    let _ = w.next();
    r.write("x.tmp", "y\n");
    assert!(w.next().is_empty(), "ignored by the first file");
    let second = rules.path().join("second");
    std::fs::write(&second, "*.bak\n").unwrap();
    r.git(&["config", "core.excludesFile", second.to_str().unwrap()]);
    assert!(w.next().git.contains(&GitChange::IgnoreRules), "the new file is a rule change");
    r.write("x.tmp", "z\n");
    assert!(paths(&w.next()).contains("x.tmp"), "the old rule no longer holds");
    std::fs::write(&second, "*.bak\n*.tmp\n").unwrap();
    assert!(w.next().git.contains(&GitChange::IgnoreRules), "the new file is watched");
}

#[test]
fn a_checkout_in_a_reftable_linked_worktree_reports_head() {
    let r = Repo::init();
    let init = std::process::Command::new("git")
        .arg("-C")
        .arg(r.path())
        .args(["init", "-q", "--ref-format=reftable", "reft"])
        .output()
        .unwrap();
    assert!(init.status.success(), "{}", String::from_utf8_lossy(&init.stderr));
    let main = r.path().join("reft");
    let git = |dir: &std::path::Path, args: &[&str]| {
        let out = std::process::Command::new("git").arg("-C").arg(dir).args(args).output().unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    };
    std::fs::write(main.join("a.txt"), "one\n").unwrap();
    git(&main, &["add", "-A"]);
    git(&main, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init"]);
    git(&main, &["branch", "other"]);
    let linked = r.path().join("linked");
    git(&main, &["worktree", "add", "-q", "-b", "side", linked.to_str().unwrap()]);
    let w = Watched::start_at(&linked, None);
    // Only this worktree's HEAD moves: no branch is created or changed.
    git(&linked, &["checkout", "-q", "other"]);
    let moved = w.next();
    assert!(moved.git.contains(&GitChange::Refs), "the linked worktree's checkout: {moved:?}");
}

#[test]
fn a_folder_no_longer_ignored_reports_its_writes() {
    let r = Repo::init();
    r.write(".gitignore", "gen/\n");
    r.write("a.txt", "one\n");
    r.commit_all("init");
    r.write("gen/old.rs", "x\n");
    let w = Watched::start(&r);

    r.write("gen/quiet.rs", "x\n");
    assert!(w.next().is_empty(), "ignored while the rule stands");

    r.write(".gitignore", "\n");
    assert!(w.next().git.contains(&GitChange::IgnoreRules), "the rule change itself");
    r.write("gen/loud.rs", "x\n");
    assert!(paths(&w.next()).contains("gen/loud.rs"), "the watch set follows the new rules");
}

#[test]
fn a_config_edit_and_an_editors_atomic_save_report_the_config() {
    let r = repo_with(&["a.txt"]);
    let config = tempfile::tempdir().unwrap();
    std::fs::write(config.path().join("config.toml"), "theme = \"gruvbox\"\n").unwrap();
    let w = Watched::start_with_config(&r, Some(config.path()));

    std::fs::write(config.path().join("config.toml"), "theme = \"nord\"\n").unwrap();
    assert!(w.next().config, "an edit");

    std::fs::write(config.path().join(".config.toml.swp"), "theme = \"dracula\"\n").unwrap();
    std::fs::rename(config.path().join(".config.toml.swp"), config.path().join("config.toml"))
        .unwrap();
    let saved = w.next();
    assert!(saved.config, "a save by renaming over the file");
    assert!(saved.worktree.is_empty(), "the config is not the worktree");
}

#[test]
fn reviewrs_index_copies_report_nothing() {
    let r = repo_with(&["a.txt"]);
    let w = Watched::start(&r);
    // A turn snapshot stages into a private copy under `<git dir>/reviewr/` and writes objects.
    herdr_reviewr::git::snapshot_worktree(r.path()).unwrap();
    let batch = w.next();
    assert!(batch.is_empty(), "the index copy and its objects are reviewr's own: {batch:?}");
}

#[test]
fn a_branch_delete_and_pack_refs_remove_ref_folders_and_the_watch_survives() {
    let r = repo_with(&["a.txt"]);
    r.git(&["update-ref", "refs/remotes/origin/team/topic", "HEAD"]);
    let w = Watched::start(&r);
    // Deleting the only ref in a folder removes the folder; packing removes every loose one.
    r.git(&["update-ref", "-d", "refs/remotes/origin/team/topic"]);
    assert!(w.next().git.contains(&GitChange::Refs), "a deleted remote ref");
    r.git(&["update-ref", "refs/remotes/origin/main", "HEAD"]);
    r.git(&["pack-refs", "--all"]);
    assert!(w.next().git.contains(&GitChange::Refs), "a packed-refs rewrite");
    r.write("a.txt", "two\n");
    assert!(paths(&w.next()).contains("a.txt"), "the watcher still reports edits");
}

#[test]
fn a_watcher_that_cannot_start_restarts_and_asks_for_everything() {
    let _slot = Slot::take();
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("later");
    let (tx, rx) = channel(&Waker::detached());
    let mut watch = Watch::start(&root, None, tx, None);
    let event = rx.recv_timeout(Duration::from_secs(5)).expect("an event");
    assert!(matches!(event, WatchEvent::Unavailable(_)), "a missing root fails: {event:?}");
    // What the loop says while it is down is kept for the next start.
    watch.set_refs(["refs/heads/develop".to_string()].into());
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git").arg("-C").arg(&root).args(args).output();
        assert!(out.unwrap().status.success(), "git {args:?}");
    };
    std::fs::create_dir(&root).unwrap();
    git(&["init", "-q", "-b", "main"]);
    git(&["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "i"]);
    let started = Instant::now();
    let ready = rx.recv_timeout(Duration::from_secs(5)).expect("the restart");
    assert!(matches!(ready, WatchEvent::Ready), "{ready:?}");
    assert!(started.elapsed() < Duration::from_secs(2), "the first retry waits about a second");
    match rx.recv_timeout(Duration::from_secs(5)).expect("the catch-up batch") {
        WatchEvent::Batch(b) => assert!(b.rescan && b.config, "everything is read again: {b:?}"),
        other => panic!("expected a batch, got {other:?}"),
    }
    git(&["update-ref", "refs/heads/develop", "HEAD"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match rx.recv_timeout(Duration::from_secs(5)).expect("the base move") {
            WatchEvent::Batch(b) if b.git.contains(&GitChange::Refs) => break,
            _ => assert!(Instant::now() < deadline, "the kept base ref never reported"),
        }
    }
}

#[test]
fn a_hidden_pane_restarts_no_watcher_until_it_is_shown() {
    let _slot = Slot::take();
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("later");
    let (tx, rx) = channel(&Waker::detached());
    let watch = Watch::start(&root, None, tx, None);
    watch.set_visible(false);
    let event = rx.recv_timeout(Duration::from_secs(5)).expect("an event");
    assert!(matches!(event, WatchEvent::Unavailable(_)), "a missing root fails: {event:?}");
    std::fs::create_dir(&root).unwrap();
    assert!(
        rx.recv_timeout(Duration::from_secs(3)).is_err(),
        "hidden, it waits past its first retry"
    );
    watch.set_visible(true);
    let ready = rx.recv_timeout(Duration::from_secs(5)).expect("shown, it restarts");
    assert!(matches!(ready, WatchEvent::Ready), "{ready:?}");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "needs fs.inotify.max_user_watches lowered below this repository's folder count"]
fn a_watcher_out_of_inotify_watches_reports_itself_unavailable() {
    let _slot = Slot::take();
    let r = repo_with(&["a.txt"]);
    let (tx, rx) = channel(&Waker::detached());
    let _watch = Watch::start(r.path(), None, tx, None);
    let ready = rx.recv_timeout(Duration::from_secs(10)).expect("the start");
    assert!(matches!(ready, WatchEvent::Ready), "a small tree fits: {ready:?}");
    // Folders created later run out of watches as they are followed.
    for i in 0..64 {
        r.write(&format!("d{i}/a.txt"), "x\n");
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match rx.recv_timeout(Duration::from_secs(5)).expect("an event") {
            WatchEvent::Unavailable(_) => break,
            _ => assert!(Instant::now() < deadline, "the watcher never gave up"),
        }
    }
}
