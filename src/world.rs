//! The world snapshot: what one refresh derives from git alone, built on the caller or the worker.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::Result;

use crate::app::Tab;
use crate::file_list::Entry;
use crate::git;
use crate::model::{ChangeKind, ChangedFile, CommitPick, Scope};

/// Everything the build reads; a snapshot lands only while the view still matches it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WorldInput {
    pub repo: PathBuf,
    pub tab: Tab,
    pub scope: Scope,
    /// The `--base` flag. The pick is read at build time, so another pane's pick lands as content.
    pub base: Option<String>,
    /// Bumped by this pane's pick, so a build of the previous pick never lands.
    pub base_epoch: u64,
    /// The `last-turn` baseline tree the changed set diffs against; `None` before a turn.
    pub turn_baseline: Option<String>,
    /// The `commits` scope's pick, so a build of a replaced pick never lands.
    pub commit_pick: Option<CommitPick>,
    /// Expanded ignored directories whose children the `All files` tree loads.
    pub toggled_dirs: HashSet<String>,
}

/// One refresh's result; the base rides along so the header and its changeset land together.
#[derive(Clone, Debug)]
pub struct WorldSnapshot {
    pub changeset: Changeset,
    pub entries: Vec<Entry>,
    pub branch_base: git::BaseStatus,
    /// The `commits` scope's pick verdict; `None` on every other scope.
    pub pick_status: Option<PickStatus>,
    /// `HEAD` at build time, the commit picker's key; `None` when unborn.
    pub head: Option<String>,
    /// The paths a path-limited build re-read; `None` for a full build, which re-read everything.
    pub touched: Option<BTreeSet<String>>,
    /// `All files` lists no ignored entries yet: their walk is still running.
    pub ignored_pending: bool,
}

/// What one build found the commit pick to be.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PickVerdict {
    /// Every commit reachable from `HEAD`.
    Live,
    /// Some commit unreachable from `HEAD`. The run still paints.
    OffBranch,
    /// A needed commit is pruned, named here. The scope is empty.
    Gone(String),
}

/// The pick's verdict and the newest commit's subject, for the header.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PickStatus {
    pub verdict: PickVerdict,
    pub subject: String,
    /// How many commits the run spans, `0` when `gone` or not a run.
    pub count: usize,
}

/// The ends a changeset was diffed between (`new` `None` for the worktree); a file's diff reads these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffEnds {
    pub old: String,
    pub new: Option<String>,
}

/// A scope's changed files by path and the ends they were diffed between, landed together.
#[derive(Clone, Debug, Default)]
pub struct Changeset {
    pub files: BTreeMap<String, ChangedFile>,
    /// `None` when the scope has nothing to diff: no baseline, base, or live pick.
    pub ends: Option<DiffEnds>,
}

/// A build's changeset and the base or pick it diffs against, landed together.
#[derive(Debug, Default)]
pub struct ScopeBuild {
    pub branch_base: git::BaseStatus,
    pub pick_status: Option<PickStatus>,
    pub changeset: Changeset,
}

/// What full builds share across inputs, each part current with every refresh the worker heard.
#[derive(Debug, Default)]
pub struct Parts {
    untracked: Part<String>,
    ignored: Part<git::WorktreeEntry>,
    /// The base resolution and its merge-base, keyed by the `--base` flag and the pick epoch.
    base: Option<((Option<String>, u64), BaseRead)>,
    /// The worktree as a tree, `last-turn`'s new end.
    snapshot: Option<String>,
    /// Leave an unread ignored listing to a walk off the build, as the worker does.
    defer_ignored: bool,
}

/// What one base resolution found.
#[derive(Clone, Debug)]
struct BaseRead {
    status: git::BaseStatus,
    merge_base: Option<String>,
}

/// A listing read once, and the paths that moved since, re-read under those paths when next asked.
#[derive(Debug, Default)]
struct Part<T> {
    listed: Option<Vec<T>>,
    moved: BTreeSet<String>,
}

impl<T> Part<T> {
    /// The listing, re-read under what moved by `read` (all of it for no paths); past the cap, whole.
    fn get(
        &mut self,
        read: impl Fn(&[String]) -> Result<Vec<T>>,
        path: impl Fn(&T) -> &str,
    ) -> Result<&[T]> {
        let moved: Vec<String> = std::mem::take(&mut self.moved).into_iter().collect();
        let listed = match self.listed.take() {
            Some(listed) if moved.is_empty() => listed,
            Some(listed) if git::fits_command_line(&moved) => {
                // A narrow read lists the collapsed directory above a path, so that goes too.
                let near = |p: &str| {
                    moved.iter().any(|m| git::under_or_eq(p, m) || git::under_or_eq(m, p))
                };
                let kept = listed.into_iter().filter(|t| !near(path(t)));
                kept.chain(read(&moved)?).collect()
            }
            _ => read(&[])?,
        };
        Ok(self.listed.insert(listed))
    }
}

impl Parts {
    /// Hear what a refresh says moved: a full one moves everything.
    pub fn note(&mut self, refresh: &Refresh) {
        match refresh {
            Refresh::Full => *self = Self { defer_ignored: self.defer_ignored, ..Self::default() },
            Refresh::Paths { paths, .. } if !paths.is_empty() => {
                self.untracked.moved.extend(paths.iter().cloned());
                self.ignored.moved.extend(paths.iter().cloned());
                self.snapshot = None;
            }
            Refresh::Paths { .. } => {}
        }
    }

    fn untracked(&mut self, repo: &Path) -> Result<&[String]> {
        self.untracked.get(|paths| git::untracked_files(repo, paths), String::as_str)
    }

    fn ignored(&mut self, repo: &Path) -> Result<&[git::WorktreeEntry]> {
        self.ignored.get(|paths| git::ignored_listing(repo, paths), |w| w.path.as_str())
    }

    /// Whether a build leaves the ignored entries to the walk: deferring, and none listed yet.
    fn ignored_deferred(&self) -> bool {
        self.defer_ignored && self.ignored.listed.is_none()
    }

    fn base(&mut self, input: &WorldInput) -> Result<BaseRead> {
        let key = (input.base.clone(), input.base_epoch);
        if let Some((of, read)) = &self.base
            && *of == key
        {
            return Ok(read.clone());
        }
        // A resolve failure fails the build, keeping the stale frame.
        let resolution = git::resolve_base(&input.repo, input.base.as_deref())?;
        let merge_base =
            resolution.status.winner.as_ref().and_then(|w| git::merge_base(&input.repo, w.oid()));
        let read = BaseRead { status: resolution.status, merge_base };
        self.base = Some((key, read.clone()));
        Ok(read)
    }

    fn snapshot(&mut self, repo: &Path) -> Result<String> {
        if let Some(tree) = &self.snapshot {
            return Ok(tree.clone());
        }
        let tree = git::snapshot_worktree(repo)?;
        Ok(self.snapshot.insert(tree).clone())
    }
}

/// Build the snapshot for `input`; the changeset is built on every tab.
pub fn build(input: &WorldInput) -> Result<WorldSnapshot> {
    build_with(input, &mut Parts::default())
}

/// [`build`], reading through `parts`, which a refresh keeps current.
pub fn build_with(input: &WorldInput, parts: &mut Parts) -> Result<WorldSnapshot> {
    // Outside a repo, paint the quiet empty state, not an error every refresh.
    if !git::is_repo(&input.repo) {
        return Ok(WorldSnapshot {
            changeset: Changeset::default(),
            entries: Vec::new(),
            branch_base: git::BaseStatus::default(),
            pick_status: None,
            head: None,
            touched: None,
            ignored_pending: false,
        });
    }
    // One read of HEAD serves the snapshot and the uncommitted diff's old end.
    let head = git::head_oid(&input.repo);
    let ScopeBuild { branch_base, pick_status, changeset } =
        scope_build(input, head.clone(), parts)?;
    // A deferred walk leaves the ignored entries out, landing them when it finishes.
    let ignored_pending = input.tab == Tab::AllFiles && parts.ignored_deferred();
    let entries = match input.tab {
        // The whole worktree (ignored included), with expanded ignored dirs loaded lazily.
        Tab::AllFiles => all_files_entries(input, &changeset.files, parts)?,
        // `Changes` (the `PR` tab never builds a snapshot).
        _ => changeset.files.values().map(Entry::from_changed).collect(),
    };
    Ok(WorldSnapshot {
        changeset,
        entries,
        branch_base,
        pick_status,
        head,
        touched: None,
        ignored_pending,
    })
}

/// `snapshot` with the ignored entries `ignored` lists, as a full build would list them.
fn with_ignored(
    mut snapshot: WorldSnapshot,
    input: &WorldInput,
    ignored: &[git::WorktreeEntry],
) -> WorldSnapshot {
    // Listed first, so `load_toggled` keeps a listed path over its ignored duplicate.
    let mut entries = std::mem::take(&mut snapshot.entries);
    for w in ignored {
        let annotation = snapshot.changeset.files.get(&w.path).cloned();
        entries.push(Entry::from_worktree(w.clone(), annotation));
    }
    let entries = load_toggled(entries, &input.repo, &input.toggled_dirs);
    WorldSnapshot { entries, ignored_pending: false, touched: None, ..snapshot }
}

/// The active scope's changeset and, on `branch`, its base.
pub fn build_changed(input: &WorldInput) -> Result<ScopeBuild> {
    if !git::is_repo(&input.repo) {
        return Ok(ScopeBuild::default());
    }
    scope_build(input, git::head_oid(&input.repo), &mut Parts::default())
}

/// [`build_changed`] against `head`, as the caller read it, inside a repo.
fn scope_build(input: &WorldInput, head: Option<String>, parts: &mut Parts) -> Result<ScopeBuild> {
    match input.scope {
        Scope::LastTurn => match input.turn_baseline.as_deref() {
            Some(t) => {
                let now = parts.snapshot(&input.repo)?;
                at_ends(&input.repo, DiffEnds { old: t.to_string(), new: Some(now) }, parts)
            }
            None => Ok(ScopeBuild::default()),
        },
        Scope::Uncommitted => {
            let base = git::diff_base(head);
            at_ends(&input.repo, DiffEnds { old: base, new: None }, parts)
        }
        Scope::Branch => {
            let base = parts.base(input)?;
            let build = match base.merge_base {
                Some(old) => at_ends(&input.repo, DiffEnds { old, new: None }, parts)?,
                None => ScopeBuild::default(),
            };
            Ok(ScopeBuild { branch_base: base.status, ..build })
        }
        Scope::Commits => {
            // A tag without a pick builds the empty changeset.
            let Some(pick) = &input.commit_pick else { return Ok(ScopeBuild::default()) };
            build_pick(&input.repo, pick)
        }
    }
}

/// The changeset between `ends`, carried beside them: the ends a file's diff reads are its input.
fn at_ends(repo: &Path, ends: DiffEnds, parts: &mut Parts) -> Result<ScopeBuild> {
    let changed = match &ends.new {
        None => git::changed_from_given(repo, &ends.old, parts.untracked(repo)?)?,
        Some(new) => git::changed_between(repo, &ends.old, new)?,
    };
    let files = changed.into_iter().map(|f| (f.path.clone(), f)).collect();
    Ok(ScopeBuild { changeset: Changeset { files, ends: Some(ends) }, ..ScopeBuild::default() })
}

/// The pick's changeset, verdict and ends in one pass; a `gone` pick has neither.
fn build_pick(repo: &Path, pick: &CommitPick) -> Result<ScopeBuild> {
    let gone = |sha: &str| {
        let status = PickStatus {
            verdict: PickVerdict::Gone(sha.to_string()),
            subject: String::new(),
            count: 0,
        };
        ScopeBuild { pick_status: Some(status), ..ScopeBuild::default() }
    };
    if !git::commit_exists(repo, &pick.newest) {
        return Ok(gone(&pick.newest));
    }
    let Some(old) = git::parent_or_empty(repo, &pick.oldest) else {
        return Ok(gone(&pick.oldest));
    };
    if old != git::EMPTY_TREE && !git::commit_exists(repo, &old) {
        return Ok(gone(&old));
    }
    let subject = git::commit_subject(repo, &pick.newest).unwrap_or_default();
    let count = git::run_length_from(repo, &old, &pick.oldest, &pick.newest).unwrap_or(0);
    let at =
        at_ends(repo, DiffEnds { old, new: Some(pick.newest.clone()) }, &mut Parts::default())?;
    // The oldest is an ancestor of the newest, so one reachability check covers the run.
    let verdict = if git::is_reachable(repo, &pick.newest) {
        PickVerdict::Live
    } else {
        PickVerdict::OffBranch
    };
    Ok(ScopeBuild { pick_status: Some(PickStatus { verdict, subject, count }), ..at })
}

/// The `All files` entries; an ignored directory is walked only once expanded.
pub(crate) fn all_files_entries(
    input: &WorldInput,
    changed: &BTreeMap<String, ChangedFile>,
    parts: &mut Parts,
) -> Result<Vec<Entry>> {
    let to_entry = |w: git::WorktreeEntry| {
        let annotation = changed.get(&w.path).cloned();
        Entry::from_worktree(w, annotation)
    };
    let untracked = parts.untracked(&input.repo)?.to_vec();
    let ignored = if parts.ignored_deferred() { &[] } else { parts.ignored(&input.repo)? };
    let listed = git::all_files_given(&input.repo, &untracked, ignored)?;
    let entries: Vec<Entry> = listed.into_iter().map(&to_entry).collect();
    Ok(load_toggled(entries, &input.repo, &input.toggled_dirs))
}

/// `entries` with each expanded ignored directory's children read from disk again, and no other
/// entry inside an ignored directory; sorted by path, which a path-limited rebuild merges into.
pub(crate) fn load_toggled(
    mut entries: Vec<Entry>,
    repo: &Path,
    toggled: &HashSet<String>,
) -> Vec<Entry> {
    let ignored_dirs: HashSet<String> =
        entries.iter().filter(|e| e.is_dir && e.ignored).map(|e| e.path.clone()).collect();
    // Children loaded before go; each expanded directory reads its own again below.
    let inside_ignored = |path: &str| {
        let mut at = path;
        while let Some((parent, _)) = at.rsplit_once('/') {
            if ignored_dirs.contains(parent) {
                return true;
            }
            at = parent;
        }
        false
    };
    entries.retain(|e| !e.ignored || !inside_ignored(&e.path));
    let mut i = 0;
    while i < entries.len() {
        let e = &entries[i];
        if e.is_dir && e.ignored && toggled.contains(&e.path) {
            let path = e.path.clone();
            let children = git::list_ignored_dir(repo, &path);
            entries.extend(children.into_iter().map(|w| Entry::from_worktree(w, None)));
        }
        i += 1;
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries.dedup_by(|a, b| a.path == b.path);
    entries
}

/// What a narrow re-read must also cover so git pairs renames as a full build would: both sides
/// of every rename it touches, and the deletions and additions it could pair with. `None` past the cap.
fn rename_partners(
    last: &BTreeMap<String, ChangedFile>,
    paths: &BTreeSet<String>,
    fresh: &[ChangedFile],
) -> Option<BTreeSet<String>> {
    let found =
        |kind| fresh.iter().any(|f: &ChangedFile| f.kind == kind || f.kind == ChangeKind::Renamed);
    let (added, deleted) = (found(ChangeKind::Added), found(ChangeKind::Deleted));
    let mut partners = BTreeSet::new();
    for f in last.values() {
        let touched = f.touched_by(paths);
        let pairs = match f.kind {
            ChangeKind::Deleted => added,
            ChangeKind::Added => deleted,
            ChangeKind::Renamed => added || deleted,
            _ => false,
        };
        if (touched && f.previous_path.is_some()) || (pairs && !git::covered(paths, &f.path)) {
            partners.insert(f.path.clone());
            partners.extend(f.previous_path.clone());
        }
    }
    partners.retain(|p| !paths.contains(p));
    let all: Vec<String> = paths.iter().chain(&partners).cloned().collect();
    git::fits_command_line(&all).then_some(partners)
}

/// Re-read only `paths` against `last`, a build of the same input, widened to rename partners.
/// `None` when that reaches past the cap: only a full build is right.
fn rebuild_paths(
    input: &WorldInput,
    last: &WorldSnapshot,
    paths: &BTreeSet<String>,
) -> Option<Result<WorldSnapshot>> {
    let (fresh, ends) = match read_paths(input, last, paths)? {
        Ok(read) => read,
        Err(e) => return Some(Err(e)),
    };
    let partners = rename_partners(&last.changeset.files, paths, &fresh)?;
    if partners.is_empty() {
        return Some(merge_paths(input, last, paths, fresh, ends));
    }
    let wide: BTreeSet<String> = paths.union(&partners).cloned().collect();
    let (fresh, ends) = match read_paths(input, last, &wide)? {
        Ok(read) => read,
        Err(e) => return Some(Err(e)),
    };
    Some(merge_paths(input, last, &wide, fresh, ends))
}

/// The changed files under `paths` as `last`'s scope reads them, and the ends they diff between.
/// `None` when last-turn's re-read files pass the cap.
fn read_paths(
    input: &WorldInput,
    last: &WorldSnapshot,
    paths: &BTreeSet<String>,
) -> Option<Result<(Vec<ChangedFile>, Option<DiffEnds>)>> {
    let ends = last.changeset.ends.clone();
    let batch: Vec<String> = paths.iter().cloned().collect();
    let read = match (&ends, input.scope) {
        // The pick reads committed trees: a worktree batch changes nothing in it.
        (None, _) | (_, Scope::Commits) => Ok((Vec::new(), ends)),
        (Some(DiffEnds { old, new: None }), _) => {
            git::changed_from_in(&input.repo, old, &batch).map(|files| (files, ends.clone()))
        }
        // The new end is last's tree with `paths` re-read, which a full snapshot would equal.
        (Some(DiffEnds { old, new: Some(new) }), _) => {
            match git::snapshot_worktree_in(&input.repo, new, &batch) {
                Ok(None) => return None,
                Ok(Some(tree)) => git::changed_between_in(&input.repo, old, &tree, &batch)
                    .map(|files| (files, Some(DiffEnds { old: old.clone(), new: Some(tree) }))),
                Err(e) => Err(e),
            }
        }
    };
    Some(read)
}

/// Merge a path-limited re-read into `last`: what `paths` covers comes from `fresh`.
fn merge_paths(
    input: &WorldInput,
    last: &WorldSnapshot,
    paths: &BTreeSet<String>,
    fresh: Vec<ChangedFile>,
    ends: Option<DiffEnds>,
) -> Result<WorldSnapshot> {
    let files: BTreeMap<String, ChangedFile> = if input.scope == Scope::Commits {
        last.changeset.files.clone()
    } else {
        let kept = last.changeset.files.values().filter(|f| !f.touched_by(paths));
        kept.cloned().chain(fresh).map(|f| (f.path.clone(), f)).collect()
    };
    let entries = match input.tab {
        Tab::AllFiles => {
            let batch: Vec<String> = paths.iter().cloned().collect();
            let annotate = |path: &str| files.get(path).cloned();
            let mut fresh: Vec<Entry> = git::all_files_in(&input.repo, &batch)?
                .into_iter()
                .map(|w| {
                    let annotation = annotate(&w.path);
                    Entry::from_worktree(w, annotation)
                })
                .collect();
            fresh.sort_by(|a, b| a.path.cmp(&b.path));
            // Both sides are sorted: one pass keeps the order, the fresh entry winning a tie.
            let kept = last.entries.iter().filter(|e| !git::covered(paths, &e.path));
            let mut entries = Vec::with_capacity(last.entries.len() + fresh.len());
            let mut fresh = fresh.into_iter().peekable();
            for e in kept {
                while fresh.peek().is_some_and(|f| f.path <= e.path) {
                    entries.extend(fresh.next());
                }
                if entries.last().is_none_or(|l: &Entry| l.path != e.path) {
                    entries.push(Entry { annotation: annotate(&e.path), ..e.clone() });
                }
            }
            entries.extend(fresh);
            entries
        }
        _ => files.values().map(Entry::from_changed).collect(),
    };
    Ok(WorldSnapshot {
        changeset: Changeset { files, ends },
        entries,
        branch_base: last.branch_base.clone(),
        pick_status: last.pick_status.clone(),
        head: last.head.clone(),
        touched: Some(paths.clone()),
        ignored_pending: last.ignored_pending,
    })
}

/// What a refresh re-reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refresh {
    /// Everything.
    Full,
    /// Some worktree paths; with `index`, a full build too if what the index stages changed.
    Paths { paths: BTreeSet<String>, index: bool },
}

impl Default for Refresh {
    fn default() -> Self {
        Self::Paths { paths: BTreeSet::new(), index: false }
    }
}

impl Refresh {
    /// Some worktree paths, past the command-line cap a full build.
    pub fn paths(paths: impl IntoIterator<Item = String>) -> Self {
        Self::Paths { paths: paths.into_iter().collect(), index: false }.settled()
    }

    /// Whether it re-reads nothing.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Paths { paths, index: false } if paths.is_empty())
    }

    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }

    /// The paths it re-reads; `None` for a full build.
    pub fn named_paths(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Paths { paths, .. } => Some(paths),
            Self::Full => None,
        }
    }

    /// What one watcher batch asks for: a rescan, or any git change but an index rewrite, moves
    /// what every path diffs against, so only a full build is right.
    pub fn from_batch(batch: &crate::watch::Batch) -> Self {
        use crate::watch::GitChange;
        if batch.rescan || batch.git.iter().any(|g| *g != GitChange::Index) {
            return Self::Full;
        }
        let index = batch.git.contains(&GitChange::Index);
        Self::Paths { paths: batch.worktree.clone(), index }.settled()
    }

    /// Fold a later request in: anything full stays full.
    pub fn absorb(&mut self, other: Refresh) {
        let merged = match (std::mem::take(self), other) {
            (Self::Paths { mut paths, index }, Self::Paths { paths: more, index: also }) => {
                paths.extend(more);
                Self::Paths { paths, index: index || also }
            }
            _ => Self::Full,
        };
        *self = merged.settled();
    }

    /// Paths past the command-line cap make it full.
    fn settled(self) -> Self {
        match self {
            Self::Paths { paths, .. } if !git::fits_command_line(&paths) => Self::Full,
            other => other,
        }
    }
}

/// One queued refresh's attributes, accumulated on `App` until the loop dispatches it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorldRequest {
    /// Re-reveal the cursor when the result lands — user-initiated switches only.
    pub reveal: bool,
    pub refresh: Refresh,
    /// Only the pacer asked for it: it spends the budget. The reviewer's work never does.
    pub background: bool,
    /// The reviewer switched what the view shows: build it, though nothing moved.
    pub switch: bool,
}

/// One refresh request; the completion echoes its tag.
#[derive(Debug)]
pub struct WorldJob {
    pub generation: u64,
    pub input: WorldInput,
    /// Whether the result re-reveals the cursor: a user's switch does, a background one never.
    pub reveal: bool,
    pub refresh: Refresh,
    /// The input changed under a switch: it builds even when nothing moved.
    pub switch: bool,
}

/// A finished job; no snapshot on the `PR` tab.
#[derive(Debug)]
pub struct WorldCompletion {
    pub generation: u64,
    pub input: WorldInput,
    pub reveal: bool,
    pub snapshot: Option<Result<WorldSnapshot>>,
    /// What the job re-read, so a failure can retry exactly that; a failed switch retries whole.
    pub refresh: Refresh,
    /// How long the build took: the pacing budget's measure, which bounds its git's CPU.
    pub took: std::time::Duration,
}

/// Run the world worker for `repo`; queued requests coalesce into the newest, keeping a reveal.
pub fn spawn(
    repo: PathBuf,
    rx: Receiver<WorldJob>,
    tx: crate::wake::Sender<WorldCompletion>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("world".into())
        .spawn(move || {
            git::sweep_dead_copies(&repo);
            let mut worker = Worker::new(tx);
            while let Ok(mut job) = rx.recv() {
                while let Ok(next) = rx.try_recv() {
                    let mut refresh = job.refresh;
                    refresh.absorb(next.refresh);
                    let (reveal, switch) = (job.reveal || next.reveal, job.switch || next.switch);
                    job = WorldJob { reveal, refresh, switch, ..next };
                }
                if !worker.run(job) {
                    break;
                }
            }
        })
        .expect("spawn world worker")
}

/// Where the ignored walk meets the worker; one walk runs at a time.
#[derive(Default)]
struct Walk {
    /// Bumped by every full refresh, so a walk from before one never lands.
    epoch: u64,
    running: bool,
    /// What a finished walk read, until the worker takes it; a full refresh drops it.
    listed: Option<Vec<git::WorktreeEntry>>,
    /// The newest build waiting on the walk: its job's generation and input, which it lands as.
    pending: Option<(u64, WorldInput, WorldSnapshot)>,
}

fn lock(walk: &Mutex<Walk>) -> std::sync::MutexGuard<'_, Walk> {
    walk.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Walk the ignored entries off the worker, then land the build waiting on them, or keep the
/// listing for the next build.
fn spawn_walk(
    shared: Arc<Mutex<Walk>>,
    tx: crate::wake::Sender<WorldCompletion>,
    repo: PathBuf,
    mut epoch: u64,
) {
    std::thread::spawn(move || {
        let (listed, mut walk) = loop {
            let listed = git::ignored_listing(&repo, &[]);
            let mut walk = lock(&shared);
            if walk.epoch == epoch {
                break (listed, walk);
            }
            // Outdated by a full refresh: walk again only if a build waits on it.
            if walk.pending.is_none() {
                walk.running = false;
                return;
            }
            epoch = walk.epoch;
        };
        walk.running = false;
        let Some((generation, input, built)) = walk.pending.take() else {
            walk.listed = listed.ok();
            return;
        };
        // A failed walk fails its build, which the pacer retries whole on a backoff.
        let (snapshot, refresh) = match listed {
            Ok(listed) => {
                walk.listed = Some(listed.clone());
                drop(walk);
                (Ok(with_ignored(built, &input, &listed)), Refresh::default())
            }
            Err(e) => (Err(e), Refresh::Full),
        };
        let (reveal, snapshot, took) = (false, Some(snapshot), std::time::Duration::ZERO);
        let _ = tx.send(WorldCompletion { generation, input, reveal, snapshot, refresh, took });
    });
}

/// What the worker keeps between jobs: its last build, what the index staged when it ran, the
/// parts full builds share, and what moved since the last build.
struct Worker {
    last: Option<(WorldInput, WorldSnapshot)>,
    staged: Option<u64>,
    parts: Parts,
    owed: Refresh,
    walk: Arc<Mutex<Walk>>,
    /// Where a build lands, and a walk's follow-up after it.
    tx: crate::wake::Sender<WorldCompletion>,
}

impl Worker {
    fn new(tx: crate::wake::Sender<WorldCompletion>) -> Self {
        let parts = Parts { defer_ignored: true, ..Parts::default() };
        let (last, staged, owed, walk) = (None, None, Refresh::default(), Arc::default());
        Self { last, staged, parts, owed, walk, tx }
    }

    /// Build one job and send its completion; `false` once nobody listens.
    fn run(&mut self, job: WorldJob) -> bool {
        let started = std::time::Instant::now();
        let built = self.refresh(&job.input, job.refresh.clone(), job.switch);
        let shared = Arc::clone(&self.walk);
        // Held through the send, so a walk's follow-up never lands before the build it completes.
        let mut walk = lock(&shared);
        let snapshot = built.map(|b| b.and_then(|b| self.settle_ignored(&mut walk, &job, b)));
        match &snapshot {
            Some(Ok(built)) => self.last = Some((job.input.clone(), built.clone())),
            // A failed build saw nothing: the next one reads everything again.
            Some(Err(_)) => self.last = None,
            None => {}
        }
        // A failed switch still owes its view, so its retry builds whole.
        let failed = job.switch && matches!(snapshot, Some(Err(_)));
        let done = WorldCompletion {
            generation: job.generation,
            input: job.input,
            reveal: job.reveal,
            snapshot,
            refresh: if failed { Refresh::Full } else { job.refresh },
            took: started.elapsed(),
        };
        self.tx.send(done).is_ok()
    }

    /// Hear what a refresh says moved; a full one also outdates the walk.
    fn hear(&mut self, refresh: &Refresh) {
        self.parts.note(refresh);
        if refresh.is_full() {
            let mut walk = lock(&self.walk);
            walk.epoch += 1;
            walk.listed = None;
        }
    }

    /// Take a finished walk's listing; with `start`, walk when nothing is listed or walking.
    fn adopt_walk(&mut self, walk: &mut Walk, repo: &Path, start: bool) {
        if let Some(listed) = walk.listed.take() {
            self.parts.ignored.listed.get_or_insert(listed);
        }
        if start && !walk.running && self.parts.ignored.listed.is_none() {
            walk.running = true;
            spawn_walk(Arc::clone(&self.walk), self.tx.clone(), repo.to_path_buf(), walk.epoch);
        }
    }

    /// A build waiting on ignored entries gets them once walked; else the walk lands it later.
    fn settle_ignored(
        &mut self,
        walk: &mut Walk,
        job: &WorldJob,
        built: WorldSnapshot,
    ) -> Result<WorldSnapshot> {
        if !built.ignored_pending {
            return Ok(built);
        }
        self.adopt_walk(walk, &job.input.repo, true);
        if self.parts.ignored.listed.is_none() {
            walk.pending = Some((job.generation, job.input.clone(), built.clone()));
            return Ok(built);
        }
        let ignored = self.parts.ignored(&job.input.repo)?.to_vec();
        Ok(with_ignored(built, &job.input, &ignored))
    }

    /// Run one refresh: only what moved when the last build is of the same input, else in full.
    /// `None` when nothing needs re-reading, and on the `PR` tab.
    fn refresh(
        &mut self,
        input: &WorldInput,
        refresh: Refresh,
        switch: bool,
    ) -> Option<Result<WorldSnapshot>> {
        // Heard on every tab, so a build after a stretch on `PR` still reads what moved.
        self.hear(&refresh);
        self.owed.absorb(refresh);
        if !input.tab.is_file_tab() {
            return None;
        }
        // A pick reads committed trees: on `Changes`, a worktree or index change moves nothing in it.
        let pick_only = input.scope == Scope::Commits && input.tab == Tab::Changes;
        let same = self.last.as_ref().is_some_and(|(of, _)| of == input);
        if pick_only && same && !self.owed.is_full() {
            self.owed = Refresh::default();
        }
        if !switch && self.owed.is_empty() {
            return None;
        }
        let mut refresh = std::mem::take(&mut self.owed);
        // Read before any build, so a stage landing during it reads as new next time.
        let mut fingerprint = None;
        if let Refresh::Paths { index: true, .. } = refresh {
            let now = git::staged_fingerprint(&input.repo).ok();
            if now.is_none() || now != self.staged {
                // What is staged moves what is untracked or ignored, at paths no batch names.
                refresh = Refresh::Full;
                self.hear(&refresh);
                fingerprint = Some(now);
            }
        }
        // An expanded ignored directory lists from disk, so a batch inside one rebuilds it whole.
        let in_expanded = |p: &String| {
            input.toggled_dirs.iter().any(|d| git::under_or_eq(p, d) || git::under_or_eq(d, p))
        };
        let last = self.last.as_ref().filter(|(of, _)| of == input).map(|(_, built)| built);
        if let (Some(last), Refresh::Paths { paths, .. }) = (last, &refresh)
            && !paths.iter().any(in_expanded)
        {
            // A switch back to the last build's input, with nothing moved since, is that build.
            if paths.is_empty() {
                return switch.then(|| Ok(WorldSnapshot { touched: None, ..last.clone() }));
            }
            match rebuild_paths(input, last, paths) {
                // A failed narrow read is retried at once as a full build.
                Some(Err(e)) => logln!("path-limited refresh failed, rebuilding in full: {e:#}"),
                Some(read) => return Some(read),
                None => {}
            }
        }
        let staged = fingerprint.unwrap_or_else(|| git::staged_fingerprint(&input.repo).ok());
        // The walk starts first, so it runs beside the build.
        let shared = Arc::clone(&self.walk);
        self.adopt_walk(&mut lock(&shared), &input.repo, input.tab == Tab::AllFiles);
        let built = build_with(input, &mut self.parts);
        if built.is_ok() {
            self.staged = staged;
        }
        Some(built)
    }
}

#[cfg(test)]
mod tests {
    use super::{Part, Refresh};
    use crate::git::WorktreeEntry;
    use crate::watch::{Batch, GitChange};

    #[test]
    fn a_part_rereads_what_moved_and_keeps_no_duplicate() {
        let ignored = |path: &str| WorktreeEntry { path: path.into(), ignored: true, is_dir: true };
        let listed = Some(vec![ignored("gen"), ignored("new/x"), ignored("target")]);
        let moved = ["new", "target/a.o"].map(String::from).into();
        let mut part = Part { listed, moved };
        // A narrow read under a path inside a collapsed directory lists that directory again.
        let read = |paths: &[String]| {
            assert_eq!(paths, ["new", "target/a.o"]);
            Ok(vec![ignored("target")])
        };
        let listed = part.get(read, |w| w.path.as_str()).unwrap();
        let paths: Vec<&str> = listed.iter().map(|w| w.path.as_str()).collect();
        assert_eq!(paths, ["gen", "target"]);
    }

    fn batch(git: &[GitChange], worktree: &[&str], rescan: bool) -> Batch {
        Batch {
            worktree: worktree.iter().map(|p| (*p).to_string()).collect(),
            git: git.iter().copied().collect(),
            rescan,
            ..Batch::default()
        }
    }

    #[test]
    fn a_batch_asks_for_exactly_what_it_moved() {
        let paths = |list: &[&str]| list.iter().map(|p| (*p).to_string()).collect();
        let rows = [
            ("a rescan", batch(&[], &["a"], true), Refresh::Full),
            ("HEAD", batch(&[GitChange::Head], &["a"], false), Refresh::Full),
            ("a ref", batch(&[GitChange::Refs], &[], false), Refresh::Full),
            ("git config", batch(&[GitChange::Config], &[], false), Refresh::Full),
            ("an ignore rule", batch(&[GitChange::IgnoreRules], &[], false), Refresh::Full),
            ("an attribute rule", batch(&[GitChange::Attributes], &[], false), Refresh::Full),
            ("another pane's pick", batch(&[GitChange::ReviewrRefs], &[], false), Refresh::Full),
            (
                "an index rewrite with paths",
                batch(&[GitChange::Index], &["a"], false),
                Refresh::Paths { paths: paths(&["a"]), index: true },
            ),
            (
                "worktree paths alone",
                batch(&[], &["a", "b/c"], false),
                Refresh::Paths { paths: paths(&["a", "b/c"]), index: false },
            ),
            ("the config alone", Batch { config: true, ..Batch::default() }, Refresh::default()),
        ];
        for (name, batch, expected) in rows {
            assert_eq!(Refresh::from_batch(&batch), expected, "{name}");
        }
    }
}
