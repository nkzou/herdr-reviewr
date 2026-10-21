//! Deferred loads: a switch builds on the world worker, the frame holds, then paints `loading…`.

mod common;

use std::time::Duration;

use common::{Repo, app_on, land_world};
use herdr_reviewr::app::{App, SWITCH_HOLD, Tab};
use herdr_reviewr::handle_key;
use herdr_reviewr::keymap::Keymap;
use herdr_reviewr::model::Scope;
use herdr_reviewr::ui;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::Rect;

/// `main` holds `a.rs`; `feature` changes `a.rs` and leaves `b.rs` uncommitted.
fn branched_repo() -> Repo {
    let r = Repo::init();
    r.write("a.rs", "one\n");
    r.commit_all("init");
    r.set_origin_default("main", "main");
    r.git(&["checkout", "-q", "-b", "feature"]);
    r.write("a.rs", "two\n");
    r.commit_all("feature work");
    r.write("b.rs", "new\n");
    r
}

/// A pane over `r` loaded inline, then deferring every later switch, as the event loop does.
fn deferred_app(r: &Repo) -> App {
    let mut app = app_on(r);
    app.defer_world_loads();
    app
}

fn paths(app: &App) -> Vec<String> {
    app.entries.iter().map(|e| e.path.clone()).collect()
}

fn screen(app: &App) -> String {
    let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
    terminal.draw(|f| ui::render(f, app)).unwrap();
    let buffer = terminal.backend().buffer();
    let mut out = String::new();
    for y in 0..buffer.area.height {
        for x in 0..buffer.area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}

fn press(app: &mut App, code: KeyCode) {
    let keymap = Keymap::default();
    handle_key(app, KeyEvent::from(code), Rect::new(0, 0, 120, 40), &keymap).unwrap();
}

#[test]
fn a_deferred_scope_switch_holds_then_lands_whole() {
    let r = branched_repo();
    let mut app = deferred_app(&r);
    assert_eq!(paths(&app), ["b.rs"]);
    app.set_scope(Scope::Branch).unwrap();
    // Nothing built yet: the old list stays, under a held frame and a queued reveal.
    assert_eq!(paths(&app), ["b.rs"]);
    assert!(app.loading());
    assert!(app.load_hold().is_some_and(|left| left <= SWITCH_HOLD));
    assert!(app.world_request.as_ref().is_some_and(|r| r.reveal && r.refresh.is_full()));
    land_world(&mut app);
    assert!(!app.loading());
    assert_eq!(paths(&app), ["a.rs", "b.rs"]);
}

#[test]
fn a_loading_tab_paints_no_list_count_or_base() {
    let r = branched_repo();
    let mut app = deferred_app(&r);
    app.set_scope(Scope::Branch).unwrap();
    let painted = screen(&app);
    assert!(painted.contains("loading…"), "{painted}");
    assert!(!painted.contains("changed"), "no count describes the old list:\n{painted}");
    assert!(!painted.contains("b.rs"), "no old row shows:\n{painted}");
    land_world(&mut app);
    let painted = screen(&app);
    assert!(!painted.contains("loading…"), "{painted}");
    assert!(painted.contains("2 changed"), "{painted}");
}

#[test]
fn keys_on_the_list_are_inert_while_loading_but_switches_answer() {
    let r = branched_repo();
    let mut app = deferred_app(&r);
    app.set_scope(Scope::Branch).unwrap();
    land_world(&mut app);
    app.set_scope(Scope::Uncommitted).unwrap();
    let cursor = app.file_cursor;
    press(&mut app, KeyCode::Down);
    assert_eq!(app.file_cursor, cursor, "a move waits for the list");
    // A newer switch supersedes the pending one.
    press(&mut app, KeyCode::Char('b'));
    assert_eq!(app.scope, Scope::Branch);
    assert!(app.loading());
    land_world(&mut app);
    assert_eq!(paths(&app), ["a.rs", "b.rs"]);
}

#[test]
fn a_startup_load_paints_loading_at_once() {
    let r = branched_repo();
    let mut app = App::new(r.path_buf(), Scope::Uncommitted, None);
    app.defer_world_loads();
    app.load_view(Duration::ZERO).unwrap();
    assert!(app.loading());
    assert_eq!(app.load_hold(), None, "nothing to hold at startup");
    assert!(screen(&app).contains("loading…"));
    land_world(&mut app);
    assert_eq!(paths(&app), ["b.rs"]);
}

#[test]
fn a_tab_left_before_its_load_landed_loads_again_on_return() {
    let r = branched_repo();
    let mut app = deferred_app(&r);
    app.set_scope(Scope::Branch).unwrap();
    // Away before the build lands: `All files` loads on its own first visit.
    app.set_tab(Tab::AllFiles).unwrap();
    assert!(app.loading(), "a first visit loads too");
    land_world(&mut app);
    assert!(!app.loading());
    app.set_tab(Tab::Changes).unwrap();
    assert!(app.loading(), "the old base's list never shows");
    assert_eq!(app.load_hold(), None, "already waited once");
    land_world(&mut app);
    assert_eq!(paths(&app), ["a.rs", "b.rs"]);
}

#[test]
fn a_scope_switch_in_all_files_defers_and_the_stash_waits_too() {
    let r = branched_repo();
    let mut app = deferred_app(&r);
    app.set_tab(Tab::AllFiles).unwrap();
    land_world(&mut app);
    app.set_tab(Tab::Changes).unwrap();
    land_world(&mut app);
    app.set_tab(Tab::AllFiles).unwrap();
    land_world(&mut app);
    app.set_scope(Scope::Branch).unwrap();
    assert!(app.loading(), "the badges are the old base's");
    land_world(&mut app);
    let badged: Vec<&str> =
        app.entries.iter().filter(|e| e.annotation.is_some()).map(|e| e.path.as_str()).collect();
    assert_eq!(badged, ["a.rs", "b.rs"]);
    app.set_tab(Tab::Changes).unwrap();
    assert!(app.loading(), "the stashed list was the old base's");
    land_world(&mut app);
    assert_eq!(paths(&app), ["a.rs", "b.rs"]);
}
