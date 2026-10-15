# herdr-review dev tasks — run `just <task>` (https://github.com/casey/just)

# default: list tasks
default:
    @just --list

# format the code
fmt:
    cargo fmt --all

# check formatting (CI parity)
fmt-check:
    cargo fmt --all --check

# lint with clippy, warnings as errors (CI parity)
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# isolated: no live herdr (a refusing binary, no socket), no machine git config (fsmonitor, hooks)
# run the test suite, isolated
test:
    {{isolated}} cargo test --all-features

# run the test suite under cargo-nextest (`brew install cargo-nextest`), each test its own process
test-fast:
    {{isolated}} cargo nextest run --all-features

isolated := "env -u HERDR_WORKSPACE_ID -u HERDR_PANE_ID -u HERDR_SOCKET_PATH HERDR_BIN_PATH=false GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1"

# build (debug)
build:
    cargo build

# run reviewr in the current repo
run:
    cargo run

# build release and install the binary into bin/ for `herdr plugin link`
install:
    cargo build --release
    mkdir -p bin
    ./scripts/swap-binary.sh target/release/herdr-reviewr bin/herdr-reviewr

# build release and swap it into the GitHub-installed plugin for local QA (docs/qa-install.md)
qa-install:
    cargo build --release
    ./scripts/qa-install.sh

# restore the released binary and manifest the last `just qa-install` replaced
qa-restore:
    ./scripts/qa-install.sh --restore

# PTY smoke test of the editor path against a real release binary
smoke-edit:
    cargo build --release
    python3 scripts/smoke_edit_file.py --binary target/release/herdr-reviewr

# everything the unix CI job runs, locally (the Windows jobs run on CI only)
ci: fmt-check lint test
    cargo build --release
    python3 scripts/idle_check.py --binary target/release/herdr-reviewr
