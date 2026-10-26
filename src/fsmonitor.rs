//! A client for git's builtin fsmonitor daemon: what changed in the worktree since the last ask,
//! over the daemon's socket, so a large tree needs none of reviewr's own inotify watches.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The token git's own client sends first; the daemon answers it with everything.
const FIRST_TOKEN: &str = "builtin:fake";
/// The longest one pkt-line may be, its 4-byte header included.
const PKT_MAX: usize = 65520;
/// How long one ask may take before the daemon counts as gone.
const ANSWER_BOUND: Duration = Duration::from_secs(2);

/// What changed since the last ask.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Changes {
    /// Worktree-relative paths, a folder ending in `/`.
    Paths(Vec<String>),
    /// The daemon cannot say: it restarted, lost events, or never saw the token.
    Everything,
}

/// One worktree's daemon, and the token that marks what was already asked.
#[derive(Debug)]
pub(crate) struct Fsmonitor {
    socket: PathBuf,
    token: String,
}

impl Fsmonitor {
    /// The daemon for `root`, started as git would start it; `None` unless `core.fsmonitor` is the
    /// builtin daemon and it answers. Linux only: elsewhere one recursive watch costs nothing.
    pub(crate) fn locate(root: &Path, git_dir: &Path) -> Option<Self> {
        if !cfg!(target_os = "linux") || !crate::git::builtin_fsmonitor(root) {
            return None;
        }
        let token = FIRST_TOKEN.to_string();
        let mut daemon = Self { socket: git_dir.join("fsmonitor--daemon.ipc"), token };
        // The first answer is everything; the watcher's own start already reads everything.
        if daemon.query().is_err() {
            crate::git::start_fsmonitor(root).ok()?;
            daemon.query().ok()?;
        }
        Some(daemon)
    }

    /// What changed since the last ask, and the token for the next.
    pub(crate) fn query(&mut self) -> io::Result<Changes> {
        let answer = exchange(&self.socket, self.token.as_bytes())?;
        let (token, changes) = parse(&answer)?;
        self.token = token;
        Ok(changes)
    }
}

/// `payload` as pkt-lines, then the flush packet.
fn encode(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 8);
    for chunk in payload.chunks(PKT_MAX - 4) {
        out.extend(format!("{:04x}", chunk.len() + 4).as_bytes());
        out.extend(chunk);
    }
    out.extend(b"0000");
    out
}

/// One message: pkt-lines up to the flush packet, joined.
fn read_message(from: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let mut header = [0; 4];
        from.read_exact(&mut header)?;
        let len = std::str::from_utf8(&header)
            .ok()
            .and_then(|h| usize::from_str_radix(h, 16).ok())
            .ok_or_else(|| invalid("a bad pkt-line header"))?;
        match len {
            0 => return Ok(out),
            1..=4 => return Err(invalid("a pkt-line shorter than its header")),
            _ => {
                let start = out.len();
                out.resize(start + len - 4, 0);
                from.read_exact(&mut out[start..])?;
            }
        }
    }
}

/// The daemon's answer: the new token, then the changed paths, each ending in NUL; a lone `/` is
/// everything.
fn parse(answer: &[u8]) -> io::Result<(String, Changes)> {
    let mut fields = answer.split(|&b| b == 0);
    let token = fields.next().filter(|t| !t.is_empty()).ok_or_else(|| invalid("no token"))?;
    let token = String::from_utf8(token.to_vec()).map_err(|_| invalid("a token not in UTF-8"))?;
    let mut paths = Vec::new();
    for field in fields.filter(|f| !f.is_empty()) {
        if field == b"/" {
            return Ok((token, Changes::Everything));
        }
        // A name the batch cannot spell as git does is read again whole.
        let Ok(path) = String::from_utf8(field.to_vec()) else {
            return Ok((token, Changes::Everything));
        };
        paths.push(path);
    }
    Ok((token, Changes::Paths(paths)))
}

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("fsmonitor answered {what}"))
}

/// Send `request` to the socket at `socket` and read the answer.
#[cfg(target_os = "linux")]
fn exchange(socket: &Path, request: &[u8]) -> io::Result<Vec<u8>> {
    use rustix::fs::{Mode, OFlags};
    use std::os::fd::AsRawFd;
    // A socket address holds 108 bytes, less than a linked worktree's git dir can take: connect
    // through the folder's descriptor instead, as git connects through a `chdir`.
    let (Some(dir), Some(name)) = (socket.parent(), socket.file_name()) else {
        return Err(io::ErrorKind::NotFound.into());
    };
    let dir =
        rustix::fs::open(dir, OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC, Mode::empty())?;
    let short = Path::new("/proc/self/fd").join(dir.as_raw_fd().to_string()).join(name);
    let mut stream = std::os::unix::net::UnixStream::connect(short)?;
    stream.set_read_timeout(Some(ANSWER_BOUND))?;
    stream.set_write_timeout(Some(ANSWER_BOUND))?;
    stream.write_all(&encode(request))?;
    read_message(&mut stream)
}

#[cfg(not(target_os = "linux"))]
fn exchange(_: &Path, _: &[u8]) -> io::Result<Vec<u8>> {
    Err(io::ErrorKind::Unsupported.into())
}

#[cfg(test)]
mod tests {
    use super::{Changes, encode, parse, read_message};

    #[test]
    fn a_request_is_one_pkt_line_and_the_flush() {
        assert_eq!(encode(b"builtin:fake"), b"0010builtin:fake0000");
    }

    #[test]
    fn a_message_joins_its_pkt_lines_up_to_the_flush() {
        let mut wire: &[u8] = b"0009tok\x00a0006b\x0000000010ignored";
        assert_eq!(read_message(&mut wire).unwrap(), b"tok\0ab\0");
        let mut short: &[u8] = b"0003x";
        assert!(read_message(&mut short).is_err(), "a length below its header");
        let mut cut: &[u8] = b"0010abc";
        assert!(read_message(&mut cut).is_err(), "a pkt-line cut short");
    }

    #[test]
    fn an_answer_is_its_token_then_paths_or_everything() {
        let paths = |list: &[&str]| Changes::Paths(list.iter().map(|p| (*p).to_string()).collect());
        assert_eq!(parse(b"t:1\0").unwrap(), ("t:1".into(), paths(&[])));
        assert_eq!(
            parse(b"t:2\0a b.rs\0dir/\0").unwrap(),
            ("t:2".into(), paths(&["a b.rs", "dir/"]))
        );
        assert_eq!(parse(b"t:3\0/\0").unwrap(), ("t:3".into(), Changes::Everything));
        assert_eq!(parse(b"t:4\0\xff\0").unwrap(), ("t:4".into(), Changes::Everything));
        assert!(parse(b"").is_err(), "no token");
    }
}
