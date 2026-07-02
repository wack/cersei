//! File search primitives — structured grep and glob.
//!
//! `grep` is a native, in-process recursive regex search built on ripgrep's own
//! library crates (`ignore` for the gitignore-aware parallel directory walker
//! and `grep` for the regex matcher/searcher). It needs no external `rg`/`grep`
//! binary, so behavior is identical on every machine.
//!
//! `glob` walks with the same `ignore` crate (gitignore-aware, hidden-skipping,
//! parallel, no symlink-following) and matches names with `globset`. It stops
//! the walk the moment `max_results` is reached and aborts with
//! [`SearchError::Timeout`] when its `deadline` expires — an unbounded pattern
//! over a huge tree (`**/*.rs` from `/`) returns an error the caller can react
//! to instead of pinning a blocking thread for minutes.

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A single search match with context.
#[derive(Debug, Clone)]
pub struct SearchMatch {
    pub file: PathBuf,
    pub line_number: usize,
    pub line_content: String,
}

/// Options for grep.
///
/// Defaults mirror ripgrep's code-search defaults: gitignore/`.ignore` rules are
/// respected and hidden + binary files are skipped. The boolean opt-outs default
/// to `false`, so `GrepOptions::default()` keeps that sensible behavior.
#[derive(Debug, Clone, Default)]
pub struct GrepOptions {
    /// Whitelist glob applied to file paths (e.g. `*.rs`). `None` searches all files.
    pub glob_filter: Option<String>,
    /// Cap on the number of matches returned. `None` is unlimited.
    pub max_results: Option<usize>,
    /// Case-insensitive matching.
    pub case_insensitive: bool,
    /// When `true`, ignore `.gitignore`/`.ignore`/hidden filtering (search everything).
    pub no_ignore: bool,
    /// When `true`, include hidden files/directories in the search.
    pub hidden: bool,
}

/// Options for glob.
///
/// Defaults mirror [`GrepOptions`]: gitignore/`.ignore` rules are respected and
/// hidden files are skipped, so a recursive pattern doesn't drown in `target/`,
/// `node_modules/`, or `.git/`.
#[derive(Debug, Clone, Default)]
pub struct GlobOptions {
    /// Cap on the number of paths returned; the walk stops as soon as it is
    /// reached. `None` is unlimited.
    pub max_results: Option<usize>,
    /// Wall-clock budget for the walk. On expiry the walk stops and
    /// [`SearchError::Timeout`] is returned. `None` is unlimited.
    pub deadline: Option<Duration>,
    /// When `true`, ignore `.gitignore`/`.ignore`/hidden filtering (walk everything).
    pub no_ignore: bool,
    /// When `true`, include hidden files/directories. Also enabled implicitly
    /// when the pattern itself names a dot-component (e.g. `.github/**`).
    pub hidden: bool,
}

/// Search errors.
#[derive(Debug)]
pub enum SearchError {
    InvalidPattern(String),
    IoError(std::io::Error),
    CommandFailed(String),
    /// The walk exceeded its wall-clock budget before finishing.
    Timeout(Duration),
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPattern(p) => write!(f, "invalid pattern: {p}"),
            Self::IoError(e) => write!(f, "I/O error: {e}"),
            Self::CommandFailed(msg) => write!(f, "command failed: {msg}"),
            Self::Timeout(budget) => write!(f, "timed out after {budget:?}"),
        }
    }
}

impl std::error::Error for SearchError {}

impl From<std::io::Error> for SearchError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

/// Recursively search file contents using a regex pattern.
///
/// Native and in-process: uses ripgrep's `ignore` crate for a gitignore-aware
/// parallel directory walk and ripgrep's `grep` crate for matching. No external
/// `rg`/`grep` binary is required. `path` may be a directory (searched
/// recursively) or a single file. Results are returned sorted by `(file,
/// line_number)` for deterministic output.
pub async fn grep(
    pattern: &str,
    path: &Path,
    opts: GrepOptions,
) -> Result<Vec<SearchMatch>, SearchError> {
    let pattern = pattern.to_string();
    let path = path.to_path_buf();

    tokio::task::spawn_blocking(move || grep_blocking(&pattern, &path, opts))
        .await
        .map_err(|e| SearchError::CommandFailed(e.to_string()))?
}

/// Synchronous core of [`grep`], intended to run on a blocking thread.
fn grep_blocking(
    pattern: &str,
    path: &Path,
    opts: GrepOptions,
) -> Result<Vec<SearchMatch>, SearchError> {
    use grep::regex::RegexMatcherBuilder;
    use grep::searcher::sinks::UTF8;
    use grep::searcher::SearcherBuilder;
    use ignore::overrides::OverrideBuilder;
    use ignore::{WalkBuilder, WalkState};

    let matcher = RegexMatcherBuilder::new()
        .case_insensitive(opts.case_insensitive)
        .line_terminator(Some(b'\n'))
        .build(pattern)
        .map_err(|e| SearchError::InvalidPattern(e.to_string()))?;

    let mut builder = WalkBuilder::new(path);
    if opts.no_ignore {
        builder.standard_filters(false);
    } else {
        // Honor .gitignore even when the search root isn't inside a git repo,
        // so filtering is predictable everywhere (not just in checked-out repos).
        builder.require_git(false);
    }
    builder.hidden(!opts.hidden);
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    builder.threads(threads);

    // Apply an optional whitelist glob (e.g. `*.rs`) over file paths.
    if let Some(ref glob) = opts.glob_filter {
        let mut ob = OverrideBuilder::new(path);
        ob.add(glob)
            .map_err(|e| SearchError::InvalidPattern(e.to_string()))?;
        let overrides = ob
            .build()
            .map_err(|e| SearchError::InvalidPattern(e.to_string()))?;
        builder.overrides(overrides);
    }

    let results: Arc<Mutex<Vec<SearchMatch>>> = Arc::new(Mutex::new(Vec::new()));
    let max = opts.max_results;

    builder.build_parallel().run(|| {
        let matcher = matcher.clone();
        let results = Arc::clone(&results);
        let mut searcher = SearcherBuilder::new().line_number(true).build();

        Box::new(move |entry| {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => return WalkState::Continue,
            };
            // Only search regular files.
            if !entry.file_type().is_some_and(|t| t.is_file()) {
                return WalkState::Continue;
            }

            let mut local: Vec<SearchMatch> = Vec::new();
            let file = entry.path().to_path_buf();
            let search_result = searcher.search_path(
                &matcher,
                entry.path(),
                UTF8(|lnum, line| {
                    local.push(SearchMatch {
                        file: file.clone(),
                        line_number: lnum as usize,
                        line_content: line.trim_end_matches(['\n', '\r']).to_string(),
                    });
                    Ok(true)
                }),
            );
            // Ignore per-file read/decoding errors (e.g. permission denied) and
            // keep walking, mirroring ripgrep's resilience.
            if search_result.is_err() || local.is_empty() {
                return WalkState::Continue;
            }

            let mut guard = results.lock().unwrap();
            guard.extend(local);
            if let Some(max) = max {
                if guard.len() >= max {
                    return WalkState::Quit;
                }
            }
            WalkState::Continue
        })
    });

    let mut matches = Arc::try_unwrap(results)
        .map(|m| m.into_inner().unwrap())
        .unwrap_or_else(|arc| arc.lock().unwrap().clone());

    // Parallel walk order is nondeterministic — sort for stable output.
    matches.sort_by(|a, b| a.file.cmp(&b.file).then(a.line_number.cmp(&b.line_number)));
    if let Some(max) = max {
        matches.truncate(max);
    }

    Ok(matches)
}

/// Find files matching a glob pattern.
///
/// `pattern` is joined onto `base_dir` (an absolute pattern replaces the base,
/// matching `Path::join`). The walk is gitignore-aware, skips hidden files by
/// default (see [`GlobOptions`]), never follows symlinks (so a link cycle
/// cannot make it unbounded), stops at `max_results`, and aborts with
/// [`SearchError::Timeout`] when `deadline` expires. Results are sorted; with
/// `max_results` set, *which* matches are returned is nondeterministic (the
/// parallel walk quits early), but the output order is stable.
pub async fn glob(
    pattern: &str,
    base_dir: &Path,
    opts: GlobOptions,
) -> Result<Vec<PathBuf>, SearchError> {
    let pattern = pattern.to_string();
    let base_dir = base_dir.to_path_buf();

    tokio::task::spawn_blocking(move || glob_blocking(&pattern, &base_dir, opts))
        .await
        .map_err(|e| SearchError::CommandFailed(e.to_string()))?
}

/// Synchronous core of [`glob`], intended to run on a blocking thread.
fn glob_blocking(
    pattern: &str,
    base_dir: &Path,
    opts: GlobOptions,
) -> Result<Vec<PathBuf>, SearchError> {
    use ignore::{WalkBuilder, WalkState};

    let full_pattern = base_dir.join(pattern);
    // `literal_separator` gives the glob crate's semantics this replaced:
    // `*`/`?` do not cross `/`, only `**` recurses.
    let matcher = globset::GlobBuilder::new(&full_pattern.display().to_string())
        .literal_separator(true)
        .build()
        .map_err(|e| SearchError::InvalidPattern(e.to_string()))?
        .compile_matcher();

    // Walk from the pattern's literal prefix (the components before the first
    // metacharacter), not from `base_dir`: for an absolute pattern the two are
    // unrelated, and for `src/**` it avoids walking siblings only to discard them.
    let walk_root = literal_prefix(&full_pattern);
    if !walk_root.exists() {
        return Ok(Vec::new());
    }

    // A pattern that names a dot-component (`.github/**`) is an explicit ask
    // for hidden files — honor it without requiring the `hidden` opt-in.
    let want_hidden = opts.hidden
        || Path::new(pattern).components().any(|c| {
            matches!(c, Component::Normal(name) if name.to_string_lossy().starts_with('.'))
        });

    let mut builder = WalkBuilder::new(&walk_root);
    if opts.no_ignore {
        builder.standard_filters(false);
    } else {
        // Honor .gitignore even when the walk root isn't inside a git repo,
        // mirroring `grep_blocking`.
        builder.require_git(false);
    }
    builder.hidden(!want_hidden);
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    builder.threads(threads);

    let results: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
    let timed_out = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let max = opts.max_results;
    let deadline = opts.deadline;

    builder.build_parallel().run(|| {
        let matcher = matcher.clone();
        let results = Arc::clone(&results);
        let timed_out = Arc::clone(&timed_out);

        Box::new(move |entry| {
            if deadline.is_some_and(|budget| started.elapsed() > budget) {
                timed_out.store(true, Ordering::Relaxed);
                return WalkState::Quit;
            }
            let Ok(entry) = entry else {
                return WalkState::Continue;
            };
            if !matcher.is_match(entry.path()) {
                return WalkState::Continue;
            }

            let mut guard = results.lock().unwrap();
            guard.push(entry.into_path());
            if max.is_some_and(|max| guard.len() >= max) {
                return WalkState::Quit;
            }
            WalkState::Continue
        })
    });

    if timed_out.load(Ordering::Relaxed) {
        return Err(SearchError::Timeout(
            deadline.unwrap_or_else(|| started.elapsed()),
        ));
    }

    let mut paths = Arc::try_unwrap(results)
        .map(|m| m.into_inner().unwrap())
        .unwrap_or_else(|arc| arc.lock().unwrap().clone());

    // Parallel walk order is nondeterministic — sort for stable output.
    paths.sort();
    if let Some(max) = max {
        paths.truncate(max);
    }

    Ok(paths)
}

/// The leading components of a glob pattern before the first one containing a
/// metacharacter — the directory the walk actually needs to start from.
fn literal_prefix(pattern: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in pattern.components() {
        if component
            .as_os_str()
            .to_string_lossy()
            .contains(['*', '?', '[', '{'])
        {
            break;
        }
        out.push(component.as_os_str());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn glob_finds_nested_files_recursively() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("src/deep")).unwrap();
        fs::write(tmp.path().join("top.rs"), "").unwrap();
        fs::write(tmp.path().join("src/mid.rs"), "").unwrap();
        fs::write(tmp.path().join("src/deep/low.rs"), "").unwrap();
        fs::write(tmp.path().join("src/readme.md"), "").unwrap();

        let paths = glob("**/*.rs", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 3);
        // Sorted, absolute paths.
        assert!(paths.windows(2).all(|w| w[0] <= w[1]));
        assert!(paths.iter().all(|p| p.is_absolute()));
    }

    #[tokio::test]
    async fn glob_star_does_not_cross_separators() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("top.rs"), "").unwrap();
        fs::write(tmp.path().join("sub/nested.rs"), "").unwrap();

        let paths = glob("*.rs", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("top.rs"));
    }

    #[tokio::test]
    async fn glob_respects_gitignore_unless_opted_out() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("target")).unwrap();
        fs::write(tmp.path().join(".gitignore"), "target/\n").unwrap();
        fs::write(tmp.path().join("kept.rs"), "").unwrap();
        fs::write(tmp.path().join("target/generated.rs"), "").unwrap();

        let paths = glob("**/*.rs", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("kept.rs"));

        let opts = GlobOptions {
            no_ignore: true,
            ..Default::default()
        };
        let paths = glob("**/*.rs", tmp.path(), opts).await.unwrap();
        assert_eq!(paths.len(), 2);
    }

    #[tokio::test]
    async fn glob_skips_hidden_unless_asked_or_named() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join(".github/workflows")).unwrap();
        fs::write(tmp.path().join("visible.yml"), "").unwrap();
        fs::write(tmp.path().join(".github/workflows/ci.yml"), "").unwrap();

        // Hidden skipped by default.
        let paths = glob("**/*.yml", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("visible.yml"));

        // Explicit opt-in includes it.
        let opts = GlobOptions {
            hidden: true,
            ..Default::default()
        };
        assert_eq!(glob("**/*.yml", tmp.path(), opts).await.unwrap().len(), 2);

        // A pattern that names the dot-component is an implicit opt-in.
        let paths = glob(".github/**/*.yml", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("ci.yml"));
    }

    #[tokio::test]
    async fn glob_stops_at_max_results() {
        let tmp = tempfile::tempdir().unwrap();
        for i in 0..50 {
            fs::write(tmp.path().join(format!("f{i:02}.rs")), "").unwrap();
        }
        let opts = GlobOptions {
            max_results: Some(10),
            ..Default::default()
        };
        let paths = glob("*.rs", tmp.path(), opts).await.unwrap();
        assert_eq!(paths.len(), 10);
    }

    #[tokio::test]
    async fn glob_expired_deadline_is_a_timeout_error() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.rs"), "").unwrap();
        let opts = GlobOptions {
            deadline: Some(Duration::ZERO),
            ..Default::default()
        };
        let r = glob("**/*.rs", tmp.path(), opts).await;
        assert!(matches!(r, Err(SearchError::Timeout(_))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn glob_does_not_follow_symlink_cycles() {
        // The `glob` crate this replaced followed directory symlinks with no
        // cycle detection, so a self-referential link made `**` unbounded.
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("real.rs"), "").unwrap();
        std::os::unix::fs::symlink(tmp.path(), tmp.path().join("loop")).unwrap();

        let paths = glob("**/*.rs", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("real.rs"));
    }

    #[tokio::test]
    async fn glob_absolute_pattern_replaces_the_base() {
        let tmp = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        fs::write(elsewhere.path().join("found.rs"), "").unwrap();

        let pattern = format!("{}/*.rs", elsewhere.path().display());
        let paths = glob(&pattern, tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("found.rs"));
    }

    #[tokio::test]
    async fn glob_nonexistent_root_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = glob("no/such/dir/**/*.rs", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert!(paths.is_empty());
    }

    #[tokio::test]
    async fn glob_literal_pattern_matches_a_single_file() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("Cargo.toml"), "").unwrap();
        let paths = glob("Cargo.toml", tmp.path(), GlobOptions::default())
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("Cargo.toml"));
    }

    #[tokio::test]
    async fn finds_match_with_line_number_and_trimmed_content() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "alpha\nbeta TARGET here\ngamma\n").unwrap();

        let m = grep("TARGET", tmp.path(), GrepOptions::default())
            .await
            .unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].line_number, 2);
        assert_eq!(m[0].line_content, "beta TARGET here");
        assert!(m[0].file.ends_with("a.txt"));
    }

    #[tokio::test]
    async fn searches_recursively_into_subdirs() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("sub/deep")).unwrap();
        fs::write(tmp.path().join("top.rs"), "NEEDLE\n").unwrap();
        fs::write(tmp.path().join("sub/mid.rs"), "no match\nNEEDLE\n").unwrap();
        fs::write(tmp.path().join("sub/deep/low.rs"), "NEEDLE\n").unwrap();

        let m = grep("NEEDLE", tmp.path(), GrepOptions::default())
            .await
            .unwrap();
        assert_eq!(m.len(), 3);
    }

    #[tokio::test]
    async fn respects_gitignore_by_default_but_not_with_no_ignore() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(".gitignore"), "ignored.txt\n").unwrap();
        fs::write(tmp.path().join("kept.txt"), "SECRET\n").unwrap();
        fs::write(tmp.path().join("ignored.txt"), "SECRET\n").unwrap();

        // Default: the gitignored file is skipped.
        let m = grep("SECRET", tmp.path(), GrepOptions::default())
            .await
            .unwrap();
        assert_eq!(m.len(), 1);
        assert!(m[0].file.ends_with("kept.txt"));

        // no_ignore: both files are searched.
        let opts = GrepOptions {
            no_ignore: true,
            ..Default::default()
        };
        let m = grep("SECRET", tmp.path(), opts).await.unwrap();
        assert_eq!(m.len(), 2);
    }

    #[tokio::test]
    async fn case_insensitive_matching() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "Hello World\n").unwrap();

        let sensitive = grep("hello", tmp.path(), GrepOptions::default())
            .await
            .unwrap();
        assert!(sensitive.is_empty());

        let opts = GrepOptions {
            case_insensitive: true,
            ..Default::default()
        };
        let insensitive = grep("hello", tmp.path(), opts).await.unwrap();
        assert_eq!(insensitive.len(), 1);
    }

    #[tokio::test]
    async fn glob_filter_restricts_file_types() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.rs"), "MATCH\n").unwrap();
        fs::write(tmp.path().join("b.txt"), "MATCH\n").unwrap();

        let opts = GrepOptions {
            glob_filter: Some("*.rs".to_string()),
            ..Default::default()
        };
        let m = grep("MATCH", tmp.path(), opts).await.unwrap();
        assert_eq!(m.len(), 1);
        assert!(m[0].file.ends_with("a.rs"));
    }

    #[tokio::test]
    async fn max_results_caps_output() {
        let tmp = tempfile::tempdir().unwrap();
        let body: String = (0..50).map(|_| "HIT\n").collect();
        fs::write(tmp.path().join("a.txt"), body).unwrap();

        let opts = GrepOptions {
            max_results: Some(10),
            ..Default::default()
        };
        let m = grep("HIT", tmp.path(), opts).await.unwrap();
        assert_eq!(m.len(), 10);
    }

    #[tokio::test]
    async fn results_are_sorted_deterministically() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("z.txt"), "X\n").unwrap();
        fs::write(tmp.path().join("a.txt"), "X\nX\n").unwrap();

        let m = grep("X", tmp.path(), GrepOptions::default())
            .await
            .unwrap();
        // Sorted by (file, line): a.txt:1, a.txt:2, z.txt:1
        assert_eq!(m.len(), 3);
        assert!(m[0].file.ends_with("a.txt") && m[0].line_number == 1);
        assert!(m[1].file.ends_with("a.txt") && m[1].line_number == 2);
        assert!(m[2].file.ends_with("z.txt"));
    }

    #[tokio::test]
    async fn searches_a_single_file_path() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("only.txt");
        fs::write(&target, "FOO\n").unwrap();
        fs::write(tmp.path().join("other.txt"), "FOO\n").unwrap();

        let m = grep("FOO", &target, GrepOptions::default())
            .await
            .unwrap();
        assert_eq!(m.len(), 1);
        assert!(m[0].file.ends_with("only.txt"));
    }

    // Real-repo smoke test (run explicitly: `cargo test -p cersei-tools
    // real_repo_smoke -- --ignored`). Searches the actual workspace and asserts
    // it (a) finds our own source, (b) skips the gitignored `target/` dir.
    #[tokio::test]
    #[ignore]
    async fn real_repo_smoke() {
        // Crate dir is .../crates/cersei-tools; workspace root is two up.
        let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = crate_dir.parent().unwrap().parent().unwrap();

        let matches = grep("WalkBuilder::new", workspace_root, GrepOptions::default())
            .await
            .unwrap();

        // Our native grep() implementation contains this call.
        assert!(
            matches.iter().any(|m| m.file.ends_with("tool_primitives/search.rs")),
            "expected to find our own source; got {} matches",
            matches.len()
        );
        // The gitignored build directory must be excluded.
        assert!(
            !matches.iter().any(|m| m.file.components().any(|c| c.as_os_str() == "target")),
            "target/ should be gitignored and skipped"
        );
        eprintln!("real_repo_smoke: {} matches across the workspace", matches.len());
    }

    #[tokio::test]
    async fn invalid_regex_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("a.txt"), "x\n").unwrap();
        let r = grep("(unclosed", tmp.path(), GrepOptions::default()).await;
        assert!(matches!(r, Err(SearchError::InvalidPattern(_))));
    }
}
