//! Glob tool: find files by pattern.
//!
//! Bounded by construction: the walk honors gitignore, skips hidden files,
//! stops at the result limit, and aborts with a recoverable tool error after
//! [`GLOB_DEADLINE`]. An unbounded pattern over a huge tree (`**/*.rs` from a
//! workspace root, or `/`) must cost the agent one corrective turn — not its
//! entire wall-clock budget pinned inside a blocking filesystem walk.

use super::*;
use crate::tool_primitives::search as psearch;
use serde::Deserialize;
use std::time::Duration;

/// Wall-clock budget for a single glob call. Generous for any sensible
/// project-sized walk; only filesystem-scale walks hit it.
const GLOB_DEADLINE: Duration = Duration::from_secs(30);

pub struct GlobTool;

#[async_trait]
impl Tool for GlobTool {
    fn name(&self) -> &str {
        "Glob"
    }
    fn description(&self) -> &str {
        "Find files matching a glob pattern. Honors .gitignore and skips hidden files by default."
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::ReadOnly
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::FileSystem
    }

    fn input_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Glob pattern (e.g. **/*.rs)" },
                "path": { "type": "string", "description": "Directory to search in (defaults to the working directory)" },
                "limit": { "type": "integer", "description": "Max results to return (default 200)" },
                "no_ignore": { "type": "boolean", "description": "Include .gitignore'd files", "default": false },
                "hidden": { "type": "boolean", "description": "Include hidden files/directories", "default": false }
            },
            "required": ["pattern"]
        })
    }

    async fn execute(&self, input: Value, ctx: &ToolContext) -> ToolResult {
        #[derive(Deserialize)]
        struct Input {
            pattern: String,
            path: Option<String>,
            limit: Option<usize>,
            #[serde(default)]
            no_ignore: bool,
            #[serde(default)]
            hidden: bool,
        }

        let input: Input = match serde_json::from_value(input) {
            Ok(i) => i,
            Err(e) => return ToolResult::error(format!("Invalid input: {}", e)),
        };

        let limit = input.limit.unwrap_or(200);

        let base_dir = input
            .path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| ctx.working_dir.clone());

        let opts = psearch::GlobOptions {
            // One extra result distinguishes "exactly limit" from "more exist"
            // now that the walk quits early instead of counting everything.
            max_results: Some(limit.saturating_add(1)),
            deadline: Some(GLOB_DEADLINE),
            no_ignore: input.no_ignore,
            hidden: input.hidden,
        };

        match psearch::glob(&input.pattern, &base_dir, opts).await {
            Ok(paths) if paths.is_empty() => ToolResult::success("No files matched the pattern."),
            Ok(paths) => {
                let truncated = paths.len() > limit;
                let output: Vec<String> = paths
                    .iter()
                    .take(limit)
                    .map(|p| p.display().to_string())
                    .collect();
                let mut result = output.join("\n");
                if truncated {
                    result.push_str(&format!(
                        "\n\n[Showing the first {limit} matches; more exist. Use a more specific pattern to narrow results.]"
                    ));
                }
                ToolResult::success(result)
            }
            Err(psearch::SearchError::Timeout(budget)) => ToolResult::error(format!(
                "Glob timed out after {}s — the tree under `{}` is too large to walk. \
Narrow `path` to a subdirectory or use a more specific pattern.",
                budget.as_secs(),
                base_dir.display(),
            )),
            Err(e) => ToolResult::error(format!("Glob failed: {}", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::AllowAll;
    use std::fs;
    use std::sync::Arc;

    fn ctx_in(dir: &std::path::Path) -> ToolContext {
        ToolContext {
            working_dir: dir.to_path_buf(),
            session_id: "glob-test".into(),
            permissions: Arc::new(AllowAll),
            cost_tracker: Arc::new(CostTracker::new()),
            mcp_manager: None,
            extensions: Extensions::default(),
        }
    }

    #[tokio::test]
    async fn defaults_to_the_working_directory() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("lib.rs"), "").unwrap();

        let res = GlobTool
            .execute(serde_json::json!({ "pattern": "*.rs" }), &ctx_in(tmp.path()))
            .await;
        assert!(!res.is_error);
        assert!(res.content.contains("lib.rs"));
    }

    #[tokio::test]
    async fn reports_that_more_matches_exist_beyond_the_limit() {
        let tmp = tempfile::tempdir().unwrap();
        for i in 0..12 {
            fs::write(tmp.path().join(format!("f{i:02}.rs")), "").unwrap();
        }

        let res = GlobTool
            .execute(
                serde_json::json!({ "pattern": "*.rs", "limit": 10 }),
                &ctx_in(tmp.path()),
            )
            .await;
        assert!(!res.is_error);
        assert_eq!(res.content.matches(".rs").count(), 10);
        assert!(res.content.contains("more exist"));
    }

    #[tokio::test]
    async fn exactly_limit_matches_is_not_reported_as_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        for i in 0..10 {
            fs::write(tmp.path().join(format!("f{i:02}.rs")), "").unwrap();
        }

        let res = GlobTool
            .execute(
                serde_json::json!({ "pattern": "*.rs", "limit": 10 }),
                &ctx_in(tmp.path()),
            )
            .await;
        assert!(!res.is_error);
        assert!(!res.content.contains("more exist"));
    }

    #[tokio::test]
    async fn gitignored_files_are_hidden_unless_no_ignore() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(".gitignore"), "skipped.rs\n").unwrap();
        fs::write(tmp.path().join("kept.rs"), "").unwrap();
        fs::write(tmp.path().join("skipped.rs"), "").unwrap();

        let res = GlobTool
            .execute(serde_json::json!({ "pattern": "*.rs" }), &ctx_in(tmp.path()))
            .await;
        assert!(res.content.contains("kept.rs"));
        assert!(!res.content.contains("skipped.rs"));

        let res = GlobTool
            .execute(
                serde_json::json!({ "pattern": "*.rs", "no_ignore": true }),
                &ctx_in(tmp.path()),
            )
            .await;
        assert!(res.content.contains("skipped.rs"));
    }
}
