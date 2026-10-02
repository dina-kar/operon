//! The `loams` CLI's JSON contract (§30 D283, AP1 Task 2, Ruling 2).
//!
//! The desktop calls the CLI; it does not link it. Every stack action is
//! `loams <args> --output json`: one JSON document on stdout, one error
//! object on stderr, and the exit code classified by D283's table. Unknown
//! codes are `internal`. Children get no stdin, `LOAMS_NO_UPDATE_CHECK=1`,
//! `LOAMS_OUTPUT=json` and no `CI`, and are killed on timeout.
//!
//! Scaffold: CLI1 (`loams stack start|stop|describe|logs`) is not on `main`
//! yet, so nothing calls `Cli::run` for stacks; the parser and the
//! classification are tested against D283's fixtures so the stacks page can
//! switch to them when CLI1 lands.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::process::Command;

/// `{"error": {"code", "message", "hint", "exit_code", "details"}}` on stderr.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub hint: Option<String>,
    #[serde(default)]
    pub details: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

/// D283's exit codes (§30 §6.3).
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("internal error: {0:?}")]
    Internal(Option<ErrorBody>),
    #[error("usage: {0:?}")]
    Usage(Option<ErrorBody>),
    #[error("action required: {0:?}")]
    ActionRequired(Option<ErrorBody>),
    #[error("not found: {0:?}")]
    NotFound(Option<ErrorBody>),
    #[error("conflict: {0:?}")]
    Conflict(Option<ErrorBody>),
    #[error("unsupported: {0:?}")]
    Unsupported(Option<ErrorBody>),
    #[error("unavailable: {0:?}")]
    Unavailable(Option<ErrorBody>),
    #[error("permission denied: {0:?}")]
    Permission(Option<ErrorBody>),
    #[error("integrity failure: {0:?}")]
    Integrity(Option<ErrorBody>),
    #[error("interrupted")]
    Interrupted,
    #[error("could not run loams: {0}")]
    Spawn(#[from] std::io::Error),
    #[error("loams did not answer in time")]
    Timeout,
    #[error("loams printed invalid JSON: {0}")]
    BadJson(String),
}

/// Classifies a non-zero exit (D283). `stderr` is the error object, if any.
#[must_use]
pub fn classify(exit: i32, stderr: &str) -> CliError {
    let body = serde_json::from_str::<ErrorEnvelope>(stderr.trim())
        .ok()
        .map(|e| e.error);
    match exit {
        2 => CliError::Usage(body),
        3 => CliError::ActionRequired(body),
        4 => CliError::NotFound(body),
        5 => CliError::Conflict(body),
        6 => CliError::Unsupported(body),
        7 => CliError::Unavailable(body),
        8 => CliError::Permission(body),
        9 => CliError::Integrity(body),
        130 => CliError::Interrupted,
        _ => CliError::Internal(body),
    }
}

/// Parses a successful command's stdout: exactly one JSON document.
///
/// # Errors
///
/// `BadJson` when stdout is not one document of the expected shape.
pub fn parse_output<T: DeserializeOwned>(stdout: &str) -> Result<T, CliError> {
    serde_json::from_str(stdout.trim()).map_err(|e| CliError::BadJson(e.to_string()))
}

/// `loams version --output json`.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct VersionInfo {
    pub version: String,
    pub output_schema: u32,
    #[serde(default)]
    pub variant: Option<String>,
}

/// The output schema this desktop understands (AP1 Ruling 2).
pub const OUTPUT_SCHEMA: u32 = 1;

#[derive(Debug, Clone)]
pub struct Cli {
    pub binary: PathBuf,
    pub loams_home: PathBuf,
    pub timeout: Duration,
}

impl Cli {
    /// Runs `loams <args> --output json` and parses its answer.
    ///
    /// # Errors
    ///
    /// The classified exit, a timeout, or invalid JSON.
    pub async fn run<T: DeserializeOwned>(&self, args: &[&str]) -> Result<T, CliError> {
        let mut command = Command::new(&self.binary);
        command
            .args(args)
            .args(["--output", "json"])
            .env("LOAMS_HOME", &self.loams_home)
            .env("LOAMS_NO_UPDATE_CHECK", "1")
            .env("LOAMS_OUTPUT", "json")
            .env("LOAMS_NO_INPUT", "1")
            .env_remove("CI")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let child = command.spawn()?;
        let output = tokio::time::timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| CliError::Timeout)??;
        let stdout = String::from_utf8_lossy(&output.stdout);
        match output.status.code() {
            Some(0) => parse_output(&stdout),
            Some(code) => Err(classify(code, &String::from_utf8_lossy(&output.stderr))),
            None => Err(CliError::Interrupted),
        }
    }

    /// `loams version`, checking the output schema.
    ///
    /// # Errors
    ///
    /// As `run`, or `Unsupported` for another output schema.
    pub async fn version(&self) -> Result<VersionInfo, CliError> {
        let info: VersionInfo = self.run(&["version"]).await?;
        if info.output_schema != OUTPUT_SCHEMA {
            return Err(CliError::Unsupported(None));
        }
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_error_is_classified_by_exit_code() {
        let stderr = r#"{"error": {"code": "stack_not_found", "message": "no stack named `dev`", "hint": "run `loams stack list`", "exit_code": 4, "details": {"name": "dev"}}}"#;
        let CliError::NotFound(Some(body)) = classify(4, stderr) else {
            panic!("expected not found");
        };
        assert_eq!(body.code, "stack_not_found");
        assert_eq!(body.hint.as_deref(), Some("run `loams stack list`"));
        assert_eq!(body.details["name"], "dev");
        let kinds: Vec<&str> = [1, 2, 3, 4, 5, 6, 7, 8, 9, 130, 42]
            .iter()
            .map(|c| match classify(*c, "") {
                CliError::Internal(_) => "internal",
                CliError::Usage(_) => "usage",
                CliError::ActionRequired(_) => "action_required",
                CliError::NotFound(_) => "not_found",
                CliError::Conflict(_) => "conflict",
                CliError::Unsupported(_) => "unsupported",
                CliError::Unavailable(_) => "unavailable",
                CliError::Permission(_) => "permission",
                CliError::Integrity(_) => "integrity",
                CliError::Interrupted => "interrupted",
                _ => "other",
            })
            .collect();
        assert_eq!(
            kinds,
            [
                "internal",
                "usage",
                "action_required",
                "not_found",
                "conflict",
                "unsupported",
                "unavailable",
                "permission",
                "integrity",
                "interrupted",
                "internal"
            ]
        );
    }

    #[test]
    fn cli_success_parses_json_and_bad_json_is_reported() {
        #[derive(Deserialize)]
        struct Stacks {
            stacks: Vec<serde_json::Value>,
        }
        let ok: Stacks = parse_output("{\"stacks\": [{\"name\": \"dev\"}]}\n").unwrap();
        assert_eq!(ok.stacks.len(), 1);
        assert!(matches!(
            parse_output::<Stacks>("[1, 2]"),
            Err(CliError::BadJson(_))
        ));
        assert!(matches!(
            parse_output::<Stacks>("{} {}"),
            Err(CliError::BadJson(_))
        ));
    }

    #[test]
    fn describe_fixture_parses() {
        // §30 §8.3's `stack describe --output json`.
        let fixture = r#"{"name": "dev", "state": "running", "pid": 41233, "version": "0.4.0", "variant": "standard",
         "binary": "/home/u/.loams/bin/loams", "binary_outdated": false, "metastore": "embedded", "object_store": "local",
         "engines": [{"engine": "qdrant", "endpoints": {"rest": "http://127.0.0.1:6333"}, "env": ["QDRANT_URL"]}],
         "auth": "none", "created_at": "2026-10-01T09:12:44Z"}"#;
        #[derive(Deserialize)]
        struct Describe {
            name: String,
            state: String,
            engines: Vec<serde_json::Value>,
            auth: String,
        }
        let d: Describe = parse_output(fixture).unwrap();
        assert_eq!(
            (d.name.as_str(), d.state.as_str(), d.auth.as_str()),
            ("dev", "running", "none")
        );
        assert_eq!(d.engines.len(), 1);
    }
}
